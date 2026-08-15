import { DurableObject } from 'cloudflare:workers';
import {
  parseChannel,
  RT_PROTOCOL_VERSION,
  type ClientMessage,
  type PresenceEntry,
  type RealtimeErrorCode,
  type RealtimeEvent,
  type ServerMessage,
} from './protocol';
import { canPublish, canSubscribe, type Identity } from './permissions';
import { resolveIdentity, type SessionCacheEntry } from './auth';
import type { Env } from './env';

const CONFIG = {
  authTimeoutMs: 5_000,
  heartbeatIntervalMs: 30_000,
  maxConnsPerIp: 32,
  msgWindowMs: 10_000,
  msgWindowLimit: 250,
  publishWindowLimit: 40,
  maxSubscriptions: 64,
  maxPayloadBytes: 64 * 1024,
};

/** Per-connection state, kept on the WebSocket via the Hibernation API. */
interface SocketState {
  id: string;
  ip: string;
  app?: string;
  connectedAt: number;
  authenticated: boolean;
  identity?: Identity;
  alive: boolean;
  awaitingPong: boolean;
  subscriptions: string[];
  publishTokens: number[];
  msgTimes: number[];
}

const now = () => Date.now();

/**
 * RealtimeHubV2 — a single global Durable Object that owns every connection.
 *
 * Using one instance (idFromName("hub")) preserves the single-connection,
 * multi-channel protocol and gives presence + fan-out without Redis. WebSocket
 * Hibernation keeps idle connections cheap; an alarm drives heartbeats.
 */
export class RealtimeHub extends DurableObject<Env> {
  private members = new Map<string, Map<string, PresenceEntry>>();
  private sessionCache = new Map<string, SessionCacheEntry>();

  private startedAt = Date.now();
  private currentConnections = 0;
  private totalConnections = 0;
  private connectionsByApp: Record<string, number> = {};
  private eventsDelivered = 0;
  private eventsPublished = 0;
  private authFailures = 0;
  private authSuccesses = 0;
  private pingsReceived = 0;
  private errors: Record<string, number> = {};
  private lastEventAt?: number;

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (request.headers.get('Upgrade')?.toLowerCase() === 'websocket') {
      return this.handleUpgrade(request);
    }

    if (request.method === 'POST' && url.pathname.endsWith('/publish/batch')) {
      return this.handlePublishBatch(request);
    }
    if (request.method === 'POST' && url.pathname.endsWith('/publish')) {
      return this.handlePublish(request);
    }
    if (url.pathname.endsWith('/metrics')) {
      return Response.json(this.snapshot());
    }
    if (url.pathname.endsWith('/health')) {
      return Response.json({ status: 'ok', service: 'tirbeo-realtime', protocol: RT_PROTOCOL_VERSION });
    }
    return new Response('Not found', { status: 404 });
  }

  // ---------------------------------------------------------------------------
  // Upgrade + connection lifecycle
  // ---------------------------------------------------------------------------

  private handleUpgrade(request: Request): Response {
    const ip =
      request.headers.get('x-rt-ip') ||
      request.headers.get('CF-Connecting-IP') ||
      request.headers.get('x-forwarded-for')?.split(',')[0]?.trim() ||
      'unknown';
    const app = new URL(request.url).searchParams.get('app') || undefined;

    const pair = new WebSocketPair();
    const server = pair[1];

    let ipConns = 0;
    for (const ws of this.ctx.getWebSockets()) {
      if (ws.deserializeAttachment()?.ip === ip) ipConns += 1;
    }
    if (ipConns >= CONFIG.maxConnsPerIp) {
      this.ctx.acceptWebSocket(server);
      server.close(4002, 'connection limit');
      return new Response(null, { status: 101, webSocket: pair[0] });
    }

    const state: SocketState = {
      id: crypto.randomUUID(),
      ip,
      app,
      connectedAt: now(),
      authenticated: false,
      alive: true,
      awaitingPong: false,
      subscriptions: [],
      publishTokens: [],
      msgTimes: [],
    };
    server.serializeAttachment(state);
    this.ctx.acceptWebSocket(server);

    this.currentConnections += 1;
    this.totalConnections += 1;
    if (app) this.connectionsByApp[app] = (this.connectionsByApp[app] ?? 0) + 1;
    this.scheduleHeartbeat();

    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  private scheduleHeartbeat() {
    if (!this.heartbeatScheduled) {
      this.heartbeatScheduled = true;
      void this.ctx.storage.setAlarm(now() + CONFIG.heartbeatIntervalMs);
    }
  }
  private heartbeatScheduled = false;

  async alarm() {
    this.heartbeatScheduled = false;
    const sockets = this.ctx.getWebSockets();
    if (sockets.length === 0) return;

    for (const ws of sockets) {
      const st = ws.deserializeAttachment();
      if (!st) continue;

      if (st.awaitingPong) {
        this.closeSocket(ws, 4003, 'heartbeat timeout');
        continue;
      }
      if (!st.authenticated && now() - st.connectedAt > CONFIG.authTimeoutMs) {
        this.send(ws, { type: 'auth_error', code: 'SESSION_EXPIRED', message: 'Authentication timeout' });
        this.closeSocket(ws, 4001, 'auth timeout');
        continue;
      }

      this.send(ws, { type: 'ping' });
      st.awaitingPong = true;
      ws.serializeAttachment(st);
    }

    if (this.ctx.getWebSockets().length > 0) {
      await this.ctx.storage.setAlarm(now() + CONFIG.heartbeatIntervalMs);
    }
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    const st = ws.deserializeAttachment();
    if (!st) return;

    if (String(message).length > CONFIG.maxPayloadBytes) {
      this.send(ws, { type: 'error', code: 'INVALID_PAYLOAD', message: 'Payload too large' });
      return;
    }
    const limited = this.rateLimited(st);
    if (limited) {
      this.send(ws, { type: 'rate_limit_exceeded', message: 'Message rate limit exceeded', retryAfter: limited });
      return;
    }
    const msg = this.parseMessage(message);
    if (!msg) {
      this.send(ws, { type: 'error', code: 'INVALID_PAYLOAD', message: 'Malformed message' });
      return;
    }

    switch (msg.type) {
      case 'auth':
        await this.handleAuth(ws, st, msg.token);
        break;
      case 'subscribe':
        this.handleSubscribe(ws, st, msg.channel);
        break;
      case 'unsubscribe':
        this.handleUnsubscribe(ws, st, msg.channel);
        break;
      case 'publish':
        this.handleClientPublish(ws, st, msg.channel, msg.event, msg.id);
        break;
      case 'ping':
        this.pingsReceived += 1;
        this.send(ws, { type: 'pong' });
        break;
      case 'pong':
        st.alive = true;
        st.awaitingPong = false;
        ws.serializeAttachment(st);
        break;
      case 'get_hints':
        this.send(ws, { type: 'server_hints', hints: {} });
        break;
      case 'get_maintenance':
        this.send(ws, { type: 'maintenance_status', maintenance: { enabled: false } });
        break;
      default:
        this.errors.INVALID_EVENT = (this.errors.INVALID_EVENT ?? 0) + 1;
        this.send(ws, { type: 'error', code: 'INVALID_EVENT', message: `Unknown message type: ${(msg as any).type}` });
    }
  }

  webSocketClose(ws: WebSocket) {
    const st = ws.deserializeAttachment();
    if (!st) return;
    this.currentConnections = Math.max(0, this.currentConnections - 1);
    if (st.app && this.connectionsByApp[st.app]) {
      this.connectionsByApp[st.app] -= 1;
      if (this.connectionsByApp[st.app] <= 0) delete this.connectionsByApp[st.app];
    }
    for (const channel of st.subscriptions) this.leaveChannel(channel, st);
  }

  webSocketError(ws: WebSocket) {
    this.webSocketClose(ws);
  }

  private closeSocket(ws: WebSocket, code: number, reason: string) {
    try {
      ws.close(code, reason);
    } catch {
      /* already closed */
    }
  }

  // ---------------------------------------------------------------------------
  // Protocol handling
  // ---------------------------------------------------------------------------

  private send(ws: WebSocket, message: ServerMessage) {
    try {
      ws.send(JSON.stringify(message));
    } catch {
      /* closing */
    }
  }

  private parseMessage(raw: string | ArrayBuffer): ClientMessage | null {
    try {
      const data = String(raw);
      const msg = JSON.parse(data) as ClientMessage;
      if (typeof msg?.type !== 'string') return null;
      return msg;
    } catch {
      return null;
    }
  }

  private rateLimited(st: SocketState): number {
    const t = now();
    st.msgTimes = st.msgTimes.filter((x) => t - x < CONFIG.msgWindowMs);
    st.msgTimes.push(t);
    if (st.msgTimes.length > CONFIG.msgWindowLimit) {
      return Math.max(1, Math.ceil((st.msgTimes[0] + CONFIG.msgWindowMs - t) / 1000));
    }
    return 0;
  }

  private async handleAuth(ws: WebSocket, st: SocketState, token: string) {
    if (st.authenticated) return;
    const identity = await this.ctx.blockConcurrencyWhile(() => resolveIdentity(token, this.env, this.sessionCache));
    if (!identity) {
      this.authFailures += 1;
      this.send(ws, { type: 'auth_error', code: 'UNAUTHORIZED', message: 'Invalid or expired session token' });
      this.closeSocket(ws, 4001, 'unauthorized');
      return;
    }
    st.identity = identity;
    st.authenticated = true;
    st.alive = true;
    ws.serializeAttachment(st);
    this.authSuccesses += 1;
    this.send(ws, {
      type: 'auth_ok',
      userId: identity.userId,
      adminRole: identity.adminRole,
      protocol: RT_PROTOCOL_VERSION,
    });
  }

  private joinChannel(st: SocketState, channel: string) {
    if (!st.identity) return;
    let members = this.members.get(channel);
    if (!members) {
      members = new Map();
      this.members.set(channel, members);
    }
    members.set(st.identity.userId, {
      userId: st.identity.userId,
      email: st.identity.email || undefined,
      status: 'online',
      lastSeen: now(),
    });
  }

  private leaveChannel(channel: string, st: SocketState) {
    const members = this.members.get(channel);
    if (members && st.identity) {
      members.delete(st.identity.userId);
      if (members.size === 0) this.members.delete(channel);
    }
  }

  private handleSubscribe(ws: WebSocket, st: SocketState, channel: string) {
    const info = parseChannel(channel);
    if (!info) {
      this.send(ws, { type: 'error', code: 'CHANNEL_NOT_FOUND', message: `Invalid channel: ${channel}` });
      return;
    }
    const allowed = canSubscribe(st.identity ?? null, info);
    if (!allowed.ok) {
      this.send(ws, { type: 'error', code: allowed.code ?? 'FORBIDDEN', message: allowed.message ?? 'Not allowed' });
      return;
    }
    if (st.subscriptions.length >= CONFIG.maxSubscriptions) {
      this.send(ws, { type: 'error', code: 'RATE_LIMITED', message: 'Subscription limit reached' });
      return;
    }
    if (st.subscriptions.includes(channel)) return;
    st.subscriptions.push(channel);
    ws.serializeAttachment(st);
    this.joinChannel(st, channel);
    this.send(ws, { type: 'subscribed', channel });

    const presence: Record<string, PresenceEntry> = {};
    const members = this.members.get(channel);
    if (members) {
      for (const entry of members.values()) presence[entry.userId] = entry;
    }
    this.send(ws, { type: 'presence', channel, presence });
  }

  private handleUnsubscribe(ws: WebSocket, st: SocketState, channel: string) {
    const idx = st.subscriptions.indexOf(channel);
    if (idx === -1) return;
    st.subscriptions.splice(idx, 1);
    ws.serializeAttachment(st);
    this.leaveChannel(channel, st);
    this.send(ws, { type: 'unsubscribed', channel });
  }

  private handleClientPublish(ws: WebSocket, st: SocketState, channel: string, event: Record<string, unknown>, id?: string) {
    if (!st.authenticated || !st.identity) {
      this.send(ws, { type: 'error', code: 'UNAUTHORIZED', message: 'Authenticate first' });
      return;
    }
    const info = parseChannel(channel);
    if (!info || !canPublish(st.identity, info)) {
      this.send(ws, { type: 'error', code: 'FORBIDDEN', message: 'Publish not allowed on this channel' });
      return;
    }
    const t = now();
    st.publishTokens = st.publishTokens.filter((x) => t - x < CONFIG.msgWindowMs);
    st.publishTokens.push(t);
    if (st.publishTokens.length > CONFIG.publishWindowLimit) {
      this.send(ws, {
        type: 'rate_limit_exceeded',
        message: 'Publish rate limit exceeded',
        retryAfter: Math.ceil(CONFIG.msgWindowMs / 1000),
      });
      return;
    }
    if (typeof event !== 'object' || event === null || Array.isArray(event)) {
      this.send(ws, { type: 'error', code: 'INVALID_PAYLOAD', message: 'Event payload must be an object' });
      return;
    }
    const envelope: RealtimeEvent = {
      id: id || crypto.randomUUID(),
      type: String(event.type || 'custom'),
      channel,
      actor: { id: st.identity.userId, email: st.identity.email },
      payload: event,
      timestamp: new Date().toISOString(),
      correlationId: typeof (event as any).correlationId === 'string' ? (event as any).correlationId : undefined,
    };
    this.eventsPublished += 1;
    this.deliver(channel, envelope);
  }

  private deliver(channel: string, event: RealtimeEvent) {
    const frame = JSON.stringify({ type: 'event', channel, event });
    for (const ws of this.ctx.getWebSockets()) {
      const st = ws.deserializeAttachment();
      if (!st?.authenticated || !st.subscriptions.includes(channel)) continue;
      try {
        ws.send(frame);
        this.eventsDelivered += 1;
        this.lastEventAt = now();
      } catch {
        /* closing */
      }
    }
  }

  // ---------------------------------------------------------------------------
  // HTTP RPC (publish from the api)
  // ---------------------------------------------------------------------------

  private async handlePublish(request: Request): Promise<Response> {
    interface PublishBody {
      userId?: string;
      channel?: string;
      broadcast?: boolean;
      event: Omit<RealtimeEvent, 'id' | 'channel' | 'timestamp'> & Partial<Pick<RealtimeEvent, 'channel' | 'timestamp'>>;
    }

    let body: PublishBody;
    try {
      body = (await request.json()) as PublishBody;
    } catch {
      return Response.json({ error: 'Invalid JSON' }, { status: 400 });
    }

    const id = this.processPublishBody(body);
    if (!id) {
      return Response.json({ error: 'Provide userId, channel, or broadcast' }, { status: 400 });
    }
    return Response.json({ ok: true, eventId: id });
  }

  /** Publish a single event; returns its id, or null when the target is missing. */
  private processPublishBody(
    body:
      | { userId?: string; channel?: string; broadcast?: boolean; event: Record<string, unknown> }
      | undefined,
  ): string | null {
    if (!body?.event || typeof body.event.type !== 'string' || !body.event.type) return null;

    const base = body.event as Partial<RealtimeEvent>;
    const event: RealtimeEvent = {
      ...base,
      id: crypto.randomUUID(),
      channel: String(base.channel || body.channel || ''),
      timestamp: String(base.timestamp || new Date().toISOString()),
      type: String(base.type),
      payload: base.payload ?? {},
    };

    if (body.userId) {
      this.deliver(`user:${body.userId}`, { ...event, channel: `user:${body.userId}` });
    } else if (body.broadcast) {
      this.deliver(event.channel, event);
    } else if (body.channel) {
      event.channel = body.channel;
      this.deliver(event.channel, event);
    } else {
      return null;
    }

    this.eventsPublished += 1;
    return event.id;
  }

  /** Batch RPC: accepts `{ events: PublishBody[] }` and delivers each in one call. */
  private async handlePublishBatch(request: Request): Promise<Response> {
    let body: { events?: unknown[] };
    try {
      body = (await request.json()) as { events?: unknown[] };
    } catch {
      return Response.json({ error: 'Invalid JSON' }, { status: 400 });
    }
    if (!Array.isArray(body?.events)) {
      return Response.json({ error: 'events[] is required' }, { status: 400 });
    }
    let processed = 0;
    for (const entry of body.events) {
      if (this.processPublishBody(entry as { userId?: string; channel?: string; broadcast?: boolean; event: Record<string, unknown> } | undefined)) processed += 1;
    }
    return Response.json({ ok: true, processed, total: body.events.length });
  }

  // ---------------------------------------------------------------------------
  // Metrics
  // ---------------------------------------------------------------------------

  private snapshot() {
    return {
      service: 'tirbeo-realtime-worker',
      startedAt: this.startedAt,
      currentConnections: this.currentConnections,
      totalConnections: this.totalConnections,
      connectionsByApp: { ...this.connectionsByApp },
      subscriptions: this.ctx
        .getWebSockets()
        .reduce((n, ws) => n + (ws.deserializeAttachment()?.subscriptions.length ?? 0), 0),
      channels: this.members.size,
      eventsDelivered: this.eventsDelivered,
      eventsPublished: this.eventsPublished,
      authFailures: this.authFailures,
      authSuccesses: this.authSuccesses,
      pingsReceived: this.pingsReceived,
      errors: { ...this.errors },
      lastEventAt: this.lastEventAt,
    };
  }
}
