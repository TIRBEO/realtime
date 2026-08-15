import { WebSocket } from 'ws';
import { randomUUID } from 'crypto';
import {
  parseChannel,
  type ClientMessage,
  type ServerMessage,
  type RealtimeEvent,
  type PresenceEntry,
  type RealtimeErrorCode,
  RT_PROTOCOL_VERSION,
} from './protocol';
import { canSubscribe, canPublish, isAdmin, type Identity } from './permissions';
import { resolveIdentity } from './auth';
import { metrics, hubState } from './metrics';
import { getRedisSubscriber, publishEvent, EVENT_CHANNEL, PRESENCE_CHANNEL, CONTROL_CHANNEL, RT_NAMESPACE } from './redis';

const INSTANCE_ID = randomUUID().slice(0, 8);

const CONFIG = {
  authTimeoutMs: 5_000,
  heartbeatIntervalMs: 30_000,
  maxConnsPerIp: 8,
  msgWindowMs: 10_000,
  msgWindowLimit: 250,
  publishWindowLimit: 40,
  maxSubscriptions: 64,
  maxPayloadBytes: 64 * 1024,
};

interface Connection {
  id: string;
  ws: WebSocket;
  ip: string;
  app?: string;
  identity?: Identity;
  authenticated: boolean;
  authenticatedAt?: number;
  alive: boolean;
  subscriptions: Set<string>;
  publishTokens: number[];
  msgTimes: number[];
  lastPingAt?: number;
}

/** channel → userId → presence entry */
const channelMembers = new Map<string, Map<string, PresenceEntry>>();
/** channel → connId */
const channelConns = new Map<string, Set<string>>();
const conns = new Map<string, Connection>();
let heartbeatStarted = false;
let subscriberStarted = false;

const now = () => Date.now();

function send(conn: Connection, message: ServerMessage) {
  if (conn.ws.readyState !== conn.ws.OPEN) return;
  try {
    conn.ws.send(JSON.stringify(message));
  } catch {
    /* closing */
  }
}

function sendError(conn: Connection, code: RealtimeErrorCode, message: string) {
  metrics.error(code);
  send(conn, { type: 'error', code, message });
}

function rateLimited(conn: Connection): { limited: boolean; retryAfter: number } {
  const t = now();
  conn.msgTimes = conn.msgTimes.filter((x) => t - x < CONFIG.msgWindowMs);
  conn.msgTimes.push(t);
  if (conn.msgTimes.length > CONFIG.msgWindowLimit) {
    return { limited: true, retryAfter: Math.max(1, Math.ceil((conn.msgTimes[0] + CONFIG.msgWindowMs - t) / 1000)) };
  }
  return { limited: false, retryAfter: 0 };
}

function publishLimited(conn: Connection): boolean {
  const t = now();
  conn.publishTokens = conn.publishTokens.filter((x) => t - x < CONFIG.msgWindowMs);
  conn.publishTokens.push(t);
  return conn.publishTokens.length > CONFIG.publishWindowLimit;
}

function parseMessage(raw: unknown): ClientMessage | null {
  try {
    const data = Buffer.isBuffer(raw) ? raw.toString('utf8') : String(raw);
    if (data.length > CONFIG.maxPayloadBytes) return null;
    const msg = JSON.parse(data);
    if (typeof msg?.type !== 'string') return null;
    return msg as ClientMessage;
  } catch {
    return null;
  }
}

function deliverLocal(channel: string, event: RealtimeEvent) {
  const set = channelConns.get(channel);
  if (!set || set.size === 0) return;
  for (const connId of set) {
    const conn = conns.get(connId);
    if (!conn?.authenticated) continue;
    send(conn, { type: 'event', channel, event });
    metrics.eventDelivered();
  }
}

/** Deliver an event to a single user via their `user:{id}` channel. */
export function serverPublishToUser(userId: string, event: RealtimeEvent): boolean {
  const channel = `user:${userId}`;
  const local = { ...event, channel };
  deliverLocal(channel, local);
  return publishEvent(JSON.stringify({ instanceId: INSTANCE_ID, target: { userId }, event: local }));
}

/** Deliver an event to everyone subscribed to `event.channel`. */
export function serverPublish(event: RealtimeEvent): boolean {
  deliverLocal(event.channel, event);
  return publishEvent(JSON.stringify({ instanceId: INSTANCE_ID, event }));
}

/** Deliver an event to everyone subscribed to a channel prefix. */
export function serverBroadcast(event: RealtimeEvent): boolean {
  deliverLocal(event.channel, event);
  return publishEvent(JSON.stringify({ instanceId: INSTANCE_ID, broadcast: true, event }));
}

function authTimeout(conn: Connection) {
  if (!conn.authenticated && conn.ws.readyState === conn.ws.OPEN) {
    send(conn, { type: 'auth_error', code: 'SESSION_EXPIRED', message: 'Authentication timeout' });
    conn.ws.close(4001, 'auth timeout');
  }
}

function handleAuth(conn: Connection, token: string) {
  if (conn.authenticated) return;
  resolveIdentity(token)
    .then((identity) => {
      if (conn.ws.readyState !== conn.ws.OPEN) return;
      if (!identity) {
        metrics.authFailure();
        send(conn, { type: 'auth_error', code: 'UNAUTHORIZED', message: 'Invalid or expired session token' });
        setTimeout(() => conn.ws.close(4001, 'unauthorized'), 100);
        return;
      }
      conn.identity = identity;
      conn.authenticated = true;
      conn.authenticatedAt = now();
      metrics.authSuccess();
      send(conn, {
        type: 'auth_ok',
        userId: identity.userId,
        adminRole: identity.adminRole,
        protocol: RT_PROTOCOL_VERSION,
      });
    })
    .catch(() => {
      send(conn, { type: 'auth_error', code: 'SERVER_ERROR', message: 'Auth service unavailable' });
    });
}

function joinChannel(conn: Connection, channel: string) {
  let connsForChannel = channelConns.get(channel);
  if (!connsForChannel) {
    connsForChannel = new Set();
    channelConns.set(channel, connsForChannel);
  }
  connsForChannel.add(conn.id);
  let members = channelMembers.get(channel);
  if (!members) {
    members = new Map();
    channelMembers.set(channel, members);
  }
  members.set(conn.identity!.userId, {
    userId: conn.identity!.userId,
    email: conn.identity!.email || undefined,
    status: 'online',
    lastSeen: now(),
  });
  syncHubState();
}

function leaveChannel(conn: Connection, channel: string) {
  const connsForChannel = channelConns.get(channel);
  if (connsForChannel) {
    connsForChannel.delete(conn.id);
    if (connsForChannel.size === 0) channelConns.delete(channel);
  }
  const members = channelMembers.get(channel);
  if (members && conn.identity) {
    members.delete(conn.identity.userId);
    if (members.size === 0) channelMembers.delete(channel);
  }
  syncHubState();
}

function handleSubscribe(conn: Connection, channel: string) {
  const info = parseChannel(channel);
  if (!info) {
    send(conn, { type: 'error', code: 'CHANNEL_NOT_FOUND', message: `Invalid channel: ${channel}` });
    return;
  }
  const allowed = canSubscribe(conn.identity ?? null, info);
  if (!allowed.ok) {
    send(conn, { type: 'error', code: allowed.code ?? 'FORBIDDEN', message: allowed.message ?? 'Not allowed' });
    return;
  }
  if (conn.subscriptions.size >= CONFIG.maxSubscriptions) {
    send(conn, { type: 'error', code: 'RATE_LIMITED', message: 'Subscription limit reached' });
    return;
  }
  if (conn.subscriptions.has(channel)) return;
  conn.subscriptions.add(channel);
  joinChannel(conn, channel);
  send(conn, { type: 'subscribed', channel });

  const members = channelMembers.get(channel);
  const presence: Record<string, PresenceEntry> = {};
  if (members) {
    for (const entry of members.values()) presence[entry.userId] = entry;
  }
  send(conn, { type: 'presence', channel, presence });
}

function handleUnsubscribe(conn: Connection, channel: string) {
  if (!conn.subscriptions.has(channel)) return;
  conn.subscriptions.delete(channel);
  leaveChannel(conn, channel);
  send(conn, { type: 'unsubscribed', channel });
}

function handleClientPublish(conn: Connection, channel: string, event: Record<string, unknown>, id?: string) {
  if (!conn.authenticated || !conn.identity) {
    send(conn, { type: 'error', code: 'UNAUTHORIZED', message: 'Authenticate first' });
    return;
  }
  const info = parseChannel(channel);
  if (!info || !canPublish(conn.identity, info)) {
    send(conn, { type: 'error', code: 'FORBIDDEN', message: 'Publish not allowed on this channel' });
    return;
  }
  if (publishLimited(conn)) {
    send(conn, { type: 'rate_limit_exceeded', message: 'Publish rate limit exceeded', retryAfter: Math.ceil(CONFIG.msgWindowMs / 1000) });
    return;
  }
  if (typeof event !== 'object' || event === null || Array.isArray(event)) {
    send(conn, { type: 'error', code: 'INVALID_PAYLOAD', message: 'Event payload must be an object' });
    return;
  }
  const envelope: RealtimeEvent = {
    id: id || randomUUID(),
    type: String(event.type || 'custom'),
    channel,
    actor: { id: conn.identity.userId, email: conn.identity.email },
    payload: event,
    timestamp: new Date().toISOString(),
    correlationId: typeof (event as any).correlationId === 'string' ? (event as any).correlationId : undefined,
  };
  metrics.eventPublished();
  serverPublish(envelope);
}

function syncHubState() {
  hubState.channels = channelConns.size;
  hubState.subscriptions = [...channelConns.values()].reduce((n, s) => n + s.size, 0);
}

function heartbeatTick() {
  for (const conn of conns.values()) {
    if (!conn.alive) {
      conn.ws.terminate();
      continue;
    }
    conn.alive = false;
    if (conn.ws.readyState === conn.ws.OPEN) {
      try {
        conn.ws.ping();
      } catch {
        conn.ws.terminate();
      }
    }
  }
}

function startHeartbeat() {
  if (heartbeatStarted) return;
  heartbeatStarted = true;
  const timer = setInterval(heartbeatTick, CONFIG.heartbeatIntervalMs);
  timer.unref?.();
}

function startSubscriber() {
  if (subscriberStarted) return;
  subscriberStarted = true;
  const sub = getRedisSubscriber();
  if (!sub) return;
  sub.on('message', (_channel, raw) => {
    if (raw.length > CONFIG.maxPayloadBytes * 2) return;
    let envelope: any;
    try {
      envelope = JSON.parse(raw);
    } catch {
      return;
    }
    const event: RealtimeEvent | undefined = envelope?.event;
    if (!event) return;
    if (envelope.instanceId === INSTANCE_ID) return; // already delivered locally
    if (envelope.target?.userId) {
      deliverLocal(`user:${envelope.target.userId}`, event);
    } else {
      deliverLocal(event.channel, event);
    }
  });
  sub.subscribe(EVENT_CHANNEL, PRESENCE_CHANNEL, CONTROL_CHANNEL);
}

/**
 * Called by the upgrade route once a connection is established.
 * The `ws` instance is the raw `ws` WebSocket handed to
 * `experimental_upgradeWebSocket`.
 */
export function handleUpgrade(ws: WebSocket, meta: { ip?: string } = {}) {
  startHeartbeat();
  startSubscriber();

  const ip = meta.ip || 'unknown';
  let ipConns = 0;
  for (const c of conns.values()) if (c.ip === ip) ipConns += 1;
  if (ipConns >= CONFIG.maxConnsPerIp) {
    ws.close(4002, 'connection limit');
    return;
  }

  let app: string | undefined;
  try {
    app = new URL(ws.url || '/ws', 'http://local').searchParams.get('app') || undefined;
  } catch {
    /* ignore */
  }

  const conn: Connection = {
    id: randomUUID(),
    ws,
    ip,
    app,
    authenticated: false,
    alive: true,
    subscriptions: new Set(),
    publishTokens: [],
    msgTimes: [],
  };
  conns.set(conn.id, conn);
  metrics.connectionOpened(app);

  const authTimer = setTimeout(() => authTimeout(conn), CONFIG.authTimeoutMs);

  ws.on('message', (raw) => {
    if (String(raw).length > CONFIG.maxPayloadBytes) {
      send(conn, { type: 'error', code: 'INVALID_PAYLOAD', message: 'Payload too large' });
      return;
    }
    const limited = rateLimited(conn);
    if (limited.limited) {
      send(conn, { type: 'rate_limit_exceeded', message: 'Message rate limit exceeded', retryAfter: limited.retryAfter });
      return;
    }
    const msg = parseMessage(raw);
    if (!msg) {
      send(conn, { type: 'error', code: 'INVALID_PAYLOAD', message: 'Malformed message' });
      return;
    }
    switch (msg.type) {
      case 'auth':
        handleAuth(conn, msg.token);
        break;
      case 'subscribe':
        handleSubscribe(conn, msg.channel);
        break;
      case 'unsubscribe':
        handleUnsubscribe(conn, msg.channel);
        break;
      case 'publish':
        handleClientPublish(conn, msg.channel, msg.event, msg.id);
        break;
      case 'ping':
        metrics.ping();
        send(conn, { type: 'pong' });
        break;
      case 'pong':
        conn.alive = true;
        conn.lastPingAt = now();
        break;
      case 'get_hints':
        send(conn, { type: 'server_hints', hints: {} });
        break;
      case 'get_maintenance':
        send(conn, { type: 'maintenance_status', maintenance: { enabled: false } });
        break;
      default:
        send(conn, { type: 'error', code: 'INVALID_EVENT', message: `Unknown message type: ${(msg as any).type}` });
    }
  });

  ws.on('pong', () => {
    conn.alive = true;
    conn.lastPingAt = now();
  });

  const cleanup = () => {
    clearTimeout(authTimer);
    for (const channel of conn.subscriptions) leaveChannel(conn, channel);
    conn.subscriptions.clear();
    conns.delete(conn.id);
    metrics.connectionClosed(conn.app);
    syncHubState();
  };
  ws.on('close', cleanup);
  ws.on('error', cleanup);
}

export function getChannelConnsCount() {
  return channelConns.size;
}

export { isAdmin, RT_NAMESPACE };
