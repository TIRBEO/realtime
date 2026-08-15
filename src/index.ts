import { RT_PROTOCOL_VERSION } from './protocol';
import { RealtimeHub } from './hub';
import type { Env } from './env';

export { RealtimeHub };

export class RealtimeHubV2 extends RealtimeHub {}

const HUB_NAME = 'hub';
const MAX_PUBLISH_BYTES = 64 * 1024;
const BATCH_MAX = 100;

interface QueuedPublish {
  userId?: string;
  channel?: string;
  broadcast?: boolean;
  event: Record<string, unknown>;
}

// The Durable Object serializes RPCs, so per-event round trips cap throughput.
// Coalesce publishes here: while a batch window is open, every request hands the
// SAME promise to ctx.waitUntil, so the isolate stays alive for the 5ms window
// to elapse and concurrent events are delivered in a single DO RPC — while the
// client ack returns immediately (no waiting on the DO round trip).
// IMPORTANT: DO stubs are request-bound I/O (an OutgoingFactory), so they must be
// created inside each request and passed into the shared drain — waitUntil keeps
// that request's context alive until the drain completes, so the stub stays valid.
const publishQueue: QueuedPublish[] = [];
let activeBatch: Promise<void> | null = null;

const BATCH_WINDOW_MS = 5;

interface HubRpcStub {
  processBatch(events: unknown[]): Promise<{ processed: number; total: number }> | { processed: number; total: number };
  metricsSnapshot(): Promise<Record<string, unknown>> | Record<string, unknown>;
}

async function drainNow(stub: DurableObjectStub): Promise<void> {
  // Window: concurrent requests push during this await, then we send them as one batch.
  await new Promise((r) => setTimeout(r, BATCH_WINDOW_MS));
  while (publishQueue.length > 0) {
    const batch = publishQueue.splice(0, BATCH_MAX);
    try {
      await (stub as unknown as HubRpcStub).processBatch(batch);
    } catch {
      // Best-effort: drop this batch rather than blocking the caller.
    }
  }
}

function flushPublishes(stub: DurableObjectStub, ctx: ExecutionContext): void {
  if (!activeBatch) {
    activeBatch = drainNow(stub).finally(() => {
      activeBatch = null;
    });
  }
  ctx.waitUntil(activeBatch);
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/api/health') {
      return Response.json({
        status: 'ok',
        service: 'tirbeo-realtime-worker',
        protocol: RT_PROTOCOL_VERSION,
        time: new Date().toISOString(),
      });
    }

    const stub = env.REALTIME_HUB.get(env.REALTIME_HUB.idFromName(HUB_NAME));

    if (url.pathname === '/api/metrics') {
      const data = await (stub as unknown as HubRpcStub).metricsSnapshot();
      return Response.json(data);
    }

    if (url.pathname === '/api/publish' && request.method === 'POST') {
      const apiToken = env.API_TOKEN;
      if (!apiToken) return Response.json({ error: 'API_TOKEN not configured' }, { status: 503 });
      const header = request.headers.get('authorization') || '';
      const key = header.replace(/^Bearer\s+/i, '').trim();
      if (key !== apiToken && request.headers.get('x-api-token') !== apiToken) {
        return Response.json({ error: 'Unauthorized' }, { status: 401 });
      }
      const body = await request.text();
      if (body.length > MAX_PUBLISH_BYTES) {
        return Response.json({ error: 'Payload too large' }, { status: 413 });
      }
      let parsed: QueuedPublish;
      try {
        parsed = JSON.parse(body) as QueuedPublish;
      } catch {
        return Response.json({ error: 'Invalid JSON' }, { status: 400 });
      }
      if (!parsed?.event || typeof parsed.event.type !== 'string' || !parsed.event.type) {
        return Response.json({ error: 'event.type is required' }, { status: 400 });
      }
      if (!parsed.userId && !parsed.broadcast && !parsed.channel) {
        return Response.json({ error: 'Provide userId, channel, or broadcast' }, { status: 400 });
      }
      publishQueue.push({ userId: parsed.userId, channel: parsed.channel, broadcast: parsed.broadcast, event: parsed.event });
      flushPublishes(stub, ctx);
      return Response.json({ ok: true, queued: publishQueue.length });
    }

    if (url.pathname === '/ws' && request.method === 'GET') {
      // Preserve the upgrade request and attach the client IP for the hub.
      const headers = new Headers(request.headers);
      const ip =
        request.headers.get('CF-Connecting-IP') ||
        request.headers.get('x-forwarded-for')?.split(',')[0]?.trim() ||
        'unknown';
      headers.set('x-rt-ip', ip);
      return stub.fetch(new Request(request, { headers }));
    }

    return new Response('Not found', { status: 404 });
  },
};
