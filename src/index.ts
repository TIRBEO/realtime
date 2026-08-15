import { RT_PROTOCOL_VERSION } from './protocol';
import { RealtimeHub } from './hub';
import type { Env } from './env';

export { RealtimeHub };

const HUB_NAME = 'hub';
const MAX_PUBLISH_BYTES = 64 * 1024;

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
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
      return stub.fetch(new Request('https://hub.local/metrics', { method: 'GET' }));
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
      return stub.fetch(
        new Request('https://hub.local/publish', {
          method: 'POST',
          body,
          headers: { 'content-type': 'application/json' },
        }),
      );
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
