# Tirbeo Realtime Platform (`ws.tirbeo.app`)

Centralized, horizontally-scalable WebSocket infrastructure for the Tirbeo
ecosystem. Runs as a standalone Next.js (App Router) app on Vercel Fluid Compute.
The api (and any other service) pushes events into it; every client app subscribes
to channels.

## Architecture

```
  client apps (dashboard, admin, support, forms, flows)
        │  WebSocket  wss://ws.tirbeo.app/ws
        ▼
   ┌─────────────────────────────┐   Redis pub/sub (tirbeo:rt:events)   ┌──────────────┐
   │  realtime hub (this app)    │ ───────────────────────────────────► │ other Vercel │
   │  - JWT auth (shared secret) │ ◄─────────────────────────────────── │ instances     │
   │  - channel subscriptions    │                                     └──────────────┘
   │  - presence, rate limits    │
   │  - heartbeat, metrics       │
   └─────────────────────────────┘
        ▲
        │  HTTP POST /api/publish (Bearer API_TOKEN) or Redis pub/sub
   api.tirbeo.app
```

- **Auth**: clients send `{ type: 'auth', token }` with their short-lived session
  access token (15m). The token is verified with `jose` HS256 using the **same
  `JWT_SECRET`** as api.tirbeo.app, then the session is checked against the
  `sessions`/`users` tables (cached in Redis for 60s). If `DATABASE_URL` is
  unset, only signature verification runs.
- **Channels**: `public:*`, `user:{id}`, `org:{id}`, `workspace:{id}`,
  `flow:{id}`, `ticket:{id}`, `document:{id}`, `deployment:{id}`, `call:{id}`,
  `chat:{id}`, and bare app channels (`admin`, `support`, `flows`, `dashboard`).
- **Cross-instance**: each Vercel instance publishes envelopes to the Redis
  `events` channel; a subscriber delivers to local connections. Local delivery
  is synchronous for latency; remote instances skip envelopes they produced.
- **Publishing**: the api calls HTTP `POST https://ws.tirbeo.app/api/publish`
  with `Authorization: Bearer $API_TOKEN` (or publishes to the Redis channel).

## Deploy on Vercel (ws.tirbeo.app)

1. Create a new project in Vercel from this repo (root = `apps/realtime`,
   framework = Next.js).
2. Enable **Fluid Compute** (Settings → Functions) — required for WebSockets.
3. Environment variables (must match the api):
   - `JWT_SECRET` — same as api.tirbeo.app
   - `DATABASE_URL` — session-mode pooler (port 5432)
   - `REDIS_URL` — Upstash Redis (same instance the api uses)
   - `API_TOKEN` — shared secret the api uses to publish
   - `RT_NAMESPACE` — optional, default `tirbeo:rt`
4. Add custom domain **ws.tirbeo.app**.
5. Deploy. Verify: `curl https://ws.tirbeo.app/api/health`.

## Local development

WebSocket upgrades only work under `vercel dev` (not `next dev`):

```bash
pnpm install
vercel env pull
pnpm dev        # → http://localhost:4001 (ws://localhost:4001/ws)
```

## Protocol

See `lib/protocol.ts`. Client → server: `auth`, `subscribe`, `unsubscribe`,
`publish`, `ping`, `pong`, `get_hints`, `get_maintenance`.
Server → client: `auth_ok`, `auth_error`, `subscribed`, `unsubscribed`,
`event`, `presence`, `ping`, `pong`, `error`, `rate_limit_exceeded`,
`maintenance_status`, `server_hints`.

The protocol is backward-compatible with the api's local dev WebSocket server
(`apps/api` port 3001), so the same client code works locally and in production.

## HTTP publish example

```bash
curl -X POST https://ws.tirbeo.app/api/publish \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "channel": "admin",
    "event": { "type": "user.flagged", "payload": { "userId": "..." } }
  }'
```
