# Tirbeo Realtime Platform (`ws.tirbeo.app`)

Centralized WebSocket infrastructure for the Tirbeo ecosystem. Runs as a
**Cloudflare Worker with a single global Durable Object** that owns every
connection. The api (and any other service) pushes events into it via HTTP;
every client app subscribes to channels over one WebSocket.

## Architecture

```
  client apps (dashboard, admin, support, forms, flows)
        │  WebSocket  wss://ws.tirbeo.app/ws
        ▼
   ┌────────────────────────────────────────────┐
   │ Worker (edge)                              │
   │  /ws            → forwards upgrade to hub  │
   │  /api/publish   → Bearer API_TOKEN → hub   │
   │  /api/health, /api/metrics                 │
   └──────────────┬─────────────────────────────┘
                  │ Durable Object stub (idFromName("hub"))
                  ▼
   ┌────────────────────────────────────────────┐
   │ RealtimeHub (single global Durable Object) │
   │  - WebSocket Hibernation API connections   │
   │  - JWT auth (jose, shared JWT_SECRET)      │
   │  - channel subscriptions, presence         │
   │  - fan-out (no Redis needed — one hub)     │
   │  - heartbeat via storage alarm             │
   └────────────────────────────────────────────┘
        ▲
        │  HTTP POST /api/publish (Bearer API_TOKEN)
   api.tirbeo.app
```

- **Auth**: clients send `{ type: 'auth', token }` with their short-lived
  session access token (15m). Verified with `jose` HS256 using the **same
  `JWT_SECRET`** as api.tirbeo.app. If `SUPABASE_URL`/`SUPABASE_SERVICE_ROLE_KEY`
  are set, the session + user status are checked via PostgREST (cached 60s in
  the Durable Object). Otherwise auth is signature-only and the `adminRole`
  JWT claim is used for channel authorization.
- **Channels**: `public:*`, `user:{id}`, `org:{id}`, `workspace:{id}`,
  `flow:{id}`, `ticket:{id}`, `document:{id}`, `deployment:{id}`, `call:{id}`,
  `chat:{id}`, and bare app channels (`admin`, `support`, `flows`, `dashboard`).
- **Cross-instance**: a single global Durable Object instance makes fan-out
  trivial and needs no Redis pub/sub. Scaling later = shard the hub by channel
  hash (the wire protocol does not change).
- **Publishing**: the api calls HTTP `POST https://ws.tirbeo.app/api/publish`
  with `Authorization: Bearer $API_TOKEN`.

## Deploy on Cloudflare (`ws.tirbeo.app`)

```bash
cd apps/realtime
pnpm install
npx wrangler login
npx wrangler secret put JWT_SECRET     # same as api.tirbeo.app
npx wrangler secret put API_TOKEN      # shared with the api's RT_API_TOKEN
# optional — enables live session/role checks:
# npx wrangler secret put SUPABASE_URL
# npx wrangler secret put SUPABASE_SERVICE_ROLE_KEY
npx wrangler deploy
```

Then in the Cloudflare dashboard: **Workers → tirbeo-realtime → Settings →
Domains & Routes → Add custom domain → `ws.tirbeo.app`**.

Verify: `curl https://ws.tirbeo.app/api/health` → `{"status":"ok",...}`.

> Note: the realtime project on Vercel is superseded by this Worker — delete it
> so pushes to this repo don't trigger failing Vercel builds.

## Local development

```bash
cp .env.example .dev.vars   # fill in real values (or copy from api/.env.local)
npx wrangler dev            # → http://localhost:8787 (ws://localhost:8787/ws)
```

The protocol is backward-compatible with the api's local dev WebSocket server
(`apps/api` port 3001), so the same client code works locally and in production.

## Protocol

See `src/protocol.ts`. Client → server: `auth`, `subscribe`, `unsubscribe`,
`publish`, `ping`, `pong`, `get_hints`, `get_maintenance`.
Server → client: `auth_ok`, `auth_error`, `subscribed`, `unsubscribed`,
`event`, `presence`, `ping`, `pong`, `error`, `rate_limit_exceeded`,
`maintenance_status`, `server_hints`.

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
