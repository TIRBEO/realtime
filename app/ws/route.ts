import { experimental_upgradeWebSocket } from '@vercel/functions';
import { handleUpgrade } from '@/lib/hub';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

/**
 * WebSocket upgrade endpoint — wss://ws.tirbeo.app/ws
 *
 * Requires Vercel Fluid Compute (Node runtime, long-lived connections).
 * Local development must run `vercel dev` (not `next dev`) for this path.
 */
export function GET(request: Request) {
  const ip = request.headers.get('x-forwarded-for')?.split(',')[0]?.trim() || 'unknown';
  return experimental_upgradeWebSocket(
    (ws) => {
      // Register + attach listeners synchronously (no await) so an `auth` frame
      // sent immediately on open is never dropped.
      handleUpgrade(ws, { ip });
    },
    { maxPayload: 64 * 1024 },
  );
}
