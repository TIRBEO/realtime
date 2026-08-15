import { NextResponse } from 'next/server';
import { metrics } from '@/lib/metrics';
import { RT_PROTOCOL_VERSION } from '@/lib/protocol';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

function unauthorized() {
  return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
}

export async function GET(req: Request) {
  const apiToken = process.env.API_TOKEN;
  if (apiToken) {
    const header = req.headers.get('authorization') || '';
    const key = header.replace(/^Bearer\s+/i, '').trim();
    if (key !== apiToken && req.headers.get('x-api-token') !== apiToken) return unauthorized();
  }
  return NextResponse.json({ protocol: RT_PROTOCOL_VERSION, ...metrics.snapshot() });
}
