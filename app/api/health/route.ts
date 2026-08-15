import { NextResponse } from 'next/server';
import { getDb, hasDb } from '@/lib/db';
import { getRedis } from '@/lib/redis';
import { getChannelConnsCount } from '@/lib/hub';
import { RT_PROTOCOL_VERSION } from '@/lib/protocol';
import { metrics } from '@/lib/metrics';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

export async function GET() {
  const checks: Record<string, string> = {};

  if (hasDb()) {
    try {
      await getDb().query('SELECT 1');
      checks.database = 'ok';
    } catch {
      checks.database = 'error';
    }
  } else {
    checks.database = 'not_configured';
  }

  const redis = getRedis();
  if (redis) {
    try {
      await redis.ping();
      checks.redis = 'ok';
    } catch {
      checks.redis = 'error';
    }
  } else {
    checks.redis = 'not_configured';
  }

  const healthy = Object.values(checks).every((v) => v === 'ok' || v === 'not_configured');

  return NextResponse.json(
    {
      status: healthy ? 'ok' : 'degraded',
      protocol: RT_PROTOCOL_VERSION,
      connections: metrics.snapshot().currentConnections,
      channels: getChannelConnsCount(),
      checks,
      timestamp: new Date().toISOString(),
    },
    { status: healthy ? 200 : 503 },
  );
}
