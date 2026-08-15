import { NextResponse } from 'next/server';
import { randomUUID } from 'crypto';
import type { RealtimeEvent } from '@/lib/protocol';
import { serverPublish, serverPublishToUser, serverBroadcast } from '@/lib/hub';
import { metrics } from '@/lib/metrics';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

interface PublishBody {
  /** Target single user via `user:{id}` channel. */
  userId?: string;
  /** Target channel name (e.g. `admin`, `flows`, `flow:123`, `public:announcements`). */
  channel?: string;
  /** Broadcast to every subscribed channel matching the channel prefix. */
  broadcast?: boolean;
  event: Omit<RealtimeEvent, 'id' | 'channel' | 'timestamp'> & Partial<Pick<RealtimeEvent, 'channel' | 'timestamp'>>;
}

export async function POST(req: Request) {
  const apiToken = process.env.API_TOKEN;
  if (!apiToken) {
    return NextResponse.json({ error: 'API_TOKEN not configured' }, { status: 503 });
  }
  const header = req.headers.get('authorization') || '';
  const key = header.replace(/^Bearer\s+/i, '').trim();
  if (key !== apiToken && req.headers.get('x-api-token') !== apiToken) {
    return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
  }

  let body: PublishBody;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: 'Invalid JSON' }, { status: 400 });
  }

  if (!body?.event || typeof body.event.type !== 'string' || !body.event.type) {
    return NextResponse.json({ error: 'event.type is required' }, { status: 400 });
  }

  const base = body.event;
  const event: RealtimeEvent = {
    ...base,
    id: randomUUID(),
    channel: base.channel || body.channel || '',
    timestamp: base.timestamp || new Date().toISOString(),
  };

  let ok = false;
  if (body.userId) {
    ok = serverPublishToUser(body.userId, event);
  } else if (body.broadcast) {
    ok = serverBroadcast(event);
  } else if (body.channel) {
    event.channel = body.channel;
    ok = serverPublish(event);
  } else {
    return NextResponse.json({ error: 'Provide userId, channel, or broadcast' }, { status: 400 });
  }

  if (!ok) {
    return NextResponse.json({ error: 'Redis not available — event delivered locally only' }, { status: 200 });
  }

  return NextResponse.json({ ok: true, eventId: event.id });
}
