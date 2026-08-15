import Redis from 'ioredis';

declare global {
  // eslint-disable-next-line no-var
  var __rtRedis: Redis | undefined;
  // eslint-disable-next-line no-var
  var __rtRedisSub: Redis | undefined;
}

export const RT_NAMESPACE = process.env.RT_NAMESPACE || 'tirbeo:rt';
export const EVENT_CHANNEL = `${RT_NAMESPACE}:events`;
export const PRESENCE_CHANNEL = `${RT_NAMESPACE}:presence`;
export const CONTROL_CHANNEL = `${RT_NAMESPACE}:control`;

function create(): Redis | null {
  if (!process.env.REDIS_URL) return null;
  const client = new Redis(process.env.REDIS_URL, {
    maxRetriesPerRequest: 3,
    connectTimeout: 5_000,
    lazyConnect: false,
    enableReadyCheck: true,
    retryStrategy: (times) => Math.min(times * 200, 3_000),
  });
  client.on('error', () => {
    /* surfaced via health checks */
  });
  return client;
}

export function getRedis(): Redis | null {
  if (!globalThis.__rtRedis) globalThis.__rtRedis = create() ?? undefined;
  return globalThis.__rtRedis ?? null;
}

/** A second client reserved for pub/sub subscription. */
export function getRedisSubscriber(): Redis | null {
  if (!globalThis.__rtRedisSub) globalThis.__rtRedisSub = create() ?? undefined;
  return globalThis.__rtRedisSub ?? null;
}

/** Publish an event envelope; fire-and-forget, never throws. */
export function publishEvent(payload: unknown): boolean {
  const client = getRedis();
  if (!client || client.status !== 'ready') return false;
  try {
    void client.publish(EVENT_CHANNEL, typeof payload === 'string' ? payload : JSON.stringify(payload));
    return true;
  } catch {
    return false;
  }
}
