import { Pool, type PoolConfig } from 'pg';

declare global {
  // eslint-disable-next-line no-var
  var __rtPgPool: Pool | undefined;
}

const poolConfig: PoolConfig = {
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
  max: 3,
  idleTimeoutMillis: 60_000,
  connectionTimeoutMillis: 5_000,
};

export function getDb(): Pool {
  if (!process.env.DATABASE_URL) {
    // Auth falls back to JWT-signature-only when no DB is configured.
    return null as unknown as Pool;
  }
  if (!globalThis.__rtPgPool) {
    globalThis.__rtPgPool = new Pool(poolConfig);
  }
  return globalThis.__rtPgPool;
}

/** No-op stub used when DATABASE_URL is missing. */
export function hasDb(): boolean {
  return !!process.env.DATABASE_URL;
}

export async function closeDb(): Promise<void> {
  if (globalThis.__rtPgPool) {
    await globalThis.__rtPgPool.end().catch(() => undefined);
    globalThis.__rtPgPool = undefined;
  }
}
