import { jwtVerify } from 'jose';
import { getDb, hasDb } from './db';

export interface VerifiedToken {
  sub: string;
  sid: string;
}

export interface ResolvedIdentity {
  userId: string;
  email: string;
  sessionId: string;
  adminRole?: string | null;
  /** Internal service accounts carry channel allow-prefixes (API token path). */
  allow?: string[];
}

/** HS256 secret shared with api.tirbeo.app. */
function getSecret(): Uint8Array {
  const secret = process.env.JWT_SECRET;
  if (!secret) throw new Error('JWT_SECRET is required');
  return new TextEncoder().encode(secret);
}

/** Verify a session access token (signature + expiry). */
export async function verifyAccessToken(token: string): Promise<VerifiedToken | null> {
  try {
    const { payload } = await jwtVerify(token, getSecret(), { algorithms: ['HS256'] });
    if (!payload.sub) return null;
    const sid = (payload.sid as string) || '';
    // Internal/CLI tokens minted by the api carry a purpose instead of a session.
    if (!sid) return payload.purpose ? { sub: payload.sub as string, sid: `cli:${payload.purpose}` } : null;
    return { sub: payload.sub as string, sid };
  } catch {
    return null;
  }
}

/** Resolve a token into a full identity, checking session + user status. */
export async function resolveIdentity(token: string): Promise<ResolvedIdentity | null> {
  const parsed = await verifyAccessToken(token);
  if (!parsed) return null;

  if (parsed.sid.startsWith('cli:')) {
    return {
      userId: parsed.sub,
      email: '',
      sessionId: parsed.sid,
      allow: ['user:', 'org:', 'workspace:', 'flow:', 'ticket:', 'document:', 'deployment:', 'call:', 'chat:'],
    };
  }

  // Session status cache (60s) to keep the hot path off the database.
  const cached = await readSessionCache(parsed.sid);
  if (cached) {
    if (cached.ok === false || !cached.identity) return null;
    return cached.identity;
  }

  if (!hasDb()) {
    // No DB configured → signature-only auth (degraded mode).
    return { userId: parsed.sub, email: '', sessionId: parsed.sid };
  }

  let row;
  try {
    const result = await getDb().query(
      `SELECT s.status AS session_status, s.revoked_at, s.expires_at,
              u.email, u.admin_role, u.is_banned, u.is_suspended
         FROM sessions s
         JOIN users u ON u.id = s.user_id
        WHERE s.id = $1
        LIMIT 1`,
      [parsed.sid],
    );
    row = result.rows[0];
  } catch {
    // DB hiccup → signature-only auth rather than dropping everyone.
    return { userId: parsed.sub, email: '', sessionId: parsed.sid };
  }

  if (!row) {
    await writeSessionCache(parsed.sid, { ok: false });
    return null;
  }

  const expired = row.expires_at && new Date(row.expires_at).getTime() < Date.now();
  const revoked = row.revoked_at != null;
  if (row.session_status !== 'active' || expired || revoked) {
    await writeSessionCache(parsed.sid, { ok: false });
    return null;
  }
  if (row.is_banned || row.is_suspended) {
    await writeSessionCache(parsed.sid, { ok: false });
    return null;
  }

  const identity: ResolvedIdentity = {
    userId: parsed.sub,
    email: row.email || '',
    sessionId: parsed.sid,
    adminRole: row.admin_role || null,
  };
  await writeSessionCache(parsed.sid, { ok: true, identity });
  return identity;
}

const SESSION_CACHE_TTL = 60;

async function readSessionCache(
  sid: string,
): Promise<{ ok: boolean; identity?: ResolvedIdentity } | null> {
  try {
    const { getRedis } = await import('./redis');
    const redis = getRedis();
    if (!redis) return null;
    const raw = await redis.get(`rt:session:${sid}`);
    if (!raw) return null;
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function writeSessionCache(sid: string, value: { ok: boolean; identity?: ResolvedIdentity }): Promise<void> {
  try {
    const { getRedis } = await import('./redis');
    const redis = getRedis();
    if (!redis) return;
    await redis.set(`rt:session:${sid}`, JSON.stringify(value), 'EX', SESSION_CACHE_TTL);
  } catch {
    /* non-fatal */
  }
}
