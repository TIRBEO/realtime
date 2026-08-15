import { jwtVerify } from 'jose';
import type { Env } from './env';
import type { Identity } from './permissions';

export interface VerifiedToken {
  sub: string;
  sid: string;
  adminRole?: string | null;
}

/** HS256 secret shared with api.tirbeo.app. */
function getSecret(env: Env): Uint8Array {
  const secret = env.JWT_SECRET;
  if (!secret) throw new Error('JWT_SECRET is required');
  return new TextEncoder().encode(secret);
}

/** Verify a session access token (signature + expiry). */
export async function verifyAccessToken(token: string, env: Env): Promise<VerifiedToken | null> {
  try {
    const { payload } = await jwtVerify(token, getSecret(env), { algorithms: ['HS256'] });
    if (!payload.sub) return null;
    const sid = (payload.sid as string) || '';
    const adminRole = (payload.adminRole as string) || null;
    // Internal/CLI tokens minted by the api carry a purpose instead of a session.
    if (!sid) return payload.purpose ? { sub: payload.sub as string, sid: `cli:${payload.purpose}`, adminRole } : null;
    return { sub: payload.sub as string, sid, adminRole };
  } catch {
    return null;
  }
}

const SESSION_CACHE_TTL_MS = 60_000;

export interface SessionCacheEntry {
  t: number;
  value: { ok: boolean; identity?: Identity };
}

/**
 * Resolve a token into a full identity.
 *
 * When Supabase REST credentials are configured the session + user status are
 * checked (best-effort: on network failure it degrades to signature-only).
 * Otherwise the `adminRole` JWT claim is used — session revocation is skipped.
 */
export async function resolveIdentity(
  token: string,
  env: Env,
  cache: Map<string, SessionCacheEntry>,
): Promise<Identity | null> {
  const parsed = await verifyAccessToken(token, env);
  if (!parsed) return null;

  if (parsed.sid.startsWith('cli:')) {
    return {
      userId: parsed.sub,
      email: '',
      sessionId: parsed.sid,
      adminRole: parsed.adminRole,
      allow: ['user:', 'org:', 'workspace:', 'flow:', 'ticket:', 'document:', 'deployment:', 'call:', 'chat:'],
    };
  }

  const cached = cache.get(parsed.sid);
  if (cached && Date.now() - cached.t < SESSION_CACHE_TTL_MS) {
    return cached.value.ok ? cached.value.identity ?? null : null;
  }

  const dbUrl = env.SUPABASE_URL?.replace(/\/$/, '');
  const dbKey = env.SUPABASE_SERVICE_ROLE_KEY;
  const hasDb = !!dbUrl && !!dbKey;

  if (!hasDb) {
    // Signature-only auth (degraded mode), role resolved from the JWT claim.
    const identity: Identity = {
      userId: parsed.sub,
      email: '',
      sessionId: parsed.sid,
      adminRole: parsed.adminRole ?? null,
    };
    cache.set(parsed.sid, { t: Date.now(), value: { ok: true, identity } });
    return identity;
  }

  // Supabase PostgREST session + user check (best-effort).
  try {
    const headers = {
      apikey: dbKey,
      Authorization: `Bearer ${dbKey}`,
      Accept: 'application/json',
    };
    const [sessRes, userRes] = await Promise.all([
      fetch(
        `${dbUrl}/rest/v1/sessions?id=eq.${encodeURIComponent(parsed.sid)}&select=user_id,status,revoked_at,expires_at&limit=1`,
        { headers },
      ),
      fetch(
        `${dbUrl}/rest/v1/users?id=eq.${encodeURIComponent(parsed.sub)}&select=email,admin_role,is_banned,is_suspended&limit=1`,
        { headers },
      ),
    ]);
    if (sessRes.ok && userRes.ok) {
      interface SessionRow {
        user_id: string;
        status: string;
        revoked_at: string | null;
        expires_at: string | null;
      }
      interface UserRow {
        email: string;
        admin_role: string | null;
        is_banned: boolean;
        is_suspended: boolean;
      }
      const s = ((await sessRes.json()) as SessionRow[])[0];
      const u = ((await userRes.json()) as UserRow[])[0];
      if (!s || !u) {
        cache.set(parsed.sid, { t: Date.now(), value: { ok: false } });
        return null;
      }
      const expired = s.expires_at && new Date(s.expires_at).getTime() < Date.now();
      const revoked = s.revoked_at != null;
      if (s.status !== 'active' || expired || revoked || u.is_banned || u.is_suspended) {
        cache.set(parsed.sid, { t: Date.now(), value: { ok: false } });
        return null;
      }
      const identity: Identity = {
        userId: parsed.sub,
        email: u.email || '',
        sessionId: parsed.sid,
        adminRole: u.admin_role || parsed.adminRole || null,
      };
      cache.set(parsed.sid, { t: Date.now(), value: { ok: true, identity } });
      return identity;
    }
    // Non-OK response → fall through to signature-only (don't drop everyone).
  } catch {
    // DB hiccup → signature-only auth rather than dropping everyone.
  }

  const identity: Identity = {
    userId: parsed.sub,
    email: '',
    sessionId: parsed.sid,
    adminRole: parsed.adminRole ?? null,
  };
  cache.set(parsed.sid, { t: Date.now(), value: { ok: true, identity } });
  return identity;
}
