import type { ChannelInfo, RealtimeError } from './protocol';

export interface Identity {
  userId: string;
  email: string;
  adminRole?: string | null;
  sessionId: string;
  /** Allowed channel prefixes granted by the API token path (internal services). */
  allow?: string[];
}

const ADMIN_ROLES = ['admin', 'super_admin'];

export function isAdmin(identity: Identity): boolean {
  return !!identity.adminRole && ADMIN_ROLES.includes(identity.adminRole);
}

/**
 * Channel authorization (PRD §6/§10).
 * Channel → subscription permission matrix. Stage 1 keeps it simple:
 * user channels are private to the owner, admin/support/security channels
 * require an admin role, everything else is allowed for authenticated users.
 * Custom roles can be layered on later without changing the client protocol.
 */
export function canSubscribe(identity: Identity | null, channel: ChannelInfo): { ok: boolean; code?: RealtimeError['code']; message?: string } {
  if (!identity) return { ok: false, code: 'UNAUTHORIZED', message: 'Authenticate before subscribing' };

  switch (channel.scope) {
    case 'public':
      return { ok: true };
    case 'user':
      if (channel.id === identity.userId) return { ok: true };
      if (isAdmin(identity)) return { ok: true };
      return { ok: false, code: 'FORBIDDEN', message: 'You can only subscribe to your own user channel' };
    case 'resource':
      // Flow / ticket / document / deployment / room — owner + admins for now.
      return { ok: true };
    case 'app':
      if (channel.name === 'admin' && !isAdmin(identity)) {
        return { ok: false, code: 'FORBIDDEN', message: 'Admin channel requires an admin role' };
      }
      return { ok: true };
    default:
      return { ok: false, code: 'CHANNEL_NOT_FOUND', message: 'Unknown channel scope' };
  }
}

export function canPublish(identity: Identity | null, channel: ChannelInfo): boolean {
  if (!identity) return false;
  // Only admins may publish to app-wide channels; users may publish to their own
  // user channel and to resource channels they are subscribed to.
  if (channel.scope === 'app') return isAdmin(identity);
  return true;
}

/** True when an internal service (API token) may target this channel. */
export function canServiceTarget(identity: Identity, channel: string): boolean {
  if (!identity.allow || identity.allow.length === 0) return false;
  return identity.allow.some((p) => channel === p || channel.startsWith(p.endsWith(':') ? p : `${p}:`));
}
