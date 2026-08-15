/**
 * Tirbeo Realtime Protocol — Stage 0 + Stage 1.
 *
 * The client and server speak a single JSON envelope. Both the standalone
 * realtime platform (apps/realtime → wss://ws.tirbeo.app/ws) and the local
 * development server (api :3001/ws) implement the same protocol so that
 * applications never care which transport/instance they are connected to.
 */

/** Protocol version — bump on breaking wire changes. */
export const RT_PROTOCOL_VERSION = '1.0.0';

/** Standardized error codes (PRD §37). */
export type RealtimeErrorCode =
  | 'UNAUTHORIZED'
  | 'FORBIDDEN'
  | 'CHANNEL_NOT_FOUND'
  | 'RATE_LIMITED'
  | 'INVALID_EVENT'
  | 'INVALID_PAYLOAD'
  | 'SESSION_EXPIRED'
  | 'CONNECTION_LIMIT'
  | 'SERVER_ERROR'
  | 'SERVICE_UNAVAILABLE';

export interface RealtimeError {
  code: RealtimeErrorCode;
  message: string;
}

/** Client → server */
export type ClientMessage =
  | { type: 'auth'; token: string }
  | { type: 'subscribe'; channel: string; since?: number }
  | { type: 'unsubscribe'; channel: string }
  | { type: 'publish'; channel: string; event: Record<string, unknown>; id?: string }
  | { type: 'ping' }
  | { type: 'pong' }
  | { type: 'get_hints' }
  | { type: 'get_maintenance' };

/** Server → client */
export type ServerMessage =
  | { type: 'auth_ok'; userId: string; adminRole?: string | null; protocol: string }
  | { type: 'auth_error'; code: RealtimeErrorCode; message: string }
  | { type: 'subscribed'; channel: string }
  | { type: 'unsubscribed'; channel: string }
  | { type: 'event'; channel: string; event: RealtimeEvent }
  | { type: 'ping' }
  | { type: 'pong' }
  | { type: 'presence'; channel: string; presence: Record<string, PresenceEntry> }
  | { type: 'error'; code: RealtimeErrorCode; message: string }
  | { type: 'rate_limit_exceeded'; message: string; retryAfter: number }
  | { type: 'maintenance_status'; maintenance: MaintenanceStatus }
  | { type: 'server_hints'; hints: Record<string, unknown> };

export interface RealtimeEvent {
  /** Unique event id — clients de-duplicate on it (PRD §25). */
  id: string;
  /** Namespaced type, e.g. "flow.submission.created" (PRD §8/§39). */
  type: string;
  channel: string;
  actor?: { id: string; email?: string };
  app?: string;
  resource?: string;
  resourceId?: string;
  org?: string;
  workspace?: string;
  payload: Record<string, unknown>;
  /** Event schema version (PRD §8/§55). */
  version?: number;
  correlationId?: string;
  timestamp: string;
}

export interface MaintenanceStatus {
  enabled: boolean;
  message?: string;
  estimatedEnd?: string;
}

export interface PresenceEntry {
  userId: string;
  email?: string;
  status: 'online' | 'away' | 'busy';
  lastSeen: number;
  meta?: Record<string, unknown>;
}

/** Channel scopes (PRD §10). */
export type ChannelScope =
  | 'public'
  | 'app'
  | 'user'
  | 'org'
  | 'workspace'
  | 'resource'
  | 'room';

export interface ChannelInfo {
  scope: ChannelScope;
  name: string;
  /** e.g. "user", "org", "workspace", "flow", "ticket", "document", "deployment", "call", "chat" */
  kind?: string;
  id?: string;
}

export const CHANNEL_PATTERN = /^[a-z0-9_.:-]{1,160}$/;

/** Parse a channel string into its scope + parts. */
export function parseChannel(channel: string): ChannelInfo | null {
  if (typeof channel !== 'string' || channel.length === 0 || channel.length > 200) return null;
  if (channel.startsWith('public:')) return { scope: 'public', name: channel, kind: 'public', id: channel.slice(7) };
  if (channel.startsWith('user:')) return { scope: 'user', name: channel, kind: 'user', id: channel.slice(5) };
  if (channel.startsWith('org:')) return { scope: 'org', name: channel, kind: 'org', id: channel.slice(4) };
  if (channel.startsWith('workspace:')) return { scope: 'workspace', name: channel, kind: 'workspace', id: channel.slice(10) };
  for (const kind of ['flow', 'ticket', 'document', 'deployment', 'call', 'chat', 'room']) {
    if (channel.startsWith(`${kind}:`)) return { scope: 'resource', name: channel, kind, id: channel.slice(kind.length + 1) };
  }
  // Bare application channels: "admin", "support", "flows", "dashboard"...
  return { scope: 'app', name: channel, kind: channel, id: channel };
}
