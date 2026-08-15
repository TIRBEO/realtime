/** Lightweight in-memory metrics for the realtime platform (PRD §52). */

interface Counter { value: number }

function counter(init = 0): Counter {
  return { value: init };
}

export interface MetricsSnapshot {
  startedAt: number;
  currentConnections: number;
  totalConnections: Counter['value'];
  connectionsByApp: Record<string, number>;
  subscriptions: number;
  channels: number;
  eventsDelivered: number;
  eventsPublished: number;
  authFailures: number;
  authSuccesses: number;
  pingsReceived: number;
  reconnectCount: number;
  errors: Record<string, number>;
  lastEventAt?: number;
}

const startedAt = Date.now();
const counters = {
  totalConnections: counter(0),
  eventsDelivered: counter(0),
  eventsPublished: counter(0),
  authFailures: counter(0),
  authSuccesses: counter(0),
  pingsReceived: counter(0),
  reconnectCount: counter(0),
};
const connectionsByApp: Record<string, number> = {};
const errors: Record<string, number> = {};
let currentConnections = 0;
let lastEventAt: number | undefined;

export const metrics = {
  connectionOpened(app?: string) {
    currentConnections += 1;
    counters.totalConnections.value += 1;
    if (app) connectionsByApp[app] = (connectionsByApp[app] ?? 0) + 1;
  },
  connectionClosed(app?: string) {
    currentConnections = Math.max(0, currentConnections - 1);
    if (app && connectionsByApp[app]) {
      connectionsByApp[app] -= 1;
      if (connectionsByApp[app] <= 0) delete connectionsByApp[app];
    }
  },
  eventDelivered() {
    counters.eventsDelivered.value += 1;
    lastEventAt = Date.now();
  },
  eventPublished() { counters.eventsPublished.value += 1; },
  authSuccess() { counters.authSuccesses.value += 1; },
  authFailure() { counters.authFailures.value += 1; },
  ping() { counters.pingsReceived.value += 1; },
  reconnect() { counters.reconnectCount.value += 1; },
  error(code: string) { errors[code] = (errors[code] ?? 0) + 1; },
  snapshot(): MetricsSnapshot {
    return {
      startedAt,
      currentConnections,
      totalConnections: counters.totalConnections.value,
      connectionsByApp: { ...connectionsByApp },
      subscriptions: hubState.subscriptions,
      channels: hubState.channels,
      eventsDelivered: counters.eventsDelivered.value,
      eventsPublished: counters.eventsPublished.value,
      authFailures: counters.authFailures.value,
      authSuccesses: counters.authSuccesses.value,
      pingsReceived: counters.pingsReceived.value,
      reconnectCount: counters.reconnectCount.value,
      errors: { ...errors },
      lastEventAt,
    };
  },
};

/** Written by hub.ts when its in-memory registry changes. */
export const hubState = {
  subscriptions: 0,
  channels: 0,
};
