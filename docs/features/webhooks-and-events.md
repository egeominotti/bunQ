# Webhooks, Events & Job Logs

> **Category:** Observability · **Source:** `src/application/webhookManager.ts`, `src/domain/types/webhook.ts`, `src/application/eventsManager.ts`, `src/application/jobLogsManager.ts`, `src/application/clientTracking.ts`, `src/infrastructure/server/tcp/connections.ts`, `src/client/events.ts`

## Purpose

This module provides the server's outbound and inbound observability surfaces for job activity. `WebhookManager` delivers HTTP callbacks (with optional HMAC signing and SSRF protection) when job lifecycle events occur; `EventsManager` is the in-process pub/sub hub that fans events out to local subscribers (SSE, dashboard, `WaitJob`) and feeds the webhook layer; `jobLogsManager` stores bounded per-job log lines; and `clientTracking` owns the client→job ownership map that releases in-flight jobs back to their queues when a TCP/SSE connection drops. Together they answer "what happened to my jobs" without coupling the core queue engine to any transport.

## Responsibilities & Scope

Owns:

- Webhook registry (in-memory `Map<WebhookId, Webhook>`), URL validation, delivery with retries, HMAC-SHA256 signing, and per-webhook success/failure counters (`webhookManager.ts:32`).
- The canonical webhook event vocabulary and event→webhook mapping (`webhook.ts:16`, `eventsManager.ts:185`).
- In-process event broadcast to subscribers and event-driven completion waiters used by `WaitJob` (`eventsManager.ts:22`).
- Bounded per-job log buffers (`jobLogsManager.ts:19`).
- Client-job ownership tracking and disconnect-time release/requeue (`clientTracking.ts:14`).

Does NOT own:

- Event _emission_ — the queue engine calls `eventsManager.broadcast(...)` and operations call `webhookManager.trigger(...)`; this module never decides when a job changes state. See [Job Lifecycle](./job-lifecycle.md) and [Core Queue Engine](./core-queue-engine.md).
- Transport/fan-out plumbing — SSE/WebSocket streaming, the `/webhooks/*` HTTP routes, and the `Stats`/`Metrics` payloads live in [HTTP / REST / SSE / WebSocket API](./http-api.md) and [Stats, Metrics & Monitoring](./stats-and-monitoring.md).

Every `EventsManager.broadcast` is also consumed by the manager-owned
`QueueTelemetryJournal`. That subscriber persists a bounded per-queue
lifecycle journal for `trimEvents`, even when no user listener or webhook is
registered. Terminal completed/failed events update a separate minute-metrics
store; retry-attempt failures stay in the journal but do not increment failed
metrics.

- Persistence — webhooks and job logs are in-memory only in the memory/SQLite
  engine. PostgreSQL mode persists job logs and lifecycle events, but webhook
  definitions/delivery remain process-local. See [Persistence](./persistence.md)
  and [PostgreSQL 15–18 Multi-Broker Persistence](./postgres-multibroker.md).
- Stall recovery — `clientTracking` only _triggers_ recovery (resets heartbeats); the stall detector in [Background Tasks](./background-tasks.md) reclaims orphaned jobs.

## Dependencies

Internal:

- `createWebhook` / `Webhook` / `WebhookPayload` / `WEBHOOK_EVENTS` from `src/domain/types/webhook.ts`.
- `validateWebhookUrl` from `src/shared/webhookValidation.ts` (SSRF guard).
- `EventType` / `JobEvent` from `src/domain/types/queue.ts`; `JobLogEntry` / `createLogEntry` from `src/domain/types/worker.ts`.
- `MapLike` (LRU) from `src/shared/lru` for the job-log store; `withWriteLock` + `shardIndex` for `clientTracking`. See [Concurrency & Locking](./concurrency-and-locking.md) and [Data Structures](./data-structures.md).
- `webhookLog` logger.

External / runtime (Bun):

- `Bun.CryptoHasher('sha256', secret)` for HMAC signing (`webhookManager.ts:23`).
- Global `fetch` + `AbortSignal.timeout(10000)` for delivery; `Bun.sleep` for retry backoff (`webhookManager.ts:144`).

## Public Interface

### Exported classes / functions / types

```ts
// webhookManager.ts
class WebhookManager {
  constructor(options?: { validateUrls?: boolean });           // default: validate ON
  setDashboardEmit(cb: (event: string, data: Record<string, unknown>) => void): void;
  add(url: string, events: string[], queue?: string, secret?: string): Webhook; // throws on bad URL
  remove(id: WebhookId): boolean;
  get(id: WebhookId): Webhook | undefined;
  setEnabled(id: WebhookId, enabled: boolean): boolean;
  list(): Webhook[];
  trigger(event: WebhookEvent, jobId: string, queue: string,
          extra?: { data?: unknown; error?: string; progress?: number }): Promise<void>;
  hasEnabledWebhooks(): boolean;                                // O(1) via running counter
  getStats(): { total: number; enabled: number };              // O(1)
}

// eventsManager.ts
type EventSubscriber = (event: JobEvent) => void;
class EventsManager {
  constructor(webhookManager: WebhookManager);
  get subscriberCount(): number;
  get completionWaiterCount(): number;
  subscribe(callback: EventSubscriber): () => void;            // returns unsubscribe fn
  clear(): void;                                               // shutdown: resolves all waiters
  waitForJobCompletion(jobId: JobId, timeoutMs: number): Promise<boolean>; // true=done, false=timeout
  needsBroadcast(): boolean;                                   // batch fast-path check
  broadcast(event: Partial<JobEvent> & { eventType; queue; jobId; timestamp; error? }): void;
}

// jobLogsManager.ts
function addJobLog(jobId, message, ctx: JobLogsContext, level?: 'info'|'warn'|'error'): boolean;
function getJobLogs(jobId, ctx): JobLogEntry[];
function clearJobLogs(jobId, ctx, keepLogs?: number): void;

// clientTracking.ts
function registerClientJob(clientId, jobId, ctx): void;        // on PULL
function unregisterClientJob(clientId | undefined, jobId, ctx): void; // on ACK/FAIL
function releaseClientJobs(clientId, ctx): Promise<number>;    // on disconnect (locked)
function forceReleaseClientJobs(clientId, ctx): number;        // lock-free fallback
```

### TCP commands handled

| Command             | Fields                                         | Purpose                                                      |
| ------------------- | ---------------------------------------------- | ------------------------------------------------------------ |
| `AddWebhook`        | `url`, `events: string[]`, `queue?`, `secret?` | Register webhook (validates URL + events)                    |
| `RemoveWebhook`     | `webhookId`                                    | Delete webhook                                               |
| `ListWebhooks`      | —                                              | List webhooks + `getStats()`                                 |
| `SetWebhookEnabled` | `id`, `enabled`                                | Toggle delivery                                              |
| `AddLog`            | `id`, `message`, `level?`                      | Append a job log line                                        |
| `GetLogs`           | `id`, `start?`, `end?`                         | Read logs (inclusive slice)                                  |
| `ClearLogs`         | `id`, `keepLogs?`                              | Clear / trim logs                                            |
| `SubscribeEvents`   | `queue`                                        | Select one queue's live `JobEvent` stream on this connection |
| `UnsubscribeEvents` | —                                              | Stop that stream without closing the connection              |

Command shapes: `src/domain/types/commands/monitoring.ts:3-28` (logs and webhooks) and `src/domain/types/commands/extended.ts:3-7` (`ClearLogs`). Handlers: `src/infrastructure/server/handlers/monitoring/health.ts:24-50` (`AddLog`/`GetLogs`), `src/infrastructure/server/handlers/monitoring/webhooks.ts:13-100`, and `src/infrastructure/server/handlers/monitoring/operations.ts:21-27` (`ClearLogs`). Routing: `src/infrastructure/server/handler-routes/monitoring.ts:45-87`.

### HTTP endpoints (thin wrappers over the TCP commands)

- `GET /webhooks` · `POST /webhooks` · `DELETE /webhooks/:id` · `PUT /webhooks/:id/enabled` (`httpRouteResources.ts:85`).
- `GET /jobs/:id/logs` · `POST /jobs/:id/logs` · `DELETE /jobs/:id/logs` (`http-routes/jobAdvanced.ts:54-75`).

### Events

- **Webhook events** (`WEBHOOK_EVENTS`, `webhook.ts:16`): `job.pushed`, `job.started`, `job.completed`, `job.failed`, `job.progress`. `job.stalled` is a legacy member of the `WebhookEvent` type kept only for backward compatibility with stored webhooks — it is never emitted and is rejected on new webhooks (`webhook.ts:24`).
- **Internal `EventType`** (`src/domain/types/queue.ts:129-145`): `pushed`, `pulled`, `completed`, `failed`, `progress`, `stalled`, `removed`, `delayed`, `duplicated`, `retried`, `waiting-children`, `drained`, `paused`, `resumed`.
- **TCP event envelope**: `{ type: 'event', event: JobEvent }`, sent only to
  authenticated connections subscribed to `event.queue`.
- **Dashboard events** emitted via `setDashboardEmit`: `webhook:fired`, `webhook:failed`, `webhook:enabled`, `webhook:disabled` (from `WebhookManager`); `webhook:added` / `webhook:removed` are emitted by the handler through `queueManager.emitDashboardEvent` (`handlers/monitoring/webhooks.ts:31-64`).

## Data Models

See [data-model](../data-model.md) for full definitions. Most relevant here:

```ts
interface Webhook {
  // webhook.ts:31
  id: WebhookId;
  url: string;
  events: WebhookEvent[];
  queue: string | null; // null = all queues
  secret: string | null; // null = no HMAC signature
  createdAt: number;
  lastTriggered: number | null;
  successCount: number;
  failureCount: number;
  enabled: boolean;
}

interface WebhookPayload {
  // webhook.ts:66 — the JSON POST body
  event: WebhookEvent;
  timestamp: number;
  jobId: string;
  queue: string;
  data?: unknown;
  error?: string;
  progress?: number;
}

interface JobEvent {
  // src/domain/types/queue.ts:148-162 — internal broadcast shape
  eventType: EventType;
  queue: string;
  jobId: string;
  timestamp: number;
  data?: unknown;
  error?: string;
  progress?: number;
  prev?: string;
  delay?: number;
}

interface JobLogEntry {
  timestamp: number;
  level: 'info' | 'warn' | 'error';
  message: string;
} // src/domain/types/worker.ts:63-67
```

> Note: `src/domain/types/queue.ts:164-170` also declares a second, unused `Webhook` interface (with `EventType[]` events). The authoritative type used by `WebhookManager` is the one in `src/domain/types/webhook.ts`.

## Business Logic / Control Flow

### Event broadcast → webhook delivery

1. The queue engine builds a `JobEvent` and calls `eventsManager.broadcast(...)` (for example from `queue-manager/control.ts` and the lifecycle operations under `application/operations/`).
2. `broadcast` computes `hasSubscribers`, `hasWebhooks` (`webhookManager.hasEnabledWebhooks()`), and `hasWaiters` (only for `Completed`). **Fast path:** if all three are false it returns immediately, doing zero work (`eventsManager.ts:133`).
3. Subscribers are invoked in a try/catch — a throwing subscriber is swallowed so one bad listener can't break the fan-out (`eventsManager.ts:139`).
4. For `Completed`, all completion waiters for that `jobId` are resolved and the map entry deleted (`eventsManager.ts:149`).
5. If webhooks are enabled, `mapEventToWebhook` translates the `EventType` (`pushed→job.pushed`, `pulled→job.started`, `completed→job.completed`, `failed→job.failed`; everything else → `null`/no webhook) and calls `webhookManager.trigger(...)` fire-and-forget (`eventsManager.ts:164`).

The TCP registry is one of those subscribers while at least one remote queue
subscription exists. It filters by the socket's selected queue, frames the
event once for all matching sockets, and writes through each socket's bounded
`SocketWriteQueue`. `QueueEvents` maps that internal shape to its typed public
payloads; TCP Workers reuse the same dedicated subscription for `stalled`.

`job.progress` is broadcast to internal subscribers, including the telemetry journal and live queue-event transports, by `updateJobProgress`. Because `mapEventToWebhook` intentionally has no progress mapping, the same operation separately calls `webhookManager.trigger('job.progress', ...)` (`src/application/operations/jobManagement.ts:170-188`).

### Webhook delivery

1. `trigger` builds the `WebhookPayload`, filters webhooks by `enabled && events.includes(event) && (queue === null || queue === eventQueue)`, then fires each `sendWebhook` fire-and-forget (`webhookManager.ts:115`).
2. `sendWebhook` POSTs JSON with headers `Content-Type: application/json`, `X-Webhook-Event`, `X-Webhook-Timestamp`, and — if a secret is set — `X-Webhook-Signature` = hex HMAC-SHA256 of the body (`webhookManager.ts:128`).
3. Up to `maxRetries` attempts. A 2xx response sets `lastTriggered`, increments `successCount`, emits `webhook:fired`, and returns. Non-2xx or thrown errors record `lastError`; between attempts it sleeps `retryDelay * (attempt + 1)` (linear backoff). After exhausting retries it increments `failureCount`, emits `webhook:failed`, and throws (the throw is caught by the fire-and-forget caller) (`webhookManager.ts:142`).

### Job logs

`addJobLog` returns `false` if the job isn't in `jobIndex` (so logs can't be added to unknown/evicted jobs). Otherwise it appends `createLogEntry(message, level)` and trims the per-job array to the most recent `maxLogsPerJob` entries via `splice` (`jobLogsManager.ts:19-37`). `GetLogs` returns the full array unless `start`/`end` are supplied, in which case it slices `[start .. end]` inclusive and still reports the untrimmed `count` (`handlers/monitoring/health.ts:39-50`). `clearJobLogs` deletes the entry entirely when `keepLogs` is unset/≤0, else keeps the most recent N (`jobLogsManager.ts:46-56`).

### Client tracking & disconnect release

- `registerClientJob` (on PULL) and `unregisterClientJob` (on ACK/FAIL) maintain `clientJobs: Map<clientId, Set<jobId>>`, deleting the set when empty (`clientTracking.ts:14`).
- On disconnect, the TCP server calls `releaseClientJobsWithRetry` (3 attempts, exponential backoff 100/200/400 ms) → `releaseClientJobs`; on persistent lock failure it falls back to `forceReleaseClientJobs` (`src/infrastructure/server/tcp/connections.ts:87-111`, retry loop in `src/infrastructure/server/tcp/clientRelease.ts:4-22`). SSE disconnect calls `releaseClientJobs` directly (`sseHandler.ts:255-260`).
- `releaseClientJobs` runs in three phases: (1) collect lock-free, skipping non-`processing` jobs and jobs whose lock has `renewalCount > 0`; (2) group by processing shard then queue shard; (3) acquire **shardLock → processingLock** and call `releaseJobToQueue` (`clientTracking.ts:47`).
- `releaseJobToQueue` removes the job from the processing shard, deletes its lock, releases concurrency/uniqueKey/groupId resources, then either **discards** cron `preventOverlap` jobs (uniqueKey `cron:*` → deleted, not requeued, fixing the #73 "starts right away on reconnect" bug, `clientTracking.ts:222`) or re-queues it with `startedAt=null` and re-indexed as `{ type: 'queue' }`.

## Concurrency & Locking

- `releaseClientJobs` follows the project lock hierarchy: **shardLocks before processingLocks** (`clientTracking.ts:122`). Reads (phase 1) are lock-free; mutations happen under both locks.
- The `renewalCount > 0` guard prevents a **double-execute** race: with a pooled client, heartbeats travel on a different connection than the one that pulled, so the pulling socket closing does not mean the worker died — such jobs are left for lock-expiry/stall detection to reclaim (`clientTracking.ts:66`).
- `forceReleaseClientJobs` is intentionally lock-free: it always drops `jobLocks[jobId]` (no stale token survives the disconnect) and, for still-`processing` jobs, sets `lastHeartbeat = 0` and `startedAt = 0` so the stall detector's grace gate passes on its next eligible tick. It accepts that a concurrent stall/lock-expiry path may mutate the same job — worst case the write lands on an object no longer in the map (wasted, never corrupting) (`clientTracking.ts:167`).
- `releaseClientJobs` clears `clientJobs` in a `finally` block even on mid-flight lock failure, preventing an unbounded `clientJobs` leak across disconnects that hit lock timeouts (`clientTracking.ts:136`).

The PostgreSQL adapter keeps the same connection-facing contract with durable
lease ownership. Disconnect release row-locks the exact generation and only
requeues a still-live, never-renewed token. A heartbeat received by another
broker increments `lease_renewals` and transfers `lease_broker_id`, so loss of
the original pull socket cannot double-deliver the job. Broker shutdown releases
its remaining untransferred leases; expired protected cron leases are discarded.

- `EventsManager`/`WebhookManager` hold no locks; `broadcast` is synchronous and webhook delivery is async fire-and-forget.

## Edge Cases & Failure Modes

- **SSRF protection:** `validateWebhookUrl` (on by default; disabled via `validateUrls: false`) rejects non-http(s) schemes, URLs > 2048 chars, localhost variants, private IPv4 (`10.*`, `172.16–31.*`, `192.168.*`), link-local `169.254.*`, `0.*`, `127.*`, **IPv4-mapped / IPv4-compatible IPv6** whose embedded IPv4 is loopback/private (`extractMappedIpv4` unwraps the dotted `::ffff:127.0.0.1`, the URL-parser-normalized hex form `[::ffff:7f00:1]`, **and** the deprecated `::`-prefixed compatible form `[::127.0.0.1]`/`[::7f00:1]` before the octet check), **IPv6 ULA `fc00::/7` and link-local `fe80::/10`** plus the unspecified `::` (`checkBlockedIpv6`), and cloud-metadata hosts (`169.254.169.254`, `metadata.google.internal`, `*.internal`) (`webhookValidation.ts:42`).
- **Dead-event rejection:** `AddWebhook` rejects events not in `WEBHOOK_EVENTS`, so a webhook can't be created against an event that would silently never fire (`handlers/monitoring/webhooks.ts:13-29`).
- **Delivery is best-effort / fire-and-forget:** failures are logged and counted but never block job processing; there is no persistent retry queue and webhooks are not persisted to SQLite, so they are lost on restart.
- **Fixed 10 s per-request timeout** via `AbortSignal.timeout(10000)`; linear (not exponential) inter-attempt backoff.
- **`hasEnabledWebhooks` / `getStats` are O(1)** thanks to the `enabledCount` running counter maintained in `add`/`remove`/`setEnabled` (`webhookManager.ts:39`).
- **Completion-waiter memory safety:** `waitForJobCompletion` registers a timer that, on timeout, marks the waiter `cancelled`, splices it out, and deletes empty arrays — preventing a leak when `WaitJob` times out without completion (`eventsManager.ts:78`). `clear()` resolves all outstanding non-cancelled waiters on shutdown.
- **Subscriber isolation:** exceptions thrown by subscribers in `broadcast` are caught and ignored (`eventsManager.ts:142`).
- **Job-log bounds:** per-job cap is `maxLogsPerJob = 100` (`queue-manager/state.ts`); the `jobLogs` LRU itself holds at most `maxJobLogs = 10_000` distinct jobs (`application/types/config.ts`), evicting whole-job entries. Adding logs to a job not in `jobIndex` returns `false`.
- **Cron `preventOverlap` invariant:** disconnect release must discard (not requeue) `cron:*` jobs, or they re-run immediately on reconnect (#73).

## Configuration

| Env var                  | Default | Effect                                                                                 |
| ------------------------ | ------- | -------------------------------------------------------------------------------------- |
| `WEBHOOK_MAX_RETRIES`    | `3`     | Max delivery attempts per webhook (`webhookManager.ts:17`)                             |
| `WEBHOOK_RETRY_DELAY_MS` | `1000`  | Base inter-attempt delay; actual wait = `delay * (attempt+1)` (`webhookManager.ts:20`) |

Options (not env): `WebhookManager({ validateUrls })` — defaults ON; wired from `config.validateWebhookUrls` in `queue-manager/state.ts`. Job-log bounds: `maxLogsPerJob = 100`, `maxJobLogs = 10_000` (config default). Webhook fetch timeout is a hardcoded 10 000 ms.

## Related Docs

- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md) — where events originate and where client jobs are registered/unregistered.
- [Stats, Metrics & Monitoring](./stats-and-monitoring.md) — consumes `EventsManager` subscribers for live counters.
- [HTTP / REST / SSE / WebSocket API](./http-api.md) — `/webhooks/*` and `/jobs/:id/logs` routes; SSE fan-out with 0-client early-return.
- [Background Tasks](./background-tasks.md) — stall detector that completes `clientTracking`'s force-release recovery.
- [Concurrency & Locking](./concurrency-and-locking.md) — the shard→processing lock order used by `releaseClientJobs`.
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md) — the resources released on disconnect.
- [PostgreSQL 15–18 Multi-Broker Persistence](./postgres-multibroker.md) — durable
  events/logs and cross-broker lease release/fencing.
- [bunqueue Cloud Dashboard Integration](./cloud-integration.md) — consumer of `setDashboardEmit` events.
- [architecture](../architecture.md) · [data-model](../data-model.md)
