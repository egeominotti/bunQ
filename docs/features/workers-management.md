# Worker Registry & Management

> **Category:** Observability · **Source:** `src/application/workerManager.ts`, `src/domain/types/worker.ts`

## Purpose

`WorkerManager` is the server-side registry of connected consumers. It tracks
which workers exist, what queues they serve, their concurrency, liveness
(`lastSeen` heartbeat timestamp), and per-worker job counters (active /
processed / failed). It exists so the server can answer "is anyone consuming
this queue?" (used by `skipIfNoWorker` crons), expose worker fleet state to the
dashboard / HTTP / CLI, and reap connections that vanish without a clean
unregister.

It is purely an observability/coordination registry: it does **not** schedule,
lease, lock, or execute jobs, and it is **not** authoritative for job-level
stall detection.

## Responsibilities & Scope

Owns:

- An in-memory `Map<WorkerId, Worker>` of registered workers (`workerManager.ts:23`).
- Worker registration / re-registration / unregistration, including bulk
  removal by TCP `clientId` on disconnect (`unregisterByClientId`, `workerManager.ts:76`).
- Worker-level heartbeat (`lastSeen` refresh) and client-reported stats
  ingestion (`heartbeat`, `workerManager.ts:95`).
- Aggregate O(1) fleet counters: `totalProcessedCounter`,
  `totalFailedCounter`, `totalActiveJobsCounter` (`workerManager.ts:28-30`).
- Liveness classification via `WORKER_TIMEOUT_MS` and stale-worker reaping
  via a background cleanup interval (`cleanupStale`, `workerManager.ts:216`).
- Emitting dashboard events for worker lifecycle (`worker:disconnected`,
  `worker:idle`, `worker:error`, `worker:removed-stale`).

Does NOT own (delegated elsewhere):

- Job-level stall detection / lock renewal — `JobHeartbeat` / `JobHeartbeatB`
  go to `QueueManager.jobHeartbeat` / `renewJobLock`, **not** to `WorkerManager`
  (`handlers/monitoring.ts:81-112`). See [Background Tasks](./background-tasks.md)
  and [Concurrency & Locking](./concurrency-and-locking.md).
- Job leasing, concurrency enforcement, and pull/ack/fail — see
  [Job Lifecycle](./job-lifecycle.md) and
  [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md).
- The client-side `Worker` that sends these commands — see
  [Client SDK: Worker](./client-worker-sdk.md).
- Persistence: the worker registry is **in-memory only**; nothing is written to
  SQLite. See [Persistence](./persistence.md).

## Dependencies

Internal:

- `src/domain/types/worker.ts` — `Worker`, `WorkerId`, `CreateWorkerOptions`,
  `createWorker()`.
- `src/shared/hash` — `uuid()` for generating worker IDs when the client does
  not supply one (`worker.ts:46`).
- Consumed by `QueueManager` (`queueManager.ts:188`), which owns the singleton
  `workerManager` instance and wires the dashboard emitter and the
  `skipIfNoWorker` callback.

External / runtime:

- `Bun.env` for `WORKER_TIMEOUT_MS` and `WORKER_CLEANUP_INTERVAL_MS`
  (`workerManager.ts:14,17`).
- `setInterval` / `clearInterval` for the cleanup loop.
- No external packages, no SQLite, no disk.

## Public Interface

### Exported class — `WorkerManager` (`workerManager.ts:22`)

```typescript
constructor()                                              // starts cleanup interval
setDashboardEmit(callback: (event: string, data: Record<string, unknown>) => void): void
register(name: string, queues: string[], concurrency?: number /* =1 */, opts?: CreateWorkerOptions): Worker
unregister(id: WorkerId): boolean
unregisterByClientId(clientId: string): number             // returns count removed
get(id: WorkerId): Worker | undefined
heartbeat(id: WorkerId, stats?: { activeJobs?: number; processed?: number; failed?: number }): boolean
incrementActive(id: WorkerId, jobId?: string): void
jobCompleted(id: WorkerId): void
jobFailed(id: WorkerId): void
list(): Worker[]
listActive(): Worker[]                                     // lastSeen within WORKER_TIMEOUT_MS
getForQueue(queue: string): Worker[]                       // active workers serving `queue`
stop(): void                                               // clears cleanup interval
getStats(): { total: number; active: number; totalProcessed: number; totalFailed: number; activeJobs: number }
```

### Exported types / fn — `src/domain/types/worker.ts`

- `type WorkerId = string` (`worker.ts:8`)
- `interface Worker` (`worker.ts:11`)
- `interface CreateWorkerOptions` (`worker.ts:29`)
- `createWorker(name, queues?, concurrency=1, opts?): Worker` (`worker.ts:38`)
- `interface JobLogEntry` + `createLogEntry()` also live in this file
  (`worker.ts:63,70`) but belong to [Webhooks, Events & Job Logs](./webhooks-and-events.md),
  not the worker registry.

### TCP commands handled (via `QueueManager` / `handlers/monitoring.ts`)

- `RegisterWorker` → `handleRegisterWorker` (`handlers/monitoring.ts:134`); fields:
  `name`, `queues`, `concurrency?`, `workerId?`, `hostname?`, `pid?`, `startedAt?`
  (`command.ts:406`). `clientId` is injected server-side from the connection
  (`handlers/monitoring.ts:144`).
- `UnregisterWorker` → `handleUnregisterWorker` (`handlers/monitoring.ts:166`); field `workerId`.
- `ListWorkers` → `handleListWorkers` (`handlers/monitoring.ts:186`).
- `Heartbeat` (worker-level) → `handleHeartbeat` (`handlers/monitoring.ts:62`); fields:
  `id`, `activeJobs?`, `processed?`, `failed?` (`command.ts:378`). This is the
  only worker command that touches `WorkerManager`.
- `JobHeartbeat` / `JobHeartbeatB` → routed to `QueueManager` job-lock subsystem,
  **not** `WorkerManager` (`handlers/monitoring.ts:81,103`).

### HTTP endpoints (`httpRouteResources.ts`, `httpRouteQueues.ts`)

- `GET /workers` → `ListWorkers` (`httpRouteResources.ts:144`)
- `POST /workers` → `RegisterWorker` (`httpRouteResources.ts:150`)
- `DELETE /workers/:id` → `UnregisterWorker` (`httpRouteResources.ts:175`)
- `POST /workers/:id/heartbeat` → `Heartbeat` (`httpRouteResources.ts:187`)
- `GET /queues/:queue/workers` → `workerManager.getForQueue(queue)` (`httpRouteQueues.ts:191`)
- Worker fleet is also embedded in the dashboard overview endpoint
  (`httpEndpoints.ts:205`).

### CLI subcommands (`src/cli/commands/worker.ts`)

- `bunqueue worker list` → `ListWorkers`
- `bunqueue worker register <name> --queues|-q a,b,c` → `RegisterWorker`
- `bunqueue worker unregister <workerId>` → `UnregisterWorker`

### Dashboard events emitted

`worker:connected` and `worker:disconnected` are emitted by `QueueManager`
wrappers (`queueManager.ts:1335,1349`). `WorkerManager` itself emits:

- `worker:disconnected` — per worker removed in `unregisterByClientId` (`workerManager.ts:82`)
- `worker:idle` — when a worker's `activeJobs` reaches 0 (`workerManager.ts:148,166`)
- `worker:error` — at cumulative failure thresholds 5/10/25/50/100 (`workerManager.ts:178`)
- `worker:removed-stale` — when the cleanup loop reaps a dead worker (`workerManager.ts:225`)
- `worker:heartbeat` — emitted by `handleHeartbeat` on success (`handlers/monitoring.ts:73`)

## Data Models

`Worker` (`worker.ts:11`) — full definition in [data-model](../data-model.md):

```typescript
interface Worker {
  id: WorkerId;
  name: string;
  queues: string[];
  concurrency: number;
  hostname: string;        // 'unknown' if not provided
  pid: number;             // 0 if not provided
  registeredAt: number;    // opts.startedAt ?? Date.now()
  lastSeen: number;        // refreshed on heartbeat/increment/complete/fail
  activeJobs: number;
  processedJobs: number;
  failedJobs: number;
  currentJob: string | null;
  clientId: string | null; // TCP client ID, for disconnect cleanup
}
```

`CreateWorkerOptions` (`worker.ts:29`): `{ workerId?, hostname?, pid?, startedAt?, clientId? }`.

The `ListWorkers` / `RegisterWorker` responses add derived fields not stored on
`Worker`: `status: 'active' | 'stale'` (computed from `lastSeen`,
`handlers/monitoring.ts:182`) and `uptime: now - registeredAt`
(`handlers/monitoring.ts:209`).

## Business Logic / Control Flow

### Register / re-register (`register`, `workerManager.ts:42`)

1. If `opts.workerId` is supplied **and** already present, the existing record
   is updated in place: `queues`, `concurrency`, `lastSeen`, and optionally
   `hostname`/`pid` are overwritten, then the existing `Worker` is returned
   (`workerManager.ts:49-58`). This makes re-registration idempotent on a known
   ID — counters (`processedJobs`, etc.) are preserved.
2. Otherwise `createWorker()` mints a record (generating a UUID if no `workerId`)
   and inserts it (`workerManager.ts:60-62`).

`QueueManager.registerWorker` wraps this and emits `worker:connected`; the TCP
handler injects the connection's `clientId` so disconnect cleanup can find it
(`handlers/monitoring.ts:139-145`).

### Heartbeat (`heartbeat`, `workerManager.ts:95`)

1. Returns `false` if the worker is unknown (`handleHeartbeat` then replies
   `Worker not found`, `handlers/monitoring.ts:76`).
2. Refreshes `lastSeen = Date.now()`.
3. If `stats` are provided, each of `activeJobs` / `processed` / `failed` is
   treated as an **absolute** value: the manager subtracts the old per-worker
   value from the aggregate counter and adds the new one, keeping the global
   counters consistent (`workerManager.ts:102-118`).

### Per-job counter mutators

- `incrementActive` (`workerManager.ts:123`): `activeJobs++`, aggregate++, set
  `currentJob`, refresh `lastSeen`.
- `jobCompleted` (`workerManager.ts:136`): decrement `activeJobs` (guarded at 0),
  `processedJobs++`, refresh `lastSeen`, and emit `worker:idle` when no jobs
  remain.
- `jobFailed` (`workerManager.ts:154`): same shape but bumps `failedJobs` and,
  on crossing failure thresholds, emits `worker:error` with a rounded
  `failureRate`.

> **Note:** in the current codebase these three mutators are not invoked on the
> live server pull/ack/fail path (only `register`, `heartbeat`,
> `unregister*`, and the read methods are). Per-worker `activeJobs`/`processed`/
> `failed` are therefore populated by client-reported `Heartbeat` stats; the
> mutators are exercised by benchmarks/embedded callers. Treat the per-worker
> counters as advisory observability data, not an authoritative ledger.

### Liveness & stale reaping

- A worker is "active" when `now - lastSeen < WORKER_TIMEOUT_MS` (default 30s).
  `listActive`, `getForQueue`, `getStats.active`, and `computeWorkerStatus` all
  use this window (`workerManager.ts:197,204,245`; `handlers/monitoring.ts:182`).
- The cleanup interval runs every `WORKER_CLEANUP_INTERVAL_MS` (default 60s,
  `workerManager.ts:210`) and removes workers whose `lastSeen` is older than
  `WORKER_TIMEOUT_MS * 3` (90s by default, `workerManager.ts:218`) — i.e. a
  worker can read as "stale" for up to ~60s before being physically reaped,
  giving a flapping connection time to recover.

### `skipIfNoWorker` integration

At startup `QueueManager` wires the cron worker check
(`queueManager.ts:190-192`):

```typescript
this.cronScheduler.setWorkerCheckCallback((queue) =>
  this.workerManager.getForQueue(queue).length > 0
);
```

When a cron with `skipIfNoWorker: true` fires, `fireCronJob` calls this
callback and, if no active worker serves the queue, skips the run and emits
`cron:skipped` with `reason: 'no-worker'` (`cronScheduler.ts:407-413`). Because
`getForQueue` filters on the `WORKER_TIMEOUT_MS` window, a worker that stopped
heartbeating is treated as absent even before it is reaped. See
[Scheduler & Cron](./scheduler-and-cron.md).

### Disconnect cleanup

The TCP and HTTP servers call `QueueManager.unregisterWorkersByClientId` when a
connection closes (`server/tcp.ts:294`, `server/http.ts:236`,
`server/sseHandler.ts:346`), which delegates to
`unregisterByClientId(clientId)` — removing every worker registered over that
connection and emitting `worker:disconnected` per removal
(`workerManager.ts:76-87`). Because pooled clients get a fresh server-side
`clientId` on reconnect, the client `Worker` re-sends `RegisterWorker` on
reconnect to stay visible (`client/worker/worker.ts:198-207`).

## Concurrency & Locking

No locks. `WorkerManager` is plain synchronous mutation of a `Map` and integer
counters; it is not part of the shard lock hierarchy
([Concurrency & Locking](./concurrency-and-locking.md)). It runs on Bun's single
JS thread, so handler calls, the cleanup `setInterval`, and counter math never
interleave mid-method. There is no lease/renewal logic here — lock leasing and
renewal live in the job subsystem (`renewJobLock`), reached via `JobHeartbeat`.

## Edge Cases & Failure Modes

- **Idempotent re-registration:** re-registering a known `workerId` updates in
  place and preserves counters; an unknown/absent `workerId` creates a new
  record (`workerManager.ts:49-62`).
- **Heartbeat on unknown worker:** returns `false`; the `Heartbeat` handler then
  responds `Worker not found` (`handlers/monitoring.ts:76`). A client that was
  reaped must re-register.
- **Counter underflow guard:** `jobCompleted`/`jobFailed` only decrement
  `activeJobs` when `> 0` (`workerManager.ts:139,157`). However, the aggregate
  `totalActiveJobsCounter` can still drift if `heartbeat` stats and the mutators
  are mixed, or if `unregister`/`unregisterByClientId` subtract a stale
  `activeJobs` value — these counters are best-effort.
- **Memory bound:** unlike the LRU-bounded collections in
  [Core Queue Engine](./core-queue-engine.md), `workers` has **no fixed cap**.
  Its size is bounded only by the stale-reaper (`WORKER_TIMEOUT_MS * 3`). A flood
  of distinct `workerId`s heartbeating faster than the timeout could grow the
  map; in normal operation it tracks one record per live consumer connection.
- **`getStats` cost:** `total`/counters are O(1), but `active` requires an O(n)
  pass over the map (time-based, can't be cached, `workerManager.ts:244`).
- **Stale-but-not-reaped window:** between `WORKER_TIMEOUT_MS` and the reaper
  cutoff a worker reports `status: 'stale'` and is excluded from `listActive` /
  `getForQueue`, but still counts toward `getStats.total` and appears in `list`.
- **No persistence:** a server restart drops the entire registry; clients must
  re-register (the client `Worker` does this automatically on (re)connect).
- **Cleanup leak on shutdown:** `stop()` must be called to clear the interval;
  `QueueManager.shutdown` calls `workerManager.stop()` (`queueManager.ts:1858`).

## Configuration

| Env var | Default | Effect |
| --- | --- | --- |
| `WORKER_TIMEOUT_MS` | `30000` | Liveness window: a worker is "active"/"stale" relative to `lastSeen` (`workerManager.ts:14`). Also re-read in `handlers/monitoring.ts:179`. |
| `WORKER_CLEANUP_INTERVAL_MS` | `60000` | How often `cleanupStale` runs (`workerManager.ts:17`). |

The stale-removal threshold is derived, not configurable directly:
`WORKER_TIMEOUT_MS * 3` (`workerManager.ts:218`).

## Related Docs

- [Scheduler & Cron](./scheduler-and-cron.md) — `skipIfNoWorker` consumer of `getForQueue`
- [Client SDK: Worker](./client-worker-sdk.md) — the consumer that issues these commands
- [Job Lifecycle](./job-lifecycle.md) — `JobHeartbeat` / lock renewal (job-level, not worker-level)
- [Concurrency & Locking](./concurrency-and-locking.md) — lock hierarchy this module sits outside of
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md) — concurrency enforcement
- [Stats, Metrics & Monitoring](./stats-and-monitoring.md) — fleet stats and dashboard surface
- [TCP Server Command Handlers](./tcp-server-handlers.md) — `Register/Unregister/ListWorkers/Heartbeat` dispatch
- [HTTP / REST / SSE / WebSocket API](./http-api.md) — `/workers` routes and disconnect cleanup
- [CLI](./cli.md) — `bunqueue worker …` subcommands
- [Webhooks, Events & Job Logs](./webhooks-and-events.md) — `JobLogEntry` co-located in `worker.ts`
- [Background Tasks](./background-tasks.md) — periodic checks incl. worker-overload detection
- [architecture](../architecture.md) · [data-model](../data-model.md)
