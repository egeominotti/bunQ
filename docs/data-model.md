# Data Model

This document is the authoritative reference for how bunqueue represents data
across its three boundaries:

| Boundary                   | Representation                                | Source of truth                                     |
| -------------------------- | --------------------------------------------- | --------------------------------------------------- |
| **In-memory**              | Plain TS objects + collections                | `src/domain/types/*`, `src/application/types/*`     |
| **Local persistence**      | SQLite rows, BLOB = MessagePack               | `src/infrastructure/persistence/schema.ts`          |
| **On-disk (multi-broker)** | PostgreSQL rows, BYTEA = MessagePack          | `src/infrastructure/persistence/postgres/schema.ts` |
| **On-wire**                | msgpack-encoded `Command` / `Response` frames | `src/domain/types/command.ts`, `response.ts`        |

The same logical `Job` crosses the selected boundaries. A client sends a
`PushCommand` over TCP (msgpack). In memory/SQLite mode the server materializes
the job in a shard's `PriorityQueue`; when a data path is configured, the
`WriteBuffer` persists it to `jobs` with structured fields stored as MessagePack
BLOBs. PostgreSQL mode instead admits the encoded job to `bunqueue_jobs` inside
a database transaction and maintains a broker-local compatibility projection
from durable events and repair reads. These shapes are **not identical** —
`childrenCompleted` is reconstructed on reload. Recovery-critical
`stallCount` and all four BullMQ-v5 child-failure policies are persisted so a
restart cannot reset lifecycle or flow semantics.

Cross-references: [Job Lifecycle](./features/job-lifecycle.md),
[Persistence](./features/persistence.md),
[Core Queue Engine](./features/core-queue-engine.md),
[Data Structures](./features/data-structures.md),
[TCP Protocol](./features/tcp-protocol.md).

---

## Job

The core record. Defined in `src/domain/types/jobs/model.ts:41-90`.

```typescript
export interface Job {
  readonly id: JobId; // UUIDv7, branded string (types/jobs/model.ts:1)
  readonly queue: string;
  readonly data: unknown; // user payload (msgpack BLOB on disk)
  readonly priority: number; // higher = sooner
  readonly createdAt: number; // epoch ms
  readonly lifo: boolean; // tie-break among equal priority

  // Scheduling
  runAt: number; // createdAt + delay; <=now means ready
  startedAt: number | null; // set when pulled (active)
  completedAt: number | null; // set when completed

  // Retry config
  attempts: number; // attempts consumed so far
  readonly maxAttempts: number;
  readonly backoff: number; // base backoff ms
  readonly backoffConfig: BackoffConfig | null; // persisted in extended_options

  // Timeouts
  readonly ttl: number | null; // time-to-live from createdAt
  readonly timeout: number | null; // max processing time

  // Deduplication
  readonly uniqueKey: string | null;
  readonly customId: string | null;

  // Dependencies & workflows
  readonly dependsOn: JobId[];
  readonly parentId: JobId | null;
  childrenIds: JobId[];
  childrenCompleted: number; // runtime only; reset to 0 on reload

  // Metadata
  readonly tags: string[];
  readonly groupId: string | null;

  // Progress tracking
  progress: number; // 0..100
  progressMessage: string | null;

  // Failure info
  stacktrace: string[] | null; // last failure stack, capped (#74)

  // Cleanup config
  readonly removeOnComplete: boolean;
  readonly removeOnFail: boolean;

  // Repeat config
  readonly repeat: RepeatConfig | null; // persisted in extended_options

  // Stall detection
  lastHeartbeat: number;
  readonly stallTimeout: number | null;
  stallCount: number; // persisted; cumulative across restarts

  // BullMQ v5 additional options
  readonly stackTraceLimit: number; // default 10
  readonly keepLogs: number | null;
  readonly sizeLimit: number | null;
  readonly failParentOnFailure: boolean;
  readonly removeDependencyOnFailure: boolean;
  readonly continueParentOnFailure: boolean;
  readonly ignoreDependencyOnFailure: boolean;
  readonly deduplicationTtl: number | null;
  readonly deduplicationExtend: boolean;
  readonly deduplicationReplace: boolean;
  readonly debounceId: string | null;
  readonly debounceTtl: number | null;
  readonly durable?: boolean; // persisted in extended_options

  timeline: JobTimelineEntry[]; // state-transition log (persisted as BLOB)
}
```

`customId` is a broker-wide identity key, not a per-queue key. When supplied it
also becomes `jobs.id`, whose SQLite primary key is global. Re-adding a live
custom ID from any queue returns the existing generation; after a terminal
generation, reuse first retires the completed/DLQ record and then admits one new
generation. At no point may two rows or two live jobs share the same custom ID.
`FlowProducer` is stricter than standalone `Queue.add`: every planned flow ID
must be unowned by live state, SQLite/DLQ rows, retained completion/timeout
tombstones, retained results, or unresolved reverse-dependency entries. Reuse
therefore rejects the whole `PUSHF` request instead of retiring or rewriting
existing topology. Pure in-memory managers keep those terminal guards in
bounded collections; configured SQLite supplies the durable ownership check.
Flow topology metadata (`__parentId`, `__parentQueue`, `__childrenIds`,
`__flowParentId`, and `__flowParentIds`) is stored inside the data BLOB while
the job name remains in its dedicated field/column. Public Worker and Queue
reads expose those engine-owned fields. Updating flow data merges them back
atomically and rejects caller attempts to replace any reserved `__*` field;
ordinary jobs without topology metadata retain their existing data semantics.

Notable supporting types:

- `JobId = string & { readonly __brand: 'JobId' }` — UUIDv7, generated by
  `Bun.randomUUIDv7()` (`src/domain/job/ids.ts:7-8`).
- `RepeatConfig` (`src/domain/types/jobs/model.ts:13-25`) — repeatable-job config (`every`, `pattern`,
  `limit`, `count`, `tz`, …). In-memory only on a `Job`; not a SQLite column.
- `BackoffConfig` (`src/domain/types/jobs/model.ts:27-31`) — `{ type: 'fixed' | 'exponential'; delay; maxDelay? }`.
  `DEFAULT_MAX_BACKOFF = 3_600_000` (1h). Backoff math in `calculateBackoff`
  (`src/domain/job/state.ts:37-53`): fixed = ±20% jitter, exponential = `delay * 2^attempts`
  with ±50% jitter, both capped at `maxDelay`.
- `JobTimelineEntry` (`src/domain/types/jobs/model.ts:33-39`) — `{ state, timestamp, worker?, error?, attempt? }`,
  capped at `MAX_TIMELINE_ENTRIES = 20` (`src/domain/job/constants.ts:2`).
- `JobLock` (`src/domain/types/jobs/model.ts:139-148`) — `{ jobId, token, owner, createdAt, expiresAt,
lastRenewalAt, renewalCount, ttl }`. `DEFAULT_LOCK_TTL = 30_000`.

### Job State Machine

The persisted/enum states live in `JobState` (`src/domain/types/jobs/model.ts:4-11`):

| State         | Enum value    | Meaning                                           |
| ------------- | ------------- | ------------------------------------------------- |
| `waiting`     | `Waiting`     | Ready to be pulled (`runAt <= now`, priority `0`) |
| `prioritized` | `Prioritized` | Ready, `priority > 0` (derived from waiting+prio) |
| `delayed`     | `Delayed`     | `runAt > now`, waiting for its scheduled time     |
| `active`      | `Active`      | Pulled by a worker, in `processingShards`         |
| `completed`   | `Completed`   | ACKed successfully                                |
| `failed`      | `Failed`      | Terminal failure → DLQ                            |

Two additional states are reported in counts/queries but are _not_ members of
the `JobState` enum (see `JobCounts`, `src/domain/types/responses/model.ts:50-59`):

- `waiting-children` — parked in `shard.waitingDeps` or `shard.waitingChildren`
  (flow dependency not yet satisfied or explicitly parked by a public move).
  It is derived for in-memory queries and is also a valid SQLite `jobs.state`
  checkpoint so a manual transition survives restart.
  The public `Queue.getJobCounts()` / `getJobCountsAsync()` result includes the
  same `'waiting-children'` bucket (including `0` for an empty queue), so job
  state, listing, and count surfaces use one classification.
- `paused` — present in `JobCounts` for BullMQ parity; pausing is a queue-level
  flag (`QueueState.paused`), jobs are not individually re-stated.

`getJobState` (`src/application/operations/query/state.ts:22-57`) derives the state from where the
job currently lives (jobIndex location → shard sub-collection → SQLite fallback
via `resolveStateFromStorage`, `src/application/operations/query/state.ts:6-19`). If the index entry
moves mid-lookup (a concurrent pull flipping queue to processing), it chases the
fresh location for up to 4 passes instead of reporting a false `unknown` (2.8.31).

Allowed transitions (enforced across `pull`/`ack`/`fail` operations and
`jobStateTransitions.ts`):

| From                              | To                 | Trigger                                                               |
| --------------------------------- | ------------------ | --------------------------------------------------------------------- |
| `delayed`                         | `waiting`          | scheduler when `runAt <= now`, or `Promote`                           |
| `waiting`                         | `active`           | `PULL` / `PULLB`                                                      |
| `waiting`                         | `waiting-children` | unmet dependency, including a linked `parent` child (`parentLink.ts`) |
| `waiting-children`                | `waiting`          | all `dependsOn` completed                                             |
| `active`                          | `completed`        | `ACK` (`ack`)                                                         |
| `active`                          | `failed`/`delayed` | `FAIL`: retry w/ backoff if `attempts < maxAttempts`                  |
| `active`                          | `waiting`          | `moveActiveToWait` (`jobStateTransitions.ts:16`)                      |
| `active`                          | `waiting-children` | `moveToWaitingChildren` (`jobStateTransitions.ts:84`)                 |
| `waiting`/`active`                | `failed`→DLQ       | `Discard` (`token?` required for a live lease)                        |
| `active`                          | `failed`→DLQ       | timeout / max stalls / max attempts                                   |
| `failed`(DLQ)                     | `waiting`          | `RetryDlq` / auto-retry                                               |
| `failed`(DLQ)                     | removed            | `RemoveDlqJob` / selective permanent deletion                         |
| `waiting`/`prioritized`/`delayed` | `delayed`          | `ChangeDelay` / `MoveToDelayed` (in-place `runAt`)                    |
| `active`                          | `delayed`          | `ChangeDelay` / `MoveToDelayed` (two-phase re-queue)                  |

> `ChangeDelay` and `MoveToDelayed` both carry a **relative** `delay` (ms) and an
> optional lease `token` on the wire; the client converts the public absolute
> `moveToDelayed(id, timestamp)` to `delay = max(0, timestamp - now)`. In-queue
> jobs route through `changeWaitingDelay`, active jobs through the two-phase
> `moveJobToDelayed` — both share `QueueManager.moveToDelayed`/`changeDelay`
> (`application/queue-manager/job-management.ts`), so `MoveToDelayed` works over TCP/HTTP/MCP for
> waiting **and** active jobs (was previously a silent no-op for waiting jobs).
> An active leased transition requires the exact token. Processor-created Job
> objects capture it for `changeDelay()`, `retry()`, and `discard()`, whose
> public signatures intentionally do not expose a token argument. The
> synchronous `discard()` call is internally awaited by Worker outcome handling
> before its processing generation is released.

Helper predicates: `isDelayed`, `isReady`, `isExpired`, `isTimedOut`,
`canRetry` (`src/domain/job/state.ts:19-58`).

---

## JobOptions

`JobInput` (`src/domain/types/jobs/model.ts:92-137`) is the creation-time shape. Defaults are applied
by `createJob` (`src/domain/job/create.ts:77-118`) using `JOB_DEFAULTS` (`src/domain/job/constants.ts:5-13`).

| Option                      | Type                                                | Default      |
| --------------------------- | --------------------------------------------------- | ------------ |
| `data`                      | `unknown` (required)                                | —            |
| `priority`                  | `number`                                            | `0`          |
| `delay`                     | `number` (ms)                                       | `0`          |
| `maxAttempts`               | `number`                                            | `3`          |
| `backoff`                   | `number \| { type: 'fixed'\|'exponential'; delay }` | `1000`       |
| `ttl`                       | `number \| null`                                    | `null`       |
| `timeout`                   | `number \| null`                                    | `null`       |
| `uniqueKey`                 | `string \| null`                                    | `null`       |
| `customId`                  | `string \| null`                                    | `null`       |
| `dependsOn`                 | `JobId[]`                                           | `[]`         |
| `childrenIds`               | `JobId[]`                                           | `[]`         |
| `parentId`                  | `JobId \| null`                                     | `null`       |
| `tags`                      | `string[]`                                          | `[]`         |
| `groupId`                   | `string \| null`                                    | `null`       |
| `lifo`                      | `boolean`                                           | `false`      |
| `removeOnComplete`          | `boolean` (coerced, #90)                            | `false`      |
| `removeOnFail`              | `boolean` (coerced, #90)                            | `false`      |
| `stallTimeout`              | `number \| null`                                    | `null`       |
| `repeat`                    | `{ every?, limit?, pattern?, count?, tz?, … }`      | `null`       |
| `dedup`                     | `{ ttl?, extend?, replace? }`                       | see below    |
| `durable`                   | `boolean` (SQLite: bypass buffer; PostgreSQL: no durability change) | `false`      |
| `stackTraceLimit`           | `number`                                            | `10`         |
| `keepLogs`                  | `number \| null`                                    | `null`       |
| `sizeLimit`                 | `number \| null`                                    | `null`       |
| `failParentOnFailure`       | `boolean`                                           | `false`      |
| `removeDependencyOnFailure` | `boolean`                                           | `false`      |
| `continueParentOnFailure`   | `boolean`                                           | `false`      |
| `ignoreDependencyOnFailure` | `boolean`                                           | `false`      |
| `debounceId`                | `string \| null`                                    | `null`       |
| `debounceTtl`               | `number \| null`                                    | `null`       |
| `timestamp`                 | `number` (overrides `createdAt`)                    | `Date.now()` |

`dedup` maps onto job fields via `parseBullMQV5Options` (`src/domain/job/create.ts:60-74`):
`deduplicationTtl = dedup.ttl ?? null`, `deduplicationExtend = dedup.extend ??
false`, `deduplicationReplace = dedup.replace ?? false`.

See [Deduplication & Unique Jobs](./features/deduplication-and-unique.md).

---

## Queue Configuration Types

Defined in `src/domain/types/queue.ts`.

```typescript
export interface QueueState {
  // src/domain/types/queue.ts:7-17
  readonly name: string;
  paused: boolean;
  rateLimit: number | null; // token-bucket capacity
  rateLimitDuration: number | null; // window ms (null = 1000ms default)
  rateLimitExpiresAt: number | null; // epoch ms auto-expiry (null = permanent)
  concurrencyLimit: number | null; // max active jobs
  activeCount: number;
}
```

`createQueueState` defaults everything off (`paused:false`, limits `null`,
`activeCount:0`). This is the row persisted in `queue_state` (see schema) for
control-state recovery (#100). `rateLimitDuration` makes the limit mean
"`rateLimit` per `duration` ms" (refill rate = `limit / (duration/1000)`
tokens/sec); `rateLimitExpiresAt` is checked lazily on acquire and on limit
reads — an expired limit clears itself broker-side, and recovery skips
already-expired rows (restoring live ones with their remaining TTL).

Two runtime limiter classes back the config:

- `RateLimiter` (`src/domain/types/queue.ts:33-81`) — token-bucket; `tryAcquire()`, `refill()`
  based on elapsed seconds × `refillRate` (defaults to `capacity`).
- `ConcurrencyLimiter` (`src/domain/types/queue.ts:84-119`) — counter with `tryAcquire`,
  `release`, `setLimit`.

Supporting enums/types:

- `JobLocation` (`src/domain/types/queue.ts:121-126`) — the jobIndex value:
  `{type:'queue',shardIdx,queueName}` | `{type:'processing',shardIdx}` |
  `{type:'completed',queueName}` | `{type:'dlq',queueName}`.
- `EventType` (`src/domain/types/queue.ts:129-145`) — 14 event types: `pushed`, `pulled`,
  `completed`, `failed`, `progress`, `stalled`, `removed`, `delayed`,
  `duplicated`, `retried`, `waiting-children`, `drained`, `paused`, `resumed`.
- `JobEvent` (`src/domain/types/queue.ts:148-162`) — broadcast shape for subscribers.

See [Rate Limiting & Concurrency](./features/rate-limiting-and-concurrency.md),
[Concurrency & Locking](./features/concurrency-and-locking.md).

---

## DLQ Entry

Defined in `src/domain/types/dlq.ts`.

```typescript
export const enum FailureReason {
  // dlq.ts:9-24
  ExplicitFail = 'explicit_fail',
  MaxAttemptsExceeded = 'max_attempts_exceeded',
  Timeout = 'timeout',
  Stalled = 'stalled',
  TtlExpired = 'ttl_expired',
  WorkerLost = 'worker_lost',
  Unknown = 'unknown',
}

export interface AttemptRecord {
  // dlq.ts:27-40
  readonly attempt: number; // 1-based
  readonly startedAt: number;
  readonly failedAt: number;
  readonly reason: FailureReason;
  readonly error: string | null;
  readonly duration: number; // ms
}

export interface DlqEntry {
  // dlq.ts:43-62
  readonly job: Job; // full original job
  readonly enteredAt: number;
  readonly reason: FailureReason;
  readonly error: string | null;
  readonly attempts: AttemptRecord[]; // full attempt history
  retryCount: number; // retries from DLQ
  lastRetryAt: number | null;
  nextRetryAt: number | null; // null = no auto-retry
  readonly expiresAt: number | null; // null = never expire
}
```

`DlqConfig` (`dlq.ts:65-76`) with `DEFAULT_DLQ_CONFIG` (`dlq.ts:79-85`):

| Field               | Default            | Meaning                          |
| ------------------- | ------------------ | -------------------------------- |
| `autoRetry`         | `false`            | enable auto-retry from DLQ       |
| `autoRetryInterval` | `3_600_000` (1h)   | base interval (exponential)      |
| `maxAutoRetries`    | `3`                | retries before giving up         |
| `maxAge`            | `604_800_000` (7d) | age before auto-purge (`null`=∞) |
| `maxEntries`        | `10000`            | per-queue cap                    |

Auto-retry uses exponential backoff: `autoRetryInterval * 2^(retryCount-1)`
(`scheduleNextRetry`, `dlq.ts:153-165`). Lifecycle helpers: `createDlqEntry`,
`addAttemptRecord`, `isDlqEntryExpired`, `canAutoRetry`. Query/stats shapes:
`DlqFilter` (`dlq.ts:168-185`), `DlqStats` (`dlq.ts:188-203`).

The entire `DlqEntry` is persisted as a single MessagePack BLOB in the `dlq`
table's `entry` column. See [Dead Letter Queue](./features/dead-letter-queue.md).

While an automatic DLQ retry is waiting or active, its original entry times,
attempt history, retry count and next backoff live in a non-enumerable Job
symbol and in `jobs.dlq_retry_state`. This state is absent from the public Job
wire shape. Completion and manual retry clear it; a terminal re-failure consumes
it to reconstruct the same bounded DLQ generation.

`DlqEntry.reason` is the final failure classification, while each
`AttemptRecord.reason` describes its own attempt. The timeout deadline scheduler passes
`FailureReason.Timeout` explicitly through the internal failure transition, so
timeout history and terminal classification survive retries and SQLite restart.
Ordinary processor failures keep their existing `ExplicitFail` retry records
and `MaxAttemptsExceeded` terminal reason. `TtlExpired` and `WorkerLost` remain
reserved enum values; current TTL removal and disconnect recovery do not emit
them as terminal DLQ entries.

---

## Cron / Scheduler Model

Defined in `src/domain/types/cron.ts`.

```typescript
export interface CronJob {
  // cron.ts:28-52
  readonly name: string; // primary key
  readonly queue: string;
  readonly data: unknown;
  readonly schedule: string | null; // cron pattern
  readonly repeatEvery: number | null; // positive safe-integer ms interval (alt. to schedule)
  readonly priority: number;
  readonly timezone: string | null; // IANA, e.g. "Europe/Rome"
  nextRun: number; // mutable
  executions: number; // mutable count
  readonly maxLimit: number | null; // null = unlimited
  readonly uniqueKey: string | null;
  readonly dedup: CronDedup | null; // { ttl?, extend?, replace? }
  readonly skipMissedOnRestart: boolean;
  readonly skipIfNoWorker: boolean;
  readonly preventOverlap: boolean;
  readonly jobOptions: CronJobOptions | null; // retry/cleanup policy (#86)
}
```

`CronJobInput` (`cron.ts:55-79`) is the creation shape; `createCronJob`
(`cron.ts:82-107`) requires either `schedule` or `repeatEvery`, rejects a supplied
interval unless it is a positive safe integer in milliseconds, and applies
defaults: `priority:0`, `executions:0`, `skipMissedOnRestart:true`,
`skipIfNoWorker:false`, `preventOverlap:true`. **Important:** `maxLimit <= 0`
is normalized to `null` (unlimited) — storing `0` would make `isAtLimit`
treat the cron as already exhausted (`cron.ts:99`). When both timing fields are
valid, `schedule` takes precedence for compatibility.

`CronJobOptions` (`cron.ts:17-25`) is the per-spawn subset of `JobInput`:
`maxAttempts`, `backoff`, `timeout`, `delay`, `stallTimeout`,
`removeOnComplete`, `removeOnFail`.

Predicates: `isAtLimit` (`cron.ts:110-113`), `isDue` (`cron.ts:116-118`).
The SQLite engine persists this in `cron_jobs`; `dedup` and `jobOptions` are
BLOBs. The PostgreSQL engine persists the complete MessagePack cron value in
`bunqueue_crons.payload` beside indexed `next_run`/execution fields.

See [Scheduler & Cron](./features/scheduler-and-cron.md).

---

## Worker Model

Defined in `src/domain/types/worker.ts`. Worker registrations are in-memory only
in the memory/SQLite engine. PostgreSQL multi-broker mode also stores the encoded
worker, broker/client ownership, queue array, and heartbeat in
`bunqueue_workers`, so every broker sees the same registry.

```typescript
export type WorkerId = string;

export interface Worker {
  // worker.ts:11-26
  id: WorkerId;
  name: string;
  queues: string[];
  concurrency: number;
  hostname: string;
  pid: number;
  registeredAt: number;
  lastSeen: number; // updated by Heartbeat
  activeJobs: number;
  processedJobs: number;
  failedJobs: number;
  currentJob: string | null;
  clientId: string | null; // TCP client that registered (for cleanup)
}
```

`createWorker` (`worker.ts:38-60`) defaults `concurrency:1`,
`hostname:'unknown'`, `pid:0`, and generates `id` via `uuid()` if not given.

```typescript
export interface JobLogEntry {
  // worker.ts:63-67
  timestamp: number;
  level: 'info' | 'warn' | 'error';
  message: string;
}
```

See [Workers Management](./features/workers-management.md).

---

## Webhook Model

Defined in `src/domain/types/webhook.ts`.

```typescript
export const WEBHOOK_EVENTS = [
  // webhook.ts:16-22
  'job.pushed',
  'job.started',
  'job.completed',
  'job.failed',
  'job.progress',
] as const;

// 'job.stalled' accepted on stored webhooks for back-compat but never emitted.
export type WebhookEvent = (typeof WEBHOOK_EVENTS)[number] | 'job.stalled';

export interface Webhook {
  // webhook.ts:31-42
  id: WebhookId; // uuid
  url: string;
  events: WebhookEvent[];
  queue: string | null; // null = all queues
  secret: string | null; // HMAC signing
  createdAt: number;
  lastTriggered: number | null;
  successCount: number;
  failureCount: number;
  enabled: boolean;
}

export interface WebhookPayload {
  // webhook.ts:66-74
  event: WebhookEvent;
  timestamp: number;
  jobId: string;
  queue: string;
  data?: unknown;
  error?: string;
  progress?: number;
}
```

`createWebhook` (`webhook.ts:45-63`) starts a webhook `enabled:true` with zeroed
counters. See [Webhooks & Events](./features/webhooks-and-events.md).

---

## Stall & Deduplication Types

### Stall (`src/domain/types/stall.ts`)

```typescript
export interface StallConfig {
  // stall.ts:9-18
  enabled: boolean; // default true
  stallInterval: number; // default 30_000 ms
  maxStalls: number; // default 3
  gracePeriod: number; // default 5_000 ms
}
// DEFAULT_STALL_CONFIG at stall.ts:21-26

export interface StallCheckResult {
  // stall.ts:29-38
  isStalled: boolean;
  stalledFor: number;
  shouldMoveToDlq: boolean;
  newStallCount: number;
}
```

`checkStall` (`stall.ts:41-89`): not-stalled while `startedAt === null` or
within `gracePeriod`; otherwise stalled when `now - lastHeartbeat > stallInterval`
(per-job `stallTimeout` overrides config). `StallAction` (`stall.ts:92-99`):
`Retry` | `MoveToDlq` | `Keep`. See [Stall detection](./features/concurrency-and-locking.md).

### Deduplication (`src/domain/types/deduplication.ts`)

```typescript
export type DeduplicationStrategy = 'reject' | 'extend' | 'replace';

export interface DeduplicationOptions {
  // deduplication.ts:14-23
  id: string; // maps to job.uniqueKey
  ttl?: number; // no expiry if unset
  extend?: boolean; // reset TTL instead of rejecting
  replace?: boolean; // replace data instead of rejecting
}

export interface UniqueKeyEntry {
  // deduplication.ts:28-35
  jobId: JobId;
  expiresAt: number | null; // null = never
  registeredAt: number;
}
```

`getDeduplicationStrategy` (`deduplication.ts:40-44`) resolves precedence
`replace > extend > reject`. See
[Deduplication & Unique Jobs](./features/deduplication-and-unique.md).

---

## Wire Protocol Types

TCP frames are msgpack-encoded `Command` (request) and `Response` (reply)
objects. Every command extends `BaseCommand` (`src/domain/types/commands/base.ts:2-5`): a `cmd`
discriminator string plus an optional `reqId` for pipelining correlation. Every
response extends `BaseResponse` (`src/domain/types/responses/model.ts:6-9`): `{ ok: boolean; reqId? }`.

### Commands

The discriminated union `Command` (`src/domain/types/commands/union.ts:13-105`) covers the protocol command surface.
Representative shapes:

```typescript
export interface PushCommand extends BaseCommand {
  // src/domain/types/commands/core.ts:5-52
  readonly cmd: 'PUSH';
  readonly queue: string;
  readonly data: unknown;
  readonly priority?: number;
  readonly delay?: number;
  readonly maxAttempts?: number;
  readonly jobId?: string; // == customId
  readonly dependsOn?: string[];
  readonly durable?: boolean;
  readonly dedup?: { ttl?; extend?; replace? };
  // … plus ttl, timeout, uniqueKey, parentId, tags, groupId, lifo,
  //   removeOnComplete/Fail, repeat, stallTimeout, flow flags, timestamp …
}

export interface PushFlowCommand extends BaseCommand {
  readonly cmd: 'PUSHF';
  readonly jobs: AtomicFlowJobInput[]; // fully-resolved IDs and graph edges
}

export interface PullCommand extends BaseCommand {
  // src/domain/types/commands/core.ts:65-72
  readonly cmd: 'PULL';
  readonly queue: string;
  readonly timeout?: number; // long-poll ms
  readonly owner?: string; // lock owner id
  readonly lockTtl?: number; // default 30000
  readonly detach?: boolean; // don't auto-release on disconnect
}

export interface AckCommand extends BaseCommand {
  // src/domain/types/commands/core.ts:83-89
  readonly cmd: 'ACK';
  readonly id: string;
  readonly result?: unknown;
  readonly token?: string; // lock token verification
}

export interface FailCommand extends BaseCommand {
  // src/domain/types/commands/core.ts:99-107
  readonly cmd: 'FAIL';
  readonly id: string;
  readonly error?: string;
  readonly token?: string;
  readonly unrecoverable?: boolean; // skip remaining retries
  readonly stack?: string[]; // persisted server-side, capped (#74)
}
```

Command families (each is a `cmd`-tagged interface under `src/domain/types/commands/`; the legacy `command.ts` file is only a compatibility barrel): **Core**
(`PUSH`/`PUSHB`/`PUSHF`/`PULL`/`PULLB`/`ACK`/`ACKB`/`FAIL`), **Query** (`GetJob`,
`GetState`, `GetResult`, `GetJobs`, `GetJobCounts`, `GetCountsPerPriority`,
`GetJobByCustomId`, `Count`, `GetProgress`, `GetQueueLimits`,
`GetDeduplicationJobId`), **Management** (`Cancel`,
`Progress`, `Update`, `ChangePriority`, `Promote`, `WaitJob`, `MoveToDelayed`,
`MoveToWaitingChildren`, `Discard`, `MoveToWait`, `PromoteJobs`, `ChangeDelay`,
`RemoveDeduplicationKey`, `RemoveJobDeduplicationKey`), **Queue Control**
(`Pause`, `Resume`, `IsPaused`, `Drain`, `Obliterate`, `ListQueues`, `Clean`),
**DLQ** (`Dlq`, `GetDlqStats`, `RetryDlq`, `PurgeDlq`, `RemoveDlqJob`, `RetryCompleted`), **Rate/Concurrency**
(`RateLimit`, `SetConcurrency`, `*Clear`), **Config** (`Set/GetStallConfig`,
`Set/GetDlqConfig`), **Cron** (`Cron`, `CronDelete`, `CronList`, `CronGet`),
**Logs** (`AddLog`, `GetLogs`, `ClearLogs`), **Heartbeat/Workers** (`Heartbeat`,
`JobHeartbeat`, `JobHeartbeatB`, `Ping`, `Register/UnregisterWorker`,
`ListWorkers`, `ExtendLock(s)`), **Webhooks** (`AddWebhook`, `RemoveWebhook`,
`ListWebhooks`, `SetWebhookEnabled`), **Flow** (`PUSHF`, `GetChildrenValues`,
`UpdateParent`, `GetFailed/IgnoredChildren*`, `RemoveChildDependency`,
`RemoveUnprocessedChildren`), **Monitoring** (`Stats`, `Metrics`, `Prometheus`,
`StorageStatus`, `CompactMemory`), **Dashboard** (`DashboardOverview/Queues/Queue`),
**Auth/Negotiation** (`Auth`, `Hello`), and **Events** (`SubscribeEvents`,
`UnsubscribeEvents`). `SubscribeEvents` carries a validated queue key;
`UnsubscribeEvents` has no payload. `HelloCommand` negotiates
`protocolVersion` and `ProtocolCapability[]`. Protocol revision 3 advertises
`pipelining` and `separate-job-name`: job envelopes expose top-level `name`
while preserving `data` as the original user value. Only the inbound legacy
decoder interprets an object-shaped `data.name` when top-level `name` is absent.

`StorageStatus` returns `{ data: { diskFull, error, since } }`. The manager keeps
the full internal diagnostic, but server/MCP/Cloud client projections replace a
non-disk storage error with `Internal server error`. SQLite disk-full responses
retain their existing actionable message and timestamp. A non-null error marks
health/readiness degraded even when `diskFull` is false.

`GetJobsCommand` carries optional `state`, `limit`, `offset`, and `asc`. The
server defaults `asc` to `true`; `false` reverses the stable createdAt/job-id
order before applying the page range.

The extended introspection/DLQ shapes are:

- `Dlq { queue, count?, filter? }` returns `{ jobs, entries }`; `entries`
  preserve the full failure metadata and embedded job snapshot.
- `GetDlqStats { queue }` returns `{ data: { stats } }`.
- `RetryDlq { queue, jobId?, count?, filter? }` returns the applied `count`.
- `RemoveDlqJob { queue, jobId }` returns `{ data: { removed } }`. It is an
  idempotent permanent deletion: `false` means no matching entry, while command
  errors remain error responses.
- `RetryCompleted { queue, id?, count?, timestamp? }` applies the optional
  terminal-time cutoff before the count cap. Each applied transition atomically
  changes the durable job to `waiting` and removes its `job_results` row before
  publishing the new in-memory generation. Attempts, progress/message,
  processing/completion timestamps, and heartbeat are reset; stacktrace and
  timeline history are preserved, with a new `waiting` timeline entry.
- `GetQueueLimits { queue, maxJobs? }` returns
  `{ data: { limits: { rateLimit, rateLimitTtl, concurrencyLimit, maxed } } }`.
- Deduplication lookup/removal responses use `data.jobId`, `data.count`, or
  `data.removed` respectively.

`UpdateParent` is retained for legacy multi-command flow clients. When
`parent.childrenIds` already contains the child, it is a child-only replacement
of the temporary `pending` parent marker and is legal even after the parent
became active or terminal. It does not reschedule or rewrite the parent.
Persisted `jobs`/DLQ child data and a `flow_failures` row keyed by the temporary
parent are updated in one transaction. If the edge is not predeclared, both
sides can be extended only while the parent is still queued.

### Responses

The `Response` union (`src/domain/types/responses/model.ts:155-176`) is also discriminated by shape, all
sharing `ok`. Key variants:

| Response                              | Shape (besides `ok`)                                      | Used by            |
| ------------------------------------- | --------------------------------------------------------- | ------------------ |
| `OkResponse`                          | `{ id? }`                                                 | PUSH, ACK, …       |
| `DataResponse<AckAlreadyFinalized>`   | `{ data: { applied:false; reason:'already-finalized' } }` | retired ACK / FAIL |
| `DataResponse<AckBatchIgnored>`       | `{ data: { ignoredIds; ignoredIndices } }`                | mixed retired ACKB |
| `BatchResponse`                       | `{ ids: string[] }`                                       | PUSHB              |
| `DataResponse<AtomicFlowBatchResult>` | `{ data: { jobs: Job[] } }`                               | PUSHF              |
| `JobResponse`                         | `{ job: Job }`                                            | GetJob             |
| `NullableJobResponse`                 | `{ job: Job \| null }`                                    | —                  |
| `PulledJobResponse`                   | `{ job: Job\|null; token: string\|null }`                 | PULL               |
| `PulledJobsResponse`                  | `{ jobs: Job[]; tokens: string[] }`                       | PULLB              |
| `JobsResponse`                        | `{ jobs: Job[] }`                                         | GetJobs            |
| `StateResponse`                       | `{ id; state: JobState }`                                 | GetState           |
| `ResultResponse`                      | `{ id; result: unknown }`                                 | GetResult          |
| `JobCountsResponse`                   | `{ counts: JobCounts }`                                   | GetJobCounts       |
| `QueuesResponse`                      | `{ queues: QueueInfo[] }`                                 | ListQueues         |
| `ProgressResponse`                    | `{ progress: number; message: string\|null }`             | GetProgress        |
| `BoolResponse`                        | `{ value: boolean }`                                      | IsPaused           |
| `CountResponse`                       | `{ count: number; ids?: string[] }`                       | Count / Clean      |
| `StatsResponse`                       | `{ stats: StatsData }`                                    | Stats              |
| `MetricsResponse`                     | `{ metrics: MetricsData }`                                | Metrics            |
| `CronResponse`                        | `{ cron: CronInfo }`                                      | Cron               |
| `CronListResponse`                    | `{ crons: CronInfo[] }`                                   | CronList           |
| `HelloResponse`                       | `{ protocolVersion; capabilities; server; version }`      | Hello              |
| `DataResponse<T>`                     | `{ data: T }`                                             | generic payloads   |
| `ErrorResponse`                       | `{ ok: false; error: string }`                            | failures           |

Normal ACK/FAIL/ACKB responses retain their historical `OkResponse` shape.
The data envelopes appear only when an exact timeout or retired-cron
generation lost the outcome race. `ignoredIndices` is aligned with the ACKB
input and remains authoritative when a job ID occurs more than once.

At the QueueManager boundary, an absent result is `undefined`, while a
completed job whose persisted result is JSON/MessagePack `null` returns `null`.
The SQLite adapter retains its lower-level nullable lookup; `hasResult(id)` is
used only to disambiguate those two public states. Falsy values are otherwise
returned unchanged.

Queue event delivery is server-initiated and therefore is not a `Response`:

```typescript
interface EventEnvelope {
  readonly type: 'event';
  readonly event: JobEvent;
}
```

It is emitted only on a connection that acknowledged `SubscribeEvents`, has no
`reqId`, and shares the same four-byte framing and MessagePack encoding.

`CronInfo` carries `name`, `queue`, nullable `schedule`/`repeatEvery`, the
authoritative numeric `nextRun`, `executions`, and optional `maxLimit`,
`timezone`, and `priority`. The `Cron` add response uses `CronResponse`; MCP and
client adapters must read the nested `cron` object rather than inventing
top-level scheduling metadata.

`JobCounts` (`src/domain/types/responses/model.ts:50-59`) carries all 8 buckets including the virtual
`'waiting-children'` and `paused`. Builder helpers (`ok`, `batch`, `job`,
`pulledJob`, `error`, `hello`, `data`, `counts`, `stats`, `metrics`) live at
`src/domain/response/builders.ts:22-68`. See [TCP Protocol](./features/tcp-protocol.md).

### Prometheus operational schema

Registration values use gauges with `_registered` names
(`bunqueue_workers_registered`, `bunqueue_cron_jobs_registered`,
`bunqueue_webhooks_registered`), while lifetime event counts retain the
Prometheus `_total` counter suffix. Worker capacity is represented by
`bunqueue_worker_active_jobs` and `bunqueue_worker_concurrency_slots`.
Push/pull/ack histogram names end in `_duration_seconds`; bucket bounds and
sums are exported in seconds even though the internal latency tracker keeps
milliseconds for the TCP API. Storage/process gauges use `_bytes` or boolean
0/1 semantics. Per-queue state metrics carry a single escaped `queue` label.

### S3 backup object schema

Every current-format committed payload key is
`<prefix>bunqueue-<ISO timestamp>-<UUID>.db` and has a pre-published
`<key>.meta.json` sibling. Metadata contains the bunqueue version, timestamp,
uncompressed size, compressed size, SHA-256 of uncompressed bytes, and
`compressed: true`. A compressed object without metadata is invalid; only
legacy uncompressed SQLite objects may omit metadata.

`BackupResult.size` is the verified, uncompressed SQLite byte length.
`BackupResult.compressedSize` is present for current-format backup/restore
operations and is the S3 gzip-object byte length used by
`bunqueue_backup_last_size_bytes`.

---

## SQLite Schema

Source: `src/infrastructure/persistence/schema.ts`. Connection PRAGMAs
(`schema.ts:6-14`): WAL journal, `synchronous=NORMAL`, 64 MB cache,
`temp_store=MEMORY`, 256 MB mmap, 4 KB page, 5 s busy timeout.

All structured/payload columns are `BLOB` holding MessagePack — `data`,
`depends_on`, `children_ids`, `tags`, `timeline`, `stacktrace`, internal
`dlq_retry_state`, `extended_options`, DLQ `entry`,
cron `dedup`/`job_options`, and `job_results.result`.

### `jobs` (schema.ts:20-51)

```sql
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    data BLOB NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    run_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    backoff INTEGER NOT NULL DEFAULT 1000,
    ttl INTEGER,
    timeout INTEGER,
    unique_key TEXT,
    custom_id TEXT,
    depends_on BLOB,
    parent_id TEXT,
    children_ids BLOB,
    tags BLOB,
    state TEXT NOT NULL DEFAULT 'waiting',
    lifo INTEGER NOT NULL DEFAULT 0,
    group_id TEXT,
    progress INTEGER DEFAULT 0,
    progress_msg TEXT,
    remove_on_complete INTEGER DEFAULT 0,
    remove_on_fail INTEGER DEFAULT 0,
    fail_parent_on_failure INTEGER NOT NULL DEFAULT 0,
    remove_dependency_on_failure INTEGER NOT NULL DEFAULT 0,
    continue_parent_on_failure INTEGER NOT NULL DEFAULT 0,
    ignore_dependency_on_failure INTEGER NOT NULL DEFAULT 0,
    stall_timeout INTEGER,
    last_heartbeat INTEGER,
    stall_count INTEGER NOT NULL DEFAULT 0,
    timeline BLOB,
    stacktrace BLOB,
    dlq_retry_state BLOB,
    extended_options BLOB
);
```

The row type `DbJob` mirrors this (BLOB → `Uint8Array`). `extended_options`
persists object backoff, repeat-chain configuration, advanced log/size options,
dedup/debounce policy, and the durable-write flag; a missing legacy blob uses
safe defaults. `childrenCompleted` remains a reconstructed runtime field. The four
flow flags map directly to their `_on_failure` columns; legacy rows receive safe
false defaults through migrations 23–26. `stallCount` maps to
`jobs.stall_count`; legacy rows receive the migration's safe zero default once,
then every recovery retry persists its increment.

`clearJobUniqueKey(jobId)` updates only `jobs.unique_key`. It is called after
an owner-aware in-memory release, preventing startup recovery from restoring a
key the public API already removed. `markWaitingChildren(jobId, timeline)`
writes `state='waiting-children'`, clears `started_at`, and persists the
transition timeline without altering the job identity or dependency blobs.

Indexes on `jobs`:

```sql
CREATE INDEX idx_jobs_queue_state        ON jobs(queue, state);
CREATE INDEX idx_jobs_queue_created      ON jobs(queue, created_at, id);
CREATE INDEX idx_jobs_queue_state_created ON jobs(queue, state, created_at, id);
CREATE INDEX idx_jobs_run_at             ON jobs(run_at) WHERE state IN ('waiting','prioritized','waiting-children','delayed');
CREATE INDEX idx_jobs_unique             ON jobs(queue, unique_key) WHERE unique_key IS NOT NULL;
CREATE INDEX idx_jobs_custom_id          ON jobs(custom_id) WHERE custom_id IS NOT NULL;
CREATE INDEX idx_jobs_parent             ON jobs(parent_id) WHERE parent_id IS NOT NULL;
CREATE INDEX idx_jobs_state_started      ON jobs(state, started_at) WHERE state = 'active';
CREATE INDEX idx_jobs_group_id           ON jobs(group_id) WHERE group_id IS NOT NULL;
CREATE INDEX idx_jobs_pending_priority   ON jobs(queue, state, priority DESC, run_at ASC) WHERE state IN ('waiting','prioritized','waiting-children','delayed');
CREATE INDEX idx_jobs_completed_order    ON jobs(completed_at DESC) WHERE state = 'completed';
```

### `flow_failures`

```sql
CREATE TABLE IF NOT EXISTS flow_failures (
    parent_id TEXT NOT NULL,
    child_id TEXT NOT NULL,
    child_queue TEXT NOT NULL,
    mode TEXT NOT NULL,
    error TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (parent_id, child_id)
);
CREATE INDEX idx_flow_failures_parent ON flow_failures(parent_id);
```

This table is both a durable propagation outbox and the live value store for
child failures. A terminal child and its outbox row commit together. Startup
replays rows before workers can observe recovered parents. `fail` and `remove`
rows are deleted after application; `ignore` and `continue` remain readable
while the parent is live and are deleted when it reaches a terminal state or is
removed.

### `dependency_completions`

```sql
CREATE TABLE IF NOT EXISTS dependency_completions (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL UNIQUE,
    queue TEXT NOT NULL,
    completed_at INTEGER NOT NULL,
    pinned INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_dependency_completions_queue
    ON dependency_completions(queue);
```

This table is the payload-free completion proof for a
`removeOnComplete` job. Inserting the proof and deleting the `jobs`,
`job_results`, and parent-side `flow_failures` rows is one transaction.
`sequence` gives deterministic FIFO pruning against `maxCompletedJobs` for
unreferenced proofs. `pinned=1` means at least one live `waitingDeps` reverse
edge still owns the proof; such a row is exempt from FIFO pruning until every
consumer is durably promoted, detached, cancelled, cleaned, or obliterated.
The RAM tracker mirrors this as an exact-size recent FIFO plus a pinned set.
Thus `recent <= maxCompletedJobs`, while pinned rows are proportional to
distinct completed dependency IDs referenced by live waiters.

Recovery deliberately loads the full table into temporary classification
state before applying a possibly smaller configured cap. After pending jobs
and reverse indexes are rebuilt, it reconciles `pinned` from those indexes,
prunes only unpinned rows, and hydrates the two RAM tiers. The record never
makes the removed job visible through Job/state/result/stats queries, and
`queue` exists so `obliterate(queue)` can delete the hidden state it owns.

### `job_results` (schema.ts:66-70)

```sql
CREATE TABLE IF NOT EXISTS job_results (
    job_id TEXT PRIMARY KEY,
    result BLOB,
    completed_at INTEGER NOT NULL
);
```

Written via `INSERT OR REPLACE` (`statements.ts:77-78`).

### `dlq` (schema.ts:73-79)

```sql
CREATE TABLE IF NOT EXISTS dlq (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    queue TEXT NOT NULL,
    entry BLOB NOT NULL,         -- full DlqEntry (msgpack)
    entered_at INTEGER NOT NULL
);
CREATE INDEX idx_dlq_queue      ON dlq(queue);
CREATE INDEX idx_dlq_job_id     ON dlq(job_id);
CREATE INDEX idx_dlq_entered_at ON dlq(entered_at);
```

### `cron_jobs` (schema.ts:103-120)

```sql
CREATE TABLE IF NOT EXISTS cron_jobs (
    name TEXT PRIMARY KEY,
    queue TEXT NOT NULL,
    data BLOB NOT NULL,
    schedule TEXT,
    repeat_every INTEGER,
    priority INTEGER NOT NULL DEFAULT 0,
    next_run INTEGER NOT NULL,
    executions INTEGER NOT NULL DEFAULT 0,
    max_limit INTEGER,
    timezone TEXT,
    unique_key TEXT,
    dedup BLOB,
    skip_missed_on_restart INTEGER NOT NULL DEFAULT 0,
    skip_if_no_worker INTEGER NOT NULL DEFAULT 0,
    prevent_overlap INTEGER NOT NULL DEFAULT 1,
    job_options BLOB
);
```

Row type `DbCron` at `statements.ts:180-198`.

### `queue_state` (`src/infrastructure/persistence/schema.ts:147-159`)

```sql
CREATE TABLE IF NOT EXISTS queue_state (
    name TEXT PRIMARY KEY,
    paused INTEGER NOT NULL DEFAULT 0,
    rate_limit INTEGER,
    concurrency_limit INTEGER,
    rate_limit_duration INTEGER,   -- window ms (migration 15)
    rate_limit_expires_at INTEGER, -- epoch ms auto-expiry (migration 16)
    stall_enabled INTEGER,         -- nullable for legacy/default policy
    stall_interval INTEGER,
    max_stalls INTEGER,
    stall_grace_period INTEGER,
    dlq_config BLOB
);
```

Persists queue control-state for recovery (#100); row type `DbQueueState`
(`statements.ts:201-213`). On boot, recovery skips rate-limit rows whose
`rate_limit_expires_at` is already in the past and restores still-live TTL'd
limits with their remaining time. The four nullable stall columns persist a
complete custom `StallConfig`; recovery applies it before classifying active
rows, so the same `maxStalls` bound governs the crash that triggered recovery.

### `queue_events`, `queue_metrics_meta`, and `queue_metric_buckets`

```sql
CREATE TABLE queue_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue TEXT NOT NULL,
    event_type TEXT NOT NULL,
    job_id TEXT NOT NULL,
    occurred_at INTEGER NOT NULL,
    payload BLOB
);
CREATE INDEX idx_queue_events_queue_id ON queue_events(queue, id DESC);

CREATE TABLE queue_metrics_meta (
    queue TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('completed', 'failed')),
    total_count INTEGER NOT NULL,
    prev_ts INTEGER NOT NULL,
    prev_count INTEGER NOT NULL,
    PRIMARY KEY (queue, type)
);
CREATE TABLE queue_metric_buckets (
    queue TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('completed', 'failed')),
    minute INTEGER NOT NULL,
    count INTEGER NOT NULL,
    PRIMARY KEY (queue, type, minute)
);
```

The event journal is ordered newest-first by its monotonic id and bounded per
queue. Metric rows are independent: trimming events never changes metric
counters. `total_count` is cumulative, while sparse minute rows are expanded to
a newest-first, zero-filled series and bounded to `maxMetricDataPoints`.
`obliterate(queue)` deletes all three table partitions.

### `migrations` (`src/infrastructure/persistence/schema.ts:194-200`)

```sql
CREATE TABLE IF NOT EXISTS migrations (
    version INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL
);
```

### Migrations (`src/infrastructure/persistence/migrations.ts:5-121`)

`SCHEMA_VERSION = 34`. The migrate routine reads
`MAX(version)`; if below current, runs the full `SCHEMA` (idempotent
`CREATE … IF NOT EXISTS`) then applies each incremental `ALTER`/`CREATE INDEX`
above the stored version in one synchronous transaction. It suppresses only
exact duplicate schema-object errors, rolls back every other failure, and
records `SCHEMA_VERSION` only after schema changes and legacy backfills finish.
Migration 6 is represented as two explicit statements so a partial historical
upgrade resumes at the missing column. A failed upgrade therefore keeps its old
version and is retried on reopen (`src/infrastructure/persistence/sqlite/state.ts`).

| Version | Change                                                                                                                |
| ------- | --------------------------------------------------------------------------------------------------------------------- |
| 1       | Base `SCHEMA` (all tables + base indexes)                                                                             |
| 5       | Performance indexes: `idx_dlq_entered_at`, `idx_jobs_state_started`, `idx_jobs_group_id`, `idx_jobs_pending_priority` |
| 6       | `cron_jobs.unique_key`, `cron_jobs.dedup` (dedup for cron jobs)                                                       |
| 7       | `jobs.timeline` BLOB                                                                                                  |
| 8       | `cron_jobs.skip_missed_on_restart`                                                                                    |
| 9       | `cron_jobs.skip_if_no_worker`                                                                                         |
| 10      | `cron_jobs.prevent_overlap` (default 1)                                                                               |
| 11      | `idx_jobs_completed_order` (completed-job recovery, #84)                                                              |
| 12      | `cron_jobs.job_options` BLOB (per-cron retry/cleanup policy, #86)                                                     |
| 13      | `jobs.stacktrace` BLOB (persist last failure stack, #74)                                                              |
| 14      | Stable `getJobs` indexes on `(queue, created_at, id)` and `(queue, state, created_at, id)`                            |
| 15      | `queue_state.rate_limit_duration` (rate-limit window)                                                                 |
| 16      | `queue_state.rate_limit_expires_at` (rate-limit TTL auto-expiry; split from 15 so each ALTER retries idempotently)    |
| 17      | `jobs.stall_count` (cumulative crash/stall budget across recovery)                                                    |
| 18      | `queue_state.stall_enabled`                                                                                           |
| 19      | `queue_state.stall_interval`                                                                                          |
| 20      | `queue_state.max_stalls`                                                                                              |
| 21      | `queue_state.stall_grace_period`                                                                                      |
| 22      | `queue_state.dlq_config` (effective per-queue DLQ policy, MessagePack)                                                |
| 23      | `jobs.fail_parent_on_failure`                                                                                         |
| 24      | `jobs.remove_dependency_on_failure`                                                                                   |
| 25      | `jobs.continue_parent_on_failure`                                                                                     |
| 26      | `jobs.ignore_dependency_on_failure`                                                                                   |
| 27      | `flow_failures` durable outbox + parent index; rebuild pending indexes for `prioritized`/`waiting-children`           |
| 28      | Bounded `dependency_completions` evidence for crash-safe `removeOnComplete` dependency recovery                       |
| 29      | `dependency_completions.pinned` ownership for proofs referenced by live waiting parents                               |
| 30      | `jobs.dlq_retry_state` (bounded auto-retry chain/history across restart)                                              |
| 31      | `jobs.name` (operation name separated from user data)                                                                 |
| 32      | `cron_jobs.job_name` (scheduled operation name separated from data)                                                   |
| 33      | Queue event journal and per-queue metric tables/indexes                                                               |
| 34      | `jobs.extended_options` (repeat and advanced generation policy across restart)                                        |

(Versions 2–4 are unused gaps; only the keys present in `MIGRATIONS` run.)

See [Persistence](./features/persistence.md).

---

## PostgreSQL 15–18 Schema

Source: `src/infrastructure/persistence/postgres/schema.ts`. This normalized
schema belongs to the optional server-only multi-broker engine. It is separate
from the SQLite schema above: selecting PostgreSQL never opens or migrates a
SQLite file. Every tenant-visible primary/unique key includes `namespace`, whose
default is `default`; all brokers intended to share work must use the same
namespace.

CI validates this schema on PostgreSQL 15, 16, 17, and the pinned/recommended
18.6 release. The bunqueue schema version below is independent from the
PostgreSQL server major.

Schema initialization runs inside a transaction guarded by a domain-separated
64-bit `hashtextextended` advisory key. The current
`POSTGRES_SCHEMA_VERSION` is **17**. Additive `ALTER TABLE ... ADD COLUMN IF NOT
EXISTS` statements upgrade earlier development schemas before version insertion.
The v17 migration upgrades the commit sequencer and runtime lock protocol to
length-prefixed, domain-separated 64-bit identities; old and new brokers must
not overlap during this coordinated migration. The v16 migration adds broker
session columns and exact-session lease/worker
indexes. The v15 migration also backfills `bunqueue_queue_state(namespace, queue)` from
distinct existing job rows so an empty-but-still-registered queue remains
discoverable after its last job is removed.

### `bunqueue_jobs`

Primary key: `(namespace, id)`. One row is the authoritative job generation.

| Column family  | Columns / meaning                                                                                                      |
| -------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Identity       | `namespace`, `id`, `queue`                                                                                             |
| Encoded model  | `payload BYTEA` — complete MessagePack `Job`                                                                           |
| Lifecycle      | `state`, `run_at`, `created_at`, `started_at`, `completed_at`, `attempts`, `max_attempts`, `ttl`, `timeout`, `version` |
| Ordering       | `priority`, `lifo`, `group_id`                                                                                         |
| Idempotency    | `unique_key`, `unique_expires_at`, `custom_id`                                                                         |
| Relationships  | `parent_id`                                                                                                            |
| Lease fence    | `lease_owner`, `lease_broker_id`, `lease_broker_session_id`, `lease_token`, `lease_until`, `lease_renewals`            |
| Terminal state | `result`, `dlq_entry`, `dlq_retry_state`, `error`, `failure_reason`                                                    |

`state` is constrained to `waiting`, `prioritized`, `delayed`,
`waiting-children`, `active`, `completed`, or `failed`. Paused is a queue view,
not a stored job state. `lease_token` is the opaque fencing credential;
`lease_until` is compared against the PostgreSQL clock, and `lease_renewals`
distinguishes an initial client-owned lease from one transferred by a heartbeat.

Indexes:

- `bunqueue_jobs_ready_idx(namespace, queue, priority DESC, run_at, id)` for
  pending rows;
- `bunqueue_jobs_state_idx(namespace, queue, state, created_at, id)`;
- partial `bunqueue_jobs_lease_idx(namespace, lease_until)` for active rows;
- partial `bunqueue_jobs_broker_session_lease_idx(namespace, lease_broker_id, lease_broker_session_id, id)` for exact-session shutdown cleanup;
- partial `bunqueue_jobs_parent_idx(namespace, parent_id)`;
- partial `bunqueue_jobs_group_ready_idx` and
  `bunqueue_jobs_group_active_idx` for grouped candidate/head ownership;
- partial `bunqueue_jobs_lifo_ready_idx` for the mixed-order probe;
- partial `bunqueue_jobs_ttl_pending_idx` for pending-expiry scans; and
- unique partial `bunqueue_jobs_live_unique_key_idx(namespace, queue,
unique_key)` for live states.

The common FIFO claim follows `bunqueue_jobs_ready_idx` ordering directly and
locks narrow ID/order tuples before payload retrieval. Indexed probes select the
mixed FIFO/LIFO or grouped path only when those features are present. All paths
use `FOR UPDATE SKIP LOCKED` and recheck current-row eligibility.

### Relationship and completion tables

| Table                    | Primary key                          | Purpose / important indexes                                                                                                                                                                 |
| ------------------------ | ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bunqueue_dependencies`  | `(namespace, job_id, dependency_id)` | Durable unresolved/completed dependency edges; reverse index `(namespace, dependency_id, job_id)`.                                                                                          |
| `bunqueue_completions`   | `(namespace, job_id)`                | Generation-scoped completion proof and optional result, including removed jobs; queue index `(namespace, queue, job_id)` and retention index `(namespace, completed_at DESC, job_id DESC)`. |
| `bunqueue_flow_failures` | `(namespace, parent_id, child_id)`   | Durable child failure policy (`fail`, `remove`, `ignore`, `continue`), error and timestamp; indexed by parent/mode.                                                                         |
| `bunqueue_repeat_links`  | `(namespace, original_id)`           | Exactly one successor generation per completed repeat job; successor reverse index.                                                                                                         |

Dependency evidence has its own namespace/ID advisory-lock domain. Names are
length-prefixed before `hashtextextended(..., 0)`; multi-key plans deduplicate
and sort the resulting 64-bit physical keys before acquiring them. Admission
locks every referenced dependency before reading proof or inserting an edge;
batch and flow admission lock the complete union in one statement. Completion
uses the same physical order, locks the affected parents before child rows, writes the
completion proof, then promotes resolved parents with their encoded payload,
timeline, state, version, and durable event in the same transaction. This closes
both commit orders of a concurrent late-consumer race without changing the
SQLite relationship model. The same child key serializes dynamic attachment,
detach, explicit terminal failure, and lease recovery. Those paths re-read
`parent_id` only after the child key is held, then acquire the sorted flow-parent
keys before job rows, closing the attachment TOCTOU window.

Custom-ID reuse retires an existing completion only while holding that same
identity lock and only after ID/key deduplication proves that exact candidate
will be inserted. A deduplicated candidate therefore preserves both
completion-only and retained terminal generations. Serial batches may exempt
only consumers inserted earlier in the same transaction, then recompute every
surviving inserted row against final proof and update its original `pushed`
payload without changing event order. A pre-existing live consumer still
rejects reuse. Unreferenced completion-only rows are retained newest-first up to
`maxCompletedJobs`; proofs referenced by live consumers are pinned outside the
cap. The local PostgreSQL Snapshot separately caps completed job objects at
`maxCompletedJobs` and results at `maxJobResults`, while durable child-result
queries continue to read this table directly.

Destructive commands discover IDs before locking, acquire sorted completion
identity locks, lock candidate/live-consumer rows, and then revalidate. A
producer is deletable only when each live consumer is part of the same atomic
delete set. This rule covers cancel/remove, clean/TTL/drain, DLQ limit/expiry,
cron lease cleanup, retry of completed generations, and obliterate; it prevents
an admitted `waiting-children` row from outliving both its job row and completion
proof.

### `bunqueue_queue_state`

Primary key: `(namespace, queue)`. The row serializes distributed control state:

- `paused`;
- `rate_limit`, `rate_duration_ms`, `rate_window_started_at`,
  `rate_expires_at`, and `rate_count`;
- `concurrency_limit`; and
- MessagePack `stall_config` and `dlq_config`.

Claim transactions ensure this row exists. Default-policy claims hold `FOR
SHARE`, so competing claimers can proceed while queue-control updates remain
ordered behind them. Configured rate/concurrency claims retry with `FOR UPDATE`
before calculating shared capacity. A missing row means default
unpaused/unlimited policy and is recreated idempotently. Rate-limit duration and
TTL use the same normalization as SQLite: non-positive/non-finite duration is
stored as `NULL` and claims use the effective 1,000 ms default; a
non-positive/non-finite TTL is stored as `NULL` and does not expire.

### Cron, worker, and broker coordination

| Table              | Primary key              | Stored data                                                                                                                                                                   |
| ------------------ | ------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `bunqueue_crons`   | `(namespace, name)`      | MessagePack cron payload, `next_run`, execution count, optional limit, update time; due index `(namespace, next_run, name)`.                                                  |
| `bunqueue_workers` | `(namespace, id)`        | Owning `broker_id` + `broker_session_id`, optional TCP `client_id`, queue array, MessagePack worker, `last_seen`; GIN queue and session cleanup indexes.                      |
| `bunqueue_brokers` | `(namespace, broker_id)` | Internal `session_id`, `started_at`, and `heartbeat_at` used for fail-fast duplicate detection, stale takeover, and deterministic oldest-live-session startup reconciliation. |

Cron slots and heartbeats use PostgreSQL time. `skipIfNoWorker` reads the shared
worker table. `preventOverlap` cron jobs are never resurrected as ordinary work
when their protected lease expires or their broker shuts down.

### Events, logs, and metrics

- `bunqueue_events`: `BIGSERIAL id`, namespace, queue, event type, job ID,
  PostgreSQL-clock timestamp, optional MessagePack payload, and
  `transaction_id`. Job events can include job/state, result, error/removal, DLQ
  entry, and DLQ retry state so a remote broker can update its compatibility
  snapshot without a queue scan. The
  `(namespace, transaction_id, id)` index joins immutable event rows to their
  commit envelope; physical-ID indexes remain for trimming and diagnostics.
- `bunqueue_event_prune_watermarks`: namespace, queue, source event ID, and the
  highest physical event ID pruned through, plus `transaction_id`, `commit_seq`,
  cumulative `pruned_commit_seq`, and the transient
  `prunes_current_transaction` marker. The primary key
  `(namespace, queue, source_event_id)` preserves concurrently committed
  checkpoints. Windowed reads retain the maximum pruned-commit frontier even
  when a later physical ID belongs to an older commit; dominated physical
  checkpoints are compacted transactionally.
- `bunqueue_event_commit_seq`: a global PostgreSQL sequence fixed at `CACHE 1`.
  A deferred constraint trigger first takes a transaction advisory lock derived
  from the namespace and then calls `nextval`, so same-namespace values follow
  commit eligibility without updating a shared heap row. `CACHE 1` is a
  correctness guard: per-session sequence blocks could otherwise be consumed
  out of order by different broker connections. Rollbacks may leave harmless
  gaps.
- `bunqueue_event_commits`: one durable commit envelope per
  `(namespace, transaction_id)`, registered by statement transition triggers.
  The deferred trigger stamps only this compact row and any prune watermark;
  event rows remain immutable and replay joins through the envelope's indexed
  `commit_seq`. Envelopes with no remaining event or watermark reference are
  collected in bounded batches at startup and by adaptive maintenance that
  retries rapidly while a backlog remains.
- `bunqueue_job_logs`: `BIGSERIAL id`, namespace/job ID, timestamp, constrained
  level (`info`, `warn`, `error`), and message; indexed by namespace/job/id.
  Writers and retention/clear operations lock the owning `bunqueue_jobs` row,
  which serializes them with job removal and makes the retained maximum exact.
- `bunqueue_metric_buckets`: one completed/failed count per
  `(namespace, queue, metric_type, minute)`.
- `bunqueue_metric_totals`: durable cumulative total and previous-sample fields
  per `(namespace, queue, metric_type)`.

A transaction writes its lifecycle event and `pg_notify` wake-up before commit.
The v13 deferred sequencer assigns one `commit_seq` to its commit envelope and
watermarks immediately before commit. The advisory lock makes that sequence
commit-ordered within each namespace, while unrelated namespaces do not contend
on a heap row. LISTEN/NOTIFY is only a hint; brokers join the envelopes to
`bunqueue_events` after their `(commit_seq, id)` cursor. Event
pruning, including explicit trimming and batches larger than the retained
window, records a cumulative per-queue pruned-commit frontier in the same
transaction. A broker refreshes only when its per-queue applied commit cursor is
behind discarded history. Explicit trim derives its frontier from the deleted
rows' already-stamped commit envelopes, so a current broker is not invalidated
by the trim transaction itself. Physical `BIGSERIAL` allocation order is never
treated as commit order.

### `bunqueue_schema_migrations`

Primary key: `version`; `applied_at` stores the application timestamp for that
schema version. The schema version is database-global because all bunqueue
namespaces share the same physical tables. PostgreSQL engine schema v17 upgrades
the advisory-lock protocol, v16 adds broker-session fencing, v15 backfills
durable queue registry rows, v14 adds
bounded-completion query indexes, and v13 adds the commit-ordered journal.
Initialization rejects a recorded version newer than the runtime and verifies
the journal tables, columns, indexes, functions, and
enabled triggers before skipping DDL. The guard verifies definitions rather than
names alone: sequence type/bounds/increment/cache/cycle, column
types/defaults/nullability, ordered index expressions and predicates, trigger
timing/transition tables/function bindings, and normalized PL/pgSQL bodies. A
semantically unchanged schema stays on the no-DDL path; drift is repaired under
the migration lock. The same catalog gate requires
`bunqueue_jobs_live_unique_key_idx` to be an exact three-key partial unique
`btree`, including its live-state predicate and lack of expressions or included
columns. A same-name weaker index is rebuilt transactionally. Existing duplicate
live keys make initialization fail and roll back without deleting or merging
rows, preserving a fail-closed operator recovery boundary.

See [PostgreSQL 15–18 Multi-Broker Persistence](./features/postgres-multibroker.md)
for transaction boundaries, lease fencing, recovery, runtime configuration, and
the dedicated PostgreSQL validation suite.

## In-Memory Collections & Bounds

Defined on `QueueManagerState` (`src/application/queue-manager/state.ts`), with
its context interfaces in `src/application/types/contexts.ts`, and sized by
`DEFAULT_CONFIG` (`src/application/types/config.ts`). Cleanup runs every `cleanupIntervalMs`
(default 10 s).

| Collection                  | Type                                           | Max                       | Eviction                                     |
| --------------------------- | ---------------------------------------------- | ------------------------- | -------------------------------------------- |
| `jobIndex`                  | `Map<JobId, JobLocation>`                      | unbounded*                | follows job lifecycle (no cap)               |
| `completedJobs`             | `BoundedSet<JobId>`                            | 50,000                    | FIFO, **10% batch**                          |
| `depCompletions`            | `DependencyCompletionTracker`                  | 50,000 recent + live pins | exact FIFO; pins released with reverse edges |
| `jobResults`                | `LRUMap<JobId, unknown>`                       | 10,000                    | LRU (1 entry on overflow)                    |
| `jobLogs`                   | `LRUMap<JobId, JobLogEntry[]>`                 | 10,000                    | LRU                                          |
| `customIdMap`               | `LRUMap<string, JobId>`                        | 50,000                    | LRU                                          |
| `timedOutJobs`              | `BoundedMap<JobId, RetiredTimeoutGeneration>`  | 50,000                    | FIFO batch                                   |
| `retiredTimeoutLeaseTokens` | `BoundedMap<string, RetiredTimeoutGeneration>` | 50,000                    | FIFO batch                                   |
| `waitingDeps`               | per-shard map                                  | unbounded*                | follows live dependency waiters              |
| `telemetryJournal.events`   | per-queue arrays                               | 10,000 each               | oldest event first                           |
| terminal metric buckets     | per queue/type map or SQLite rows              | 20,160 each               | minutes older than newest window             |

\* `jobIndex` and `waitingDeps` are keyed by live jobs; entries are removed with
their lifecycle or dependency edges rather than capped by size. The
`maxWaitingDeps` compatibility option has a default of `10_000` but is not
currently an admission or eviction limit. The other caps above reflect the
source defaults in `src/application/types/config.ts` (for example
`maxJobResults: 10_000`), matching the architecture summary in `CLAUDE.md`.

**Eviction policies:**

- `BoundedSet` (`src/shared/boundedSet.ts`) — pure FIFO, no recency tracking.
  When `size >= maxSize`, `evictBatch()` removes `floor(maxSize * 0.1)` oldest
  entries at once (`boundedSet.ts:22, 30-49`) to amortize iterator cost. Used by
  `completedJobs`.
- `BoundedMap` (`src/shared/boundedMap.ts`) uses the same bounded FIFO batch
  policy for timeout-generation records. The job-ID map retains the latest
  generation, while the token-keyed map can recognize multiple retired leases
  for one reused/retried ID.
- `DependencyCompletionTracker` (`src/application/dependencyCompletions.ts`) —
  exact FIFO eviction for at most `maxCompletedJobs` recent bare IDs plus a
  separate set for proofs owned by live dependency edges. Eviction deletes
  only an unpinned SQLite row; pin reconciliation is driven by reverse indexes.
- `LRUMap` (`src/shared/lruMap.ts`) — doubly-linked list, O(1)
  `moveToFront`. On `set` over capacity it evicts the single tail (least
  recently used) node and fires the optional `onEvict` callback
  (`lruMap.ts:85-106`). Used by `jobResults`, `jobLogs`, `customIdMap`.

See [Data Structures](./features/data-structures.md),
[Core Queue Engine](./features/core-queue-engine.md).

---

## Workflow execution model

The Bun-only workflow engine owns a separate synchronous SQLite store in
`src/client/workflow/`. It is not part of the TCP command model and is not
shared with the external SDKs.

### Public execution types

```typescript
interface Execution {
  id: string; // wf_ + 32 lowercase hex digits
  workflowName: string;
  state: 'running' | 'waiting' | 'completed' | 'failed' | 'compensating' | 'compensation-stuck';
  input: unknown;
  steps: Record<string, StepRecord>;
  currentNodeIndex: number;
  resolvedSteps?: string[];
  decisions?: Record<string, unknown>;
  definitionHash?: string;
  rollbackStatus?: 'completed' | 'not-applicable' | 'stuck';
  failureReason?: string;
  committedAt?: number;
  signals: Record<string, unknown>;
  parentExecutionId?: string;
  createdAt: number;
  updatedAt: number;
}

interface ExecutionListOptions {
  limit?: number; // default 100, integer 1..1000
  offset?: number; // default 0, non-negative safe integer
}
```

Execution IDs use 16 bytes from Web Crypto in the real runtime. Existing IDs
are opaque strings, so rows using the previous timestamp/random format remain
valid and require no migration. The simulated workflow clock has a separate
seeded entropy stream so IDs replay without perturbing retry jitter.

`decisions` is the durable control-flow journal. Keys identify branch choices,
loop conditions, `forEach` item snapshots, and sub-workflow input mapping by
node/iteration. The decision is written before selected effects run.

`definitionHash` is the SHA-256 identity of the sealed workflow name, explicit
revision, graph shape and scheduling options. Handler closure bodies cannot be
hashed safely; callers bump `new Workflow(name, { revision })` when semantics
change without a structural graph change. Legacy rows without a hash bind to
the first registered definition used to recover them.

`StepRecord` carries lifecycle timestamps, cumulative attempts, result/error,
forward idempotency identity, loop item/index/occurrence, child ownership and
one terminal compensation outcome. `map` nodes use the same
`running`/`completed`/`failed` record lifecycle even though they have no retry
or compensation handler.

### Workflow SQLite tables

`workflow_executions`:

| Column               | Type               | Meaning                                  |
| -------------------- | ------------------ | ---------------------------------------- |
| `id`                 | `TEXT PRIMARY KEY` | Opaque execution ID                      |
| `workflow_name`      | `TEXT NOT NULL`    | Registered definition name               |
| `state`              | `TEXT NOT NULL`    | `ExecutionState`                         |
| `input`              | `BLOB`             | MessagePack workflow input               |
| `steps`              | `BLOB`             | MessagePack `Record<string, StepRecord>` |
| `current_node_index` | `INTEGER NOT NULL` | Durable graph cursor                     |
| `resolved_steps`     | `BLOB`             | Selected branch step names               |
| `signals`            | `BLOB`             | First-writer-wins signal payload map     |
| `created_at`         | `INTEGER NOT NULL` | Creation time in milliseconds            |
| `updated_at`         | `INTEGER NOT NULL` | Last persisted transition                |
| `meta`               | `BLOB NULL`        | Version-tolerant lifecycle metadata      |

`meta` currently packs `rollbackStatus`, `failureReason`, `committedAt`,
`parentExecutionId`, `decisions`, and `definitionHash`. It was added with a
guarded `ALTER TABLE` so pre-meta databases remain readable.

`workflow_executions_archive` has the same columns plus
`archived_at INTEGER NOT NULL`. Archive copies and deletes up to 1000 rows in
one transaction, ordered by `updated_at ASC, id ASC`.

Indexes on the live table:

- `idx_wf_name(workflow_name)`
- `idx_wf_state(state)`
- `idx_wf_created(created_at DESC, id DESC)`
- `idx_wf_name_created(workflow_name, created_at DESC, id DESC)`
- `idx_wf_state_created(state, created_at DESC, id DESC)`
- `idx_wf_name_state_created(workflow_name, state, created_at DESC, id DESC)`
- `idx_wf_state_updated(state, updated_at ASC, id ASC)`

All execution-list filter combinations apply `WHERE`, then the total order
`created_at DESC, id DESC`, then `LIMIT/OFFSET`. The ID tie-breaker makes a
static dataset deterministic. Offset pagination is not a snapshot: concurrent
insertion can shift a later offset.

The `signals` column has a single writer, `SignalCoordinator`. General
execution updates never rewrite it from a stale in-memory snapshot. Signal
acceptance is first-writer-wins, and payload insertion plus the
`waiting -> running` resume claim occur in one immediate transaction.

## Serialization

Two layers:

### MessagePack (on-disk + on-wire)

`src/shared/msgpack.ts` owns the canonical codec used by TCP, the CLI/client,
and SQLite serialization. The common path uses msgpackr directly. A frame/blob
containing `__proto__` is decoded as maps and materialized with
`Object.defineProperty`, preserving `__proto__` and `__proto_` as distinct own
data properties without invoking prototype setters. The TCP transport frames
the same msgpack-encoded `Command`/`Response` objects.

- `pack(data)` → `Uint8Array`; written into BLOB columns. The single-row and
  batch insert binders only pack non-empty arrays
  (`depends_on`/`children_ids`/`tags`/`timeline` are stored as `NULL` when empty
  — `persistence/sqlite/jobs.ts:31-67`, `persistence/batchInsert.ts:95-129`).
- `unpack(buffer, fallback, context)` decodes with a try/catch that logs and
  returns the fallback on corruption (`sqliteSerializer.ts:19-27`).
- `rowToJob` (`sqliteSerializer.ts:71-151`) rebuilds a full `Job`, applying
  defaults for the non-persisted fields and re-branding ids.
- **Corrupt-dependency safety:** a `depends_on` blob that fails to decode is
  _not_ silently turned into `[]` (which the recovery path would treat as
  "ready, no deps" → out-of-order execution). `decodeDependsOn`
  (`sqliteSerializer.ts:54-68`) flags corruption and `rowToJob` stamps the job
  with the non-enumerable `CORRUPT_DEPENDS_ON` symbol (`sqliteSerializer.ts:42-47`)
  so recovery routes it to the DLQ.
- `reconstructDlqEntry` (`sqliteSerializer.ts:167-180`) re-brands ids and
  restores `stacktrace: null` for pre-#74 blobs.
- `brandId` (`sqliteSerializer.ts:162-164`) preserves the decoded runtime type
  (only stringifies genuine non-strings) so id equality survives the
  loss → DLQ → restart path.

### JSON (HTTP/CLI output)

`src/shared/serialization.ts` handles human-facing/HTTP JSON:

- `serializeJob` (`serialization.ts:12-45`) projects a `Job` into a plain object,
  stringifying `id`/`parentId`/`dependsOn`/`childrenIds` (BigInt-safe).
- `bigIntReplacer` + `jsonStringify` (`serialization.ts:57-69`) emit BigInt as
  strings so `JSON.stringify` never throws.

See [Persistence](./features/persistence.md),
[TCP Protocol](./features/tcp-protocol.md).
