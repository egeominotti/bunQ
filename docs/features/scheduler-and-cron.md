# Scheduler & Cron

> **Category:** Scheduling · **Source:** `src/infrastructure/scheduler/cron/`, `src/infrastructure/scheduler/cronParser.ts`, `src/domain/types/cron.ts`, `src/client/queue/scheduler.ts`, `src/client/queue/schedulerPagination.ts`

## Purpose

The scheduler is the server-side engine that owns recurring/repeatable jobs ("crons"). Given either a 5–6 field cron expression (with optional IANA timezone) or a fixed millisecond interval (`repeatEvery`), it computes each job's next run time and pushes a fresh job onto the target queue when it falls due. The memory/SQLite engine is event-driven through `CronScheduler` and persists execution count/next run to SQLite. PostgreSQL multi-broker mode instead locks due `bunqueue_crons` rows, uses database time, and atomically advances the slot before job admission. The matching client surface (`upsertJobScheduler`, `every`, `cron`) is unchanged across server backends.

## Responsibilities & Scope

Owns:

- In-memory cron registry keyed by name, ordered by `nextRun` via a min-heap for
  `O(k log n)` ticks (`infrastructure/scheduler/cron/runtime.ts:22-25`).
- Cron expression validation, shortcut expansion, and next-run computation (delegated to `cronParser.ts`, which wraps the `croner` library).
- Firing due crons: limit checks, overlap/`skipIfNoWorker`/`preventOverlap`
  gating, and the push callback that creates each spawned job
  (`infrastructure/scheduler/cron/execution.ts:9-140`).
- Persisting cron state before pushing (executions + nextRun) to guarantee
  at-most-once-per-slot on crash/retry
  (`infrastructure/scheduler/cron/execution.ts:69-90`).
- Restart recovery: recalculating past `nextRun` values forward and
  (optionally) skipping missed runs
  (`infrastructure/scheduler/cron/runtime.ts:126-144`).
- The client-side scheduler operations and name-prefixing for namespaced queues (`scheduler.ts`).

Does NOT own (delegated elsewhere):

- The actual job push / enqueue mechanics — done via the `PushJobCallback` wired to `QueueManager.push` ([Job Lifecycle](./job-lifecycle.md), [Core Queue Engine](./core-queue-engine.md)).
- SQLite reads/writes — the scheduler only invokes callbacks; the `cron_jobs` table, statements, and migrations live in [Persistence](./persistence.md).
- **Delayed-job promotion** (a delayed one-off job becoming `waiting` when its `run_at` arrives). This is NOT handled here; it is a property of the `PriorityQueue` ordering by `runAt` and the push/dependency paths, not the cron scheduler. See [Data Structures](./data-structures.md) and [Job Lifecycle](./job-lifecycle.md).
- Deduplication / unique-key enforcement on spawned jobs — the scheduler only supplies the `uniqueKey`/`dedup` fields; enforcement is in [Deduplication & Unique Jobs](./deduplication-and-unique.md).
- Worker registration that `skipIfNoWorker` consults — see [Worker Registry & Management](./workers-management.md).

## Dependencies

Internal:

- `src/domain/types/cron.ts` — `CronJob`, `CronJobInput`, `CronJobOptions`, `CronDedup`, `createCronJob`, `isAtLimit`.
- `src/infrastructure/scheduler/cronParser.ts` — `validateCronExpression`, `getNextCronRun`, `getNextIntervalRun`, `expandCronShortcut` (+ unused-by-scheduler helpers `isDue`, `describeCron`).
- `src/shared/minHeap.ts` — `MinHeap` for the next-run-ordered heap.
- `src/shared/logger.ts` — `cronLog`.
- `QueueManager` wires push, persistence, and worker-presence callbacks in
  `application/queue-manager/state.ts`; dashboard wiring and the public cron
  methods live in `application/queue-manager/services.ts`.
- `src/client/manager.ts` (`getSharedManager`) and `src/client/tcpPool.ts` for the embedded vs TCP split in `scheduler.ts`.
- `src/client/queue/schedulerPagination.ts` provides the shared deterministic
  ordering and inclusive range operation for embedded and TCP listings.

External / runtime:

- **`croner`** — the only external runtime dependency in this module; all cron-expression parsing and next-run math runs through `new Cron(expression, { timezone })` (`cronParser.ts:6,16,35`).
- Bun/Node timers: `setTimeout` (precise wake) + `setInterval` (60s safety fallback).
- SQLite via the persistence layer (indirect, through callbacks).

## Public Interface

### Server-side class — `CronScheduler` (`cronScheduler.ts:10`)

`CronScheduler` is the public façade over `CronExecution` and `CronRuntime`;
the callbacks and registry methods below live in `cron/runtime.ts`, while
`tick`, `fireCronJob`, and `getStats` live in `cron/execution.ts`. Callback
types are isolated in `types/cronScheduler.ts`.

```typescript
class CronScheduler {
  constructor(_config?: CronSchedulerConfig); // config kept for back-compat, unused
  setPushCallback(callback: PushJobCallback): void;
  setPersistCallback(callback: PersistCronCallback): void;
  setWorkerCheckCallback(callback: (queue: string) => boolean): void;
  setDashboardEmit(callback: (event: string, data: Record<string, unknown>) => void): void;
  start(): void;
  stop(): void;
  add(input: CronJobInput): CronJob; // also performs upsert
  remove(name: string): boolean; // O(1) lazy deletion
  get(name: string): CronJob | undefined;
  list(): CronJob[];
  load(crons: CronJob[]): void; // restart recovery
  getStats(): { total: number; pending: number; nextRun: number | null };
}

type PushJobCallback = (queue: string, input: JobInput) => Promise<void>;
type PersistCronCallback = (name: string, executions: number, nextRun: number) => void;
```

The `QueueManager` re-exposes these as `addCron(input)`, `removeCron(name)`,
`getCron(name)`, and `listCrons()` (`application/queue-manager/services.ts`).

### Parser functions (`cronParser.ts`)

```typescript
validateCronExpression(expression: string, timezone?: string): string | null; // null = valid
getNextCronRun(expression: string, fromTime?: number, timezone?: string): number; // ms epoch
getNextIntervalRun(intervalMs: number, lastRun?: number): number;                 // lastRun + intervalMs
expandCronShortcut(expression: string): string; // @daily, @hourly, ...
describeCron(expression: string): string;        // human-readable (not used by scheduler)
```

`CRON_SHORTCUTS` (`cronParser.ts:63-71`): `@yearly`/`@annually` → `0 0 1 1 *`, `@monthly` → `0 0 1 * *`, `@weekly` → `0 0 * * 0`, `@daily`/`@midnight` → `0 0 * * *`, `@hourly` → `0 * * * *`.

### Client SDK (`scheduler.ts`, surfaced on `Queue`)

```typescript
Queue.upsertJobScheduler(schedulerId: string, repeatOpts: RepeatOpts, jobTemplate?: JobTemplate<T>): Promise<SchedulerInfo | null>;
Queue.removeJobScheduler(schedulerId: string): Promise<boolean>;
Queue.getJobScheduler(schedulerId: string): Promise<SchedulerInfo | null>;
Queue.getJobSchedulers(start?, end?, asc?): Promise<SchedulerInfo[]>;
Queue.getJobSchedulersCount(): Promise<number>;
```

`RepeatOpts` (`scheduler.ts:40-55`): `pattern?`, `every?`, `limit?`, `immediately?`, `count?`, `prevMillis?`, `offset?`, `jobId?`, `timezone?`, `skipMissedOnRestart?`, `skipIfNoWorker?`, `preventOverlap?`. `limit` is forwarded to the cron definition as `maxLimit` (the run cap; see #111) and echoed back on `SchedulerInfo.limit`. (`count`, `prevMillis`, `offset`, `jobId` are still accepted for BullMQ shape compatibility but have no cron-side equivalent, so they are not forwarded.)

`getJobSchedulers` orders definitions by their next execution time. `start` and
`end` are zero-based inclusive offsets (`end: -1` means the end of the list),
and `asc` defaults to `false`, matching BullMQ. Equal execution times use the
scheduler ID as a deterministic tie-breaker in the requested direction. The
same client-side range operation is applied to embedded and TCP results.

### Simple-mode helpers (`src/client/bunqueue.ts:58-90`)

```typescript
Bunqueue.cron(id, pattern, data?, opts?: { timezone?; limit?; jobOpts? }): Promise<SchedulerInfo | null>;
Bunqueue.every(id, intervalMs, data?, opts?: { limit?; jobOpts? }): Promise<SchedulerInfo | null>;
Bunqueue.removeCron(id); Bunqueue.listCrons();
```

Both delegate to `Queue.upsertJobScheduler` — `cron` sets `pattern`, `every` sets the `every` interval.

### Completion-chained repeats (`Queue.add`)

`Queue.add(..., { repeat })` creates a completion-chained repeat rather than a
named `CronJob`. The directly added generation runs normally; after each
successful completion, `repeatJobs.ts` calculates one future deadline with the
same `croner` parser and shortcut expansion used by `CronScheduler`. Pattern
chains respect `tz`, `startDate`, `endDate`, `offset`, `prevMillis`, `limit`,
`immediately`, and `repeat.jobId`. Interval chains skip elapsed slots instead of
creating a catch-up loop. `limit: N` retains the established contract of the
initial generation plus at most N successors.

`offset` shifts the cron tick itself: the parser is queried from
`now - offset` (and `prevMillis - offset` for later generations), so a positive
offset does not skip a base tick whose shifted deadline is still future and a
negative offset cannot turn the next deadline into an immediate/past run.
Interval repeats apply the offset to the first successor only, advance by whole
`every` slots when that deadline has elapsed, and then remain anchored to
`prevMillis`; `immediately` never makes every successor immediate.

The successor copies retry, timeout, retention, stall, logging, size, priority,
group, failure-policy, deduplication, debounce, and durable-write settings while
resetting execution-owned fields such as attempts, progress, timeline, and the
generation timestamp. `updateData()` on a completed generation follows the
repeat-chain link to the pending successor. Parent/dependency relationships and
outer `JobOptions.jobId` are rejected before insertion because reusing those
identities across independent generations is ambiguous; `repeat.jobId` remains
the compatible repeat metadata field.

SQLite `jobs.extended_options` persists the repeat definition, object backoff,
advanced options, dedup/debounce policy, and durable flag. A delayed successor
therefore recovers with the same policy and can continue the chain after a
broker restart.

### TCP commands handled

- `Cron` — create/upsert a cron (`handlers/cron.ts:13-65`, routed at `handler-routes/control.ts:93-106`).
- `CronGet` — fetch one cron by name (`handlers/cron.ts:68-92`).
- `CronDelete` — remove a cron (`handlers/cron.ts:95-103`).
- `CronList` — list all crons (`handlers/cron.ts:106-127`).

Command shapes in `src/domain/types/commands/cron.ts:4-33`.

### HTTP / CLI / MCP

- HTTP cron routes proxy to the same handlers (`httpRouteResources.ts:31-82`:
  `CronList`, `Cron`, `CronGet`, `CronDelete`).
- In PostgreSQL mode, TCP/HTTP `DashboardOverview` and the periodic
  WebSocket/SSE stats snapshot read `listCronsDurable()`, so a schedule created
  through broker A is visible immediately from broker B even before its local
  compatibility snapshot catches up. Memory/SQLite keeps the synchronous local
  scheduler list.
- CLI: `bunqueue cron list | add | delete` (`cli/commands/cron.ts`). `add` requires `--queue` + `--data` and one of `--schedule`/`--every`; rejects `--every <= 0` and `--max-limit < 0` (`cron.ts:55-89`).
- MCP tools `bunqueue_add_cron` / `bunqueue_list_crons` /
  `bunqueue_get_cron` / `bunqueue_delete_cron` (`mcp/tools/cronTools.ts`).

### Dashboard events emitted (via `setDashboardEmit`)

`cron:created`, `cron:updated`, `cron:deleted` (from the TCP handler),
`cron:fired`, `cron:skipped` (reason `no-worker` or `overlap`), and
`cron:missed` (persist or push failure) —
`infrastructure/scheduler/cron/execution.ts:47-101,120-140` and
`handlers/cron.ts:39-45,95-102`.

## Data Models

Canonical definition is `CronJob` (`src/domain/types/cron.ts:29-55`). See [data-model](../data-model.md) for the full schema. Most relevant fields:

| Field                    | Type                           | Notes                                                                                        |
| ------------------------ | ------------------------------ | -------------------------------------------------------------------------------------------- |
| `name`                   | `string`                       | PRIMARY KEY in `cron_jobs`; global, so multi-prefix queues must namespace (see Concurrency). |
| `jobName`                | `string`                       | Name assigned to every spawned Job; stored separately from user `data`.                      |
| `queue`                  | `string`                       | target queue for spawned jobs.                                                               |
| `schedule`               | `string \| null`               | cron expression (mutually fills with `repeatEvery`).                                         |
| `repeatEvery`            | `number \| null`               | fixed interval in ms.                                                                        |
| `priority`               | `number`                       | spawned-job priority (default `0`).                                                          |
| `timezone`               | `string \| null`               | IANA tz for `schedule`.                                                                      |
| `nextRun` / `executions` | `number`                       | mutable runtime state, persisted.                                                            |
| `maxLimit`               | `number \| null`               | `null`/0/negative ⇒ unlimited (`cron.ts:104-106`).                                           |
| `uniqueKey` / `dedup`    | dedup config for spawned jobs. |
| `skipMissedOnRestart`    | `boolean`                      | **default `true`** in `createCronJob` (`cron.ts:109`).                                       |
| `skipIfNoWorker`         | `boolean`                      | default `false`.                                                                             |
| `preventOverlap`         | `boolean`                      | default `true`.                                                                              |
| `jobOptions`             | `CronJobOptions \| null`       | per-cron retry/cleanup policy (issue #86).                                                   |

`CronJobOptions` (`cron.ts:19-27`): `maxAttempts`, `backoff`, `timeout`, `delay`,
`stallTimeout`, `removeOnComplete`, `removeOnFail`. `SchedulerInfo`
(`scheduler.ts:64-72`): `{ id, name, next, pattern?, every?, limit? }`
(`limit` = the persisted `maxLimit` run cap, `undefined` when unlimited; #111).
The `cron_jobs` SQLite table is defined at `schema.ts:125-144`; migrations 6,
8-10, 12, and 32 added deduplication, skip/overlap policy, job options, and the
first-class spawned-job name.

## Business Logic / Control Flow

### Add / upsert (`add`, `cron/runtime.ts:79-107`)

1. Reject if neither `schedule` nor `repeatEvery` is set.
2. If `schedule`, expand shortcut then `validateCronExpression`; throw on invalid.
3. Compute `nextRun` via `getNextCronRun` (cron) or `getNextIntervalRun` (interval) from `now`.
4. `createCronJob` builds the immutable record. If a cron with the same name
   exists, **preserve its `executions`** and clear `lastFiredAt` for that name
   (`cron/runtime.ts:94-99`, fixes H10).
5. `immediately` sets `nextRun = now` **only on first creation**, never on
   upsert (so it doesn't clobber restart-skip logic) (`cron/runtime.ts:100`).
6. Assign a fresh `generation`, store in map + heap, then `scheduleNext()`.

At the manager layer, `addCron` first removes an orphaned queued job when
`preventOverlap` is set, so a stale job from a disconnect window is not
immediately consumed by the new worker (`application/queue-manager/services.ts`,
#73). It then persists via `storage.saveCron`.

### Event-driven wake (`scheduleNext`, `runtime.ts`)

Clears the current timer, pops stale heap entries (generation mismatch), then
arms one `setTimeout` for the soonest live cron. Runtime timers cannot represent
delays above `2_147_483_647ms`, so a farther `nextRun` is reached through bounded
timer chunks. Each intermediate wake runs the normal due-time guard and rearms
from the unchanged absolute `nextRun`; it does not persist, increment, or push.
This prevents Bun from coercing a far-future timer to `1ms` and hot-looping.
The timer is rearmed after every mutation (add/remove/load/tick).

### Tick / fire (`tick`, `cron/execution.ts:9-108`)

1. Drain heap entries whose `nextRun <= now` (`O(k log n)`).
2. Skip stale entries (generation mismatch from a removed/updated cron).
3. If `isAtLimit`, mark for removal from the map and drop.
4. **Compute `newExecutions` and `newNextRun` before pushing.** Interval crons anchor `newNextRun` to the _scheduled_ slot, not wall-clock now, so a slow job does not cumulatively drift the schedule (M4).
5. **Skip gating BEFORE the budget increment** (`getSkipReason`): `skipIfNoWorker` (no workers registered for the queue → reason `no-worker`) and overlap detection (last fire for this name within `interval * 0.8`, `interval = repeatEvery ?? 60000` → reason `overlap`). A skipped fire advances `nextRun` (persist best-effort, in-memory always) and emits `cron:skipped`, but does **not** touch `executions`: skips never consume the `maxLimit` budget. Before this ordering, a `skipIfNoWorker` cron with no worker burned its entire budget with zero deliveries. Corollary: a `skipIfNoWorker` cron with a `maxLimit` whose queue never gets a worker defers forever — it never increments, never reaches its cap, and never self-removes; it keeps waking the scheduler once per interval until a worker appears or the cron is deleted.
6. **Persist state first** (`persistCron` with `newExecutions`). If persist throws: emit `cron:missed`, do **not** push, re-insert to retry next tick.
7. Update in-memory `executions`/`nextRun`, then `fireCronJob`. If the push throws: state was already persisted, the job for this slot is lost, emit `cron:missed`, re-insert to continue (no duplicate on retry).
8. Re-insert processed entries, delete limit-reached names, `scheduleNext()`.

### Fire (`fireCronJob`, `cron/execution.ts:120-140`)

- Skip decisions live in `getSkipReason` (see tick step 5), not here: by the time `fireCronJob` runs the push is committed.
- **`preventOverlap`**: when no explicit `uniqueKey`, auto-derives `cron:<name>` so the dedup layer blocks a new push while the prior job is still active.
- Push with `data`, `priority`, `uniqueKey`, `dedup`, and the `jobOptions` subset; record `lastFiredAt`; emit `cron:fired`.

### Restart recovery (`load`, `cron/runtime.ts:126-144`)

For each loaded cron, if (`skipMissedOnRestart || skipIfNoWorker`) and `nextRun < now`, recompute `nextRun` forward and persist it (fixes #73). Since `skipMissedOnRestart` defaults to `true`, missed runs are skipped by default. Heap is rebuilt with `buildFrom` in `O(n)`.

In PostgreSQL mode, startup reconciliation runs only when the namespace has no
other active broker. It collapses an overdue skipped schedule to its next future
slot, preventing a newly added broker from consuming another broker's schedule.
Due rows are locked with `SKIP LOCKED`; `skipIfNoWorker` reads the shared worker
table, and expired/shutdown `preventOverlap` generations are discarded instead
of requeued. See
[PostgreSQL 18.6 Multi-Broker Persistence](./postgres-multibroker.md).

### Client upsert mapping (`upsertJobScheduler`, `scheduler.ts`)

Builds cron `data` from the template (`buildCronData`), merges queue `defaultJobOptions` under per-scheduler `opts` into `CronJobOptions` (`buildCronJobOptions`, issue #86), extracts dedup from `opts.deduplication`, derives spawned-job `priority` from `opts.priority`/queue default (carried on the top-level field the handler reads), and namespaces the id via `toCronName`. Embedded mode calls `manager.addCron` (timezone defaults to `'UTC'`); TCP mode sends the `Cron` command. `removeJobScheduler`/`getJobScheduler(s)` mirror this over `CronDelete`/`CronList`.
The immediate upsert result preserves the scheduler's normalized `pattern` or
`every` field and exact `nextRun`. Embedded mode uses the `CronJob` returned by
`manager.addCron`; TCP uses the broker's nested `cron` response. Both therefore
match a subsequent `getJobScheduler()` read without approximating pattern
schedules as 60-second intervals.

## Concurrency & Locking

- The scheduler holds **no shard/jobIndex locks**; it runs on the single JS event loop and mutates only its own `Map`/`MinHeap`. Concurrency safety comes from the single-threaded tick.
- **Lazy deletion via generation numbers**: `remove`/upsert just bumps the map;
  stale heap entries are detected by generation mismatch and skipped/popped in
  `scheduleNext`/`tick`, giving `O(1)` removal without heap rebuilds
  (`cron/runtime.ts:95-115,146-168`, `cron/execution.ts:16-22`).
- **Persist-before-push ordering** is the cross-process invariant: persisting
  `nextRun`/`executions` before enqueuing means a crash between the two yields
  at most a lost job for that slot, never a duplicate
  (`cron/execution.ts:69-90`).
- Cron names are a **global PRIMARY KEY** (`schema.ts:104`). The client prefixes scheduler ids with `prefixKey` so two queues with different prefixes cannot collide (`scheduler.ts:21-37`).
- `preventOverlap`/overlap-window guard against concurrent in-flight runs of the same cron across workers; enforcement is via uniqueKey dedup in the push path.

## Edge Cases & Failure Modes

- **Persist failure** → job not pushed, `cron:missed` emitted, retried next tick (no duplicate).
- **Push failure** → state already persisted, slot's job lost, `cron:missed` emitted, scheduling continues.
- **`maxLimit` exhaustion** → cron removed from the in-memory map on the tick that would exceed it (`isAtLimit`); `0`/negative limits are normalized to "unlimited" everywhere (`cron.ts:97-99`, `cli/commands/cron.ts:81-89`).
- **Timer drift / missed `setTimeout`** → the 60s `SAFETY_FALLBACK_MS` interval
  re-runs `tick` as a backstop (`cron/runtime.ts:17-18,58-65`).
- **Far-future timer** → delays above the runtime's signed 32-bit timer ceiling
  are chunked without changing the persisted absolute `nextRun`; no early
  execution or 1ms hot loop occurs.
- **Overlap from slow jobs** → suppressed via the `interval * 0.8` window and `preventOverlap` uniqueKey; interval-rate crons anchor to the scheduled slot to avoid drift.
- **`every <= 0`** → CLI rejects it; otherwise `getNextIntervalRun` would always be in the past and fire every tick (`cli/commands/cron.ts:70-75`).
- **Restart with past `nextRun`** → recalculated forward; missed runs skipped by default (`skipMissedOnRestart` default `true`).
- **Upsert preserves `executions`** and resets `lastFiredAt` (H10) so a redefined cron isn't blocked by the prior definition's last fire.
- **#103 (fixed 2.8.14)**: with `skipIfNoWorker: true`, after a TCP reconnect the cron silently stops firing if the worker is not re-registered — `fireCronJob` sees no worker and emits `cron:skipped` instead of pushing. The fix re-registers workers on reconnect; `Worker.resume()` alone does not recover it. Upgrade clients to ≥ 2.8.14.
- **No-storage mode**: with no `storage` wired, `persistCron` is null and crons
  are purely in-memory (lost on restart) — the persist guard is skipped
  (`cron/execution.ts:69-85`).

## Configuration

- `CronSchedulerConfig.checkIntervalMs` — **deprecated/no-op**; the scheduler
  uses precise `setTimeout` (`types/cronScheduler.ts:4-7`,
  `cron/runtime.ts:38-40,146-168`).
- `SAFETY_FALLBACK_MS = 60_000` — internal constant, not env-configurable.
- `MAX_TIMER_DELAY_MS = 2_147_483_647` — internal timer-chunk ceiling, not
  env-configurable; it does not cap cron intervals or persisted timestamps.
- Per-cron knobs (via `CronJobInput`/`RepeatOpts`): `schedule`/`repeatEvery`, `timezone` (embedded default `'UTC'`), `priority`, `maxLimit`, `uniqueKey`/`dedup`, `skipMissedOnRestart` (default `true`), `skipIfNoWorker` (default `false`), `preventOverlap` (default `true`), `immediately`, and `jobOptions` (`maxAttempts`/`backoff`/`timeout`/`delay`/`stallTimeout`/`removeOnComplete`/`removeOnFail`).
- Queue-level `defaultJobOptions` feed `buildCronJobOptions` as the base,
  overridden by the per-scheduler template `opts` (`scheduler.ts:85-99`).
- Data path / persistence env vars (`BUNQUEUE_DATA_PATH`, etc.) are owned by [Configuration & Entrypoint](./configuration.md) and [Persistence](./persistence.md); this module has no dedicated env vars.

## Related Docs

- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md)
- [Core Queue Engine (QueueManager & Shards)](./core-queue-engine.md)
- [Persistence (SQLite, WriteBuffer, ReadThrough)](./persistence.md)
- [PostgreSQL 18.6 Multi-Broker Persistence](./postgres-multibroker.md)
- [Deduplication & Unique Jobs](./deduplication-and-unique.md)
- [Worker Registry & Management](./workers-management.md)
- [Background Tasks](./background-tasks.md)
- [Data Structures (PriorityQueue, heaps, maps)](./data-structures.md)
- [Client SDK: Queue](./client-queue-sdk.md)
- [Simple Mode (Bunqueue all-in-one)](./simple-mode.md)
- [TCP Server Command Handlers](./tcp-server-handlers.md)
- [CLI](./cli.md)
- [Native MCP Server](./mcp-server.md)
- [architecture](../architecture.md)
- [data-model](../data-model.md)
