# Scheduler & Cron

> **Category:** Scheduling · **Source:** `src/infrastructure/scheduler/cronScheduler.ts`, `src/infrastructure/scheduler/cronParser.ts`, `src/domain/types/cron.ts`, `src/client/queue/scheduler.ts`

## Purpose

The scheduler is the server-side engine that owns recurring/repeatable jobs ("crons"). Given either a 5–6 field cron expression (with optional IANA timezone) or a fixed millisecond interval (`repeatEvery`), it computes each job's next run time and pushes a fresh job onto the target queue when it falls due. It is event-driven: a precise `setTimeout` wakes the scheduler at the exact moment the soonest cron is due, backed by a 60s safety `setInterval` to absorb timer drift. State (execution count, next run) is persisted to SQLite so crons survive restarts. The matching client surface (`upsertJobScheduler`, `every`, `cron`) is a BullMQ-v5-style API that translates repeatable-job definitions into cron definitions over TCP or the embedded manager.

## Responsibilities & Scope

Owns:
- In-memory cron registry keyed by name, ordered by `nextRun` via a min-heap for `O(k log n)` ticks (`cronScheduler.ts:49-51`).
- Cron expression validation, shortcut expansion, and next-run computation (delegated to `cronParser.ts`, which wraps the `croner` library).
- Firing due crons: limit checks, overlap/`skipIfNoWorker`/`preventOverlap` gating, and the push callback that creates each spawned job (`cronScheduler.ts:296-447`).
- Persisting cron state before pushing (executions + nextRun) to guarantee at-most-once-per-slot on crash/retry (`cronScheduler.ts:344-369`).
- Restart recovery: recalculating past `nextRun` values forward and (optionally) skipping missed runs (`cronScheduler.ts:230-256`).
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
- `QueueManager` (`src/application/queueManager.ts`) wires callbacks: push → `push()`, persist → `storage.updateCron()`, worker-check → `workerManager.getForQueue()` (`queueManager.ts:175-192`); dashboard emit is wired later via `setDashboardEmit` (`queueManager.ts:1318`).
- `src/client/manager.ts` (`getSharedManager`) and `src/client/tcpPool.ts` for the embedded vs TCP split in `scheduler.ts`.

External / runtime:
- **`croner`** — the only external runtime dependency in this module; all cron-expression parsing and next-run math runs through `new Cron(expression, { timezone })` (`cronParser.ts:6,16,35`).
- Bun/Node timers: `setTimeout` (precise wake) + `setInterval` (60s safety fallback).
- SQLite via the persistence layer (indirect, through callbacks).

## Public Interface

### Server-side class — `CronScheduler` (`cronScheduler.ts:47`)

```typescript
class CronScheduler {
  constructor(_config?: CronSchedulerConfig); // config kept for back-compat, unused
  setPushCallback(callback: PushJobCallback): void;
  setPersistCallback(callback: PersistCronCallback): void;
  setWorkerCheckCallback(callback: (queue: string) => boolean): void;
  setDashboardEmit(callback: (event: string, data: Record<string, unknown>) => void): void;
  start(): void;
  stop(): void;
  add(input: CronJobInput): CronJob;           // also performs upsert
  remove(name: string): boolean;               // O(1) lazy deletion
  get(name: string): CronJob | undefined;
  list(): CronJob[];
  load(crons: CronJob[]): void;                // restart recovery
  getStats(): { total: number; pending: number; nextRun: number | null };
}

type PushJobCallback = (queue: string, input: JobInput) => Promise<void>;
type PersistCronCallback = (name: string, executions: number, nextRun: number) => void;
```

The `QueueManager` re-exposes these as `addCron(input)`, `removeCron(name)`, `getCron(name)`, `listCrons()` (`queueManager.ts:1262-1311`).

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

`RepeatOpts` (`scheduler.ts:39-55`): `pattern?`, `every?`, `limit?`, `immediately?`, `count?`, `prevMillis?`, `offset?`, `jobId?`, `timezone?`, `skipMissedOnRestart?`, `skipIfNoWorker?`, `preventOverlap?`. `limit` is forwarded to the cron definition as `maxLimit` (the run cap; see #111) and echoed back on `SchedulerInfo.limit`. (`count`, `prevMillis`, `offset`, `jobId` are still accepted for BullMQ shape compatibility but have no cron-side equivalent, so they are not forwarded.)

### Simple-mode helpers (`src/client/bunqueue.ts:231-262`)

```typescript
Bunqueue.cron(id, pattern, data?, opts?: { timezone?; limit?; jobOpts? }): Promise<SchedulerInfo | null>;
Bunqueue.every(id, intervalMs, data?, opts?: { limit?; jobOpts? }): Promise<SchedulerInfo | null>;
Bunqueue.removeCron(id); Bunqueue.listCrons();
```

Both delegate to `Queue.upsertJobScheduler` — `cron` sets `pattern`, `every` sets the `every` interval.

### TCP commands handled

- `Cron` — create/upsert a cron (`handlers/cron.ts:12`, routed at `handlerRoutes.ts:291`).
- `CronGet` — fetch one cron by name (`handlers/cron.ts:62`).
- `CronDelete` — remove a cron (`handlers/cron.ts:88`).
- `CronList` — list all crons (`handlers/cron.ts:99`).

Command shapes in `src/domain/types/command.ts:341-378` (`CronGet` at `:481`).

### HTTP / CLI / MCP

- HTTP cron routes proxy to the same handlers (`httpRouteResources.ts:31-82`: `CronList`, `Cron`, `CronGet`, `CronDelete`).
- CLI: `bunqueue cron list | add | delete` (`cli/commands/cron.ts`). `add` requires `--queue` + `--data` and one of `--schedule`/`--every`; rejects `--every <= 0` and `--max-limit < 0` (`cron.ts:55-89`).
- MCP tools `bunqueue_add_cron` / `bunqueue_list_crons` / `bunqueue_delete_cron` (`mcp/tools/cronTools.ts`).

### Dashboard events emitted (via `setDashboardEmit`)

`cron:created`, `cron:updated`, `cron:deleted` (from the TCP handler), `cron:fired`, `cron:skipped` (reason `no-worker` or `overlap`), `cron:missed` (persist or push failure) — `cronScheduler.ts:354,380,408,420,446` and `handlers/cron.ts:36,94`.

## Data Models

Canonical definition is `CronJob` (`src/domain/types/cron.ts:28-52`). See [data-model](../data-model.md) for the full schema. Most relevant fields:

| Field | Type | Notes |
| --- | --- | --- |
| `name` | `string` | PRIMARY KEY in `cron_jobs`; global, so multi-prefix queues must namespace (see Concurrency). |
| `queue` | `string` | target queue for spawned jobs. |
| `schedule` | `string \| null` | cron expression (mutually fills with `repeatEvery`). |
| `repeatEvery` | `number \| null` | fixed interval in ms. |
| `priority` | `number` | spawned-job priority (default `0`). |
| `timezone` | `string \| null` | IANA tz for `schedule`. |
| `nextRun` / `executions` | `number` | mutable runtime state, persisted. |
| `maxLimit` | `number \| null` | `null`/0/negative ⇒ unlimited (`cron.ts:99`). |
| `uniqueKey` / `dedup` | dedup config for spawned jobs. |
| `skipMissedOnRestart` | `boolean` | **default `true`** in `createCronJob` (`cron.ts:102`). |
| `skipIfNoWorker` | `boolean` | default `false`. |
| `preventOverlap` | `boolean` | default `true`. |
| `jobOptions` | `CronJobOptions \| null` | per-cron retry/cleanup policy (issue #86). |

`CronJobOptions` (`cron.ts:17-25`): `maxAttempts`, `backoff`, `timeout`, `delay`, `stallTimeout`, `removeOnComplete`, `removeOnFail`. `SchedulerInfo` (`scheduler.ts:63-71`): `{ id, name, next, pattern?, every?, limit? }` (`limit` = the persisted `maxLimit` run cap, `undefined` when unlimited; #111). The `cron_jobs` SQLite table is defined at `schema.ts:103-120` (migrations 6, 8-10, and 12 added the dedup/skip/overlap/jobOptions columns).

## Business Logic / Control Flow

### Add / upsert (`add`, `cronScheduler.ts:136-189`)
1. Reject if neither `schedule` nor `repeatEvery` is set.
2. If `schedule`, expand shortcut then `validateCronExpression`; throw on invalid.
3. Compute `nextRun` via `getNextCronRun` (cron) or `getNextIntervalRun` (interval) from `now`.
4. `createCronJob` builds the immutable record. If a cron with the same name exists, **preserve its `executions`** and clear `lastFiredAt` for that name (`cronScheduler.ts:165-171`, fixes H10).
5. `immediately` sets `nextRun = now` **only on first creation**, never on upsert (so it doesn't clobber restart-skip logic) (`cronScheduler.ts:175-177`).
6. Assign a fresh `generation`, store in map + heap, then `scheduleNext()`.

At the manager layer, `addCron` first calls `removeOrphanedCronJob` when `preventOverlap` is set: it deletes any still-`waiting`/queued job under the cron's uniqueKey before re-arming, so a stale job left over a disconnect window isn't immediately consumed by the new worker (`queueManager.ts:1262-1297`, #73). It then persists via `storage.saveCron`.

### Event-driven wake (`scheduleNext`, `cronScheduler.ts:262-289`)
Clears the current timer, pops stale heap entries (generation mismatch), then arms a single `setTimeout` for `max(0, nextRun - now)` on the soonest live cron. Re-run after every mutation (add/remove/load/tick).

### Tick / fire (`tick`, `cronScheduler.ts:296-402`)
1. Drain heap entries whose `nextRun <= now` (`O(k log n)`).
2. Skip stale entries (generation mismatch from a removed/updated cron).
3. If `isAtLimit`, mark for removal from the map and drop.
4. **Compute `newExecutions` and `newNextRun` before pushing.** Interval crons anchor `newNextRun` to the *scheduled* slot, not wall-clock now, so a slow job does not cumulatively drift the schedule (`cronScheduler.ts:330-342`, M4).
5. **Persist state first** (`persistCron`). If persist throws: emit `cron:missed`, do **not** push, re-insert to retry next tick (`cronScheduler.ts:344-362`).
6. Update in-memory `executions`/`nextRun`, then `fireCronJob`. If the push throws: state was already persisted, the job for this slot is lost, emit `cron:missed`, re-insert to continue (no duplicate on retry) (`cronScheduler.ts:373-387`).
7. Re-insert processed entries, delete limit-reached names, `scheduleNext()`.

### Fire gating (`fireCronJob`, `cronScheduler.ts:404-447`)
- **`skipIfNoWorker`**: if set and no workers registered for the queue, emit `cron:skipped` (reason `no-worker`) and return without pushing.
- **Overlap detection**: if the last fire for this name was within `interval * 0.8` (`interval = repeatEvery ?? 60000`), emit `cron:skipped` (reason `overlap`) and return.
- **`preventOverlap`**: when no explicit `uniqueKey`, auto-derives `cron:<name>` so the dedup layer blocks a new push while the prior job is still active.
- Push with `data`, `priority`, `uniqueKey`, `dedup`, and the `jobOptions` subset; record `lastFiredAt`; emit `cron:fired`.

### Restart recovery (`load`, `cronScheduler.ts:230-256`)
For each loaded cron, if (`skipMissedOnRestart || skipIfNoWorker`) and `nextRun < now`, recompute `nextRun` forward and persist it (fixes #73). Since `skipMissedOnRestart` defaults to `true`, missed runs are skipped by default. Heap is rebuilt with `buildFrom` in `O(n)`.

### Client upsert mapping (`upsertJobScheduler`, `scheduler.ts:114-183`)
Builds cron `data` from the template (`buildCronData`), merges queue `defaultJobOptions` under per-scheduler `opts` into `CronJobOptions` (`buildCronJobOptions`, issue #86), extracts dedup from `opts.deduplication`, derives spawned-job `priority` from `opts.priority`/queue default (carried on the top-level field the handler reads), and namespaces the id via `toCronName`. Embedded mode calls `manager.addCron` (timezone defaults to `'UTC'`); TCP mode sends the `Cron` command. `removeJobScheduler`/`getJobScheduler(s)` mirror this over `CronDelete`/`CronList`.

## Concurrency & Locking

- The scheduler holds **no shard/jobIndex locks**; it runs on the single JS event loop and mutates only its own `Map`/`MinHeap`. Concurrency safety comes from the single-threaded tick.
- **Lazy deletion via generation numbers**: `remove`/upsert just bumps the map; stale heap entries are detected by generation mismatch and skipped/popped in `scheduleNext`/`tick`, giving `O(1)` removal without heap rebuilds (`cronScheduler.ts:179-211, 277-280, 311-316`).
- **Persist-before-push ordering** is the cross-process invariant: persisting `nextRun`/`executions` before enqueuing means a crash between the two yields at most a lost job for that slot, never a duplicate (`cronScheduler.ts:344-369`).
- Cron names are a **global PRIMARY KEY** (`schema.ts:104`). The client prefixes scheduler ids with `prefixKey` so two queues with different prefixes cannot collide (`scheduler.ts:21-37`).
- `preventOverlap`/overlap-window guard against concurrent in-flight runs of the same cron across workers; enforcement is via uniqueKey dedup in the push path.

## Edge Cases & Failure Modes

- **Persist failure** → job not pushed, `cron:missed` emitted, retried next tick (no duplicate).
- **Push failure** → state already persisted, slot's job lost, `cron:missed` emitted, scheduling continues.
- **`maxLimit` exhaustion** → cron removed from the in-memory map on the tick that would exceed it (`isAtLimit`); `0`/negative limits are normalized to "unlimited" everywhere (`cron.ts:97-99`, `cli/commands/cron.ts:81-89`).
- **Timer drift / missed `setTimeout`** → the 60s `SAFETY_FALLBACK_MS` interval re-runs `tick` as a backstop (`cronScheduler.ts:25-26,110-112`).
- **Overlap from slow jobs** → suppressed via the `interval * 0.8` window and `preventOverlap` uniqueKey; interval-rate crons anchor to the scheduled slot to avoid drift.
- **`every <= 0`** → CLI rejects it; otherwise `getNextIntervalRun` would always be in the past and fire every tick (`cli/commands/cron.ts:70-75`).
- **Restart with past `nextRun`** → recalculated forward; missed runs skipped by default (`skipMissedOnRestart` default `true`).
- **Upsert preserves `executions`** and resets `lastFiredAt` (H10) so a redefined cron isn't blocked by the prior definition's last fire.
- **#103 (fixed 2.8.14)**: with `skipIfNoWorker: true`, after a TCP reconnect the cron silently stops firing if the worker is not re-registered — `fireCronJob` sees no worker and emits `cron:skipped` instead of pushing. The fix re-registers workers on reconnect; `Worker.resume()` alone does not recover it. Upgrade clients to ≥ 2.8.14.
- **No-storage mode**: with no `storage` wired, `persistCron` is null and crons are purely in-memory (lost on restart) — persist guard is skipped (`cronScheduler.ts:345`).

## Configuration

- `CronSchedulerConfig.checkIntervalMs` — **deprecated/no-op**; the scheduler uses precise `setTimeout` (`cronScheduler.ts:19-23,68-71`).
- `SAFETY_FALLBACK_MS = 60_000` — internal constant, not env-configurable.
- Per-cron knobs (via `CronJobInput`/`RepeatOpts`): `schedule`/`repeatEvery`, `timezone` (embedded default `'UTC'`), `priority`, `maxLimit`, `uniqueKey`/`dedup`, `skipMissedOnRestart` (default `true`), `skipIfNoWorker` (default `false`), `preventOverlap` (default `true`), `immediately`, and `jobOptions` (`maxAttempts`/`backoff`/`timeout`/`delay`/`stallTimeout`/`removeOnComplete`/`removeOnFail`).
- Queue-level `defaultJobOptions` feed `buildCronJobOptions` as the base, overridden by the per-scheduler template `opts` (`scheduler.ts:87-101`).
- Data path / persistence env vars (`BUNQUEUE_DATA_PATH`, etc.) are owned by [Configuration & Entrypoint](./configuration.md) and [Persistence](./persistence.md); this module has no dedicated env vars.

## Related Docs

- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md)
- [Core Queue Engine (QueueManager & Shards)](./core-queue-engine.md)
- [Persistence (SQLite, WriteBuffer, ReadThrough)](./persistence.md)
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
