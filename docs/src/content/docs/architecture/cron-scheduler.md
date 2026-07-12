---
title: "bunqueue Cron Scheduler: MinHeap Execution & Timezone Support"
description: "bunqueue cron scheduler internals: MinHeap-based execution with O(1) lazy deletion, timezone support, and SQLite-backed persistence."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og-image.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">architecture · cron scheduler</span>
  <h1 class="bq-hero-h1 bq-bench-h1">The cron scheduler, time <em>persisted.</em></h1>
  <p class="bq-hero-sub">The bunqueue cron scheduler uses a MinHeap-based execution model with generation-based lazy deletion, timezone-aware schedules, and SQLite-backed state for recovery on restart.</p>
</div>

## System Overview

<div class="bq-diag">
  <div class="bq-diag-head"><b>CronScheduler</b><span>heap plus generation map</span></div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">cronJobs <i>Map&lt;name, {cron, generation}&gt;, O(1) lookup</i></div>
    <div class="bq-diag-cell bq-diag-accent">cronHeap <i>MinHeap&lt;CronHeapEntry&gt;, O(k log n)</i></div>
    <div class="bq-diag-cell">generation <i>number, lazy deletion</i></div>
  </div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">tick(), event-driven</span>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">1. Pop due crons from heap <i>nextRun &lt;= now</i></div>
      <div class="bq-diag-cell">2. Check stale <i>generation mismatch: skip</i></div>
      <div class="bq-diag-cell">3. Check execution limit <i>auto-remove if reached</i></div>
      <div class="bq-diag-cell">4. Persist new state to SQLite <i>before pushing, prevents duplicates</i></div>
    </div>
    <div class="bq-diag-row">
      <div class="bq-diag-cell">5. Push job to queue</div>
      <div class="bq-diag-cell">6. Re-insert with same generation</div>
      <div class="bq-diag-cell">7. scheduleNext() <i>arm a precise setTimeout for the next due cron</i></div>
    </div>
  </div>
  <p class="bq-diag-note">The scheduler is event-driven: a precise setTimeout wakes it exactly when the next cron is due, rearmed after every add/remove/load/tick. A 60s setInterval acts only as a safety fallback against timer drift or missed events.</p>
</div>

## Core Data Structures

### CronJob Interface

```typescript
interface CronJob {
  name: string;               // Unique identifier
  queue: string;              // Target queue
  data: unknown;              // Job payload
  schedule: string | null;    // Cron expression (5-6 fields)
  repeatEvery: number | null; // Interval in ms
  priority: number;           // Job priority
  timezone: string | null;    // IANA timezone
  nextRun: number;            // Next execution timestamp (absolute ms)
  executions: number;         // Current execution count
  maxLimit: number | null;    // Max executions (null = unlimited)
  uniqueKey: string | null;   // Dedup key for spawned jobs
  dedup: CronDedup | null;    // Dedup options (ttl, extend, replace)
  skipMissedOnRestart: boolean; // Skip missed runs on restart
  skipIfNoWorker: boolean;    // Skip push if no worker registered
  preventOverlap: boolean;    // Auto uniqueKey `cron:<name>` (default: true)
  jobOptions: CronJobOptions | null; // Per-spawned-job retry/cleanup policy
}
```

Source: `src/domain/types/cron.ts`.

### Generation-Based Lazy Deletion

Instead of O(n) heap removals, we use generation numbers:

```typescript
interface CronHeapEntry {
  cron: CronJob;
  generation: number;  // Unique per entry
}

// Remove operation: O(1)
remove(name: string): boolean {
  this.cronJobs.delete(name);  // Just delete from map
  // Heap entry becomes "stale" - skipped in tick()
  return true;
}

// In tick(): skip stale entries
const current = this.cronJobs.get(entry.cron.name);
if (current?.generation !== entry.generation) {
  continue;  // Stale entry, skip
}
```

Source: `src/infrastructure/scheduler/cronScheduler.ts`.

## Scheduling Modes

### Cron Expressions

Supports standard 5-6 field cron syntax with shortcuts:

| Shortcut | Expression | Description |
|----------|-----------|-------------|
| `@yearly` | `0 0 1 1 *` | Once per year |
| `@monthly` | `0 0 1 * *` | First day of month |
| `@weekly` | `0 0 * * 0` | Sunday at midnight |
| `@daily` | `0 0 * * *` | Every day at midnight |
| `@hourly` | `0 * * * *` | Every hour |

### Timezone Support

Uses the `croner` library for timezone-aware scheduling:

```typescript
const cron = new Cron('0 2 * * *', { timezone: 'Europe/Rome' });
const nextDate = cron.nextRun(new Date(fromTime));
```

### Interval-Based (RepeatEvery)

Simple offset-based scheduling:

```typescript
function getNextIntervalRun(intervalMs: number, lastRun: number): number {
  return lastRun + intervalMs;
}
```

Interval crons run at a fixed rate: the next run is anchored to the slot the fire was scheduled for, not to wall-clock time at execution, so a slow or late fire does not cumulatively drift the schedule forward.

## Execution Flow

<div class="bq-diag">
  <div class="bq-diag-head"><b>Execution flow</b><span>tick() fires when the precise timer (or the 60s safety fallback) wakes</span></div>
  <div class="bq-diag-layer">while heap.peek().nextRun &lt;= now</div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">entry = heap.pop() <i>O(log n)</i></div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">Stale? gen mismatch <i>yes: skip, continue</i></div>
  <div class="bq-diag-arrow">↓ no</div>
  <div class="bq-diag-layer">At execution limit? <i>yes: auto-remove</i></div>
  <div class="bq-diag-arrow">↓ no</div>
  <div class="bq-diag-layer bq-diag-accent">1. Calculate new executions and nextRun, 2. persist to SQLite FIRST, 3. update in-memory state, 4. fire the job (push to queue), 5. re-insert to heap</div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-layer">scheduleNext() <i>arm a precise setTimeout for the next non-stale heap entry</i></div>
</div>

**Fire guards** (checked just before pushing the job):

- `skipIfNoWorker`: the push is skipped when no worker is registered for the target queue.
- Overlap detection: the fire is skipped if the last fire for this cron happened within 80 percent of the interval window.
- `preventOverlap` (default true): the spawned job gets an automatic `uniqueKey` of `cron:<name>`, so a new job is deduplicated while the previous one is still active.

## Persistence & Recovery

### SQLite Schema

```sql
CREATE TABLE cron_jobs (
  name TEXT PRIMARY KEY,
  queue TEXT NOT NULL,
  data BLOB NOT NULL,          -- MessagePack
  schedule TEXT,
  repeat_every INTEGER,
  priority INTEGER NOT NULL DEFAULT 0,
  next_run INTEGER NOT NULL,   -- absolute ms timestamp
  executions INTEGER NOT NULL DEFAULT 0,
  max_limit INTEGER,
  timezone TEXT,
  unique_key TEXT,
  dedup BLOB,                  -- MessagePack
  skip_missed_on_restart INTEGER NOT NULL DEFAULT 0,
  skip_if_no_worker INTEGER NOT NULL DEFAULT 0,
  prevent_overlap INTEGER NOT NULL DEFAULT 1,
  job_options BLOB             -- MessagePack
);
```

### Recovery on Startup

```typescript
// In QueueManager initialization
this.cronScheduler.load(this.storage.loadCronJobs());  // O(n) heapify
```

During `load()`, any cron whose persisted `nextRun` is in the past has it recalculated forward (and re-persisted) when `skipMissedOnRestart` or `skipIfNoWorker` is set, so missed runs are skipped instead of firing immediately on boot.

## Error Handling

### Persist-First Execution

State is persisted before the job is pushed, so a crash between the two steps can never produce a duplicate fire:

```typescript
// 1. Calculate new state BEFORE anything else
const newExecutions = cron.executions + 1;
const newNextRun = calculateNextRun(cron);  // interval crons anchor to the scheduled slot

// 2. Persist FIRST; on failure: do NOT push, re-insert entry, retry on next tick
this.persistCron(cron.name, newExecutions, newNextRun);

// 3. Update in-memory state AFTER successful persist
cron.executions = newExecutions;
cron.nextRun = newNextRun;

// 4. NOW push the job (state already persisted, safe from duplicates).
// If the push fails, the job is lost but the schedule stays consistent;
// a `cron:missed` dashboard event is emitted and the next run proceeds.
await this.fireCronJob(cron, now);
```

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| `add()` | O(log n) | Heap push + map insert, rearms the precise timer |
| `remove()` | **O(1)** | Lazy deletion via generation |
| `tick()` | O(k log n) | k = due crons |
| `scheduleNext()` | O(1) amortized | Peek heap, pop stale entries, arm setTimeout |
| `list()` | O(n) | Iterate map |
| `load()` | O(n) | Heapify from array |

## Timing Model

There is no configurable polling interval. The scheduler is event-driven:

```typescript
// Precise timer: wakes exactly when the next cron is due
const delay = Math.max(0, nextEntry.cron.nextRun - Date.now());
this.nextTimer = setTimeout(() => void this.tick(), delay);

// Safety fallback: catches timer drift and missed events
const SAFETY_FALLBACK_MS = 60_000;
this.safetyInterval = setInterval(() => void this.tick(), SAFETY_FALLBACK_MS);
```

The legacy `checkIntervalMs` config option is still accepted for backward compatibility but is deprecated and ignored.

## Usage Example

The client SDK exposes the scheduler through `Queue.upsertJobScheduler()` (embedded mode calls `QueueManager.addCron()` directly; TCP mode sends the `Cron` command):

```typescript
// Add a cron job (2 AM daily, Rome time, at most 365 runs)
await queue.upsertJobScheduler(
  'daily-cleanup',
  { pattern: '0 2 * * *', timezone: 'Europe/Rome', limit: 365 },
  { data: { type: 'cleanup' } },
);

// Add an interval-based job (every minute)
await queue.upsertJobScheduler('health-check', { every: 60_000 }, { data: { check: 'ping' } });

// Remove a scheduler
await queue.removeJobScheduler('daily-cleanup');

// Inspect a scheduler
const info = await queue.getJobScheduler('health-check');
```

:::tip[Related]
- [Architecture Overview](/architecture/) - Full component map
- [Data Structures](/architecture/data-structures/) - Skip list behind time-ordered scheduling
- [Persistence](/architecture/persistence/) - Where schedulers survive restarts
- [Cron Jobs Guide](/guide/cron/) - Using schedulers from the client
:::
