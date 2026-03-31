---
title: "bunqueue Cron Scheduler: MinHeap Execution & Timezone Support"
description: "bunqueue cron scheduler internals: MinHeap-based execution with O(1) lazy deletion, timezone support, and SQLite-backed persistence."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og-image.png
---

# Cron Scheduler Architecture

The bunqueue Cron Scheduler uses a **MinHeap-based execution model** with generation-based lazy deletion for optimal performance.

## System Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    CronScheduler                             │
├─────────────────────────────────────────────────────────────┤
│  cronJobs: Map<name, {cron, generation}>  ◄── O(1) lookup   │
│  cronHeap: MinHeap<CronHeapEntry>         ◄── O(k log n)    │
│  generation: number                        ◄── Lazy deletion │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                     tick() every 1s                          │
│  1. Pop due crons from heap (nextRun <= now)                │
│  2. Check stale (generation mismatch) → skip                │
│  3. Check execution limit → auto-remove if reached          │
│  4. Push job to queue                                       │
│  5. Persist state to SQLite                                 │
│  6. Calculate new nextRun                                   │
│  7. Re-insert with same generation                          │
└─────────────────────────────────────────────────────────────┘
```

## Core Data Structures

### CronJob Interface

```typescript
interface CronJob {
  name: string;              // Unique identifier
  queue: string;             // Target queue
  data: unknown;             // Job payload
  schedule: string | null;   // Cron expression (5-6 fields)
  repeatEvery: number | null; // Interval in ms
  priority: number;          // Job priority
  timezone: string | null;   // IANA timezone
  nextRun: number;           // Next execution timestamp
  executions: number;        // Current execution count
  maxLimit: number | null;   // Max executions (null = unlimited)
}
```

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

## Execution Flow

```
tick() fires (every 1 second)
     │
     ▼
┌──────────────────────────────┐
│ while heap.peek().nextRun    │
│       <= now                 │
└──────────┬───────────────────┘
           │
           ▼
┌──────────────────────────────┐
│ entry = heap.pop()           │  O(log n)
└──────────┬───────────────────┘
           │
           ▼
┌──────────────────────────────┐
│ Stale? (gen mismatch)        │──── Yes ──► Skip, continue
└──────────┬───────────────────┘
           │ No
           ▼
┌──────────────────────────────┐
│ At execution limit?          │──── Yes ──► Auto-remove
└──────────┬───────────────────┘
           │ No
           ▼
┌──────────────────────────────┐
│ 1. Push job to queue         │
│ 2. Persist to SQLite         │
│ 3. Update executions++       │
│ 4. Calculate new nextRun     │
│ 5. Re-insert to heap         │
└──────────────────────────────┘
```

## Persistence & Recovery

### SQLite Schema

```sql
CREATE TABLE cron_jobs (
  name TEXT PRIMARY KEY,
  queue TEXT NOT NULL,
  data BLOB NOT NULL,          -- MessagePack
  schedule TEXT,
  repeat_every INTEGER,
  priority INTEGER DEFAULT 0,
  next_run INTEGER NOT NULL,
  executions INTEGER DEFAULT 0,
  max_limit INTEGER,
  timezone TEXT
);
```

### Recovery on Startup

```typescript
// In QueueManager constructor
if (this.storage) {
  const crons = this.storage.loadCronJobs();
  this.cronScheduler.load(crons);  // O(n) heapify
}
```

## Error Handling

### Two-Phase Persistence

```typescript
try {
  // 1. Push job to queue
  await this.pushJob(cron.queue, { data: cron.data, priority: cron.priority });

  // 2. Calculate new state BEFORE persisting
  const newExecutions = cron.executions + 1;
  const newNextRun = calculateNextRun(cron);

  // 3. Persist FIRST
  this.persistCron(cron.name, newExecutions, newNextRun);

  // 4. Only update in-memory AFTER successful persist
  cron.executions = newExecutions;
  cron.nextRun = newNextRun;

} catch (persistErr) {
  // Re-insert with original state to retry on next tick
  toReinsert.push(entry);
}
```

## Performance Characteristics

| Operation | Complexity | Notes |
|-----------|-----------|-------|
| `add()` | O(log n) | Heap push + map insert |
| `remove()` | **O(1)** | Lazy deletion via generation |
| `tick()` | O(k log n) | k = due crons |
| `list()` | O(n) | Iterate map |
| `load()` | O(n) | Heapify from array |

## Configuration

```typescript
const DEFAULT_CONFIG = {
  checkIntervalMs: 1000,  // Tick every 1 second
};
```

## Usage Example

```typescript
// Add a cron job
queue.addCron({
  name: 'daily-cleanup',
  schedule: '0 2 * * *',      // 2 AM daily
  timezone: 'Europe/Rome',
  queue: 'maintenance',
  data: { type: 'cleanup' },
  maxLimit: 365,              // Run for 1 year max
});

// Add interval-based job
queue.addCron({
  name: 'health-check',
  repeatEvery: 60000,         // Every minute
  queue: 'monitoring',
  data: { check: 'ping' },
});

// Remove cron job
queue.removeCron('daily-cleanup');

// List all crons
const crons = queue.listCrons();
```
