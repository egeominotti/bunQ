---
title: "Cron Jobs: Run Jobs on a Schedule"
description: Schedule recurring jobs in bunqueue with cron expressions or plain intervals. Timezone support, persisted in SQLite, survives restarts.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/cron.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · cron</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Cron jobs that <em>survive restarts.</em></h1>
  <p class="bq-hero-sub">Run a job every day at 9 AM, or every 5 minutes, without an external scheduler. Schedules are stored in SQLite, so they come back after a restart.</p>
</div>

A cron job is a job that bunqueue creates for you on a schedule. You describe the schedule once, with a cron expression (a five-field pattern like `0 9 * * *`) or a plain interval in milliseconds, and bunqueue enqueues a fresh job each time it fires.

## Quick Start

Create a scheduler with `upsertJobScheduler`. It works in both embedded and TCP mode, and calling it again with the same name updates the schedule instead of duplicating it:

```typescript
import { Queue, Worker } from 'bunqueue/client';

const queue = new Queue('reports', { embedded: true });

// Every day at 9:00 AM
await queue.upsertJobScheduler('daily-report', {
  pattern: '0 9 * * *',
}, {
  name: 'daily-report',
  data: { type: 'sales' },
});

// A normal worker processes the scheduled jobs
new Worker('reports', async (job) => {
  console.log('Running report:', job.data.type);
}, { embedded: true });
```

Or from the CLI, against a running server:

```bash
bunqueue cron add daily-report -q reports -d '{"type":"daily"}' -s "0 9 * * *"
bunqueue cron list
bunqueue cron delete daily-report
```

## Common Tasks

### Run every N milliseconds

Use `every` instead of a cron pattern when you just want a fixed interval:

```typescript
await queue.upsertJobScheduler('heartbeat', {
  every: 60000,  // every minute
  limit: 100,    // optional: stop after 100 runs
}, {
  data: { check: 'health' },
});
```

CLI equivalent: `bunqueue cron add heartbeat -q system -d '{"check":"health"}' -e 60000`.

### Schedule in a specific timezone

Pass an IANA timezone (like `Europe/Rome` or `America/New_York`) and the pattern is evaluated in that timezone, daylight saving included:

```typescript
// 6 PM New York time, weekdays only
await queue.upsertJobScheduler('end-of-day', {
  pattern: '0 18 * * 1-5',
  timezone: 'America/New_York',
}, {
  name: 'end-of-day',
  data: { type: 'summary' },
});
```

From the CLI, pass `--timezone` (`-z`):

```bash
bunqueue cron add daily-report -q reports -d '{"type":"daily"}' \
  -s "0 9 * * *" -z Europe/Rome
```

### Repeat a job after each completion

For simple repetition tied to job completion, `queue.add` with `repeat` also works: the job re-enqueues itself `every` milliseconds after each run completes.

```typescript
await queue.add('sync', { source: 'crm' }, { repeat: { every: 30000, limit: 10 } });
```

Note: `repeat.pattern` on `queue.add` is not evaluated. Cron patterns require `upsertJobScheduler` or the CLI.

## Cron Expression Cheat Sheet

Five fields, left to right: minute (0-59), hour (0-23), day of month (1-31), month (1-12), day of week (0-6, Sunday = 0).

| Expression | Meaning |
|---|---|
| `0 9 * * *` | Every day at 9:00 AM |
| `*/15 * * * *` | Every 15 minutes |
| `0 0 * * MON` | Every Monday at midnight |
| `0 0 1 * *` | First day of every month |

Shortcuts (`@daily`, `@hourly`, `@weekly`, `@monthly`, `@yearly`, `@midnight`) and a six-field form with a leading seconds field are also accepted.

## Scheduler Options

Options on the repeat object of `upsertJobScheduler`:

| Option | Default | Description |
|---|---|---|
| `pattern` | - | Cron expression |
| `every` | - | Interval in ms (alternative to `pattern`) |
| `timezone` | `UTC` (embedded) / server timezone (TCP) | IANA timezone for `pattern` evaluation |
| `limit` | unlimited | Max executions, then the scheduler is removed |
| `immediately` | `false` | Fire once right away on first creation |
| `skipIfNoWorker` | `false` | Skip a run when no worker is registered for the queue |
| `preventOverlap` | `true` | Skip a run while the previous job is still active |
| `skipMissedOnRestart` | `true` | On server restart, recompute the next run instead of firing missed runs |

## AI Agents (MCP)

AI agents can manage cron jobs in natural language ("create a cron that cleans old sessions every hour") through the [MCP Server](/guide/mcp/):

```bash
bun add bunqueue @modelcontextprotocol/sdk
claude mcp add bunqueue -- bunx bunqueue-mcp
```

:::tip[Related Guides]
- [Queue API](/guide/queue/) - Job options for cron-created jobs
- [CLI Commands](/guide/cli/) - Manage cron jobs via CLI
- [MCP Server](/guide/mcp/) - AI agent integration
:::
