---
title: "Queue API: Jobs, Priorities, Delays, Deduplication"
description: "Complete bunqueue Queue API reference: add jobs with priorities, delays, retries, bulk operations, durable writes, and DLQ configuration."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/queue.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · queue api</span>
  <h1 class="bq-hero-h1 bq-bench-h1">The Queue API, every job where it <em>belongs.</em></h1>
  <p class="bq-hero-sub">The Queue class is how you add and manage jobs: priorities, delays, retries, bulk adds, deduplication, and guaranteed writes, all from one API.</p>
</div>

A `Queue` is the producer side of bunqueue: you put jobs in, a [Worker](/guide/worker/) takes them out. This page covers everything a queue can do.

## Create a queue

```typescript
import { Queue } from 'bunqueue/client';

const queue = new Queue('my-queue', { embedded: true });

await queue.add('job-name', { key: 'value' });
```

:::caution[Embedded vs TCP]
`embedded: true` runs the queue inside your process. Without it, the Queue connects to a bunqueue server on `localhost:6789` (see [Server Mode](/guide/server/)). The Queue and its Worker must use the same mode.
:::

Useful variations:

```typescript
// Typed queue: job.data is type-checked
interface TaskData {
  userId: number;
  action: string;
}
const typedQueue = new Queue<TaskData>('tasks', { embedded: true });

// Default options applied to every job
const emailQueue = new Queue('emails', {
  embedded: true,
  defaultJobOptions: {
    attempts: 3,
    backoff: 1000,
    removeOnComplete: true,
  }
});

// TCP mode with a custom connection
const remoteQueue = new Queue('tasks', {
  connection: {
    host: '192.168.1.100',
    port: 6789,
    token: 'secret-token',  // If AUTH_TOKENS is set on the server
    poolSize: 4,            // Connection pool size
  }
});
```

## Add jobs

```typescript
const job = await queue.add('job-name', { key: 'value' });

// With options
const job = await queue.add('job-name', data, {
  priority: 10,           // Higher = processed first
  delay: 5000,            // Wait 5s before processing
  attempts: 5,            // Max retries if the processor throws (default: 3)
  backoff: 2000,          // Wait between retries in ms (default: 1000, jitter applied)
  // OR: backoff: { type: 'exponential', delay: 2000 }  // 'fixed' | 'exponential'
  timeout: 30000,         // Fail the job if processing takes longer
  jobId: 'custom-id',     // Custom ID, makes the add idempotent (see Deduplication)
  removeOnComplete: true, // Delete job data after it completes
});
```

The full option list is in the [reference table](#job-options-reference) at the bottom.

### Add many at once

`addBulk` inserts all jobs in one batch, much faster than a loop of `add`:

```typescript
const jobs = await queue.addBulk([
  { name: 'task-1', data: { id: 1 } },
  { name: 'task-2', data: { id: 2 }, opts: { priority: 10 } },
  { name: 'task-3', data: { id: 3 }, opts: { delay: 5000 } },
]);
```

### Repeat on a schedule

```typescript
// Every 5 seconds
await queue.add('heartbeat', {}, { repeat: { every: 5000 } });

// Every 24 hours, at most 30 times
await queue.add('daily-report', {}, { repeat: { every: 86400000, limit: 30 } });

// Cron pattern
await queue.add('weekly', {}, { repeat: { pattern: '0 9 * * MON' } });
```

You can change the data for future runs at any point in the lifecycle with `updateData()`, even after the current run completes (the update follows the repeat chain to the next scheduled execution):

```typescript
const job = await queue.add('sync', { endpoint: '/api/v1' }, { repeat: { every: 60000 } });
await job.updateData({ endpoint: '/api/v2' });  // Next run uses /api/v2
```

For named, managed schedules, see [Job Schedulers](#job-schedulers-repeatable-jobs) below and the [Cron guide](/guide/cron/).

### Durable jobs (no data loss)

By default bunqueue batches writes to disk every 10ms for speed (~100k jobs/sec). A crash inside that window can lose the not-yet-flushed jobs. For jobs where that is unacceptable, `durable: true` writes to disk before `add()` returns:

```typescript
await queue.add('process-payment', { orderId: '123', amount: 99.99 }, {
  durable: true,
});
```

| Mode | Throughput | Data loss window | Use for |
|------|------------|------------------|---------|
| Default | ~100k jobs/sec | Up to 10ms | Emails, notifications, analytics |
| Durable | ~10k jobs/sec | None | Payments, orders, audit records |

## Deduplication

### With `jobId` (idempotent adds)

Give a job a custom `jobId` and adding it twice does nothing: if a job with that id is still queued (`waiting` / `delayed` / `prioritized`), the existing job is returned instead of creating a duplicate. This makes `add()` safe to call repeatedly, which is what "idempotent" means. Works in embedded and TCP modes, BullMQ-compatible.

```typescript
const job1 = await queue.add('process', { orderId: 123 }, { jobId: 'order-123' });
const job2 = await queue.add('process', { orderId: 123 }, { jobId: 'order-123' });

console.log(job1.id === job2.id); // true, same job returned
```

Typical uses: webhook retries, double-submits from a UI, restoring jobs on service startup without duplicating them.

:::note[Completed ids are reused, not returned]
Deduplication only collapses onto a job that is still pending. If the previous job with that `jobId` already completed (or is currently processing), re-adding the same id starts a fresh job under it, and the stale completed record is evicted first. So a `jobId` like `report-2026-06-17` is safe to reuse: you get idempotency within a run and a clean re-run afterwards.
:::

### With a TTL window

The `deduplication` option dedupes within a time window instead of permanently. The `id` field is required:

```typescript
// Same id within 1 hour = no new job. After the TTL, a new job is allowed.
await queue.add('notification', { userId: '123' }, {
  deduplication: { id: 'notify-123', ttl: 3600000 }
});
```

Two strategies change what happens when a duplicate arrives:

```typescript
// extend: keep the existing job, reset its TTL (debouncing, "keep quiet while active")
await queue.add('sync-task', { action: 'sync' }, {
  deduplication: { id: 'sync-task', ttl: 60000, extend: true }
});

// replace: remove the pending job, insert a new one with the latest data (last write wins)
await queue.add('latest-data', { data: newData }, {
  deduplication: { id: 'data-job', ttl: 300000, replace: true }
});
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `id` | `string` | (required) | Unique deduplication key |
| `ttl` | `number` | - | Time in ms before the key expires |
| `extend` | `boolean` | `false` | Reset TTL on duplicate, keep existing job |
| `replace` | `boolean` | `false` | Remove pending job, create a new one (new internal id) |

:::caution[Replace details]
`replace: true` never touches a job that is already processing (active). If both `extend` and `replace` are set, `replace` wins.
:::

Managing keys directly:

```typescript
const jobId = await queue.getDeduplicationJobId('my-unique-key'); // look up
await queue.removeDeduplicationKey('my-unique-key');              // allow re-adding
```

## Query jobs

```typescript
// One job
const job = await queue.getJob('job-id');
const state = await queue.getJobState('job-id');
// 'waiting' | 'prioritized' | 'delayed' | 'active' | 'completed'
// | 'failed' | 'waiting-children' | 'unknown'

// Counts per state
const counts = await queue.getJobCountsAsync();
// { waiting, prioritized, active, completed, failed, delayed,
//   'waiting-children', paused }

// Lists, filtered by state
const failed = await queue.getJobsAsync({ state: 'failed', start: 0, end: 50 });
```

Most read methods come in two flavors: a sync version that only works in embedded mode (`getJobCounts()`, `getJobs()`, `getCountsPerPriority()`, `count()`, `isPaused()`) and an async version that works in both modes (`getJobCountsAsync()`, `getJobsAsync()`, `getCountsPerPriorityAsync()`, `countAsync()`, `isPausedAsync()`). Prefer the async ones unless you know you're embedded.

Per-state shortcuts:

```typescript
// Sync (embedded only): getWaiting, getActive, getCompleted, getFailed, getDelayed
const waiting = queue.getWaiting(0, 10);

// Async (both modes): same names + Async
const failed = await queue.getFailedAsync(0, 10);

// Counts (async, both modes)
const failedCount = await queue.getFailedCount();
// also: getWaitingCount, getActiveCount, getCompletedCount, getDelayedCount

// BullMQ-compatible extras
const prioritized = await queue.getPrioritized(0, 10);        // jobs with priority > 0
const waitingChildren = await queue.getJobsAsync({
  state: 'waiting-children', start: 0, end: 10,
});
```

`getPrioritized()` works in embedded and TCP modes. The BullMQ compatibility
helper `getWaitingChildren()` is currently embedded-only; use the explicit
`getJobsAsync({ state: 'waiting-children' })` query above when the same code must
work in both modes.

:::note[Two states worth knowing]
**Prioritized:** jobs with `priority > 0` report the state `'prioritized'`, not `'waiting'` (BullMQ v5 behavior). Both are pullable; prioritized jobs go first.

**Paused:** while a queue is paused, its ready jobs are counted under `paused`, never `waiting` or `prioritized`. On `resume()` each job returns to its logical `waiting` or `prioritized` state. Pause state survives server restarts when persistence is on.
:::

Jobs that exhaust their retries move to the dead letter queue (DLQ, a holding area for permanently failed jobs) but remain visible: they are counted by `failed`, returned by `getJob(id)`, and listed by `getFailed()`.

## Control the queue

```typescript
queue.pause();            // Workers stop pulling (fire-and-forget)
queue.resume();           // Back to normal (fire-and-forget)
queue.drain();            // Remove all waiting jobs (fire-and-forget)
queue.obliterate();       // Remove ALL queue data (fire-and-forget)

await queue.pauseAsync();      // Pause and wait for it
await queue.resumeAsync();     // Resume and wait for it
const n = await queue.drainAsync();    // Drain, wait, get removed count
await queue.obliterateAsync(); // Remove ALL queue data and wait for it

queue.remove('job-id');            // Remove one job (fire-and-forget)
await queue.removeAsync('job-id'); // Remove one job and wait for it

await queue.waitUntilReady();      // Wait until queue/server is ready
queue.close();                     // Close TCP connection (no-op in embedded mode)
```

Gotcha: in TCP mode the fire-and-forget forms return before the server has processed them. If you drain or obliterate and immediately add new jobs, the wipe can land after the add and delete the new job. Use the `Async` variants when the next step depends on the command being done.

## Maintenance

```typescript
// Remove completed jobs older than 1 hour, max 100 (async works in both modes)
const removed = await queue.cleanAsync(3600000, 100, 'completed');

// Promote delayed jobs to waiting now
const promoted = await queue.promoteJobs({ count: 50 });

// Re-queue failed jobs from the DLQ
await queue.retryJobs({ state: 'failed', count: 100 });

// Re-queue completed jobs (e.g. after a logic change)
const count = await queue.retryCompletedAsync();       // all completed, use with care
const one = queue.retryCompleted('job-id-123');        // one job (sync, embedded; TCP returns 0)
```

## Progress, logs, dependencies

```typescript
// Progress and logs (also available on the job object inside a processor)
await queue.updateJobProgress('job-id', 75);
await queue.addJobLog('job-id', 'Processing step 3 completed');
const { logs, count } = await queue.getJobLogs('job-id', 0, 100);

// Parent/child flows (see the Flow guide)
const childValues = await queue.getChildrenValues('parent-job-id');
const deps = await queue.getJobDependencies('job-id');
const processed = await queue.getDependencies('parent-id', 'processed', 0, 10);

// Wait for a job to finish (requires a QueueEvents instance)
import { QueueEvents } from 'bunqueue/client';
const queueEvents = new QueueEvents('my-queue');
const result = await queue.waitJobUntilFinished('job-id', queueEvents, 30000);
```

Manual state transitions (BullMQ-compatible, `token` is the worker's lock token):

```typescript
await queue.moveJobToCompleted('job-id', { success: true }, token);
await queue.moveJobToFailed('job-id', new Error('reason'), token);
await queue.moveJobToWait('job-id', token);
await queue.moveJobToDelayed('job-id', Date.now() + 60000, token);
await queue.moveJobToWaitingChildren('job-id', token);
```

## Rate limiting and concurrency

```typescript
// Cap parallel processing across ALL workers on this queue
queue.setGlobalConcurrency(10);
queue.removeGlobalConcurrency();
await queue.setGlobalConcurrencyAsync(10);   // same, but waits for the server

// Cap throughput: max jobs per window (default window: 1 second)
queue.setGlobalRateLimit(100);          // max 100 jobs per second
queue.setGlobalRateLimit(100, 60_000);  // max 100 jobs per minute
queue.removeGlobalRateLimit();
await queue.setGlobalRateLimitAsync(100, 60_000); // same, but waits for the server

// Temporary throttle to ~1 job/sec; the server clears it after 5s on its own
await queue.rateLimit(5000);
```

:::caution[Semantics and stubs]
- `rateLimit(ms)` throws on non-positive or non-finite `ms`. The expiry lives on the server, so it also survives your process exiting.
- `getGlobalConcurrency()`, `getGlobalRateLimit()`, `getRateLimitTtl()`, and `isMaxed()` exist for BullMQ API compatibility but are **stubs**: they return `null` / `null` / `0` / `false` regardless of actual server state.
:::

See [Rate Limiting](/guide/rate-limiting/) for worker-side limiting too.

## Job Schedulers (Repeatable Jobs)

Named, updatable schedules (the managed alternative to `repeat` in job options):

```typescript
await queue.upsertJobScheduler('daily-report', {
  pattern: '0 9 * * *',       // cron pattern
  // or: every: 3600000,      // interval in ms
}, {
  name: 'generate-report',
  data: { type: 'daily' },
});

const scheduler = await queue.getJobScheduler('daily-report');
const schedulers = await queue.getJobSchedulers(0, 100);
const count = await queue.getJobSchedulersCount();
await queue.removeJobScheduler('daily-report');
```

## DLQ operations

The dead letter queue collects jobs that failed permanently (retries exhausted, stalled too often, timed out). Configure how it behaves and act on its entries:

```typescript
queue.setDlqConfig({
  autoRetry: true,             // Periodically re-queue DLQ entries
  autoRetryInterval: 3600000,  // Every hour
  maxAutoRetries: 3,
  maxAge: 604800000,           // Drop entries older than 7 days
  maxEntries: 10000,
});

// Inspect (embedded mode only; returns [] / zeros in TCP mode)
const entries = queue.getDlq();
const stalledJobs = queue.getDlq({ reason: 'stalled' });
const stats = queue.getDlqStats(); // { total, byReason, pendingRetry, ... }

// Act
queue.retryDlq();           // Retry all
queue.retryDlq('job-123');  // Retry one
queue.purgeDlq();           // Clear all
```

:::note[TCP mode]
Enumerating DLQ entries from this client API works in embedded mode only. Over TCP, use the [CLI](/guide/cli/) (`bunqueue dlq list|retry|purge`); `retryDlq()` and `purgeDlq()` still send the command but return 0 instead of a count. `getDlqConfigAsync()` fetches the real config from the server.
:::

See [Dead Letter Queue](/guide/dlq/) for the full guide, and [Stall Detection](/guide/stall-detection/) for `setStallConfig()`, which controls when unresponsive jobs are recovered.

## Workers and metrics

```typescript
const workers = await queue.getWorkers();       // Active workers on this queue
const count = await queue.getWorkersCount();

const completedMetrics = await queue.getMetrics('completed', 0, 100);
const failedMetrics = await queue.getMetrics('failed', 0, 100);

await queue.trimEvents(1000);                   // Trim the event log
```

## Namespace Isolation (`prefixKey`)

`prefixKey` lets multiple environments, tenants, or services share one server without their jobs, crons, stats, pause state, DLQ, or rate limits overlapping. The prefix is added to the queue name server-side; `Queue.name` keeps reporting the logical name.

```typescript
// Same server, fully isolated namespaces
const devQueue  = new Queue('emails', { prefixKey: 'dev:' });
const prodQueue = new Queue('emails', { prefixKey: 'prod:' });

await devQueue.add('send', { to: 'tester@example.com' });
await prodQueue.getJobCountsAsync();  // never sees dev jobs
```

A Worker must use the same `prefixKey` to consume the prefixed queue:

```typescript
const devWorker = new Worker('emails', processor, { prefixKey: 'dev:' });
```

Common patterns: `dev:` / `staging:` / `prod:` on one server, `tenant-${id}:` per customer, per-service prefixes in a monorepo, `test-${runId}:` for parallel test isolation.

Notes:

- Everything is isolated per prefix: jobs, worker locks, counts, pause/drain/obliterate, rate limits, and cron schedulers (two prefixes can reuse the same `schedulerId`).
- Backward compatible: without `prefixKey`, behavior is unchanged. Works in embedded and TCP modes.
- The only user-visible side effect: `Job.queueName` inside processors shows the prefixed key (e.g. `dev:emails`).

## Auto-batching (TCP mode)

In TCP mode, concurrent `queue.add()` calls are transparently combined into single bulk commands. Enabled by default, no code changes: sequential `await add()` sends immediately with no penalty (~10k ops/s), while concurrent adds (`Promise.all`) batch into one round-trip (~145k ops/s).

```typescript
const queue = new Queue('tasks', {
  autoBatch: {
    enabled: true,   // default
    maxSize: 50,     // flush when the buffer reaches this size (default: 50)
    maxDelayMs: 5,   // max wait before flushing (default: 5)
  },
});
```

:::caution[Durable jobs bypass the batcher]
Jobs with `durable: true` are always sent individually so they hit the disk immediately, they are never batched.
:::

## Store-and-forward: `queue.forward()`

Drain a local embedded queue to a remote bunqueue server, the edge/IoT pattern (local queue as offline buffer, central server as destination):

```typescript
const forwarder = queue.forward({
  to: { host: 'queue.example.com', port: 6789, tls: true, token: process.env.BQ_TOKEN },
  queue: 'central-name', // optional remote queue name (default: same)
  concurrency: 4,        // parallel forwards (default: 4)
  durable: true,         // push remotely with durable: true (default: false)
});

forwarder.on('forwarded', ({ id, remoteId, name }) => {});
forwarder.on('error', (err) => {});
await forwarder.close();
```

If the remote is down, jobs stay local (retry, then DLQ), nothing is lost. Full guide: [IoT & Edge](/guide/iot-edge/).

## Job Options Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `priority` | `number` | `0` | Higher = processed first |
| `delay` | `number` | `0` | Delay in ms before processing |
| `attempts` | `number` | `3` | Max retry attempts |
| `backoff` | `number \| { type, delay }` | `1000` | Backoff base in ms, or `{ type: 'fixed' \| 'exponential', delay }` |
| `timeout` | `number` | - | Processing timeout in ms |
| `jobId` | `string` | - | Custom ID for idempotent adds |
| `deduplication` | `object` | - | TTL-based dedup (`id`, `ttl`, `extend`, `replace`) |
| `removeOnComplete` | `boolean` | `false` | Auto-delete after completion |
| `removeOnFail` | `boolean` | `false` | Auto-delete after failure |
| `stallTimeout` | `number` | - | Per-job stall timeout override |
| `repeat` | `object` | - | Repeating job config (`every`, `pattern`, `limit`) |
| `durable` | `boolean` | `false` | Write to disk before returning |
| `lifo` | `boolean` | `false` | Process newest first |
| `parent` | `{ id, queue }` | - | Parent job reference for [flows](/guide/flow/) |

:::tip[Related Guides]
- [Worker API](/guide/worker/), process jobs from queues
- [Dead Letter Queue](/guide/dlq/), handle failed jobs
- [Rate Limiting](/guide/rate-limiting/), control processing rates
- [Queue Group](/guide/queue-group/), manage multiple queues
:::
