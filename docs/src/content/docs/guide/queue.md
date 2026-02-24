---
title: Queue API Reference
description: Master bunqueue Queue API for adding jobs with priorities, delays, retries, and deduplication. Includes bulk operations, durable writes, and DLQ config
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/queue.png
---

The `Queue` class is used to add and manage jobs.

:::caution[Important]
In embedded mode, the Queue **must** have `embedded: true`.
Without it, the Queue defaults to TCP mode and tries to connect to a bunqueue server.
:::

## Creating a Queue

```typescript
import { Queue } from 'bunqueue/client';

// Basic queue - embedded mode
const queue = new Queue('my-queue', { embedded: true });

// Typed queue
interface TaskData {
  userId: number;
  action: string;
}
const typedQueue = new Queue<TaskData>('tasks', { embedded: true });

// With default job options
const queue = new Queue('emails', {
  embedded: true,
  defaultJobOptions: {
    attempts: 3,
    backoff: 1000,
    removeOnComplete: true,
  }
});
```

### TCP Mode (Server)

```typescript
// Connect to bunqueue server (no embedded option)
const queue = new Queue('tasks');

// With custom connection
const queue = new Queue('tasks', {
  connection: {
    host: '192.168.1.100',
    port: 6789,
    token: 'secret-token',
    poolSize: 4,  // Connection pool size
  }
});
```

## Adding Jobs

### Single Job

```typescript
const job = await queue.add('job-name', { key: 'value' });

// With options
const job = await queue.add('job-name', data, {
  priority: 10,           // Higher = processed first
  delay: 5000,            // Delay in ms before processing
  attempts: 5,            // Max retry attempts (default: 3)
  backoff: 2000,          // Backoff between retries (default: 1000ms, jitter applied)
  backoffConfig: {        // Advanced backoff configuration
    type: 'exponential',  // 'fixed' or 'exponential'
    delay: 2000,          // Base delay in ms
    maxDelay: 60000,      // Max cap in ms (default: 3,600,000 = 1 hour)
  },
  timeout: 30000,         // Job timeout in ms
  jobId: 'custom-id',     // Custom job ID for deduplication (BullMQ-style)
  removeOnComplete: true, // Remove job data after completion
  removeOnFail: false,    // Keep failed jobs
  stallTimeout: 60000,    // Per-job stall timeout (overrides queue config)
});
```

### Bulk Add

```typescript
// Batch optimized - single lock, batch INSERT
const jobs = await queue.addBulk([
  { name: 'task-1', data: { id: 1 } },
  { name: 'task-2', data: { id: 2 }, opts: { priority: 10 } },
  { name: 'task-3', data: { id: 3 }, opts: { delay: 5000 } },
]);
```

### Repeatable Jobs

```typescript
// Repeat every 5 seconds
await queue.add('heartbeat', {}, {
  repeat: {
    every: 5000,
  }
});

// Repeat with limit
await queue.add('daily-report', {}, {
  repeat: {
    every: 86400000, // 24 hours
    limit: 30,       // Max 30 repetitions
  }
});

// Cron pattern (server mode)
await queue.add('weekly', {}, {
  repeat: {
    pattern: '0 9 * * MON', // Every Monday at 9am
  }
});
```

### Durable Jobs

By default, bunqueue uses a **write buffer** for high throughput: jobs are batched in memory and flushed to SQLite every 10ms. This achieves ~100k jobs/sec but means jobs could be lost if the process crashes before the buffer is flushed.

For **critical jobs** where data loss is unacceptable, use the `durable` option:

```typescript
// Critical job: immediate disk write, guaranteed persistence
await queue.add('process-payment', { orderId: '123', amount: 99.99 }, {
  durable: true,
});

// Batch of critical jobs
await queue.addBulk([
  { name: 'payment-1', data: { orderId: '1' }, opts: { durable: true } },
  { name: 'payment-2', data: { orderId: '2' }, opts: { durable: true } },
]);
```

:::tip[When to use durable]
Use `durable: true` for:
- **Payment processing** - financial transactions must not be lost
- **Order creation** - e-commerce orders require guaranteed persistence
- **Critical events** - audit logs, compliance data, legal records
- **Idempotency keys** - when retry is expensive or impossible
:::

:::note[Performance trade-off]
| Mode | Throughput | Data Loss Window | Use Case |
|------|------------|------------------|----------|
| Default | ~100k jobs/sec | Up to 10ms | Emails, notifications, analytics |
| Durable | ~10k jobs/sec | None | Payments, orders, critical events |
:::

### Job Deduplication (BullMQ-style)

Use `jobId` to prevent duplicate jobs. When a job with the same `jobId` already exists, **the existing job is returned** instead of creating a duplicate. This works in both **embedded** and **TCP** modes (including auto-batched operations). This is BullMQ-compatible idempotent behavior.

```typescript
// Basic deduplication with jobId (BullMQ-style idempotency)
// If job with same jobId exists, returns existing job instead of creating duplicate
const job = await queue.add('send-email', { to: 'user@test.com' }, {
  jobId: 'email-user-123'
});

// First call: creates the job
const job1 = await queue.add('process', { orderId: 123 }, {
  jobId: 'order-123'
});

// Second call with same jobId: returns existing job (no duplicate)
const job2 = await queue.add('process', { orderId: 123 }, {
  jobId: 'order-123'
});

console.log(job1.id === job2.id); // true - same job returned
```

:::tip[Use cases for jobId]
- **Idempotent operations**: Safe to call multiple times without side effects
- **Service restart recovery**: Restore jobs without creating duplicates
- **Webhook deduplication**: Prevent duplicate processing of retried webhooks
- **User action deduplication**: Prevent double-submits from UI
:::

```typescript
// Example: Restore jobs on service startup
async function restoreJobs(jobsToRestore: SavedJob[]) {
  for (const saved of jobsToRestore) {
    // Safe: existing jobs are returned, not duplicated
    await queue.add('process', saved.data, {
      jobId: saved.id
    });
  }
}
```

### Advanced Deduplication

For more control over deduplication behavior, use the `deduplication` option with TTL-based unique keys and strategies.

#### TTL-Based Deduplication

While `jobId` provides permanent idempotency (via `customId`), the `deduplication` option uses a separate `uniqueKey` mechanism with TTL-based expiry. The `id` field is required:

```typescript
// TTL-based deduplication - unique key expires after 1 hour
await queue.add('notification', { userId: '123' }, {
  deduplication: {
    id: 'notify-123',   // Required: unique deduplication key
    ttl: 3600000        // 1 hour in ms
  }
});

// After TTL expires, the same id can create a new job
// This is useful for rate-limiting or time-windowed deduplication
```

#### Extend Strategy

The `extend` strategy resets the TTL of an existing job when a duplicate is detected. The existing job is returned (not replaced), but its deduplication window is extended:

```typescript
// Extend strategy - reset TTL if duplicate, return existing job
await queue.add('rate-limited-task', { action: 'sync' }, {
  deduplication: {
    id: 'sync-task',   // Required: unique deduplication key
    ttl: 60000,
    extend: true       // Extend TTL on duplicate
  }
});
```

:::tip[When to use extend]
- **Rate limiting**: Prevent action spam while keeping the dedup window active
- **Debouncing**: Extend the quiet period on each trigger
- **Session activity**: Keep deduplication active while user is active
:::

#### Replace Strategy

The `replace` strategy removes the existing job and inserts a new one with the updated data. This is useful when you always want the latest data to be processed:

```typescript
// Replace strategy - remove old job, insert new one
await queue.add('latest-data', { data: newData }, {
  deduplication: {
    id: 'data-job',    // Required: unique deduplication key
    ttl: 300000,
    replace: true      // Replace existing job with new data
  }
});
```

:::caution[Replace behavior]
When using `replace: true`:
- The old job is **removed** from the queue
- A **new job** is created with the new data
- The new job gets a **new internal ID**
- Jobs that are already being processed (active state) will **not** be replaced
:::

:::tip[When to use replace]
- **Latest state sync**: Always process the most recent data
- **Configuration updates**: Replace pending config changes with latest
- **Aggregated events**: Collapse multiple events into one with latest payload
:::

#### Deduplication Options Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `ttl` | `number` | - | Time in ms before unique key expires |
| `extend` | `boolean` | `false` | Reset TTL on duplicate (returns existing job) |
| `replace` | `boolean` | `false` | Remove old job and create new one |

:::note[Strategy precedence]
If both `extend` and `replace` are set to `true`, `replace` takes precedence.
:::

## Query Operations

```typescript
// Get job by ID
const job = await queue.getJob('job-id');

// Get job state
const state = await queue.getJobState('job-id');
// 'waiting' | 'active' | 'completed' | 'failed' | 'delayed'

// Get job counts (sync - embedded mode only)
const counts = queue.getJobCounts();
// { waiting: 10, active: 2, completed: 100, failed: 3 }

// Get job counts (async - works with TCP)
const counts = await queue.getJobCountsAsync();

// Get jobs with filtering (sync - embedded mode only)
const jobs = queue.getJobs({ state: 'waiting', start: 0, end: 10 });

// Get jobs with filtering (async - works with TCP)
const jobs = await queue.getJobsAsync({ state: 'failed', start: 0, end: 50 });

// Get counts grouped by priority
const byPriority = queue.getCountsPerPriority();
// { 0: 50, 10: 20, 100: 5 }

// Async version
const byPriority = await queue.getCountsPerPriorityAsync();
```

### Jobs by State

```typescript
// Sync (embedded mode only)
const waiting = queue.getWaiting(0, 10);
const active = queue.getActive(0, 10);
const completed = queue.getCompleted(0, 10);
const failed = queue.getFailed(0, 10);
const delayed = queue.getDelayed(0, 10);

// Async (works with TCP)
const waiting = await queue.getWaitingAsync(0, 10);
const active = await queue.getActiveAsync(0, 10);
const completed = await queue.getCompletedAsync(0, 10);
const failed = await queue.getFailedAsync(0, 10);
const delayed = await queue.getDelayedAsync(0, 10);
```

### Count Methods

```typescript
// Sync (embedded mode only)
const waitingCount = queue.getWaitingCount();
const activeCount = queue.getActiveCount();
const completedCount = queue.getCompletedCount();
const failedCount = queue.getFailedCount();
const delayedCount = queue.getDelayedCount();
const total = queue.count();

// Async (works with TCP)
const total = await queue.countAsync();

// Check if paused
const paused = queue.isPaused();           // sync
const paused = await queue.isPausedAsync(); // async
```

### BullMQ Compatibility Methods

```typescript
// Get waiting jobs sorted by priority (highest first)
const prioritized = await queue.getPrioritized(0, 10);
const count = await queue.getPrioritizedCount();

// Get jobs waiting for children to complete
const waitingChildren = await queue.getWaitingChildren(0, 10);
const count = await queue.getWaitingChildrenCount();
```

## Queue Control

```typescript
// Pause processing (workers stop pulling)
queue.pause();

// Resume processing
queue.resume();

// Remove all waiting jobs
queue.drain();

// Remove all queue data
queue.obliterate();

// Remove a specific job
queue.remove('job-id');

// Wait until queue/server is ready
await queue.waitUntilReady();

// Close TCP connection (when done)
queue.close();

// Async disconnect (compatibility)
await queue.disconnect();
```

## Clean & Maintenance

```typescript
// Remove completed jobs older than 1 hour (sync)
queue.clean(3600000, 100, 'completed');

// Async version (works with TCP)
const removed = await queue.cleanAsync(3600000, 100, 'completed');

// Promote delayed jobs to waiting
queue.promoteJobs({ count: 50 });

// Bulk retry failed or completed jobs
const retried = await queue.retryJobs({ state: 'failed', count: 100 });
```

## Job Progress & Logs

```typescript
// Update job progress
await queue.updateJobProgress('job-id', 75);

// Get job logs
const logs = queue.getJobLogs('job-id', 0, 100);

// Add log entry to a job
await queue.addJobLog('job-id', 'Processing step 3 completed');
```

## Job Dependencies

```typescript
// Get child job results
const childValues = await queue.getChildrenValues('parent-job-id');

// Get job dependencies info
const deps = await queue.getJobDependencies('job-id');
const depCounts = await queue.getJobDependenciesCount('job-id');

// Get child jobs with filter
const processed = await queue.getDependencies('parent-id', 'processed', 0, 10);
const unprocessed = await queue.getDependencies('parent-id', 'unprocessed', 0, 10);

// Wait for a job to finish
const result = await queue.waitJobUntilFinished('job-id', queueEvents, 30000);
```

## Job State Transitions

```typescript
// Move job to completed with return value
await queue.moveJobToCompleted('job-id', { success: true }, token);

// Move job to failed with error
await queue.moveJobToFailed('job-id', new Error('reason'), token);

// Move job back to waiting
await queue.moveJobToWait('job-id', token);

// Move job to delayed with specific timestamp
await queue.moveJobToDelayed('job-id', Date.now() + 60000, token);

// Move job to waiting-for-children state
await queue.moveJobToWaitingChildren('job-id', token);
```

## Rate Limiting & Concurrency

```typescript
// Set global concurrency limit (max parallel jobs across all workers)
queue.setGlobalConcurrency(10);
const concurrency = await queue.getGlobalConcurrency();
queue.removeGlobalConcurrency();

// Set global rate limit (max jobs per time window)
queue.setGlobalRateLimit(100, 1000); // 100 jobs per second
const rateLimit = await queue.getGlobalRateLimit();
queue.removeGlobalRateLimit();

// Throttle queue for specified duration
await queue.rateLimit(5000); // pause for 5 seconds

// Check remaining throttle time
const ttl = await queue.getRateLimitTtl();

// Check if queue hit rate/concurrency limit
const maxed = await queue.isMaxed();
```

## Job Schedulers (Repeatable Jobs)

```typescript
// Create or update a job scheduler
await queue.upsertJobScheduler('daily-report', {
  pattern: '0 9 * * *',       // cron pattern
  // or: every: 3600000,      // interval in ms
}, {
  name: 'generate-report',
  data: { type: 'daily' },
});

// Get a scheduler
const scheduler = await queue.getJobScheduler('daily-report');

// List all schedulers
const schedulers = await queue.getJobSchedulers(0, 100);
const count = await queue.getJobSchedulersCount();

// Remove a scheduler
await queue.removeJobScheduler('daily-report');
```

## Deduplication Management

```typescript
// Look up job ID by deduplication key
const jobId = await queue.getDeduplicationJobId('my-unique-key');

// Remove deduplication key (allows re-adding same jobId)
await queue.removeDeduplicationKey('my-unique-key');
```

## Workers & Metrics

```typescript
// List active workers
const workers = await queue.getWorkers();
const count = await queue.getWorkersCount();

// Get historical metrics
const completedMetrics = await queue.getMetrics('completed', 0, 100);
const failedMetrics = await queue.getMetrics('failed', 0, 100);

// Trim event log
await queue.trimEvents(1000);
```

## Stall Configuration

Configure stall detection to recover unresponsive jobs.

```typescript
queue.setStallConfig({
  enabled: true,
  stallInterval: 30000,  // 30 seconds without heartbeat = stalled
  maxStalls: 3,          // Move to DLQ after 3 stalls
  gracePeriod: 5000,     // 5 second grace period after job starts
});

// Get current config
const config = queue.getStallConfig();
```

See [Stall Detection](/guide/stall-detection/) for more details.

## DLQ Operations

```typescript
// Configure DLQ
queue.setDlqConfig({
  autoRetry: true,
  autoRetryInterval: 3600000,  // 1 hour
  maxAutoRetries: 3,
  maxAge: 604800000,           // 7 days
  maxEntries: 10000,
});

// Get current DLQ config
const dlqConfig = queue.getDlqConfig();

// Get DLQ entries
const entries = queue.getDlq();

// Filter entries
const stalledJobs = queue.getDlq({ reason: 'stalled' });
const recentFails = queue.getDlq({ newerThan: Date.now() - 3600000 });

// Get stats
const stats = queue.getDlqStats();
// { total, byReason, pendingRetry, expired, oldestEntry, newestEntry }

// Retry from DLQ
queue.retryDlq();           // Retry all
queue.retryDlq('job-123');  // Retry specific

// Retry by filter
queue.retryDlqByFilter({ reason: 'timeout', limit: 10 });

// Purge DLQ
queue.purgeDlq();
```

See [Dead Letter Queue](/guide/dlq/) for more details.

## Retry Completed Jobs

The `retryCompleted()` method allows re-queuing completed jobs for reprocessing. This is useful when you need to re-run a job that completed successfully, for example when business logic changes or you need to regenerate outputs.

```typescript
// Retry a specific completed job
const success = queue.retryCompleted('job-id-123');
if (success) {
  console.log('Job re-queued for processing');
}

// Retry all completed jobs (use with caution!)
const count = queue.retryCompleted();
console.log(`Re-queued ${count} completed jobs`);

// Async version for TCP mode
const count = await queue.retryCompletedAsync();
```

:::caution[Use with care]
Retrying all completed jobs can re-queue a large number of jobs at once. Consider filtering or limiting the scope when dealing with high-volume queues.
:::

## Job Options Reference

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `priority` | `number` | `0` | Higher = processed first |
| `delay` | `number` | `0` | Delay in ms before processing |
| `attempts` | `number` | `3` | Max retry attempts |
| `backoff` | `number` | `1000` | Backoff base in ms (exponential, jitter applied) |
| `backoffConfig` | `object` | - | Advanced backoff: `{ type, delay, maxDelay? }` |
| `timeout` | `number` | - | Processing timeout in ms |
| `jobId` | `string` | - | Custom ID for deduplication (BullMQ-style idempotent) |
| `deduplication` | `object` | - | Advanced deduplication config (`ttl`, `extend`, `replace`) |
| `removeOnComplete` | `boolean` | `false` | Auto-delete after completion |
| `removeOnFail` | `boolean` | `false` | Auto-delete after failure |
| `stallTimeout` | `number` | - | Per-job stall timeout override |
| `repeat` | `object` | - | Repeating job config |
| `durable` | `boolean` | `false` | Immediate disk write (bypass buffer) |

## Closing

```typescript
// Close TCP connection (no-op in embedded mode)
queue.close();
```

:::tip[Related Guides]
- [Rate Limiting & Concurrency Control](/guide/rate-limiting/) - Control job processing rates
- [Dead Letter Queue](/guide/dlq/) - Handle failed jobs
- [Worker API](/guide/worker/) - Process jobs from queues
- [Queue Group](/guide/queue-group/) - Manage multiple queues
:::
