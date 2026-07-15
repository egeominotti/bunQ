---
title: "Bun Worker: Process Background Jobs with Retries"
description: "Run a Bun worker: process background jobs with concurrency, retries, heartbeats, batch pulling and lock-based ownership."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/worker.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · worker api</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Pull, process, ack, <em>repeat.</em></h1>
  <p class="bq-hero-sub">A Worker takes jobs from a queue and runs your function on each one. This page covers concurrency, events, error handling, and the safety nets that recover jobs when things go wrong.</p>
</div>

## Create a worker

```typescript
import { Worker } from 'bunqueue/client';

const worker = new Worker('my-queue', async (job) => {
  // Process the job; the return value is stored as the job's result
  return { success: true };
}, { embedded: true });
```

The worker starts polling immediately. If your processor throws, the job is retried automatically (up to the job's `attempts`, default 3) and then moved to the dead letter queue, a holding area for jobs that keep failing.

:::caution[Embedded vs TCP]
`embedded: true` runs in-process alongside an embedded Queue. Without it, the Worker connects to a bunqueue server on `localhost:6789`. Worker and Queue must use the same mode.
:::

## Process jobs in parallel

```typescript
const worker = new Worker('my-queue', processor, {
  embedded: true,
  concurrency: 5,  // Up to 5 jobs at once (default: 1)
});
```

You can change concurrency at runtime, without restarting:

```typescript
worker.concurrency = 10;  // Scale up under load
worker.concurrency = 2;   // Scale back down (minimum: 1)
```

## Use the job object

Inside the processor you get the full job:

```typescript
const worker = new Worker('queue', async (job) => {
  job.id;           // Job ID
  job.name;         // Job name
  job.data;         // Job data (typed if you use Worker<T>)
  job.attemptsMade; // Current attempt number
  job.timestamp;    // When the job was created

  await job.updateProgress(50, 'Halfway done');  // Report progress
  await job.log('Processing step 1');             // Attach a log line

  return result;
}, { embedded: true });
```

## React to events

```typescript
worker.on('completed', (job, result) => {
  console.log(`Completed: ${job.id}`, result);
});

worker.on('failed', (job, error) => {
  console.error(`Failed: ${job.id}`, error.message);
});

worker.on('progress', (job, progress) => {
  console.log(`Progress: ${job.id} - ${progress}%`);
});
```

All events are fully typed. The complete list:

| Event | Callback Parameters | Description |
|-------|-------------------|-------------|
| `ready` | `()` | Worker started polling |
| `active` | `(job: Job<T>)` | Job started processing |
| `completed` | `(job: Job<T>, result: R)` | Job completed successfully |
| `failed` | `(job: Job<T>, error: Error)` | Job processing failed |
| `progress` | `(job: Job<T> \| null, progress: number)` | Job progress updated |
| `stalled` | `(jobId: string, reason: string)` | Job stalled (no heartbeat) |
| `drained` | `()` | Queue has no more waiting jobs |
| `error` | `(error: Error)` | Worker-level error |
| `cancelled` | `({ jobId: string, reason: string })` | Job was cancelled |
| `log` | `(job: Job<T>, message: string)` | Log written via `job.log()` |
| `closed` | `()` | Worker shut down |

Remove a listener with `worker.off('completed', handler)`.

## Handle errors

Throwing inside the processor fails the current attempt; bunqueue retries with backoff (a growing wait between attempts) until `attempts` is exhausted:

```typescript
const worker = new Worker('queue', async (job) => {
  await riskyOperation();  // Just let errors throw, retries are automatic
}, { embedded: true });

worker.on('failed', (job, error) => {
  if (job.attemptsMade >= 3) {
    alertOps(job, error);  // Final failure, tell someone
  }
});
```

## Control and shut down

```typescript
worker.run();     // Start processing (if created with autorun: false)
worker.pause();   // Stop pulling new jobs
worker.resume();  // Resume

await worker.close();      // Wait for active jobs, then stop
await worker.close(true);  // Force close immediately
```

For a clean process exit, also shut down the shared machinery:

```typescript
import { shutdownManager, closeSharedTcpClient } from 'bunqueue/client';

process.on('SIGINT', async () => {
  await worker.close();
  shutdownManager();       // Embedded mode: flush writes, close SQLite
  closeSharedTcpClient();  // TCP mode: close the shared connection pool
  process.exit(0);
});
```

## Options reference

```typescript
const worker = new Worker('queue', processor, {
  embedded: true,
  concurrency: 5,
  batchSize: 100,      // Pull up to 100 jobs per request
  pollTimeout: 5000,   // Long-poll: wait up to 5s for jobs instead of busy polling
  limiter: { max: 10, duration: 1000 },  // Max 10 jobs per second
});
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `embedded` | `boolean` | `false` | Use in-process mode |
| `concurrency` | `number` | `1` | Parallel job processing |
| `autorun` | `boolean` | `true` | Start polling automatically |
| `heartbeatInterval` | `number` | `10000` | Heartbeat interval in ms (0 = disabled) |
| `batchSize` | `number` | `10` | Jobs to pull per batch (max: 1000) |
| `pollTimeout` | `number` | `0` | Long-poll timeout in ms (max: 30000) |
| `useLocks` | `boolean` | `true` | Enable BullMQ-style job locks |
| `limiter` | `{ max, duration, groupKey? }` | - | Without `groupKey`: max jobs per window. With `groupKey`: per-group concurrency cap of `max` (jobs grouped by `job.data[groupKey]`, `duration` unused) |
| `lockDuration` | `number` | `30000` | Job lock TTL in ms |
| `maxStalledCount` | `number` | `1` | Max stalls before the job moves to failed |
| `skipStalledCheck` | `boolean` | `false` | Skip stalled job detection |
| `skipLockRenewal` | `boolean` | `false` | Skip lock renewal via heartbeat |
| `drainDelay` | `number` | `50` | Delay between polls when the queue is empty (ms) |
| `removeOnComplete` | `boolean \| number \| KeepJobs` | `false` | Auto-remove completed jobs. Only `true` is honored; `number` / `{ age?, count? }` are accepted for BullMQ type compatibility but treated as `false` |
| `removeOnFail` | `boolean \| number \| KeepJobs` | `false` | Same behavior as `removeOnComplete` (boolean only) |
| `connection` | `ConnectionOptions` | - | TCP connection (`host`, `port`, `token`, `poolSize`) |
| `prefixKey` | `string` | - | Namespace prefix; must match the producing Queue's. See [Namespace Isolation](/guide/queue/#namespace-isolation-prefixkey) |

**Connection pool sizing (TCP):** when `poolSize` is not set, it defaults to `min(concurrency, 8)`. Override it by setting `poolSize` explicitly.

## Heartbeats and stall detection

While a job is processing, the worker automatically pings the queue ("I'm still working on this"). That ping is the heartbeat. If a job stops receiving heartbeats, for example because the worker crashed, the queue marks it stalled and recovers it, so no job is silently lost.

```typescript
const worker = new Worker('queue', processor, {
  embedded: true,
  heartbeatInterval: 5000, // Ping every 5 seconds
});
```

Keep `heartbeatInterval` shorter than the queue's `stallInterval` to avoid false positives. See [Stall Detection](/guide/stall-detection/).

## Lock-based ownership

With `useLocks: true` (the default), each pulled job gets a lock, a temporary claim that says "this worker owns this job". The lock is renewed by heartbeats (`lockDuration` sets its TTL) and must be presented when completing or failing the job. This prevents two workers from processing the same job, and prevents a slow worker's job from being grabbed by a faster one.

Locks matter most in **server mode** with multiple workers. In embedded mode with a single process you can trade the safety for a bit of throughput:

```typescript
const worker = new Worker('queue', processor, {
  embedded: true,
  useLocks: false, // Rely on stall detection only
});
```

Locks can also be extended explicitly: `worker.extendJobLocks(jobIds, tokens, duration)`.

## Batch pulling

For high-volume queues, pull many jobs per round-trip and long-poll while idle:

```typescript
const worker = new Worker('queue', processor, {
  embedded: true,
  batchSize: 100,     // Pull 100 jobs at once
  pollTimeout: 5000,  // Wait up to 5s for jobs (long polling)
});
```

Bulk pushes wake idle long-polling workers immediately, they never wait out the timeout.

## SandboxedWorker

:::danger[Experimental, not recommended for production]
`SandboxedWorker` relies on [Bun Workers](https://bun.sh/docs/runtime/workers), which are currently **experimental** in Bun. Known issues include unexpected memory growth and inconsistent behavior across Bun versions. For production workloads, use the standard `Worker`.
:::

`SandboxedWorker` runs each job in an isolated Bun Worker thread. If a job crashes (out of memory, infinite loop, uncaught exception), only that thread dies; the main process keeps running and the thread is restarted automatically. Use it for CPU-heavy or untrusted code.

```typescript
import { SandboxedWorker } from 'bunqueue/client';

const worker = new SandboxedWorker('cpu-intensive', {
  processor: './processor.ts',  // Path to processor file
  concurrency: 4,               // 4 parallel worker threads
  timeout: 60000,               // Per-job timeout (default: 30000, 0 = disabled)
  maxMemory: 256,               // MB per worker (default: 256)
});

await worker.start();
```

**Processor file** (`processor.ts`):

```typescript
export default async (job: {
  id: string;
  data: any;
  queue: string;
  attempts: number;
  parentId?: string;
  progress: (value: number) => void;
  log: (message: string) => void;
  fail: (error: string | Error) => void;
}) => {
  job.progress(50);
  const result = await heavyComputation(job.data);
  job.progress(100);
  return result;
};
```

To connect to a remote server instead of running embedded, pass a `connection` option (`host`, `port`, `token`), everything else is the same.

Control:

```typescript
await worker.start();
worker.isRunning();
const stats = worker.getStats(); // { total, busy, idle, recycled, restarts }
await worker.stop();             // Graceful (waits for busy workers)
await worker.stop(true);         // Force
```

If your jobs process large files (100MB+), raise `maxMemory` above the 256MB default to avoid OOM crashes.

### SandboxedWorker options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `processor` | `string` | (required) | Path to processor file |
| `concurrency` | `number` | `1` | Parallel worker threads |
| `maxMemory` | `number` | `256` | Max memory per thread in MB (Bun smol mode if <= 64) |
| `timeout` | `number` | `30000` | Per-job timeout in ms (0 = disabled) |
| `autoRestart` | `boolean` | `true` | Auto-restart crashed threads |
| `maxRestarts` | `number` | `10` | Max restart attempts per thread |
| `pollInterval` | `number` | `10` | Job poll interval in ms |
| `heartbeatInterval` | `number` | `5000` (embedded) / `10000` (TCP) | Heartbeat for stall detection / lock renewal |
| `idleTimeout` | `number` | `0` | Stop the pool after this many idle ms (0 = disabled) |
| `idleRecycleMs` | `number` | `30000` | Recycle idle threads after this many ms (0 = disabled) |
| `autoStart` | `boolean` | `false` | Restart the pool when new jobs arrive after an idle shutdown |
| `autoStartPollMs` | `number` | `5000` | Poll interval while idle-stopped |
| `connection` | `ConnectionOptions` | - | TCP connection (omit for embedded) |

SandboxedWorker emits 8 of the Worker events: `ready`, `active`, `completed`, `failed`, `progress`, `log`, `error`, `closed`. It does **not** emit `stalled`, `drained`, or `cancelled`.

### Worker vs SandboxedWorker

| | Worker | SandboxedWorker |
|---|--------|-----------------|
| **Production ready** | ✅ Stable, no experimental APIs | ⚠️ Depends on experimental Bun Workers |
| **I/O-bound tasks** (HTTP, DB, APIs) | ✅ Best choice | Overkill |
| **CPU-intensive tasks** | ⚠️ Blocks event loop | ✅ Runs in separate thread |
| **Untrusted code** | ❌ Runs in main thread | ✅ Isolated |
| **Crash isolation / memory limits** | ❌ | ✅ |
| **Events** | 11 events | 8 events |
| **Concurrency, retries, heartbeats** | ✅ | ✅ Same behavior |

Most workloads are I/O-bound (API calls, database queries, file operations); for those, `Worker` is the right choice. If CPU-heavy work over TCP is dropping connections, see [CPU-Intensive Workers](/guide/cpu-intensive-workers/) for tuning patterns.

:::tip[Related Guides]
- [Queue API](/guide/queue/), add and manage jobs
- [Stall Detection & Recovery](/guide/stall-detection/), handle unresponsive workers
- [Monitoring & Prometheus Metrics](/guide/monitoring/), watch worker performance
:::
