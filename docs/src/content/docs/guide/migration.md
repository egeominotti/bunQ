---
title: "Migrate from BullMQ to Bunqueue: Step-by-Step Guide"
description: Migrate from BullMQ to bunqueue with minimal code changes. Drop Redis, keep your Queue and Worker API, and switch to Bun-native SQLite.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/api-reference.png
---


This guide helps you migrate from BullMQ to bunqueue with minimal code changes.

## Overview

bunqueue provides a BullMQ-compatible API, making migration straightforward for most use cases.

| BullMQ | bunqueue | Notes |
|--------|----------|-------|
| `new Queue()` | `new Queue()` | ✅ Same API |
| `new Worker()` | `new Worker()` | ✅ Same API |
| `QueueEvents` | `QueueEvents` | ✅ Same API |
| `FlowProducer` | `FlowProducer` | ✅ Same API |
| `jobId` deduplication | `jobId` deduplication | ✅ Same behavior (idempotent) |
| Redis connection | Not needed | ✅ Simpler |

## Step 1: Install bunqueue

```bash
# Remove BullMQ and Redis
bun remove bullmq ioredis

# Install bunqueue
bun add bunqueue
```

## Step 2: Update Imports

```typescript
// Before (BullMQ)
import { Queue, Worker, QueueEvents } from 'bullmq';
import Redis from 'ioredis';

const connection = new Redis();
const queue = new Queue('my-queue', { connection });

// After (bunqueue)
import { Queue, Worker, QueueEvents } from 'bunqueue/client';

const queue = new Queue('my-queue', { embedded: true });
// No connection needed - uses in-process SQLite!
```

:::caution[Persistence Setup]
For data persistence in embedded mode, pass `dataPath` in the constructor or set the `DATA_PATH` environment variable:
```typescript
const queue = new Queue('my-queue', { embedded: true, dataPath: './data/bunq.db' });
```
Without this, data is stored in-memory only and will be lost on restart. For server mode, use a [configuration file](/guide/configuration/).
:::

## Step 3: Remove Redis Configuration

```typescript
// Before (BullMQ)
const queue = new Queue('emails', {
  connection: {
    host: 'localhost',
    port: 6379,
    password: 'secret',
  },
  defaultJobOptions: {
    attempts: 3,
    backoff: { type: 'exponential', delay: 1000 },
  },
});

// After (bunqueue)
const queue = new Queue('emails', {
  embedded: true,
  defaultJobOptions: {
    attempts: 3,
    backoff: 1000, // Base delay for exponential backoff
  },
});
```

## Step 4: Update Worker

```typescript
// Before (BullMQ)
const worker = new Worker('emails', async (job) => {
  await sendEmail(job.data);
  return { sent: true };
}, {
  connection,
  concurrency: 5,
  limiter: { max: 100, duration: 1000 },
});

// After (bunqueue)
const worker = new Worker('emails', async (job) => {
  await sendEmail(job.data);
  return { sent: true };
}, {
  embedded: true,
  concurrency: 5,
  limiter: { max: 100, duration: 1000 }, // per-worker rate limit (BullMQ-compatible, works embedded)
});
```

## Step 5: Update Events

Events work the same way:

```typescript
// Same in both BullMQ and bunqueue
worker.on('completed', (job, result) => {
  console.log(`Job ${job.id} completed`);
});

worker.on('failed', (job, err) => {
  console.error(`Job ${job.id} failed:`, err.message);
});

worker.on('progress', (job, progress) => {
  console.log(`Job ${job.id}: ${progress}%`);
});
```

## Step 6: Update Job Options

```typescript
// Before (BullMQ)
await queue.add('task', data, {
  priority: 1,
  delay: 5000,
  attempts: 3,
  backoff: { type: 'exponential', delay: 1000 },
  removeOnComplete: true,
  removeOnFail: false,
  jobId: 'custom-id',
});

// After (bunqueue) - Almost identical
// Queue created with: new Queue('tasks', { embedded: true })
await queue.add('task', data, {
  priority: 1,
  delay: 5000,
  attempts: 3,
  backoff: 1000, // Base delay (exponential: 1s, 2s, 4s, 8s...)
  removeOnComplete: true,
  removeOnFail: false,
  jobId: 'custom-id',
});
```

## API Differences

### Backoff Configuration

```typescript
// BullMQ supports both types
backoff: { type: 'exponential', delay: 1000 }
backoff: { type: 'fixed', delay: 5000 }

// bunqueue supports both fixed and exponential backoff
backoff: { type: 'exponential', delay: 1000 } // Same as BullMQ
backoff: { type: 'fixed', delay: 5000 }       // Same as BullMQ
backoff: 1000 // Shorthand: base delay with exponential backoff
```

:::note[Backoff Options]
bunqueue supports both `fixed` and `exponential` backoff types, matching BullMQ's behavior:

**Exponential** (`type: 'exponential'`):
- Attempt 1 fails → wait ~1000ms
- Attempt 2 fails → wait ~2000ms
- Attempt 3 fails → wait ~4000ms
- Formula: `delay * 2^(attempt-1)` with ±50% jitter

**Fixed** (`type: 'fixed'`):
- Every retry waits approximately the same delay (e.g., ~5000ms each time, ±20% jitter)

All delays include automatic **jitter** to prevent thundering herd when many jobs retry simultaneously. Delays are capped at 1 hour by default.

```typescript
backoff: { type: 'exponential', delay: 1000 } // 'fixed' | 'exponential' + base delay
```

You can also pass a plain number as shorthand for exponential backoff with that base delay.
:::

### Rate Limiting

```typescript
// BullMQ (on worker)
new Worker('queue', processor, {
  limiter: { max: 100, duration: 1000 }
});

// bunqueue (same per-worker limiter, works in embedded mode)
new Worker('queue', processor, {
  embedded: true,
  limiter: { max: 100, duration: 1000 }
});
```

:::note
bunqueue supports BullMQ's per-worker `limiter: { max, duration }` (works embedded). You can also set a queue-level limit with `queue.setGlobalRateLimit(max, duration?)`.
:::

### Sandboxed Processors

```typescript
// BullMQ sandboxed processors
new Worker('queue', './processor.js', { connection });

// bunqueue — recommended: inline Worker (production-ready)
import { Worker } from 'bunqueue/client';

const worker = new Worker('queue', async (job) => {
  // same logic from your processor.js
  return result;
}, { embedded: true, concurrency: 4 });

// bunqueue — alternative: SandboxedWorker (experimental, Bun Workers)
import { SandboxedWorker } from 'bunqueue/client';

const worker = new SandboxedWorker('queue', {
  processor: './processor.ts',
  concurrency: 4,
  timeout: 30000,
});
worker.start();
```

:::caution
`SandboxedWorker` depends on experimental Bun Workers. For production, use the standard `Worker` with an inline processor. See [Worker vs SandboxedWorker](/guide/worker/#worker-vs-sandboxedworker).
:::

### Repeatable Jobs

```typescript
// BullMQ
await queue.add('task', data, {
  repeat: { cron: '0 * * * *' }
});

// bunqueue (queue created with embedded: true)
await queue.add('task', data, {
  repeat: { pattern: '0 * * * *' }
});
// Or use interval
await queue.add('task', data, {
  repeat: { every: 3600000 }
});
```

## BullMQ v5 State Parity

bunqueue implements the full BullMQ v5 job state machine:

| State | BullMQ v5 | bunqueue | Notes |
|-------|-----------|----------|-------|
| `waiting` | ✅ | ✅ | Jobs with priority = 0 |
| `prioritized` | ✅ | ✅ | Jobs with priority > 0 |
| `delayed` | ✅ | ✅ | Jobs waiting for delay |
| `active` | ✅ | ✅ | Currently processing |
| `completed` | ✅ | ✅ | Successfully finished |
| `failed` | ✅ | ✅ | Failed after all retries (DLQ) |
| `waiting-children` | ✅ | ✅ | Waiting for child flows |

## BullMQ v5 Flow Parity

| Feature | BullMQ v5 | bunqueue | Notes |
|---------|-----------|----------|-------|
| `FlowProducer.add()` | ✅ | ✅ | Children before parent |
| `FlowProducer.addBulk()` | ✅ | ✅ | With atomic rollback |
| `FlowProducer.getFlow()` | ✅ | ✅ | Retrieve flow tree |
| `FlowOpts.queuesOptions` | ✅ | ✅ | Per-queue defaults |
| `failParentOnFailure` | ✅ | ✅ | Propagate child failure to parent |
| `removeDependencyOnFailure` | ✅ | ✅ | Remove dep on failure |
| `EventEmitter` | ✅ | ✅ | FlowProducer extends EventEmitter |
| `close()` returns Promise | ✅ | ✅ | Async shutdown |
| Atomic flow creation | ✅ (Redis Lua) | ✅ (rollback) | Different mechanism, same guarantee |

## Features Comparison

| Feature | BullMQ | bunqueue | Notes |
|---------|--------|----------|-------|
| Sandboxed processors | ✅ | ✅ | Use `SandboxedWorker` (experimental — Bun Workers) |
| Redis Cluster | ✅ | ❌ | Single instance |
| Redis Streams | ✅ | ❌ | SQLite storage |
| Rate limit per worker | ✅ | ✅ | `WorkerOptions.limiter: { max, duration }` (BullMQ v5 compatible) |

## Migration Checklist

- [ ] Remove `bullmq` and `ioredis` packages
- [ ] Install `bunqueue`
- [ ] Update imports to `bunqueue/client`
- [ ] Remove all Redis connection configuration
- [ ] Update backoff configuration (simplified)
- [ ] Move rate limiting from worker to queue
- [ ] Update sandboxed processors to use `SandboxedWorker`
- [ ] Update repeat config (`cron` → `pattern`)
- [ ] Test all job processing
- [ ] Remove Redis server from infrastructure

## Getting Help

If you encounter issues during migration:

- [GitHub Issues](https://github.com/egeominotti/bunqueue/issues)
- [GitHub Discussions](https://github.com/egeominotti/bunqueue/discussions)

:::tip[Related Guides]
- [Queue API](/guide/queue/) - Full queue API reference
- [Worker API](/guide/worker/) - Worker configuration
- [FAQ](/faq/) - Common migration questions
:::
