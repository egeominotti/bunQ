---
title: "Simple Mode: Queue + Worker in One Object | bunqueue"
description: "bunqueue Simple Mode combines Queue and Worker in one object: named routes, onion middleware, cron, events, and 12 built-in features with zero boilerplate."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/simple-mode.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · simple mode</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Queue and worker, <em>one object.</em></h1>
  <p class="bq-hero-sub">Simple Mode gives you a Queue and a Worker in a single object. Add jobs, process them, add middleware, schedule crons, all from one place, one thing to close on shutdown.</p>
</div>

If your producer and consumer live in the same process, creating a `Queue` and a `Worker` separately is boilerplate. `Bunqueue` wraps both:

```typescript
import { Bunqueue } from 'bunqueue/client';

const app = new Bunqueue<{ to: string }>('emails', {
  embedded: true,
  processor: async (job) => {
    console.log(`Sending to ${job.data.to}`);
    return { sent: true };
  },
});

await app.add('send', { to: 'alice@example.com' });
```

:::tip[When to use]
Use `Bunqueue` when producer and consumer are in the **same process**. For distributed systems (separate producer and worker services), use [`Queue`](/guide/queue/) + [`Worker`](/guide/worker/) directly.
:::

Under the hood, `Bunqueue` is exactly `new Queue()` + `new Worker()` plus optional subsystems. Each job flows through: circuit breaker check → TTL check → cancellation setup → retry wrapper → middleware → your processor. Every subsystem is off until you configure it.

## Routes

Route jobs to different handlers by name:

```typescript
const app = new Bunqueue<{ to: string }>('notifications', {
  embedded: true,
  routes: {
    'send-email': async (job) => {
      await sendEmail(job.data.to);
      return { channel: 'email' };
    },
    'send-sms': async (job) => {
      await sendSMS(job.data.to);
      return { channel: 'sms' };
    },
  },
});

await app.add('send-email', { to: 'alice' });
await app.add('send-sms', { to: 'bob' });
```

:::caution
Use **one** of `processor`, `routes`, or `batch`. Passing multiple or none throws an error.
:::

## Middleware

Wraps every job execution, like middleware in a web framework. Each middleware receives the job and a `next()` function:

```typescript
// Timing middleware
app.use(async (job, next) => {
  const start = Date.now();
  const result = await next();
  console.log(`${job.name}: ${Date.now() - start}ms`);
  return result;
});

// Error recovery middleware
app.use(async (job, next) => {
  try {
    return await next();
  } catch (err) {
    return { recovered: true, error: err.message };
  }
});
```

Execution order is onion-style: `mw1 → mw2 → processor → mw2 → mw1`. With no middleware added, there is zero overhead.

## Batch processing

Accumulate N jobs and process them together, ideal for bulk database inserts:

```typescript
const app = new Bunqueue('db-inserts', {
  embedded: true,
  batch: {
    size: 50,        // flush every 50 jobs
    timeout: 2000,   // or every 2 seconds, whichever comes first
    processor: async (jobs) => {
      const rows = jobs.map(j => j.data.row);
      await db.insertMany('table', rows);
      return jobs.map(() => ({ inserted: true }));
    },
  },
});
```

On `close()`, remaining buffered jobs are flushed.

## Advanced retry

Five backoff strategies (how long to wait between retry attempts) plus a predicate to decide what is worth retrying:

```typescript
const app = new Bunqueue('api-calls', {
  embedded: true,
  processor: async (job) => {
    const res = await fetch(job.data.url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return { status: res.status };
  },
  retry: {
    maxAttempts: 5,
    delay: 1000,
    strategy: 'jitter',   // 'fixed' | 'exponential' | 'jitter' | 'fibonacci' | 'custom'
    retryIf: (error) => error.message.includes('503'),  // only retry on 503
  },
});
```

| Strategy | Formula | Use case |
|----------|---------|----------|
| `fixed` | `delay` every time | Rate-limited APIs |
| `exponential` | `delay × 2^(attempt-1)` | General purpose |
| `jitter` | `delay × 2^(attempt-1) × random(0.5-1.5)` | Avoid retry storms |
| `fibonacci` | `delay × fib(attempt)` (1x, 2x, 3x, 5x, 8x, ...) | Gradual backoff |
| `custom` | `customBackoff(attempt, error) → ms` | Anything |

This is **in-process retry**: the job stays active while retrying. Different from core `attempts`/`backoff`, which re-queues the job.

## Graceful cancellation

Cancel running jobs via an AbortController signal (the standard way to tell async code to stop):

```typescript
const app = new Bunqueue('encoding', {
  embedded: true,
  processor: async (job) => {
    const signal = app.getSignal(job.id);
    for (const chunk of chunks) {
      if (signal?.aborted) throw new Error('Cancelled');
      await encode(chunk);
    }
    return { done: true };
  },
});

const job = await app.add('video', { file: 'big.mp4' });
app.cancel(job.id);        // cancel immediately
app.cancel(job.id, 5000);  // cancel after 5s grace period
```

The signal works with `fetch` too: `await fetch(url, { signal })`.

## Circuit breaker

When a downstream service is down, retrying every job just burns attempts. A circuit breaker pauses the worker after too many consecutive failures, then probes periodically until the service recovers:

```typescript
const app = new Bunqueue('payments', {
  embedded: true,
  processor: async (job) => paymentGateway.charge(job.data),
  circuitBreaker: {
    threshold: 5,         // open (pause) after 5 consecutive failures
    resetTimeout: 30000,  // try again after 30s
    onOpen: () => alert('Gateway down!'),
    onClose: () => alert('Gateway recovered'),
  },
});

app.getCircuitState();  // 'closed' | 'open' | 'half-open'
app.resetCircuit();     // force close + resume worker
```

When both retry and circuit breaker are active: one job exhausting all its retries counts as one circuit breaker failure.

## Event triggers

Create follow-up jobs automatically when a job completes or fails:

```typescript
const app = new Bunqueue('orders', {
  embedded: true,
  routes: {
    'place-order': async (job) => ({ orderId: job.data.id, total: 99 }),
    'send-receipt': async (job) => ({ sent: true }),
    'fraud-alert': async (job) => ({ alerted: true }),
  },
});

// On complete → create follow-up
app.trigger({
  on: 'place-order',
  create: 'send-receipt',
  data: (result, job) => ({ id: job.data.id }),
});

// Conditional trigger; `result` is typed as unknown, cast it
app.trigger({
  on: 'place-order',
  create: 'fraud-alert',
  data: (result) => ({ amount: (result as { total: number }).total }),
  condition: (result) => (result as { total: number }).total > 1000,
});
```

Triggers chain: `step-1 → step-2 → step-3`. For anything more complex, use the [Workflow Engine](/guide/workflow/).

## Job TTL

Expire jobs that waited too long, checked when the worker picks the job up:

```typescript
const app = new Bunqueue('otp', {
  embedded: true,
  processor: async (job) => verifyOTP(job.data.code),
  ttl: {
    defaultTtl: 300000,           // 5 minutes for all jobs
    perName: {
      'verify-otp': 60000,       // 1 minute for OTP
      'daily-report': 0,         // never expires
    },
  },
});

// Update at runtime
app.setDefaultTtl(120000);
app.setNameTtl('flash-sale', 30000);
```

Resolution order: `perName[job.name]` → `defaultTtl` → `0` (no TTL).

## Priority aging

Low-priority jobs can starve behind a stream of high-priority ones. Priority aging automatically boosts jobs the longer they wait:

```typescript
const app = new Bunqueue('tasks', {
  embedded: true,
  processor: async (job) => ({ done: true }),
  priorityAging: {
    interval: 60000,    // check every 60s
    minAge: 300000,     // start boosting after 5 minutes
    boost: 2,           // +2 priority per tick
    maxPriority: 100,   // cap
    maxScan: 200,       // max jobs per tick
  },
});
```

## Deduplication defaults

Prevent duplicate jobs automatically: jobs with the same name + data get the same dedup ID within the TTL window:

```typescript
const app = new Bunqueue('webhooks', {
  embedded: true,
  processor: async (job) => processWebhook(job.data),
  deduplication: {
    ttl: 60000,       // dedup window: 60 seconds
  },
});

await app.add('hook', { event: 'user.created', userId: '123' });
await app.add('hook', { event: 'user.created', userId: '123' }); // deduplicated!
await app.add('hook', { event: 'user.updated', userId: '123' }); // different data → new job
```

Override per job: `await app.add('task', data, { deduplication: { id: 'my-id', ttl: 5000 } })`. Strategies (`extend`, `replace`) are explained in [Queue → Deduplication](/guide/queue/#deduplication).

### Debounce

The `debounce: { ttl }` option attaches a default debounce id (the job name) to every job. It is BullMQ-compatible metadata, visible via `job.opts.debounce`, but it does **not** suppress duplicates by itself in the current engine. To actually coalesce rapid duplicates, use `deduplication` with `replace: true` (last write wins).

## Rate limiting

Control processing speed:

```typescript
const app = new Bunqueue('api', {
  embedded: true,
  processor: async (job) => callExternalAPI(job.data),
  rateLimit: { max: 100, duration: 1000 },  // max 100 jobs per second
});

// Per-group limiting (e.g. per customer). With groupKey set, `max` becomes
// a per-group concurrency cap (max active jobs per group) and duration is ignored.
const app2 = new Bunqueue('api', {
  embedded: true,
  processor: async (job) => callAPI(job.data),
  rateLimit: { max: 10, duration: 1000, groupKey: 'customerId' },
});

// Runtime updates
app.setGlobalRateLimit(50, 1000);
app.removeGlobalRateLimit();
```

## DLQ (Dead Letter Queue)

The DLQ collects jobs that failed permanently. Simple Mode can auto-retry and prune it:

```typescript
const app = new Bunqueue('critical', {
  embedded: true,
  processor: async (job) => riskyOperation(job.data),
  dlq: {
    autoRetry: true,            // re-queue failed jobs periodically
    autoRetryInterval: 3600000, // every hour
    maxAutoRetries: 3,
    maxAge: 604800000,          // purge entries older than 7 days
    maxEntries: 10000,
  },
});

// Query
const entries = app.getDlq();
const stats = app.getDlqStats();                    // { total, byReason, ... }
const timeouts = app.getDlq({ reason: 'timeout' });

// Act
app.retryDlq();           // retry all
app.retryDlq('job-id');   // retry one
app.purgeDlq();           // clear all
app.setDlqConfig({ autoRetry: false });
```

Failure reasons tracked: `explicit_fail`, `max_attempts_exceeded`, `timeout`, `stalled`, `ttl_expired`, `worker_lost`, plus `unknown` as a fallback.

## Cron jobs

```typescript
await app.cron('daily-report', '0 9 * * *', { type: 'report' });
await app.cron('eu-digest', '0 8 * * 1', { type: 'weekly' }, { timezone: 'Europe/Rome' });
await app.every('healthcheck', 30000, { type: 'ping' });

await app.listCrons();
await app.removeCron('healthcheck');
```

See the [Cron guide](/guide/cron/) for advanced options.

## Events, control, direct access

```typescript
// Events (same as Worker)
app.on('completed', (job, result) => { });
app.on('failed', (job, error) => { });
// also: active, progress, stalled, error, ready, drained, closed

// Control
app.pause();           // pause queue + worker
app.resume();          // resume both
await app.close();     // graceful shutdown
await app.close(true); // force shutdown
app.isRunning(); app.isPaused(); app.isClosed();

// Escape hatch: the underlying Queue and Worker are yours
app.queue.setStallConfig({ stallInterval: 30000 });
app.worker.concurrency = 20;
```

## Full example

```typescript
import { Bunqueue, shutdownManager } from 'bunqueue/client';

const app = new Bunqueue<{ payload: string }>('my-app', {
  embedded: true,
  routes: {
    'process': async (job) => ({ id: job.data.payload, status: 'done' }),
    'notify': async (job) => ({ sent: true }),
    'alert': async (job) => ({ alerted: true }),
  },
  concurrency: 10,

  retry: { maxAttempts: 3, delay: 1000, strategy: 'jitter' },
  circuitBreaker: { threshold: 5, resetTimeout: 30000 },
  ttl: { defaultTtl: 600000, perName: { 'verify-otp': 60000 } },
  priorityAging: { interval: 60000, minAge: 300000, boost: 1 },
  deduplication: { ttl: 5000 },
  rateLimit: { max: 100, duration: 1000 },
  dlq: { autoRetry: true, maxAge: 604800000 },
});

app.use(async (job, next) => {
  const start = Date.now();
  const result = await next();
  console.log(`${job.name}: ${Date.now() - start}ms`);
  return result;
});

app
  .trigger({ on: 'process', create: 'notify', data: (r) => ({ payload: (r as { id: string }).id }) })
  .trigger({ on: 'process', event: 'failed', create: 'alert', data: (_, j) => j.data });

await app.cron('cleanup', '0 2 * * *', { payload: 'nightly' });
await app.add('process', { payload: 'ORD-001' });

process.on('SIGINT', async () => {
  await app.close();
  shutdownManager();
});
```

## API reference

### Constructor options

**Processing mode** (pick one):

| Option | Type | Description |
|--------|------|-------------|
| `processor` | `(job) => Promise<R>` | Single handler |
| `routes` | `Record<string, Processor>` | Named handlers |
| `batch` | `{ size, timeout, processor }` | Batch processing |

**Worker:**

| Option | Default | Description |
|--------|---------|-------------|
| `concurrency` | `1` | Parallel jobs |
| `embedded` | `false` | Use embedded SQLite (`BUNQUEUE_EMBEDDED=1` forces it on) |
| `connection` | localhost:6789 | TCP server connection |
| `autorun` | `true` | Start worker immediately |

**Features:**

| Option | Description |
|--------|-------------|
| `retry` | `{ maxAttempts, delay, strategy, retryIf, customBackoff }` |
| `circuitBreaker` | `{ threshold, resetTimeout, onOpen, onClose, onHalfOpen }` |
| `ttl` | `{ defaultTtl, perName }` |
| `priorityAging` | `{ interval, minAge, boost, maxPriority, maxScan }` |
| `deduplication` | `{ ttl, extend, replace }` |
| `debounce` | `{ ttl }` |
| `rateLimit` | `{ max, duration, groupKey }` |
| `dlq` | `{ autoRetry, autoRetryInterval, maxAutoRetries, maxAge, maxEntries }` |

### Methods

| Method | Description |
|--------|-------------|
| `add(name, data, opts?)` | Add a job |
| `addBulk(jobs)` | Add multiple jobs |
| `getJob(id)` | Get job by ID |
| `getJobCounts()` / `count()` | Job counts |
| `use(middleware)` | Add middleware |
| `cron(id, pattern, data?, opts?)` | Schedule cron |
| `every(id, ms, data?, opts?)` | Schedule interval |
| `removeCron(id)` / `listCrons()` | Manage crons |
| `cancel(id, grace?)` | Cancel running job |
| `isCancelled(id)` / `getSignal(id)` | Cancellation state |
| `getCircuitState()` / `resetCircuit()` | Circuit breaker |
| `trigger(rule)` | Register event trigger |
| `setDefaultTtl(ms)` / `setNameTtl(name, ms)` | TTL updates |
| `setDlqConfig(config)` / `getDlqConfig()` | DLQ config |
| `getDlq(filter?)` / `getDlqStats()` | Query DLQ |
| `retryDlq(id?)` / `purgeDlq()` | DLQ actions |
| `setGlobalRateLimit(max, duration?)` | Set rate limit |
| `removeGlobalRateLimit()` | Remove rate limit |
| `on(event, listener)` / `once()` / `off()` | Events |
| `pause()` / `resume()` | Control |
| `close(force?)` | Shutdown |

### Properties

| Property | Type | Description |
|----------|------|-------------|
| `name` | `string` | Queue name |
| `queue` | `Queue<T>` | Internal Queue |
| `worker` | `Worker<T, R>` | Internal Worker |
