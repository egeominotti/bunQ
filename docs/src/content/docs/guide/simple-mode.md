---
title: "Simple Mode"
description: "Bunqueue Simple Mode: Queue + Worker in one object. 12 built-in features, zero boilerplate."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/getting-started.png
---

Simple Mode gives you a Queue and a Worker in a single object. Add jobs, process them, add middleware, schedule crons — all from one place.

```typescript
import { Bunqueue } from 'bunqueue/client';

const app = new Bunqueue('emails', {
  embedded: true,
  processor: async (job) => {
    console.log(`Sending to ${job.data.to}`);
    return { sent: true };
  },
});

await app.add('send', { to: 'alice@example.com' });
```

:::tip[When to use]
Use `Bunqueue` when producer and consumer are in the **same process**. For distributed systems, use [`Queue`](/guide/queue/) + [`Worker`](/guide/worker/) separately.
:::

## Architecture

```
new Bunqueue('emails', opts)
    │
    ├── this.queue  = new Queue('emails', ...)
    ├── this.worker = new Worker('emails', ...)
    │
    └── Subsystems (all optional):
        ├── RetryEngine         ── jitter, fibonacci, exponential, custom
        ├── CircuitBreaker      ── pauses worker after N failures
        ├── BatchAccumulator    ── groups N jobs into one call
        ├── TriggerManager      ── "on complete → create job B"
        ├── TtlChecker          ── rejects expired jobs
        ├── PriorityAger        ── boosts old jobs' priority
        ├── CancellationManager ── AbortController per job
        └── DedupDebounceMerger ── deduplication & debounce defaults
```

Processing pipeline per job:

```
Job → Circuit Breaker → TTL check → AbortController → Retry → Middleware → Processor
```

---

## Routes

Route jobs to different handlers by name:

```typescript
const app = new Bunqueue('notifications', {
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

Wraps every job execution. Receives the job and a `next()` function:

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

Execution order is onion-style: `mw1 → mw2 → processor → mw2 → mw1`. When no middleware is added, zero overhead.

## Batch Processing

Accumulates N jobs and processes them together:

```typescript
const app = new Bunqueue('db-inserts', {
  embedded: true,
  batch: {
    size: 50,        // flush every 50 jobs
    timeout: 2000,   // or every 2 seconds
    processor: async (jobs) => {
      const rows = jobs.map(j => j.data.row);
      await db.insertMany('table', rows);
      return jobs.map(() => ({ inserted: true }));
    },
  },
});
```

Flushes when buffer reaches `size` **or** `timeout` expires. On `close()`, remaining jobs are flushed.

## Advanced Retry

5 strategies + retry predicate:

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
| `exponential` | `delay × 2^attempt` | General purpose |
| `jitter` | `delay × 2^attempt × random(0.5-1.0)` | Thundering herd prevention |
| `fibonacci` | `delay × fib(attempt)` | Gradual backoff |
| `custom` | `customBackoff(attempt, error) → ms` | Anything |

This is **in-process retry** — the job stays active. Different from core `attempts`/`backoff` which re-queues.

## Graceful Cancellation

Cancel running jobs with AbortController:

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

## Circuit Breaker

Pauses the worker after too many consecutive failures:

```
CLOSED ──→ failures ≥ threshold ──→ OPEN (worker paused)
                                       │
              ←── success ──── HALF-OPEN ←── timeout expires
```

```typescript
const app = new Bunqueue('payments', {
  embedded: true,
  processor: async (job) => paymentGateway.charge(job.data),
  circuitBreaker: {
    threshold: 5,         // open after 5 failures
    resetTimeout: 30000,  // try again after 30s
    onOpen: () => alert('Gateway down!'),
    onClose: () => alert('Gateway recovered'),
  },
});

app.getCircuitState();  // 'closed' | 'open' | 'half-open'
app.resetCircuit();     // force close + resume worker
```

When both retry and circuit breaker are active: all retries exhausted = 1 circuit breaker failure.

## Event Triggers

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

// Conditional trigger (only for large orders)
app.trigger({
  on: 'place-order',
  create: 'fraud-alert',
  data: (result) => ({ amount: result.total }),
  condition: (result) => result.total > 1000,
});

// Chain triggers
app
  .trigger({ on: 'step-1', create: 'step-2', data: (r) => r })
  .trigger({ on: 'step-2', create: 'step-3', data: (r) => r });
```

## Job TTL

Expire unprocessed jobs. Checked when the worker picks up the job:

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

Resolution: `perName[job.name]` → `defaultTtl` → `0` (no TTL).

## Priority Aging

Automatically boosts priority of old waiting jobs to prevent starvation:

```typescript
const app = new Bunqueue('tasks', {
  embedded: true,
  processor: async (job) => ({ done: true }),
  priorityAging: {
    interval: 60000,    // check every 60s
    minAge: 300000,     // boost after 5 minutes
    boost: 2,           // +2 priority per tick
    maxPriority: 100,   // cap
    maxScan: 200,       // max jobs per tick
  },
});
```

A job with priority 1, after 5 min: 3, after 10 min: 5, ... capped at 100.

## Job Deduplication

Prevent duplicate jobs automatically. Jobs with the same name + data get the same dedup ID:

```typescript
const app = new Bunqueue('webhooks', {
  embedded: true,
  processor: async (job) => processWebhook(job.data),
  deduplication: {
    ttl: 60000,       // dedup window: 60 seconds
    extend: false,    // don't extend TTL on duplicate
    replace: false,   // don't replace data
  },
});

await app.add('hook', { event: 'user.created', userId: '123' });
await app.add('hook', { event: 'user.created', userId: '123' }); // deduplicated!
await app.add('hook', { event: 'user.updated', userId: '123' }); // different data → new job
```

Override per-job with explicit `deduplication` in options:

```typescript
await app.add('task', data, { deduplication: { id: 'my-custom-id', ttl: 5000 } });
```

## Job Debouncing

Coalesce rapid same-name jobs. Only the last one in the TTL window gets processed:

```typescript
const app = new Bunqueue('search', {
  embedded: true,
  processor: async (job) => executeSearch(job.data.query),
  debounce: {
    ttl: 500,  // 500ms debounce window
  },
});

// User types fast — only the last query runs
await app.add('search', { query: 'h' });
await app.add('search', { query: 'he' });
await app.add('search', { query: 'hello' });  // only this one processes
```

Debounce groups by job name. Different names = independent debounce windows.

## Rate Limiting

Control job processing speed:

```typescript
const app = new Bunqueue('api', {
  embedded: true,
  processor: async (job) => callExternalAPI(job.data),

  // Worker-level: max 100 jobs per second
  rateLimit: {
    max: 100,
    duration: 1000,
  },
});

// Or per-group rate limiting (e.g., per customer)
const app2 = new Bunqueue('api', {
  embedded: true,
  processor: async (job) => callAPI(job.data),
  rateLimit: {
    max: 10,
    duration: 1000,
    groupKey: 'customerId',  // limits per job.data.customerId
  },
});

// Runtime updates
app.setGlobalRateLimit(50, 1000);   // change to 50/sec
app.removeGlobalRateLimit();        // remove limit
```

## DLQ (Dead Letter Queue)

Automatically manage failed jobs:

```typescript
const app = new Bunqueue('critical', {
  embedded: true,
  processor: async (job) => riskyOperation(job.data),
  dlq: {
    autoRetry: true,           // auto-retry failed jobs
    autoRetryInterval: 3600000, // retry every hour
    maxAutoRetries: 3,         // max 3 retries
    maxAge: 604800000,         // purge after 7 days
    maxEntries: 10000,         // max DLQ size
  },
});

// Query DLQ
const entries = app.getDlq();                              // all entries
const stats = app.getDlqStats();                           // { total, byReason, ... }
const timeouts = app.getDlq({ reason: 'timeout' });       // filter by reason

// Manual actions
app.retryDlq();           // retry all
app.retryDlq('job-id');   // retry one
app.purgeDlq();           // clear all

// Update config at runtime
app.setDlqConfig({ autoRetry: false });
```

Failure reasons tracked: `explicit_fail`, `max_attempts_exceeded`, `timeout`, `stalled`, `ttl_expired`, `worker_lost`.

## Cron Jobs

```typescript
await app.cron('daily-report', '0 9 * * *', { type: 'report' });
await app.cron('eu-digest', '0 8 * * 1', { type: 'weekly' }, { timezone: 'Europe/Rome' });
await app.every('healthcheck', 30000, { type: 'ping' });

await app.listCrons();
await app.removeCron('healthcheck');
```

See [Cron Jobs guide](/guide/cron/) for advanced options.

## Events

```typescript
app.on('completed', (job, result) => { });
app.on('failed', (job, error) => { });
app.on('active', (job) => { });
app.on('progress', (job, progress) => { });
app.on('stalled', (jobId, reason) => { });
app.on('error', (error) => { });
app.on('ready', () => { });
app.on('drained', () => { });
app.on('closed', () => { });
```

## Adding Jobs

```typescript
// Single
await app.add('task', { key: 'value' });

// With options
await app.add('urgent', data, {
  priority: 10,
  delay: 5000,
  attempts: 5,
  durable: true,
});

// Bulk
await app.addBulk([
  { name: 'email', data: { to: 'alice' } },
  { name: 'email', data: { to: 'bob' }, opts: { priority: 10 } },
]);
```

## Control

```typescript
app.pause();           // pause queue + worker
app.resume();          // resume both
await app.close();     // graceful shutdown
await app.close(true); // force shutdown

app.isRunning();
app.isPaused();
app.isClosed();
```

## Direct Access

For operations not exposed by Simple Mode:

```typescript
app.queue.getWaiting();
app.queue.setStallConfig({ stallInterval: 30000 });
app.worker.concurrency = 20;
```

## Full Example

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
  .trigger({ on: 'process', create: 'notify', data: (r) => ({ payload: r.id }) })
  .trigger({ on: 'process', event: 'failed', create: 'alert', data: (_, j) => j.data });

await app.cron('cleanup', '0 2 * * *', { payload: 'nightly' });
await app.add('process', { payload: 'ORD-001' });

process.on('SIGINT', async () => {
  await app.close();
  shutdownManager();
});
```

## API Reference

### Constructor Options

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
| `embedded` | auto | Use embedded SQLite |
| `connection` | — | TCP server connection |
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
