---
name: bunqueue
description: Use bunqueue job queue library - Queue, Worker, Bunqueue (simple mode), FlowProducer, cron, DLQ, embedded and TCP modes
disable-model-invocation: false
user-invocable: true
allowed-tools: Read, Grep, Glob, Bash, Edit, Write
---

# bunqueue - Job Queue for Bun

You are helping a developer use **bunqueue**, a high-performance job queue for Bun with SQLite persistence.

## Installation

```bash
bun add bunqueue
```

## Quick Decision: Which Mode?

- **Embedded mode**: Single process, no server needed. Best for most apps.
- **TCP mode**: Separate server process. Best for distributed systems with multiple producers/consumers.
- **Simple Mode (`Bunqueue`)**: All-in-one wrapper. Best for getting started fast.

## Simple Mode (Recommended Start)

Simple Mode gives you a Queue and a Worker in a single object. Add jobs, process them, add middleware, schedule crons — all from one place. Use `Bunqueue` when producer and consumer are in the same process. For distributed systems, use `Queue` + `Worker` separately.

For full API details, see [reference.md](reference.md)

### Architecture

```
new Bunqueue('emails', opts)
    │
    ├── this.queue  = new Queue('emails', ...)
    ├── this.worker = new Worker('emails', ...)
    │
    └── Subsystems (all optional):
        ├── RetryEngine         — jitter, fibonacci, exponential, custom
        ├── CircuitBreaker      — pauses worker after N failures
        ├── BatchAccumulator    — groups N jobs into one call
        ├── TriggerManager      — "on complete → create job B"
        ├── TtlChecker          — rejects expired jobs
        ├── PriorityAger        — boosts old jobs' priority
        ├── CancellationManager — AbortController per job
        └── DedupDebounceMerger — deduplication & debounce defaults
```

Processing pipeline per job: `Job → Circuit Breaker → TTL check → AbortController → Retry → Middleware → Processor`

### Basic Usage

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

### Routes (Named Handlers)

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

> Use one of `processor`, `routes`, or `batch`. Passing multiple or none throws an error.

### Middleware (Onion Model)

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

Execution order: mw1 → mw2 → processor → mw2 → mw1. Zero overhead when no middleware.

### Batch Processing

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

### Advanced Retry (5 Strategies)

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
    strategy: 'jitter',  // 'fixed' | 'exponential' | 'jitter' | 'fibonacci' | 'custom'
    retryIf: (error) => error.message.includes('503'),
  },
});
```

Strategies: `fixed` (constant delay), `exponential` (delay × 2^attempt), `jitter` (exponential × random 0.5-1.0), `fibonacci` (delay × fib(attempt)), `custom` (customBackoff(attempt, error) → ms). This is in-process retry — the job stays active.

### Graceful Cancellation

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

Works with fetch too: `await fetch(url, { signal })`.

### Circuit Breaker

Pauses the worker after too many consecutive failures: `CLOSED → OPEN (paused) → HALF-OPEN → CLOSED`

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

### Event Triggers

```typescript
const app = new Bunqueue('orders', {
  embedded: true,
  routes: {
    'place-order': async (job) => ({ orderId: job.data.id, total: 99 }),
    'send-receipt': async (job) => ({ sent: true }),
    'fraud-alert': async (job) => ({ alerted: true }),
  },
});

app.trigger({ on: 'place-order', create: 'send-receipt', data: (result, job) => ({ id: job.data.id }) });
app.trigger({ on: 'place-order', create: 'fraud-alert', data: (r) => ({ amount: r.total }), condition: (r) => r.total > 1000 });

// Chain triggers
app.trigger({ on: 'step-1', create: 'step-2', data: (r) => r })
   .trigger({ on: 'step-2', create: 'step-3', data: (r) => r });
```

### Job TTL

```typescript
const app = new Bunqueue('otp', {
  embedded: true,
  processor: async (job) => verifyOTP(job.data.code),
  ttl: {
    defaultTtl: 300000,           // 5 minutes for all jobs
    perName: { 'verify-otp': 60000 },  // 1 minute for OTP
  },
});
```

Resolution: `perName[job.name]` → `defaultTtl` → 0 (no TTL).

### Priority Aging

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
  },
});
```

### Deduplication

```typescript
const app = new Bunqueue('webhooks', {
  embedded: true,
  processor: async (job) => processWebhook(job.data),
  deduplication: { ttl: 60000 },
});

await app.add('hook', { event: 'user.created', userId: '123' });
await app.add('hook', { event: 'user.created', userId: '123' }); // deduplicated!
```

### Debouncing

```typescript
const app = new Bunqueue('search', {
  embedded: true,
  processor: async (job) => executeSearch(job.data.query),
  debounce: { ttl: 500 },
});

await app.add('search', { query: 'h' });
await app.add('search', { query: 'he' });
await app.add('search', { query: 'hello' });  // only this one processes
```

### Rate Limiting

```typescript
const app = new Bunqueue('api', {
  embedded: true,
  processor: async (job) => callExternalAPI(job.data),
  rateLimit: { max: 100, duration: 1000 },
});

// Per-group rate limiting
const app2 = new Bunqueue('api', {
  embedded: true,
  processor: async (job) => callAPI(job.data),
  rateLimit: { max: 10, duration: 1000, groupKey: 'customerId' },
});
```

### DLQ (Dead Letter Queue)

```typescript
const app = new Bunqueue('critical', {
  embedded: true,
  processor: async (job) => riskyOperation(job.data),
  dlq: {
    autoRetry: true,
    autoRetryInterval: 3600000,
    maxAutoRetries: 3,
    maxAge: 604800000,
    maxEntries: 10000,
  },
});

app.getDlq();              // all entries
app.getDlqStats();         // { total, byReason, ... }
app.retryDlq();            // retry all
app.purgeDlq();            // clear all
```

### Cron & Events

```typescript
await app.cron('daily-report', '0 9 * * *', { type: 'report' });
await app.cron('eu-digest', '0 8 * * 1', { type: 'weekly' }, { timezone: 'Europe/Rome' });
await app.every('healthcheck', 30000, { type: 'ping' });

app.on('completed', (job, result) => {});
app.on('failed', (job, error) => {});
app.on('active', (job) => {});
app.on('stalled', (jobId, reason) => {});
app.on('drained', () => {});
```

### Full Example

```typescript
import { Bunqueue, shutdownManager } from 'bunqueue/client';

const app = new Bunqueue('my-app', {
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

app.trigger({ on: 'process', create: 'notify', data: (r) => ({ payload: r.id }) })
   .trigger({ on: 'process', event: 'failed', create: 'alert', data: (_, j) => j.data });

await app.cron('cleanup', '0 2 * * *', { payload: 'nightly' });
await app.add('process', { payload: 'ORD-001' });

process.on('SIGINT', async () => {
  await app.close();
  shutdownManager();
});
```

## Queue + Worker (Full Control)

```typescript
import { Queue, Worker } from 'bunqueue/client';

// Embedded mode
const queue = new Queue('emails', {
  embedded: true,
  dataPath: './data/myapp.db',
});

// TCP mode (requires bunqueue server running)
// const queue = new Queue('emails', { connection: { port: 6789 } });

// Add jobs
await queue.add('welcome', { userId: 123 });
await queue.add('urgent', { alert: true }, { priority: 10 });
await queue.add('later', { data: 1 }, { delay: 60000 });
await queue.add('critical', { data: 1 }, { durable: true }); // Immediate disk write

// Bulk add
await queue.addBulk([
  { name: 'task1', data: { x: 1 } },
  { name: 'task2', data: { x: 2 }, opts: { priority: 5 } },
]);

// Worker
const worker = new Worker('emails', async (job) => {
  await job.updateProgress(50);
  await job.log('Processing...');
  return { sent: true };
}, {
  concurrency: 5,
  heartbeatInterval: 10000,
});

worker.on('completed', (job, result) => {});
worker.on('failed', (job, err) => {});

// Graceful shutdown
process.on('SIGTERM', async () => {
  await worker.close();
  await queue.close();
});
```

## Key Concepts

### Job Options
- `priority`: Higher number = processed sooner
- `delay`: Milliseconds before job becomes available
- `attempts`: Max retries (default: 3)
- `backoff`: Retry delay in ms (default: 1000)
- `timeout`: Processing timeout in ms
- `jobId`: Custom ID for idempotency/deduplication
- `durable`: Bypass write buffer, immediate disk write
- `removeOnComplete` / `removeOnFail`: Auto-cleanup

### Worker Options
- `concurrency`: Parallel jobs (default: 1)
- `heartbeatInterval`: Stall detection interval (default: 10000ms, 0=disabled)
- `batchSize`: Jobs to pull per batch (default: 10, max: 1000)
- `pollTimeout`: Long poll timeout (default: 0, max: 30000ms)

### Dead Letter Queue (DLQ)
```typescript
queue.setDlqConfig({ autoRetry: true, maxAge: 604800000, maxEntries: 10000 });
const entries = queue.getDlq({ reason: 'timeout' });
queue.retryDlq();  // Retry all DLQ jobs
queue.purgeDlq();  // Delete all DLQ jobs
```

### Stall Detection (embedded only)
```typescript
queue.setStallConfig({ stallInterval: 30000, maxStalls: 3, gracePeriod: 5000 });
```

### Flow Producer (Parent-Child Dependencies)
```typescript
import { FlowProducer } from 'bunqueue/client';

const flow = new FlowProducer({ embedded: true });
await flow.add({
  name: 'parent-job',
  queueName: 'pipeline',
  data: { step: 'final' },
  children: [
    { name: 'child-1', queueName: 'pipeline', data: { step: 1 } },
    { name: 'child-2', queueName: 'pipeline', data: { step: 2 } },
  ],
});
// Parent waits for all children to complete before running
```

### Queue Control
```typescript
queue.pause();       // Stop processing
queue.resume();      // Resume processing
queue.drain();       // Remove all waiting jobs
queue.obliterate();  // Delete everything
queue.clean('completed', 3600000); // Clean old completed jobs
```

## Queue + Worker: Auto-Batching (TCP)

Transparent batching for TCP mode. Sequential adds have zero overhead; concurrent adds get ~3x speedup.

```typescript
const queue = new Queue('jobs', {
  connection: { port: 6789 },
  autoBatch: { maxSize: 50, maxDelayMs: 5 }, // defaults, enabled by default in TCP
});

// Sequential: same speed as without batching
for (const item of items) {
  await queue.add('task', item);
}

// Concurrent: batched into single PUSHB round-trip (~3x faster)
await Promise.all(tasks.map(t => queue.add('process', t)));

// Durable jobs bypass the batcher
await queue.add('critical', data, { durable: true });
```

## QueueGroup (Multi-Queue Namespace)

```typescript
import { QueueGroup } from 'bunqueue/client';

const billing = new QueueGroup('billing');
const invoices = billing.getQueue('invoices');   // → "billing:invoices"
const payments = billing.getQueue('payments');   // → "billing:payments"

const worker = billing.getWorker('invoices', async (job) => {
  return await generateInvoice(job.data);
}, { concurrency: 5 });

await invoices.add('monthly', { customerId: '123' });

billing.pauseAll();    // pause all billing:* queues
billing.resumeAll();
billing.drainAll();
billing.obliterateAll();
```

## Webhooks

Receive HTTP notifications on job events. SSRF-protected, with HMAC signing and retry.

```typescript
// Via SDK (TCP mode)
await queue.addWebhook({
  url: 'https://api.example.com/hooks/jobs',
  events: ['job.completed', 'job.failed'],
  queue: 'emails',       // null = all queues
  secret: 'hmac-secret', // optional, enables X-Webhook-Signature header
});

// Via CLI
// bunqueue webhook add https://api.example.com/hooks --events job.completed,job.failed

// Via MCP
// bunqueue_add_webhook
```

Events: `job.pushed`, `job.started`, `job.completed`, `job.failed`, `job.progress`, `job.stalled`

Features:
- HMAC-SHA256 signature (`X-Webhook-Signature` header) when secret is set
- Automatic retries (default 3) with exponential backoff
- SSRF protection (blocks localhost, private IPs, cloud metadata)
- Enable/disable without deleting
- Per-queue or global

## S3 Backup

Built-in automatic backup to S3-compatible storage.

```bash
S3_BACKUP_ENABLED=1
S3_BUCKET=my-bunqueue-backups
S3_ACCESS_KEY_ID=AKIA...
S3_SECRET_ACCESS_KEY=...
S3_REGION=us-east-1
S3_BACKUP_INTERVAL=21600000   # every 6 hours
S3_BACKUP_RETENTION=7         # keep 7 days

# Optional: custom endpoint (MinIO, R2, etc.)
S3_ENDPOINT=https://s3.custom.endpoint
```

## Running the Server (TCP mode)

```bash
# Start server
bunqueue start --tcp-port 6789 --data-path ./data/queue.db

# Or with env vars
TCP_PORT=6789 BUNQUEUE_DATA_PATH=./data/queue.db bunqueue start
```

## MCP Server (AI Agent Integration)

bunqueue includes a native MCP server with 73 tools, 5 resources, and 3 diagnostic prompts. AI agents can manage queues, add/pull jobs, monitor stats, and auto-process jobs via HTTP handlers.

For full setup and tool list, see [mcp.md](mcp.md)

```json
{
  "mcpServers": {
    "bunqueue": {
      "command": "bunx",
      "args": ["--package=bunqueue", "bunqueue-mcp"],
      "env": { "BUNQUEUE_MODE": "embedded" }
    }
  }
}
```

## Migration from BullMQ

bunqueue is largely API-compatible with BullMQ. Replace the import and connection config:

```typescript
// Before (BullMQ + Redis)
import { Queue, Worker } from 'bullmq';
const queue = new Queue('tasks', { connection: { host: 'redis', port: 6379 } });

// After (bunqueue, no Redis needed)
import { Queue, Worker } from 'bunqueue/client';
const queue = new Queue('tasks', { embedded: true });
```

Same API: `add()`, `addBulk()`, `Worker(name, processor, opts)`, `FlowProducer.add()`, events, job options. For full migration guide, see [examples.md](examples.md)

## Performance

| Mode | Throughput | Data Loss Risk |
|------|-----------|---------------|
| Buffered (default) | ~100k jobs/sec | Up to 10ms |
| Durable | ~10k jobs/sec | None |
| Auto-batch (TCP) | ~145k ops/s concurrent | None |

## More

- [reference.md](reference.md) — Full API reference (Queue, Worker, Bunqueue, FlowProducer, QueueGroup)
- [examples.md](examples.md) — Real-world patterns (email, ETL, webhooks, batch DB, cron, distributed)
- [mcp.md](mcp.md) — MCP server setup, 73 tools, resources, diagnostic prompts
