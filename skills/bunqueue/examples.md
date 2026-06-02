# bunqueue Examples & Patterns

Real-world patterns for common use cases.

## Email Service with Retries & DLQ

```typescript
import { Bunqueue } from 'bunqueue/client';

const emailService = new Bunqueue('emails', {
  embedded: true,
  routes: {
    'welcome': async (job) => {
      await sendEmail(job.data.to, 'Welcome!', welcomeTemplate(job.data));
      return { sent: true, to: job.data.to };
    },
    'password-reset': async (job) => {
      const token = generateResetToken(job.data.userId);
      await sendEmail(job.data.to, 'Reset Password', resetTemplate(token));
      return { sent: true, token };
    },
    'newsletter': async (job) => {
      await sendBulkEmail(job.data.recipients, job.data.content);
      return { sent: job.data.recipients.length };
    },
  },
  concurrency: 5,
  retry: {
    maxAttempts: 3,
    delay: 2000,
    strategy: 'jitter',
    retryIf: (err) => !err.message.includes('invalid_email'),
  },
  dlq: {
    autoRetry: true,
    autoRetryInterval: 3600000, // retry failed emails every hour
    maxAutoRetries: 5,
    maxAge: 604800000,          // keep for 7 days
  },
  rateLimit: { max: 50, duration: 1000 }, // 50 emails/sec
});

emailService.use(async (job, next) => {
  console.log(`[email] Sending ${job.name} to ${job.data.to}`);
  const result = await next();
  console.log(`[email] Sent ${job.name}: ${JSON.stringify(result)}`);
  return result;
});

// Usage
await emailService.add('welcome', { to: 'alice@example.com', name: 'Alice' });
await emailService.add('password-reset', { to: 'bob@example.com', userId: '123' });
```

## API Gateway with Rate Limiting & Circuit Breaker

```typescript
import { Bunqueue } from 'bunqueue/client';

const gateway = new Bunqueue('api-gateway', {
  embedded: true,
  routes: {
    'stripe-charge': async (job) => {
      const res = await fetch('https://api.stripe.com/v1/charges', {
        method: 'POST',
        headers: { Authorization: `Bearer ${STRIPE_KEY}` },
        body: JSON.stringify(job.data),
      });
      if (!res.ok) throw new Error(`Stripe ${res.status}`);
      return await res.json();
    },
    'sendgrid-send': async (job) => {
      const res = await fetch('https://api.sendgrid.com/v3/mail/send', {
        method: 'POST',
        headers: { Authorization: `Bearer ${SENDGRID_KEY}` },
        body: JSON.stringify(job.data),
      });
      if (!res.ok) throw new Error(`SendGrid ${res.status}`);
      return { status: res.status };
    },
  },
  concurrency: 10,
  retry: { maxAttempts: 3, delay: 1000, strategy: 'exponential' },
  circuitBreaker: {
    threshold: 5,
    resetTimeout: 30000,
    onOpen: () => console.error('API gateway circuit OPEN - pausing'),
    onClose: () => console.log('API gateway circuit CLOSED - resuming'),
  },
  rateLimit: { max: 100, duration: 1000 },
});
```

## ETL Pipeline with Flows

```typescript
import { FlowProducer, Queue, Worker } from 'bunqueue/client';

const queue = new Queue('etl', { embedded: true });
const flow = new FlowProducer({ embedded: true });

// Worker handles all ETL steps
const worker = new Worker('etl', async (job) => {
  switch (job.name) {
    case 'extract':
      const raw = await fetchFromAPI(job.data.source);
      return { rows: raw.length, data: raw };

    case 'transform':
      const parent = await queue.getJob(job.parent!.id);
      const transformed = (parent?.returnvalue as any)?.data.map(transformRow);
      return { rows: transformed.length, data: transformed };

    case 'load':
      const parentJob = await queue.getJob(job.parent!.id);
      await insertIntoDB((parentJob?.returnvalue as any)?.data);
      return { loaded: true };
  }
}, { embedded: true, concurrency: 3 });

// Create ETL pipeline (children run first, then parent)
await flow.add({
  name: 'load',
  queueName: 'etl',
  data: { destination: 'warehouse' },
  children: [{
    name: 'transform',
    queueName: 'etl',
    data: { format: 'normalized' },
    children: [{
      name: 'extract',
      queueName: 'etl',
      data: { source: 'https://api.example.com/data' },
    }],
  }],
});

// Or use chain for sequential steps — single array, each step carries its own queueName
await flow.addChain([
  { name: 'extract', queueName: 'etl', data: { source: 'api' } },
  { name: 'transform', queueName: 'etl', data: { format: 'csv' } },
  { name: 'load', queueName: 'etl', data: { table: 'reports' } },
]);
```

## Webhook Processor with Deduplication

```typescript
import { Bunqueue } from 'bunqueue/client';

const webhookProcessor = new Bunqueue('webhooks', {
  embedded: true,
  processor: async (job) => {
    const { event, payload } = job.data;
    switch (event) {
      case 'user.created':
        await provisionUser(payload);
        break;
      case 'payment.completed':
        await fulfillOrder(payload);
        break;
      case 'subscription.cancelled':
        await handleCancellation(payload);
        break;
    }
    return { processed: event };
  },
  concurrency: 20,
  deduplication: { ttl: 300000 }, // 5 min dedup window
  retry: { maxAttempts: 5, delay: 2000, strategy: 'jitter' },
  ttl: { defaultTtl: 3600000 },  // expire after 1 hour
});

// HTTP endpoint receives webhooks
Bun.serve({
  port: 3000,
  async fetch(req) {
    if (req.method === 'POST' && req.url.endsWith('/webhook')) {
      const body = await req.json();
      await webhookProcessor.add(body.event, body);
      return new Response('OK', { status: 200 });
    }
    return new Response('Not Found', { status: 404 });
  },
});
```

## Image Processing Pipeline

```typescript
import { Bunqueue } from 'bunqueue/client';

const imageProcessor = new Bunqueue('images', {
  embedded: true,
  routes: {
    'resize': async (job) => {
      const signal = imageProcessor.getSignal(job.id);
      const { path, sizes } = job.data;
      const results = [];
      for (const size of sizes) {
        if (signal?.aborted) throw new Error('Cancelled');
        await job.updateProgress((results.length / sizes.length) * 100);
        const output = await sharp(path).resize(size.w, size.h).toFile(`${path}-${size.w}x${size.h}`);
        results.push(output);
      }
      return { resized: results.length };
    },
    'optimize': async (job) => {
      await sharp(job.data.path).webp({ quality: 80 }).toFile(`${job.data.path}.webp`);
      return { optimized: true };
    },
  },
  concurrency: 4, // CPU-bound, limit concurrency
  retry: { maxAttempts: 2, delay: 5000, strategy: 'fixed' },
  priorityAging: { interval: 60000, minAge: 300000, boost: 1 },
});

// Chain: resize then optimize
imageProcessor
  .trigger({ on: 'resize', create: 'optimize', data: (result, job) => ({ path: job.data.path }) });

await imageProcessor.add('resize', {
  path: '/uploads/photo.jpg',
  sizes: [{ w: 1200, h: 800 }, { w: 400, h: 300 }, { w: 100, h: 100 }],
});
```

## Batch Database Inserts

```typescript
import { Bunqueue } from 'bunqueue/client';

const dbBatcher = new Bunqueue('db-inserts', {
  embedded: true,
  batch: {
    size: 100,       // batch 100 rows
    timeout: 1000,   // or flush every second
    processor: async (jobs) => {
      const rows = jobs.map(j => j.data);
      await db.query(
        `INSERT INTO events (type, payload, created_at) VALUES ${rows.map(() => '(?, ?, ?)').join(',')}`,
        rows.flatMap(r => [r.type, JSON.stringify(r.payload), new Date()])
      );
      return jobs.map(() => ({ inserted: true }));
    },
  },
});

// High-throughput event ingestion
Bun.serve({
  port: 3001,
  async fetch(req) {
    if (req.method === 'POST') {
      const event = await req.json();
      await dbBatcher.add('insert', event);
      return new Response('OK');
    }
    return new Response('Not Found', { status: 404 });
  },
});
```

## Multi-Queue Service with QueueGroup

```typescript
import { QueueGroup } from 'bunqueue/client';

const billing = new QueueGroup('billing');

const invoices = billing.getQueue('invoices');
const payments = billing.getQueue('payments');
const refunds = billing.getQueue('refunds');

const invoiceWorker = billing.getWorker('invoices', async (job) => {
  return await generateInvoice(job.data);
}, { concurrency: 5 });

const paymentWorker = billing.getWorker('payments', async (job) => {
  return await processPayment(job.data);
}, { concurrency: 3 });

const refundWorker = billing.getWorker('refunds', async (job) => {
  return await processRefund(job.data);
}, { concurrency: 2 });

// Add jobs to specific queues
await invoices.add('monthly', { customerId: '123', month: '2026-03' });
await payments.add('charge', { amount: 99, currency: 'USD' });

// Bulk operations on all billing queues
billing.pauseAll();   // pause invoices + payments + refunds
billing.resumeAll();  // resume all
```

## Scheduled Reports with Cron

```typescript
import { Bunqueue } from 'bunqueue/client';

const reports = new Bunqueue('reports', {
  embedded: true,
  routes: {
    'daily-summary': async (job) => {
      const stats = await db.query('SELECT ... FROM orders WHERE date = ?', [today()]);
      await sendSlack('#reports', formatDailySummary(stats));
      return { sent: true, date: today() };
    },
    'weekly-digest': async (job) => {
      const data = await db.query('SELECT ... FROM metrics WHERE week = ?', [thisWeek()]);
      await sendEmail('team@company.com', 'Weekly Digest', formatDigest(data));
      return { sent: true };
    },
    'health-check': async (job) => {
      const services = await checkAllServices();
      const unhealthy = services.filter(s => !s.healthy);
      if (unhealthy.length > 0) {
        await sendPagerDuty(unhealthy);
      }
      return { healthy: unhealthy.length === 0, checked: services.length };
    },
  },
});

await reports.cron('daily-summary', '0 9 * * *', {}, { timezone: 'Europe/Rome' });
await reports.cron('weekly-digest', '0 8 * * 1', {});
await reports.every('health-check', 30000, {}); // every 30s
```

## Distributed System (TCP Mode)

```typescript
// === Server (start separately) ===
// bunqueue start --tcp-port 6789 --data-path ./data/prod.db

// === Producer (any process) ===
import { Queue } from 'bunqueue/client';

const queue = new Queue('tasks', {
  connection: { host: 'queue-server', port: 6789, token: 'secret' },
  autoBatch: { maxSize: 50, maxDelayMs: 5 }, // enabled by default
});

// Sequential adds: zero overhead
for (const task of tasks) {
  await queue.add('process', task);
}

// Concurrent adds: ~3x faster via auto-batching
await Promise.all(
  tasks.map(t => queue.add('process', t))
);

// Critical jobs bypass the batcher
await queue.add('payment', data, { durable: true });

// === Consumer (any process, can scale horizontally) ===
import { Worker } from 'bunqueue/client';

const worker = new Worker('tasks', async (job) => {
  return await processTask(job.data);
}, {
  connection: { host: 'queue-server', port: 6789, token: 'secret' },
  concurrency: 20,
  heartbeatInterval: 10000,
  batchSize: 10,
});
```

## Search with Debouncing

```typescript
import { Bunqueue } from 'bunqueue/client';

const search = new Bunqueue('search', {
  embedded: true,
  processor: async (job) => {
    const results = await elasticSearch(job.data.query, {
      index: job.data.index,
      filters: job.data.filters,
    });
    return { hits: results.hits.total, results: results.hits.hits };
  },
  debounce: { ttl: 300 }, // 300ms debounce
  rateLimit: { max: 20, duration: 1000 }, // max 20 searches/sec
});

// Rapid calls — only the last one in each 300ms window processes
await search.add('query', { query: 'h', index: 'products' });
await search.add('query', { query: 'he', index: 'products' });
await search.add('query', { query: 'hel', index: 'products' });
await search.add('query', { query: 'hello', index: 'products' }); // only this runs
```

## OTP Verification with TTL

```typescript
import { Bunqueue } from 'bunqueue/client';

const otp = new Bunqueue('auth', {
  embedded: true,
  routes: {
    'verify-otp': async (job) => {
      const valid = await checkOTP(job.data.code, job.data.userId);
      if (!valid) throw new Error('Invalid OTP');
      return { verified: true, userId: job.data.userId };
    },
    'send-otp': async (job) => {
      const code = generateOTP();
      await storeOTP(job.data.userId, code);
      await sendSMS(job.data.phone, `Your code: ${code}`);
      return { sent: true };
    },
  },
  ttl: {
    defaultTtl: 0,               // no TTL by default
    perName: {
      'verify-otp': 120000,      // OTP expires in 2 minutes
      'send-otp': 30000,         // send within 30 seconds or discard
    },
  },
});
```

## Migration from BullMQ

bunqueue is largely API-compatible with BullMQ. Key differences:

```typescript
// BullMQ
import { Queue, Worker } from 'bullmq';
const queue = new Queue('tasks', { connection: { host: 'redis', port: 6379 } });

// bunqueue (embedded — no Redis needed!)
import { Queue, Worker } from 'bunqueue/client';
const queue = new Queue('tasks', { embedded: true, dataPath: './data/app.db' });

// bunqueue (TCP — replace Redis with bunqueue server)
const queue = new Queue('tasks', { connection: { host: 'localhost', port: 6789 } });
```

### What stays the same:
- `Queue.add()`, `Queue.addBulk()` — same API
- `Worker` constructor — same signature `(name, processor, opts)`
- `worker.on('completed' | 'failed' | ...)` — same events
- `FlowProducer.add()` — same tree structure
- `job.updateProgress()`, `job.log()` — same methods
- Job options: `priority`, `delay`, `attempts`, `backoff`, `jobId`, `removeOnComplete`, `removeOnFail`

### What's different:
- **No Redis** — SQLite persistence (embedded) or TCP protocol
- **`embedded: true`** — run everything in-process, no server needed
- **`Bunqueue` class** — all-in-one Simple Mode (no BullMQ equivalent)
- **`durable: true`** — immediate disk write (BullMQ has no equivalent, Redis is always in-memory)
- **Auto-batching** — transparent batching in TCP mode (BullMQ batches manually)
- **S3 Backup** — built-in backup to S3 (BullMQ relies on Redis persistence)
- **MCP Server** — native AI agent integration (no BullMQ equivalent)
- **`QueueGroup`** — namespace isolation (BullMQ uses prefix option)

### Migration checklist:
1. Replace `bullmq` import with `bunqueue/client`
2. Replace `connection: { host, port }` (Redis) with `embedded: true` or `connection: { port: 6789 }` (TCP)
3. Remove Redis dependency
4. (Optional) Switch to `Bunqueue` Simple Mode for single-process apps
5. (Optional) Add `durable: true` for critical jobs
6. (Optional) Configure S3 backup for disaster recovery
