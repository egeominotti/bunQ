# Migrating from BullMQ to bunqueue

Hey! Switching from BullMQ is pretty straightforward — the API is intentionally similar. Here's a point-by-point mapping based on your current setup.

## TL;DR

- No Redis required (SQLite persistence, embedded or TCP server)
- Lower memory footprint
- ~100k jobs/sec buffered, ~10k/sec durable
- API is very close to BullMQ v5 — mostly renaming imports

---

## 1. Spawned Worker Process (`processorPath`)

bunqueue has `SandboxedWorker`, which is the equivalent of BullMQ's `processorPath`:

```typescript
// BullMQ (your current code)
export const productQueue = await Queue.upsert<UniversalJobData>({
  queueName: 'product',
  jobHandlerOptions: {
    bunPath: 'PATH_DOES_NOT_MATTER',
    concurrency: 2,
    handlersAmount: 2,
    processorPath: `${process.cwd()}/src/modules/product/index.sync.ts`,
  }
});

// bunqueue
import { SandboxedWorker } from 'bunqueue/client';

const worker = new SandboxedWorker('product', {
  processor: './src/modules/product/index.sync.ts',
  concurrency: 4,       // 2 concurrency * 2 handlers = 4 parallel jobs
  autoRestart: true,     // auto-restart crashed workers
  maxMemory: 256,        // memory limit per worker (MB)
});
await worker.start();
```

Your processor file stays almost the same — just export a default async function:

```typescript
// src/modules/product/index.sync.ts
export default async (job: { id: string; data: UniversalJobData; queue: string; attempts: number }) => {
  const { data } = job;
  // your sync logic here...
  return { success: true };
};
```

---

## 2. Adding Jobs with Cron/Repeat

```typescript
// BullMQ (your current code)
await productQueue.add({
  data: {
    ...customerJobData,
    onlyIndex: !productIndexModule ? false : !productIndexModule.disabled,
    addSalePrices: !productSalePricesModule ? false : !productSalePricesModule.disabled,
    syncProductCategories: !productCategoriesModule ? false : !productCategoriesModule.disabled,
  },
  repeat: {
    pattern: '0 0 0 * * *' // at 12 am
  }
});

// bunqueue — same syntax
import { Queue } from 'bunqueue/client';

const queue = new Queue<UniversalJobData>('product', {
  connection: { host: 'localhost', port: 6789 }
});

await queue.add('sync', {
  ...customerJobData,
  onlyIndex: !productIndexModule ? false : !productIndexModule.disabled,
  addSalePrices: !productSalePricesModule ? false : !productSalePricesModule.disabled,
  syncProductCategories: !productCategoriesModule ? false : !productCategoriesModule.disabled,
}, {
  repeat: { pattern: '0 0 0 * * *' }
});
```

Or use server-side schedulers (more robust, survives restarts):

```typescript
await queue.upsertJobScheduler('daily-product-sync',
  { pattern: '0 0 0 * * *' },
  {
    name: 'sync',
    data: { ...customerJobData }
  }
);
```

---

## 3. Queue Events

Your current event listeners map directly:

```typescript
// BullMQ (your current code)
queueEvents.on('add-handler', (queueName, jobHandlerId) => { ... });
queueEvents.on('start', (queueName, jobHandlerId, jobId, processId) => { ... });
queueEvents.on('error', (queueName, jobHandlerId, jobId, error, signalCode) => { ... });
queueEvents.on('log', (queueName, jobHandlerId, jobId, message, type) => { ... });
queueEvents.on('end', (queueName, jobHandlerId, jobId, stdout, exitCode) => { ... });

// bunqueue
import { QueueEvents, Worker } from 'bunqueue/client';

// Option A: QueueEvents (separate listener, like yours)
const queueEvents = new QueueEvents('product');

queueEvents.on('active',    ({ jobId }) => { /* job started */ });
queueEvents.on('completed', ({ jobId, returnvalue }) => { /* job finished */ });
queueEvents.on('failed',    ({ jobId, failedReason }) => { /* job errored */ });
queueEvents.on('progress',  ({ jobId, data }) => { /* progress update */ });
queueEvents.on('stalled',   ({ jobId }) => { /* worker unresponsive */ });
queueEvents.on('delayed',   ({ jobId, delay }) => { /* job delayed */ });
queueEvents.on('waiting',   ({ jobId }) => { /* job waiting */ });

// Option B: Worker events (if using inline worker)
const worker = new Worker('product', processor, { concurrency: 4 });

worker.on('active',    (job) => { /* started */ });
worker.on('completed', (job, result) => { /* done */ });
worker.on('failed',    (job, error) => { /* failed */ });
worker.on('progress',  (job, progress) => { /* progress */ });
worker.on('stalled',   (jobId) => { /* stalled */ });
worker.on('error',     (error) => { /* worker error */ });
worker.on('drained',   () => { /* queue empty */ });
```

### Event name mapping

| BullMQ (yours) | bunqueue | Notes |
|---|---|---|
| `start` | `active` | Job started processing |
| `end` | `completed` | Job finished successfully |
| `error` | `failed` | Job failed |
| `log` | `progress` | Use `job.log()` + `job.updateProgress()` |
| `add-handler` | `ready` (on Worker) | Worker registered and ready |

---

## 4. Job Logging (for sync tracking)

```typescript
// Inside your processor
export default async (job) => {
  await job.log('Starting WooCommerce sync...');

  const wooOrders = await woonty.orders.list({ status: 'processing' });
  await job.updateProgress(25);
  await job.log(`Fetched ${wooOrders.length} orders`);

  // process orders...
  await job.updateProgress(75);
  await job.log('Orders processed, finalizing...');

  await job.updateProgress(100);
  return { ordersProcessed: wooOrders.length };
};

// Query logs from outside
const logs = await queue.getJobLogs(jobId);
```

---

## 5. Your handleOrders Pattern

Your current spawned worker pattern translates like this:

```typescript
// BullMQ (your current code)
const handleOrders = () => {
  const worker = new SpawnedWorkerUniversalJobData();
  // ...
  const wooOrders = await woonty.orders.list({ status: 'processing' });
  if (wooOrders.error) {
    worker.getPayloadSender().initFail(wooOrders.body.error.message, ...);
  }
  // ...
  worker.initComplete('handleOrders');
};

// bunqueue — processor file
export default async (job: { id: string; data: UniversalJobData }) => {
  const woonty = createWoontyClient({ /* ... */ });

  const wooOrders = await woonty.orders.list({ status: 'processing' });
  if (wooOrders.error) {
    // Throwing makes the job fail and retry (up to `attempts` times)
    // After max attempts, it goes to DLQ automatically
    throw new Error(wooOrders.body.error.message);
  }

  for (const order of wooOrders.data) {
    await handleOrder(order);
  }

  return { processed: wooOrders.data.length };
};
```

Key difference: instead of `worker.initFail()` / `worker.initComplete()`, you just `throw` on error or `return` on success. bunqueue handles retries and DLQ automatically.

---

## 6. Queue Setup (replacing Queue.upsert)

```typescript
// BullMQ (your current code)
export const productQueue = await Queue.upsert<UniversalJobData>({
  queueName: 'product',
  events: queueEvents,
  jobHandlerOptions: {
    ...defaultJobHandlerOptions,
    processorPath: `${process.cwd()}/src/modules/product/index.sync.ts`,
  }
});

// bunqueue
import { Queue, SandboxedWorker, QueueEvents } from 'bunqueue/client';

const connection = { host: 'localhost', port: 6789 };

// Queue (for adding jobs, querying, control)
const productQueue = new Queue<UniversalJobData>('product', { connection });

// Worker (for processing jobs — replaces processorPath + jobHandlerOptions)
const productWorker = new SandboxedWorker('product', {
  processor: './src/modules/product/index.sync.ts',
  concurrency: 4,
  autoRestart: true,
});
await productWorker.start();

// Events (optional, for tracking)
const productEvents = new QueueEvents('product');
productEvents.on('completed', ({ jobId, returnvalue }) => {
  // update sync entry in database
});
productEvents.on('failed', ({ jobId, failedReason }) => {
  // log failure
});
```

---

## 7. Full Migration Example

Here's a complete before/after:

```typescript
// ============ BEFORE (BullMQ) ============

import { Queue, QueueEvents } from 'your-bullmq-package';

const queueEvents = new QueueEvents();

queueEvents.on('start', async (queueName, jobHandlerId, jobId, processId) => {
  // mark sync as started in DB
});
queueEvents.on('end', async (queueName, jobHandlerId, jobId, stdout, exitCode) => {
  // mark sync as completed in DB
});
queueEvents.on('error', async (queueName, jobHandlerId, jobId, error, signalCode) => {
  // log error
});

const productQueue = await Queue.upsert({
  queueName: 'product',
  events: queueEvents,
  jobHandlerOptions: {
    bunPath: 'PATH_DOES_NOT_MATTER',
    concurrency: 2,
    handlersAmount: 2,
    processorPath: `${process.cwd()}/src/modules/product/index.sync.ts`,
  }
});

await productQueue.add({
  data: { ...customerJobData },
  repeat: { pattern: '0 0 0 * * *' }
});


// ============ AFTER (bunqueue) ============

import { Queue, SandboxedWorker, QueueEvents } from 'bunqueue/client';

const connection = { host: 'localhost', port: 6789 };

// 1. Create queue
const productQueue = new Queue<UniversalJobData>('product', { connection });

// 2. Create worker (replaces processorPath + handlersAmount)
const productWorker = new SandboxedWorker('product', {
  processor: './src/modules/product/index.sync.ts',
  concurrency: 4,
  autoRestart: true,
});
await productWorker.start();

// 3. Event listeners
const productEvents = new QueueEvents('product');

productEvents.on('active', async ({ jobId }) => {
  // mark sync as started in DB
});
productEvents.on('completed', async ({ jobId, returnvalue }) => {
  // mark sync as completed in DB
});
productEvents.on('failed', async ({ jobId, failedReason }) => {
  // log error
});

// 4. Schedule recurring job
await productQueue.upsertJobScheduler('daily-product-sync',
  { pattern: '0 0 0 * * *' },
  { name: 'sync', data: { ...customerJobData } }
);
```

---

## 8. What You Get

| Your current issue | bunqueue solution |
|---|---|
| Slow performance | ~100k jobs/sec buffered, auto-batching for concurrent adds |
| High memory usage | SQLite persistence with configurable memory bounds, no Redis overhead |
| Redis dependency | Zero external dependencies — SQLite embedded or lightweight TCP server |
| Complex setup | Single binary: `bunqueue start` and you're running |

### Running the server

```bash
# Install
bun add bunqueue

# Start server (TCP mode)
bunqueue start --tcp-port 6789 --data-path ./data/queue.db

# Or embedded mode (no server needed, same process)
const queue = new Queue('product', { embedded: true });
```

### Memory comparison

- **BullMQ + Redis**: Redis holds everything in memory. 100k jobs = hundreds of MB.
- **bunqueue**: SQLite WAL mode with bounded in-memory indexes. 100k jobs = ~20-50 MB in memory, rest on disk.

---

## Questions?

Feel free to ask if anything is unclear about the migration. The API surface is intentionally close to BullMQ v5, so most patterns translate 1:1.
