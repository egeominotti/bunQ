# bunqueue API Reference

## Imports

```typescript
import { Queue, Worker, FlowProducer, Bunqueue, QueueGroup } from 'bunqueue/client';
```

## Queue

### Constructor

```typescript
// Embedded (in-process, no server)
const queue = new Queue<TData>('queue-name', {
  embedded: true,
  dataPath: './data/myapp.db',  // SQLite path
});

// TCP (requires bunqueue server)
const queue = new Queue<TData>('queue-name', {
  connection: { host: 'localhost', port: 6789 },
  // Optional auth:
  // connection: { host: 'localhost', port: 6789, token: 'secret' },
});
```

### Methods

```typescript
// Add jobs
await queue.add(name: string, data: TData, opts?: JobOptions): Promise<Job>
await queue.addBulk(jobs: { name: string, data: TData, opts?: JobOptions }[]): Promise<Job[]>

// Query
await queue.getJob(id: string): Promise<Job | null>
await queue.getJobState(id: string): Promise<string>
queue.getJobCounts(): Record<string, number>   // sync (getJobCountsAsync() for TCP)
queue.getJobs(opts?: { state?: string | string[]; start?: number; end?: number; asc?: boolean }): Job[]   // sync (getJobsAsync() for TCP)
await queue.count(): Promise<number>

// Control
queue.pause(): void
queue.resume(): void
queue.drain(): void
queue.obliterate(): void
queue.clean(grace: number, limit: number, type?: string): number  // grace ms, max jobs, optional state

// DLQ (embedded only)
queue.setDlqConfig(config: DlqConfig): void
queue.getDlq(filter?: DlqFilter): DlqEntry[]
queue.retryDlq(filter?: DlqFilter): number
queue.purgeDlq(filter?: DlqFilter): number

// Stall detection (embedded only)
queue.setStallConfig(config: StallConfig): void

// Cron (embedded only)
await queue.upsertJobScheduler(name: string, schedule: { pattern?: string, every?: number }, template: { data: any }): void
await queue.removeJobScheduler(name: string): void
await queue.getJobSchedulers(): Promise<CronEntry[]>

// Auto-batching (TCP only, enabled by default)
const queue = new Queue('jobs', {
  autoBatch: { maxSize: 50, maxDelayMs: 5 },  // defaults
});
```

### Job Options

```typescript
interface JobOptions {
  priority?: number;          // Higher = processed sooner (default: 0)
  delay?: number;             // ms before available (default: 0)
  attempts?: number;          // Max retries (default: 3)
  backoff?: number;           // Retry delay ms (default: 1000)
  timeout?: number;           // Processing timeout ms
  jobId?: string;             // Custom ID (idempotency/dedup)
  durable?: boolean;          // Bypass write buffer (default: false)
  removeOnComplete?: boolean; // Auto-remove on success
  removeOnFail?: boolean;     // Auto-remove on failure
}
```

## Worker

### Constructor

```typescript
const worker = new Worker<TData>(
  'queue-name',
  async (job: Job<TData>) => {
    // Process job
    await job.updateProgress(50);
    await job.log('Processing step 2...');
    return { result: 'data' }; // Stored as job result
  },
  {
    concurrency: 5,           // Parallel jobs (default: 1)
    heartbeatInterval: 10000, // Stall detection ms (default: 10000, 0=off)
    batchSize: 10,            // Pull batch size (default: 10, max: 1000)
    pollTimeout: 0,           // Long poll ms (default: 0, max: 30000)
    useLocks: true,           // Lock-based ownership (default: true)
    // Connection (same as Queue):
    embedded: true,
    // or: connection: { host: 'localhost', port: 6789 },
  }
);
```

### Events

```typescript
worker.on('completed', (job: Job, result: any) => {});
worker.on('failed', (job: Job, error: Error) => {});
worker.on('error', (error: Error) => {});
worker.on('stalled', (jobId: string) => {});
```

### Methods

```typescript
await worker.close(): Promise<void>   // Graceful shutdown
worker.pause(): void
worker.resume(): void
```

## Job Object

```typescript
interface Job<TData = any> {
  id: string;
  name: string;
  data: TData;
  queueName: string;
  priority: number;
  delay: number;
  attemptsMade: number;
  attemptsStarted: number;
  progress: number;
  returnvalue?: unknown;        // result of the job
  failedReason?: string;
  timestamp: number;            // created at
  processedOn?: number;         // started at
  finishedOn?: number;          // completed/failed at
  parent?: { id: string; queueQualifiedName: string };
  parentKey?: string;
  opts: JobOptions;

  // Methods (available during processing)
  updateProgress(progress: number, message?: string): Promise<void>
  log(message: string): Promise<void>
  getState(): Promise<JobStateType>            // state is a method, not a field
  getChildrenValues<R = unknown>(): Promise<Record<string, R>>
  moveToDelayed(timestamp: number): Promise<void>   // absolute timestamp
  updateData(data: TData): Promise<void>
  remove(): Promise<void>
  retry(): Promise<void>
}
```

## Bunqueue (Simple Mode)

All-in-one Queue + Worker in a single object. Use when producer and consumer are in the same process.

### Constructor Options

Processing mode (pick exactly one):

| Option | Type | Description |
|--------|------|-------------|
| `processor` | `(job) => Promise<R>` | Single handler for all jobs |
| `routes` | `Record<string, Processor>` | Named handlers by job name |
| `batch` | `{ size, timeout, processor }` | Batch processing |

Worker options:

| Option | Default | Description |
|--------|---------|-------------|
| `concurrency` | 1 | Parallel jobs |
| `embedded` | auto | Use embedded SQLite |
| `connection` | — | TCP server connection |
| `autorun` | true | Start worker immediately |

Feature options (all optional):

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

### Processing Pipeline

```
Job → Circuit Breaker → TTL check → AbortController → Retry → Middleware → Processor
```

### Basic Usage

```typescript
const app = new Bunqueue('queue-name', {
  embedded: true,
  dataPath: './data/app.db',
  processor: async (job) => { return result; },
  concurrency: 10,
});
```

### Routes

```typescript
const app = new Bunqueue('notifications', {
  embedded: true,
  routes: {
    'send-email': async (job) => { return { channel: 'email' }; },
    'send-sms': async (job) => { return { channel: 'sms' }; },
  },
});
```

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

### Middleware (Onion Model)

```typescript
app.use(async (job, next) => {
  // Before processing
  const result = await next();
  // After processing
  return result;
});
```

Execution order: mw1 → mw2 → processor → mw2 → mw1. Zero overhead when no middleware.

### Advanced Retry (5 Strategies)

```typescript
const app = new Bunqueue('api', {
  embedded: true,
  processor: async (job) => fetch(job.data.url),
  retry: {
    maxAttempts: 5,
    delay: 1000,
    strategy: 'jitter',  // 'fixed' | 'exponential' | 'jitter' | 'fibonacci' | 'custom'
    retryIf: (error) => error.message.includes('503'),
    // customBackoff: (attempt, error) => attempt * 2000,  // for 'custom' strategy
  },
});
```

| Strategy | Formula | Use case |
|----------|---------|----------|
| `fixed` | delay every time | Rate-limited APIs |
| `exponential` | delay × 2^attempt | General purpose |
| `jitter` | delay × 2^attempt × random(0.5-1.0) | Thundering herd prevention |
| `fibonacci` | delay × fib(attempt) | Gradual backoff |
| `custom` | customBackoff(attempt, error) → ms | Anything |

This is in-process retry — the job stays active. Different from core `attempts`/`backoff` which re-queues.

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

app.cancel(job.id);        // cancel immediately
app.cancel(job.id, 5000);  // cancel after 5s grace period
app.isCancelled(job.id);   // check status
```

Works with fetch: `await fetch(url, { signal })`.

### Circuit Breaker

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

### Event Triggers

```typescript
// On complete → create follow-up job
app.trigger({
  on: 'place-order',
  create: 'send-receipt',
  data: (result, job) => ({ id: job.data.id }),
});

// Conditional trigger
app.trigger({
  on: 'place-order',
  create: 'fraud-alert',
  data: (result) => ({ amount: result.total }),
  condition: (result) => result.total > 1000,
});

// On failure trigger
app.trigger({
  on: 'process',
  event: 'failed',
  create: 'alert',
  data: (_, job) => job.data,
});

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
    defaultTtl: 300000,             // 5 minutes for all jobs
    perName: { 'verify-otp': 60000 }, // 1 minute for OTP
  },
});

app.setDefaultTtl(120000);
app.setNameTtl('flash-sale', 30000);
```

Resolution: `perName[job.name]` → `defaultTtl` → 0 (no TTL).

### Priority Aging

Boosts priority of old waiting jobs to prevent starvation.

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

### Deduplication

Jobs with the same name + data get the same dedup ID.

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

// Override per-job
await app.add('task', data, { deduplication: { id: 'my-custom-id', ttl: 5000 } });
```

### Debouncing

Only the last job in the TTL window gets processed. Groups by job name.

```typescript
const app = new Bunqueue('search', {
  embedded: true,
  processor: async (job) => executeSearch(job.data.query),
  debounce: { ttl: 500 },
});
```

### Rate Limiting

```typescript
// Global rate limit
const app = new Bunqueue('api', {
  embedded: true,
  processor: async (job) => callExternalAPI(job.data),
  rateLimit: { max: 100, duration: 1000 },
});

// Per-group rate limiting (e.g., per customer)
const app2 = new Bunqueue('api', {
  embedded: true,
  processor: async (job) => callAPI(job.data),
  rateLimit: { max: 10, duration: 1000, groupKey: 'customerId' },
});

// Runtime updates
app.setGlobalRateLimit(50, 1000);
app.removeGlobalRateLimit();
```

### DLQ (Dead Letter Queue)

```typescript
const app = new Bunqueue('critical', {
  embedded: true,
  processor: async (job) => riskyOperation(job.data),
  dlq: {
    autoRetry: true,
    autoRetryInterval: 3600000, // retry every hour
    maxAutoRetries: 3,
    maxAge: 604800000,          // purge after 7 days
    maxEntries: 10000,
  },
});

app.getDlq();                            // all entries
app.getDlq({ reason: 'timeout' });       // filter by reason
app.getDlqStats();                       // { total, byReason, ... }
app.retryDlq();                          // retry all
app.retryDlq('job-id');                  // retry one
app.purgeDlq();                          // clear all
app.setDlqConfig({ autoRetry: false });  // update at runtime
```

Failure reasons: `explicit_fail`, `max_attempts_exceeded`, `timeout`, `stalled`, `ttl_expired`, `worker_lost`.

### Cron & Intervals

```typescript
await app.cron('daily-report', '0 9 * * *', { type: 'report' });
await app.cron('eu-digest', '0 8 * * 1', { type: 'weekly' }, { timezone: 'Europe/Rome' });
await app.every('healthcheck', 30000, { type: 'ping' });

await app.listCrons();
await app.removeCron('healthcheck');
```

### Events

```typescript
app.on('completed', (job, result) => {});
app.on('failed', (job, error) => {});
app.on('active', (job) => {});
app.on('progress', (job, progress) => {});
app.on('stalled', (jobId, reason) => {});
app.on('error', (error) => {});
app.on('ready', () => {});
app.on('drained', () => {});
app.on('closed', () => {});
```

### Methods Summary

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
| `queue` | `Queue<T>` | Internal Queue instance |
| `worker` | `Worker<T, R>` | Internal Worker instance |

### Direct Access

For operations not exposed by Simple Mode:

```typescript
app.queue.getWaiting();
app.queue.setStallConfig({ stallInterval: 30000 });
app.worker.concurrency = 20;
```

## FlowProducer (Parent-Child Dependencies)

```typescript
const flow = new FlowProducer({
  embedded: true,
  // or: connection: { port: 6789 },
});

// Single flow
await flow.add({
  name: 'parent',
  queueName: 'pipeline',
  data: { step: 'final' },
  children: [
    { name: 'child-1', queueName: 'pipeline', data: { step: 1 } },
    { name: 'child-2', queueName: 'pipeline', data: { step: 2 } },
  ],
});

// Chain (sequential: A -> B -> C) — single array, each step carries its own queueName
await flow.addChain([
  { name: 'step-1', queueName: 'pipeline', data: { n: 1 } },
  { name: 'step-2', queueName: 'pipeline', data: { n: 2 } },
  { name: 'step-3', queueName: 'pipeline', data: { n: 3 } },
]);

// Bulk fan-out (many children, then one parent) — parallel steps first, final step second
await flow.addBulkThen([
  { name: 'fetch-1', queueName: 'pipeline', data: { url: 'a' } },
  { name: 'fetch-2', queueName: 'pipeline', data: { url: 'b' } },
], { name: 'aggregate', queueName: 'pipeline', data: {} });

await flow.close();
```

## QueueGroup (Multiple Queues)

```typescript
const group = new QueueGroup('media');   // namespace; queues become media:emails, media:images

// Per-name queues + workers
const emails = group.getQueue('emails');
group.getWorker('emails', async (job) => { /* send email */ }, { concurrency: 5 });
group.getWorker('images', async (job) => { /* resize image */ }, { concurrency: 10 });

await emails.add('welcome', { userId: 1 });
await group.getQueue('images').add('resize', { path: 'photo.jpg' });

// Group-wide control
group.pauseAll();
group.resumeAll();
group.drainAll();
group.obliterateAll();
```

## Environment Variables

```bash
# Server
TCP_PORT=6789                    # TCP server port
HTTP_PORT=6790                   # HTTP server port
HOST=0.0.0.0                    # Bind address
BUNQUEUE_DATA_PATH=./data/bunq.db  # SQLite database path
AUTH_TOKENS=token1,token2        # Comma-separated auth tokens

# Timeouts
SHUTDOWN_TIMEOUT_MS=30000
WORKER_TIMEOUT_MS=30000
LOCK_TIMEOUT_MS=5000

# S3 Backup
S3_BACKUP_ENABLED=0
S3_BUCKET=my-bucket
S3_ACCESS_KEY_ID=key
S3_SECRET_ACCESS_KEY=secret
S3_REGION=us-east-1
S3_BACKUP_INTERVAL=21600000      # 6 hours
S3_BACKUP_RETENTION=7            # days
```

## CLI

```bash
# Start server
bunqueue start [--tcp-port 6789] [--data-path ./data/queue.db]

# Job operations
bunqueue push <queue> <json> [--priority N] [--delay ms]
bunqueue pull <queue> [--timeout ms]
bunqueue ack <id> [--result json]
bunqueue fail <id> [--error msg]

# Job management
bunqueue job get <id>
bunqueue job state <id>
bunqueue job cancel <id>
bunqueue job promote <id>

# Queue management
bunqueue queue pause <queue>
bunqueue queue resume <queue>
bunqueue queue drain <queue>

# DLQ
bunqueue dlq list <queue>
bunqueue dlq retry <queue>
bunqueue dlq purge <queue>

# Cron
bunqueue cron list
bunqueue cron add --name <name> --queue <queue> --schedule "* * * * *" --data '{}'
bunqueue cron delete <name>

# Monitoring
bunqueue stats
bunqueue metrics
bunqueue health
```

## Common Patterns

### Email Queue with Retries
```typescript
const queue = new Queue('emails', { embedded: true });
await queue.add('welcome', { userId: 123 }, {
  attempts: 5,
  backoff: 2000,  // 2s between retries
  timeout: 30000, // 30s timeout
});
```

### Priority Processing
```typescript
await queue.add('vip', data, { priority: 100 });   // Processed first
await queue.add('normal', data, { priority: 0 });   // Default
await queue.add('low', data, { priority: -10 });     // Last
```

### Delayed/Scheduled Jobs
```typescript
await queue.add('reminder', data, { delay: 3600000 }); // 1 hour
```

### Idempotent Jobs (Deduplication)
```typescript
await queue.add('process', data, { jobId: 'unique-key-123' });
await queue.add('process', data, { jobId: 'unique-key-123' }); // Ignored, same ID
```

### ETL Pipeline with Flows
```typescript
const flow = new FlowProducer({ embedded: true });
await flow.add({
  name: 'load',
  queueName: 'etl',
  data: { destination: 'warehouse' },
  children: [
    {
      name: 'transform',
      queueName: 'etl',
      data: { format: 'parquet' },
      children: [
        { name: 'extract', queueName: 'etl', data: { source: 'api' } },
      ],
    },
  ],
});
```
