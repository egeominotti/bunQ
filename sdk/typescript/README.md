# bunqueue-client

Official TypeScript client for [bunqueue](https://github.com/egeominotti/bunqueue), the high performance job queue server. The client implements the native TCP protocol (msgpack, pipelined) and provides full feature parity with the built in Bun client, while running on every modern JavaScript runtime.

The bunqueue server runs on Bun, distributed as a binary or a Docker image. This client allows any Node.js, Bun, Deno, or Cloudflare Workers service to produce and consume jobs against it: one queue, any language, any runtime.

## Compatibility

| Runtime | Status | Notes |
|---|---|---|
| Node.js 20 or later | Supported, 100/100 e2e and 8/8 integration tests | ESM. TypeScript files run directly on Node 22 or later via `--experimental-strip-types` |
| Bun | Supported, 100/100 e2e and 8/8 integration tests | No additional configuration required |
| Deno 2 or later | Supported, 100/100 e2e and 8/8 integration tests | Uses `node:` builtins and the npm `msgpackr` package |
| tsx, ts-node, vitest, jest | Supported | These environments execute on Node.js |
| Cloudflare Workers | Supported, 16/16 e2e tests inside workerd, including Simple Mode and the full API surface | Requires the `nodejs_compat` compatibility flag. The runtime is request scoped, so long lived worker loops are not available: consume in batches from Cron Triggers or Durable Object alarms, a pattern covered by the test suite. TLS connections require a publicly trusted certificate |
| Browser | Not supported | Raw TCP sockets are unavailable. Use the server HTTP API instead |

Portability is guaranteed by design: the client relies exclusively on `node:*` builtins (`net`, `tls`, `events`, `crypto`, `os`), uses no `Bun.*` globals and no runtime specific imports, and carries a single runtime dependency, `msgpackr`.

## Installation

```bash
npm install bunqueue-client
# or: bun add bunqueue-client / pnpm add bunqueue-client / deno add npm:bunqueue-client
```

## Quick start, step by step

Every step from zero to a production ready queue.

### Step 1. Run the bunqueue server

The server is the only component that requires [Bun](https://bun.sh). Pick one:

```bash
# Option A: one command, no install (requires Bun)
bunx bunqueue start

# Option B: Docker, with persistent data
docker run -d --name bunqueue \
  -p 6789:6789 -p 6790:6790 \
  -v bunqueue-data:/app/data \
  ghcr.io/egeominotti/bunqueue:latest
```

Port 6789 is the TCP protocol (what this client uses), port 6790 is the HTTP API with `/health`, `/metrics`, and dashboard endpoints.

### Step 2. Install the client

```bash
npm install bunqueue-client
# or: bun add bunqueue-client / pnpm add bunqueue-client / deno add npm:bunqueue-client
```

### Step 3. Add your first job and process it

Create `app.ts`, one file for the sake of the demo:

```typescript
import { Queue, Worker } from 'bunqueue-client';

const worker = new Worker('hello', async (job) => {
  console.log('processing', job.data);
  return { greeted: job.data.name };
});
worker.on('completed', (job, result) => {
  console.log('completed', job.id, result);
  worker.close();
});

const queue = new Queue('hello');
await queue.add('greet', { name: 'world' });
queue.close();
```

Run it with the runtime you already use:

```bash
node --experimental-strip-types app.ts    # Node 22 or later
bun app.ts                                # Bun
deno run -A app.ts                        # Deno 2 or later
```

Expected output:

```
processing { name: 'world' }
completed 019f40a5-... { greeted: 'world' }
```

Defaults are `host: 'localhost'` and `port: 6789`, so constructors need no options on a local setup.

### Step 4. Split producer and worker

In production the producer and the worker are separate services, often in different languages. The producer is typically an API endpoint:

```typescript
// api-service: adds jobs, no processing
import { Queue } from 'bunqueue-client';
const queue = new Queue('emails', { host: 'queue.internal', port: 6789 });
await queue.add('welcome', { to: 'user@example.com' }, { attempts: 3 });
```

The worker is a long running process:

```typescript
// worker-service: processes jobs, no HTTP
import { Worker } from 'bunqueue-client';
new Worker('emails', sendEmail, { host: 'queue.internal', port: 6789, concurrency: 10 });
```

The [Python client](https://github.com/egeominotti/bunqueue/tree/main/sdk/python) speaks the same protocol against the same queue, so the worker can be a Python service instead.

### Step 5. Observe and operate

```typescript
await queue.getJobCounts();      // { waiting, active, completed, failed, delayed, ... }
await queue.getDlq();            // jobs that exhausted their retries
await queue.retryDlq();          // send them back to the queue
await queue.getWorkers();        // connected workers
await queue.getStats();          // throughput and totals
```

Or hit the HTTP side: `curl http://localhost:6790/health`.

### Step 6. Go to production

```typescript
const queue = new Queue('emails', {
  host: 'queue.example.com',
  port: 6789,
  token: process.env.BUNQUEUE_TOKEN,   // server started with AUTH_TOKENS=...
  tls: true,                            // or { caFile: './ca.pem' }
});
```

Checklist: set `AUTH_TOKENS` on the server, enable TLS (`TLS_CERT_FILE`/`TLS_KEY_FILE`), mount a volume for the SQLite data path, monitor `/health` and `/metrics`, and size worker `concurrency` to your workload. Full guide: [bunqueue.dev/guide/deployment](https://bunqueue.dev/guide/deployment/).

## Producing jobs

```typescript
import { Queue } from 'bunqueue-client';

const queue = new Queue('emails', { host: 'localhost', port: 6789 });

await queue.add('send', { to: 'user@example.com' }, { priority: 5, attempts: 3 });
await queue.addBulk([
  { name: 'send', data: { to: 'a@b.c' } },
  { name: 'send', data: { to: 'x@y.z' }, opts: { delay: 5000 } },
]);

const counts = await queue.getJobCounts();
queue.close();
```

## Processing jobs

```typescript
import { Worker, UnrecoverableError } from 'bunqueue-client';

const worker = new Worker(
  'emails',
  async (job) => {
    await job.updateProgress(50);
    if (job.data.invalid) throw new UnrecoverableError('bad payload'); // no retries, straight to the DLQ
    return { sent: true };
  },
  { host: 'localhost', port: 6789, concurrency: 10 }
);

worker.on('completed', (job, result) => console.log(job.id, result));
worker.on('failed', (job, err) => console.error(job.id, err.message));

// later: await worker.close();  // graceful shutdown, waits for in flight jobs
```

Retry, backoff, dead letter queue, stall detection, priorities, and rate limiting all execute server side. The worker only pulls, heartbeats, and acknowledges, which keeps the client thin and the behavior consistent across languages.

## Flows

```typescript
import { FlowProducer } from 'bunqueue-client';

const flow = new FlowProducer({ host: 'localhost', port: 6789 });

// sequential chain: step1, then step2, then step3
await flow.addChain([
  { name: 'step1', queueName: 'pipeline' },
  { name: 'step2', queueName: 'pipeline' },
  { name: 'step3', queueName: 'pipeline' },
]);

// fan in: parallel jobs converge into a final job that reads their results
const { finalId } = await flow.addBulkThen(
  [
    { name: 'part1', queueName: 'pipeline' },
    { name: 'part2', queueName: 'pipeline' },
  ],
  { name: 'merge', queueName: 'pipeline' }
);
// inside the 'merge' processor: await job.getChildrenValues()

// parent and child tree: children always run before the parent
const node = await flow.add({
  name: 'assemble', queueName: 'orders',
  children: [
    { name: 'reserve-stock', queueName: 'orders' },
    { name: 'charge-card', queueName: 'orders' },
  ],
});
```

## Simple Mode

`Bunqueue` bundles a Queue and a Worker in one object, with routes, onion middleware, in process retry strategies, a circuit breaker, batch accumulation, event triggers, job TTL, priority aging, cooperative cancellation, and dedup or debounce defaults. It is a 1:1 port of the official client's Simple Mode, TCP only (the `embedded` option raises).

```typescript
import { Bunqueue, type Job } from 'bunqueue-client';

const app = new Bunqueue('notifications', {
  connection: { host: 'localhost', port: 6789 },
  concurrency: 10,
  routes: {
    'send-email': async (job: Job<{ to: string }>) => ({ channel: 'email' }),
    'send-sms': async (job: Job<{ to: string }>) => ({ channel: 'sms' }),
  },
  retry: { maxAttempts: 5, delay: 1000, strategy: 'jitter' },
  circuitBreaker: { threshold: 5, resetTimeout: 30_000 },
  ttl: { perName: { 'verify-otp': 60_000 } },
  deduplication: { ttl: 5000 },
});

app.use(async (job, next) => {
  const start = Date.now();
  const result = await next();
  console.log(`${job.name}: ${Date.now() - start}ms`);
  return result;
});

app.trigger({
  on: 'send-email',
  create: 'send-sms',
  data: (result, job) => job.data,
  condition: (result) => (result as { channel: string }).channel === 'email',
});

await app.cron('daily-digest', '0 9 * * *', { to: 'all' });
await app.add('send-email', { to: 'alice@example.com' });
// later: await app.close();
```

Use `processor` for a single handler, `routes` to dispatch by job name, or `batch` to accumulate N jobs into one call. Exactly one of the three is required.

## Scheduling

```typescript
await queue.addCron('daily-report', '0 9 * * *', { type: 'report' });
await queue.every('health-ping', 30_000, { type: 'ping' });
await queue.removeJobScheduler('daily-report');
```

## Security

```typescript
const queue = new Queue('emails', {
  host: 'queue.example.com',
  port: 6789,
  token: process.env.BUNQUEUE_TOKEN,
  tls: { caFile: './ca.pem' }, // or `true` for system certificate authorities
});
```

Authentication uses server side tokens (`AUTH_TOKENS`). Transport security uses native TLS, with support for system certificate authorities, a custom CA bundle, or disabled verification for development environments.

## Observability

Inject a logger and a telemetry sink to bridge the client into your stack. There are no hard dependencies — you wire OpenTelemetry, Prometheus or your own logger. Defaults are silent.

```typescript
import { Queue, consoleLogger, type TelemetryEvent } from 'bunqueue-client';

const queue = new Queue('emails', {
  logger: consoleLogger('info'), // or your own { debug, info, warn, error }
  onTelemetry: (e: TelemetryEvent) => {
    // per-command latency, connect/disconnect/reconnect, auth, backpressure
    if (e.type === 'command') metrics.observe(e.cmd, e.durationMs, e.ok);
  },
});

// Connection is an EventEmitter for lifecycle hooks:
queue.connection.on('reconnect_scheduled', (i) => log.warn('reconnecting', i));
queue.connection.on('disconnect', () => log.warn('link down'));
```

## Throughput and resilience

```typescript
// Bound in-flight commands (backpressure) — parks callers instead of growing memory:
const queue = new Queue('emails', { maxInFlight: 10_000 });

// Fan producer commands across N connections (round-robin, producer-side):
const pooled = new Queue('emails', { poolSize: 4 });

// Batch worker ACKs into ACKB round-trips (opt-in) for high-volume consumers:
const worker = new Worker('emails', process, {
  ackBatch: { enabled: true, maxSize: 50, maxDelayMs: 5 },
});
```

Always attach a `worker.on('error', …)` listener: per Node `EventEmitter` semantics an unhandled `error` event throws. The worker frees each job's concurrency slot before emitting, so even a throwing listener cannot degrade throughput — but the error itself is yours to observe.

Half-open links are detected via TCP keepalive and a consecutive-timeout teardown, so a silently dropped connection (cloud LB/NAT idle drop) recovers in seconds rather than minutes.

## Typed responses

`connection.call<R>()` and `queue.call<R>()` are generic over the exported response shapes (`JobResponse`, `PulledJobsResponse`, `JobCountsResponse`, `WaitJobResponse`, …), so raw command access is fully typed without casting.

## API surface

| Area | Capabilities |
|---|---|
| Queue | `add`, `addBulk`, full `JobOptions`: priority, delay, attempts, backoff, ttl, timeout, jobId, deduplication, dependsOn, tags, groupId, lifo, removeOnComplete, removeOnFail, durable, repeat, debounce |
| Query | `getJob`, `getJobByCustomId`, `getJobs` with per state helpers, state, result, progress, `waitForJob` (throws on timeout, BullMQ contract), counts, counts per priority, children values, job logs |
| Control | pause, resume, drain, obliterate, clean, remove, discard, promote, `retryJob`, `retryJobs`, move to wait or delayed, change priority or delay, update data, extend lock |
| Dead letter queue | `getDlq`, `retryDlq`, `purgeDlq`, DLQ configuration |
| Administration | rate limiting, global concurrency, stall configuration, webhooks, stats, metrics, `listQueues`, `getWorkers` |
| Worker events | `ready`, `active`, `completed`, `failed`, `progress`, `drained`, `error`, `closed`, with automatic lock heartbeats so that jobs longer than the lock TTL survive |
| Simple Mode | `Bunqueue`: routes, middleware, in process retry (fixed, exponential, jitter, fibonacci, custom), circuit breaker, batch accumulation, triggers, TTL, priority aging, cancellation via `getSignal`, dedup and debounce defaults, cron shorthands |

The following features require the in process Bun runtime and are intentionally out of scope for this client: embedded mode, sandboxed workers, and `QueueEvents`. Use webhooks or the HTTP SSE and WebSocket endpoints for event streaming.

Note on numeric payloads: JavaScript numbers are IEEE 754 doubles, exact up to 2^53. Pass larger 64 bit identifiers, for example snowflake IDs, as strings to avoid silent precision loss. Never place `BigInt` values in job data.

## Quality assurance

Every release is validated against a real bunqueue server, spawned fresh for each run, across every supported runtime:

```bash
bun install
bun run build          # tsc, emits dist/
bun run check          # Biome lint and format verification

bun tests/integration.ts                            # smoke suite
bun tests/e2e.ts                                    # full surface, edge cases, realistic load
node --experimental-strip-types tests/e2e.ts        # identical suite on Node 22 or later
deno run -A tests/e2e.ts                            # identical suite on Deno 2 or later
bun run test:workers                                # full suite inside workerd, the Cloudflare Workers runtime
```

The e2e suite includes payload limits, unicode integrity, pipelining under concurrency, server crash and restart with automatic reconnection, and a realistic multi queue production scenario with zero loss accounting. Engineering standards: Biome, a maximum of 250 lines per file, and relative imports with explicit `.js` extensions for NodeNext resolution. See `CLAUDE.md` for the full development guide and wire protocol notes.

## License

MIT. See the [LICENSE](./LICENSE) file. Documentation: [bunqueue.dev/guide/sdks](https://bunqueue.dev/guide/sdks/). Issues and feature requests: [GitHub issues](https://github.com/egeominotti/bunqueue/issues).
