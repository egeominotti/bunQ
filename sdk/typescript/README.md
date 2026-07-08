# bunqueue-client

Official TypeScript client for [bunqueue](https://github.com/egeominotti/bunqueue), the high performance job queue server. The client implements the native TCP protocol (msgpack, pipelined) and provides full feature parity with the built in Bun client, while running on every modern JavaScript runtime.

The bunqueue server runs on Bun, distributed as a binary or a Docker image. This client allows any Node.js, Bun, Deno, or Cloudflare Workers service to produce and consume jobs against it: one queue, any language, any runtime.

## Compatibility

| Runtime | Status | Notes |
|---|---|---|
| Node.js 20 or later | Supported, 58/58 e2e and 8/8 integration tests | ESM. TypeScript files run directly on Node 22 or later via `--experimental-strip-types` |
| Bun | Supported, 58/58 e2e and 8/8 integration tests | No additional configuration required |
| Deno 2 or later | Supported, 58/58 e2e and 8/8 integration tests | Uses `node:` builtins and the npm `msgpackr` package |
| tsx, ts-node, vitest, jest | Supported | These environments execute on Node.js |
| Cloudflare Workers | Supported, 11/11 e2e tests inside workerd | Requires the `nodejs_compat` compatibility flag. The runtime is request scoped, so long lived worker loops are not available: consume in batches from Cron Triggers or Durable Object alarms, a pattern covered by the test suite. TLS connections require a publicly trusted certificate |
| Browser | Not supported | Raw TCP sockets are unavailable. Use the server HTTP API instead |

Portability is guaranteed by design: the client relies exclusively on `node:*` builtins (`net`, `tls`, `events`, `crypto`, `os`), uses no `Bun.*` globals and no runtime specific imports, and carries a single runtime dependency, `msgpackr`.

## Installation

```bash
npm install bunqueue-client
# or: bun add bunqueue-client / pnpm add bunqueue-client / deno add npm:bunqueue-client
```

## Quick start

Sixty seconds from zero to a working queue. Step 1, start the server (requires [Bun](https://bun.sh), or use the Docker image):

```bash
bunx bunqueue start
```

Step 2, create `app.ts`: add a job and process it, in the same file for the sake of the demo:

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

Step 3, run it with the runtime you already use:

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

That is the whole model: the server owns state, retries, and scheduling, your code only adds and processes. In production the producer and the worker are separate services, often in different languages: the [Python client](https://github.com/egeominotti/bunqueue/tree/main/sdk/python) speaks the same protocol against the same queue. Defaults are `host: 'localhost'`, `port: 6789`, so constructors need no options on a local setup.

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

## API surface

| Area | Capabilities |
|---|---|
| Queue | `add`, `addBulk`, full `JobOptions`: priority, delay, attempts, backoff, ttl, timeout, jobId, deduplication, dependsOn, tags, groupId, lifo, removeOnComplete, removeOnFail, durable, repeat, debounce |
| Query | `getJob`, `getJobByCustomId`, `getJobs` with per state helpers, state, result, progress, `waitForJob`, counts, counts per priority, children values, job logs |
| Control | pause, resume, drain, obliterate, clean, remove, discard, promote, `retryJob`, `retryJobs`, move to wait or delayed, change priority or delay, update data, extend lock |
| Dead letter queue | `getDlq`, `retryDlq`, `purgeDlq`, DLQ configuration |
| Administration | rate limiting, global concurrency, stall configuration, webhooks, stats, metrics, `listQueues`, `getWorkers` |
| Worker events | `ready`, `active`, `completed`, `failed`, `progress`, `drained`, `error`, `closed`, with automatic lock heartbeats so that jobs longer than the lock TTL survive |

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
