<p align="center">
  <a href="https://bunqueue.dev/">
    <img src=".github/banner.svg" alt="bunqueue" width="400" />
  </a>
</p>

<p align="center">
  <a href="https://www.npmjs.com/package/bunqueue"><img src="https://img.shields.io/npm/v/bunqueue?style=flat-square" alt="npm version"></a>
  <a href="https://www.npmjs.com/package/bunqueue"><img src="https://img.shields.io/npm/dm/bunqueue?style=flat-square" alt="npm downloads"></a>
  <a href="https://github.com/egeominotti/bunqueue/actions"><img src="https://img.shields.io/github/actions/workflow/status/egeominotti/bunqueue/ci.yml?style=flat-square&label=CI" alt="CI"></a>
  <a href="https://github.com/egeominotti/bunqueue/stargazers"><img src="https://img.shields.io/github/stars/egeominotti/bunqueue?style=flat-square" alt="GitHub Stars"></a>
  <a href="https://github.com/egeominotti/bunqueue/blob/main/LICENSE"><img src="https://img.shields.io/github/license/egeominotti/bunqueue?style=flat-square" alt="License"></a>
</p>

<p align="center">
  High-performance job queue for Bun. Built for AI agents and automation.<br/>
  Zero external dependencies. MCP-native. TypeScript-first.
</p>

<p align="center">
  <a href="https://bunqueue.dev/">Documentation</a> &middot;
  <a href="https://bunqueue.dev/guide/benchmarks/">Benchmarks</a> &middot;
  <a href="https://www.npmjs.com/package/bunqueue">npm</a>
</p>

---

## Requirements

The bunqueue **server** (and the embedded mode) is **Bun-only** (`bun >= 1.3.9`):
persistence and transport rely on Bun's runtime APIs. Install Bun from
[bun.sh](https://bun.sh) and run with `bun`. Importing the `bunqueue` package from
Node fails fast with a clear error rather than a cryptic resolver crash.

**Your producers and workers don't have to run on Bun.** Official client SDKs
connect to the server from **Node.js, Deno, Cloudflare Workers**
([`bunqueue-client`](https://www.npmjs.com/package/bunqueue-client) on npm) and
**Python** (`bunqueue-client`, PyPI coming soon) — see [One Queue, Any Language](#one-queue-any-language-sdks).

## Quickstart

```bash
bun add bunqueue
```

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

That's it. Queue + Worker in one object. No Redis, no config, no setup.

---

## 🪶 Featherweight install

`bun add bunqueue` pulls **7 packages and 5.4 MB** — and that includes the binary
queue server, the CLI, and the MCP server. Most queue libraries pull a Redis
client and its dependency tree before you've queued a single job.

| | Before (2.7.x) | Now | |
| --- | --- | --- | --- |
| **`node_modules`** | 93 MB | **5.4 MB** | **−94%** |
| **packages** | 117 | **7** | **−110** |
| **cold install** | ~6 s | **~1.2 s** | **~5× faster** |

Just **2 runtime dependencies** (`croner` + `msgpackr`). SQLite, S3, HTTP and
WebSocket are all Bun built-ins. The MCP SDK is an optional peer dependency —
queue users never download it. ([details →](https://bunqueue.dev/changelog/))

---

## Simple Mode

Simple Mode gives you a Queue and a Worker in a single object. Add jobs, process them, add middleware, schedule crons — all from one place.

Use `Bunqueue` when producer and consumer are in the same process. For distributed systems, use `Queue` + `Worker` separately. For AI agent workflows, use the [MCP Server](#built-for-ai-agents-mcp-server) instead — agents control queues via natural language without writing code.

<details>
<summary><b>Architecture</b></summary>

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
        └── DedupDebounceMerger ── deduplication & debounce
```

Processing pipeline per job:

```
Job → Circuit Breaker → TTL check → AbortController → Retry → Middleware → Processor
```

</details>

### Routes

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

> **Note:** Use one of `processor`, `routes`, or `batch`. Passing multiple or none throws an error.

### Middleware

Wraps every job execution. Execution order is onion-style: `mw1 → mw2 → processor → mw2 → mw1`. When no middleware is added, zero overhead.

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

### Batch Processing

Accumulates N jobs and processes them together. Flushes when buffer reaches `size` or `timeout` expires. On `close()`, remaining jobs are flushed.

```typescript
const app = new Bunqueue('db-inserts', {
  embedded: true,
  batch: {
    size: 50,
    timeout: 2000,
    processor: async (jobs) => {
      const rows = jobs.map(j => j.data.row);
      await db.insertMany('table', rows);
      return jobs.map(() => ({ inserted: true }));
    },
  },
});
```

### Advanced Retry

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
    strategy: 'jitter',
    retryIf: (error) => error.message.includes('503'),
  },
});
```

| Strategy | Formula | Use case |
| --- | --- | --- |
| `fixed` | `delay` every time | Rate-limited APIs |
| `exponential` | `delay × 2^attempt` | General purpose |
| `jitter` | `delay × 2^attempt × random(0.5-1.0)` | Thundering herd prevention |
| `fibonacci` | `delay × fib(attempt)` | Gradual backoff |
| `custom` | `customBackoff(attempt, error) → ms` | Anything |

> This is in-process retry — the job stays active. Different from core `attempts`/`backoff` which re-queues.

### Graceful Cancellation

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

### Circuit Breaker

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
    threshold: 5,
    resetTimeout: 30000,
    onOpen: () => alert('Gateway down!'),
    onClose: () => alert('Gateway recovered'),
  },
});

app.getCircuitState();  // 'closed' | 'open' | 'half-open'
app.resetCircuit();     // force close + resume worker
```

### Event Triggers

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

### Job TTL

Expire unprocessed jobs. Checked when the worker picks up the job:

```typescript
const app = new Bunqueue('otp', {
  embedded: true,
  processor: async (job) => verifyOTP(job.data.code),
  ttl: {
    defaultTtl: 300000,
    perName: {
      'verify-otp': 60000,
      'daily-report': 0,
    },
  },
});

app.setDefaultTtl(120000);
app.setNameTtl('flash-sale', 30000);
```

Resolution: `perName[job.name]` → `defaultTtl` → `0` (no TTL).

### Priority Aging

Automatically boosts priority of old waiting jobs to prevent starvation:

```typescript
const app = new Bunqueue('tasks', {
  embedded: true,
  processor: async (job) => ({ done: true }),
  priorityAging: {
    interval: 60000,
    minAge: 300000,
    boost: 2,
    maxPriority: 100,
    maxScan: 200,
  },
});
```

A job with priority 1, after 5 min: 3, after 10 min: 5, … capped at 100.

### Deduplication

Prevent duplicate jobs. Jobs with the same name + data get the same dedup ID:

```typescript
const app = new Bunqueue('webhooks', {
  embedded: true,
  processor: async (job) => processWebhook(job.data),
  deduplication: {
    ttl: 60000,
    extend: false,
    replace: false,
  },
});

await app.add('hook', { event: 'user.created', userId: '123' });
await app.add('hook', { event: 'user.created', userId: '123' }); // deduplicated!
await app.add('hook', { event: 'user.updated', userId: '123' }); // different data → new job
```

Override per-job: `await app.add('task', data, { deduplication: { id: 'my-id', ttl: 5000 } })`.

### Debouncing

Coalesce rapid same-name jobs. Only the last one in the TTL window gets processed:

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

// Per-group rate limiting (e.g., per customer)
const app2 = new Bunqueue('api', {
  embedded: true,
  processor: async (job) => callAPI(job.data),
  rateLimit: { max: 10, duration: 1000, groupKey: 'customerId' },
});

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
    autoRetryInterval: 3600000,
    maxAutoRetries: 3,
    maxAge: 604800000,
    maxEntries: 10000,
  },
});

app.getDlq();                        // all entries
app.getDlqStats();                   // { total, byReason, ... }
app.getDlq({ reason: 'timeout' });   // filter by reason
app.retryDlq();                      // retry all
app.purgeDlq();                      // clear all
```

Failure reasons: `explicit_fail`, `max_attempts_exceeded`, `timeout`, `stalled`, `ttl_expired`, `worker_lost`.

### Cron Jobs

```typescript
await app.cron('daily-report', '0 9 * * *', { type: 'report' });
await app.cron('eu-digest', '0 8 * * 1', { type: 'weekly' }, { timezone: 'Europe/Rome' });
await app.every('healthcheck', 30000, { type: 'ping' });

await app.listCrons();
await app.removeCron('healthcheck');
```

### Events

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

### Adding Jobs

```typescript
await app.add('task', { key: 'value' });
await app.add('urgent', data, { priority: 10, delay: 5000, attempts: 5, durable: true });
await app.addBulk([
  { name: 'email', data: { to: 'alice' } },
  { name: 'email', data: { to: 'bob' }, opts: { priority: 10 } },
]);
```

### Control

```typescript
app.pause();           // pause queue + worker
app.resume();          // resume both
await app.close();     // graceful shutdown
await app.close(true); // force shutdown
```

### Inspecting jobs & counts

`getJobCounts()` and the per-state lists follow BullMQ semantics:

```typescript
const counts = await queue.getJobCountsAsync();
// { waiting, prioritized, active, completed, failed, delayed, paused }

// Failed jobs (those that exhausted their attempts) are enumerable by state —
// the failed list reflects the same jobs that `failed` counts.
const failed = await queue.getFailedAsync(0, 50);
const alsoFailed = await queue.getJobsAsync({ state: 'failed', start: 0, end: 50 });

// Paused queue: ready jobs are reported under `paused`, never double-counted as
// `waiting`. While paused, getJobCounts() returns waiting:0 / paused:N, and the
// jobs are listed by `getJobsAsync({ state: 'paused' })` (not by `waiting`).
queue.pause();
const c = await queue.getJobCountsAsync(); // waiting: 0, paused: N
const pausedJobs = await queue.getJobsAsync({ state: 'paused', start: 0, end: 50 });
```

### Full Example

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

[Simple Mode docs →](https://bunqueue.dev/guide/simple-mode/)

---

## Workflow Engine

Orchestrate multi-step business processes with branching, saga compensation, and human-in-the-loop signals. Built on top of bunqueue — no new infrastructure.

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const orderFlow = new Workflow('order-pipeline')
  .step('validate', async (ctx) => {
    const { orderId, amount } = ctx.input as { orderId: string; amount: number };
    if (amount <= 0) throw new Error('Invalid amount');
    return { orderId };
  })
  .step('reserve-stock', async () => {
    await inventory.reserve();
    return { reserved: true };
  }, {
    compensate: async () => await inventory.release(), // Auto-rollback on failure
  })
  .step('charge', async () => {
    return { txId: await payments.charge() };
  }, {
    compensate: async () => await payments.refund(),
  })
  .step('confirm', async (ctx) => {
    const { txId } = ctx.steps['charge'] as { txId: string };
    return { emailSent: true, txId };
  });

const engine = new Engine({ embedded: true });
engine.register(orderFlow);
await engine.start('order-pipeline', { orderId: 'ORD-1', amount: 99.99 });
```

**Features:**

- **Saga pattern** — Compensation handlers run in reverse when a step fails
- **Branching** — Route to different paths based on runtime conditions
- **Parallel steps** — Run independent steps concurrently with `.parallel()`
- **Human-in-the-loop** — `waitFor('event')` pauses execution, `engine.signal()` resumes it
- **Signal timeout** — `waitFor('event', { timeout })` fails if signal doesn't arrive in time
- **Step retry** — Automatic retry with exponential backoff and jitter
- **Nested workflows** — Compose workflows with `.subWorkflow()`, child results passed back
- **Loops** — `doUntil()` and `doWhile()` for conditional iteration with safety limits
- **forEach** — Iterate over dynamic item lists with indexed step results (`step:0`, `step:1`, ...)
- **Map** — Synchronous data transforms between steps with `.map()`
- **Schema validation** — Validate step input/output with Zod, ArkType, Valibot, or any `.parse()` schema
- **Subscribe** — `engine.subscribe(id, callback)` to monitor a specific execution's events
- **Observability** — Typed event emitter with 11 event types (`engine.on/onAny`)
- **Cleanup & archival** — `engine.cleanup()` / `engine.archive()` for execution history management
- **Step timeouts** — Prevent steps from running indefinitely
- **Context passing** — Each step accesses input and all previous step results
- **SQLite persistence** — Execution state survives restarts

**vs Competitors:**

| | **bunqueue** | **Temporal** | **Inngest** | **Trigger.dev** |
|---|---|---|---|---|
| **Infrastructure** | None (embedded) | PostgreSQL + 7 services | Cloud-only | Redis + PostgreSQL |
| **Saga compensation** | Built-in | Manual | Manual | Manual |
| **Human-in-the-loop** | `.waitFor()` | Signals API | `step.waitForEvent()` | Waitpoint tokens |
| **Self-hosted** | Zero-config | Complex | No | Complex |
| **Pricing** | Free (MIT) | Free / Cloud $$ | Per-execution | Free tier, then $50/mo+ |

```typescript
// Branching
const flow = new Workflow('tiered')
  .step('classify', async (ctx) => {
    const { amount } = ctx.input as { amount: number };
    return { tier: amount > 1000 ? 'vip' : 'basic' };
  })
  .branch((ctx) => (ctx.steps['classify'] as { tier: string }).tier)
  .path('vip', (w) => w.step('vip-handler', async () => ({ discount: 20 })))
  .path('basic', (w) => w.step('basic-handler', async () => ({ discount: 0 })))
  .step('done', async () => ({ processed: true }));

// Human-in-the-loop
const approvalFlow = new Workflow('expense')
  .step('submit', async (ctx) => {
    const { amount } = ctx.input as { amount: number };
    return { amount };
  })
  .waitFor('manager-approval')
  .step('reimburse', async (ctx) => {
    const decision = ctx.signals['manager-approval'] as { approved: boolean };
    return { status: decision.approved ? 'paid' : 'rejected' };
  });

// Signal a waiting workflow
await engine.signal(run.id, 'manager-approval', { approved: true });
```

[Workflow Engine docs →](https://bunqueue.dev/guide/workflow/)

---

<p align="center">
  <strong>bunqueue Dashboard</strong><br/>
  <sub>A web dashboard that fully drives your bunqueue server &mdash; queues, jobs, DLQ, cron, webhooks, workers, live activity, SQLite inspector, and an AI Copilot. Open source, currently in beta. Requires Bun.</sub>
</p>

https://github.com/user-attachments/assets/e8a8d38e-b4a6-4dc8-8360-876c0f24d116

```bash
bunx bunqueue-dashboard
```

<p align="center">
  <sub>
    <a href="https://egeominotti.github.io/bunqueue-dashboard/">Live demo</a> &middot;
    <a href="https://egeominotti.github.io/bunqueue-dashboard/docs/user-guide">User guide</a> &middot;
    <a href="https://github.com/egeominotti/bunqueue-dashboard">GitHub</a> &middot;
    <a href="https://www.npmjs.com/package/bunqueue-dashboard">npm: bunqueue-dashboard</a>
  </sub>
</p>

---

## Why bunqueue?

| Library      | Requires    | AI-native |
| ------------ | ----------- | --------- |
| BullMQ       | Redis       | No        |
| Agenda       | MongoDB     | No        |
| pg-boss      | PostgreSQL  | No        |
| **bunqueue** | **Nothing** | **Yes**   |

- **MCP server included** — 73 tools, 5 resources, 3 prompts. AI agents get full control out of the box
- **BullMQ-compatible API** — Same `Queue`, `Worker`, `QueueEvents`
- **Zero dependencies** — No Redis, no MongoDB
- **Tiny footprint** — 5.4 MB installed, 7 packages (just 2 runtime deps: `croner` + `msgpackr`)
- **SQLite persistence** — Survives restarts, WAL mode for concurrent access
- **Up to 286K ops/sec** — [Verified benchmarks](https://bunqueue.dev/guide/benchmarks/)

## Built for AI Agents (MCP Server)

<p align="center">
  <img src=".github/mcp-flow.svg" alt="HTTP Handler Flow: Cron/Add Job → Queue → Embedded Worker → HTTP API → Job Result" width="700" />
</p>

bunqueue is the **first job queue with native MCP support**. AI agents get a full-featured scheduler, task queue, and monitoring system — no glue code needed.

**HTTP Handlers** solve a fundamental problem: an AI agent can schedule jobs and manage queues, but it cannot run a persistent worker. When the agent registers an HTTP handler, bunqueue spawns an embedded Worker that continuously pulls jobs and calls your HTTP endpoint. Responses are saved as results. Failed calls retry automatically via DLQ.

**What AI agents can do with bunqueue:**

- **Schedule tasks** — cron jobs, delayed execution, recurring workflows
- **Manage job pipelines** — push jobs, monitor progress, retry failures
- **Full pull/ack/fail cycle** — agents can consume and process jobs directly
- **Monitor everything** — stats, memory, Prometheus metrics, logs, DLQ
- **Control flow** — pause/resume queues, set rate limits, manage concurrency
- **73 MCP tools + 5 resources + 3 prompts** — complete control over every feature
- **HTTP handlers** — register a URL, bunqueue auto-processes jobs via HTTP calls

```bash
# bunqueue-mcp is a binary bundled inside the bunqueue package (not a standalone npm package),
# so install bunqueue first, then connect Claude Code:
bun add bunqueue
# Since v2.8.0 the MCP SDK is an optional peer dependency. Queue-only users skip it entirely:
# a clean install drops from 93 MB / 117 packages to 5.4 MB / 7 packages (−94%, up to ~5× faster on a cold install).
# bunx --package=bunqueue does NOT auto-install optional peers, so install the SDK once to run the MCP server:
bun add @modelcontextprotocol/sdk
claude mcp add bunqueue -- bunx bunqueue-mcp
```

```json
// Claude Desktop / Cursor / Windsurf — --package=bunqueue lets bunx resolve the bundled
// bunqueue-mcp binary without a separate install (the @modelcontextprotocol/sdk optional
// peer dependency must still be installed once: bun add @modelcontextprotocol/sdk):
{
  "mcpServers": {
    "bunqueue": {
      "command": "bunx",
      "args": ["--package=bunqueue", "bunqueue-mcp"]
    }
  }
}
```

**Example agent interactions:**

- *"Schedule a cleanup job every day at 3 AM"*
- *"Add 500 email jobs to the queue with priority 10"*
- *"Show me all failed jobs and retry them"*
- *"Set rate limit to 50/sec on the api-calls queue"*
- *"What's the memory usage and queue throughput?"*

**Plugin ecosystem** — bunqueue ships with auto-discovery (`.mcp.json`), a custom Claude Code agent for bunqueue tasks, and installable skills for setup, API reference, and real-world patterns. Drop bunqueue into any project and your AI tools discover it automatically.

Supports **embedded** (local SQLite) and **TCP** (remote server) modes. [Full MCP documentation →](https://bunqueue.dev/guide/mcp/)

## When to use bunqueue

**Great for:**

- **AI agents that need a scheduler** — cron jobs, delayed tasks, retries, all via MCP
- **Agentic workflows** — agents push jobs, workers process, agents monitor results
- Single-server deployments
- Prototypes and MVPs
- Moderate to high workloads (up to 286K ops/sec)
- Teams that want to avoid Redis operational overhead
- Embedded use cases (CLI tools, edge functions, serverless)

**Not ideal for:**

- Multi-region distributed systems requiring HA
- Workloads that need automatic failover today
- Systems already running Redis with existing infrastructure

## Why not just use BullMQ?

If you're already running Redis, BullMQ is great — battle-tested and feature-rich.

bunqueue is for when you **don't want to run Redis**. SQLite with WAL mode handles surprisingly high throughput for single-node deployments (tested up to 286K ops/sec). You get persistence, priorities, delays, retries, cron jobs, and DLQ — without the operational overhead of another service.

## Install

```bash
bun add bunqueue
```

> The server and embedded mode require the [Bun](https://bun.sh) runtime. To
> connect **clients** from Node.js, Deno, Cloudflare Workers or Python, use the
> [SDKs](#one-queue-any-language-sdks) instead.

## Two Modes

bunqueue runs in two modes depending on your architecture:

|                  | Embedded                              | Server (TCP)                                 |
| ---------------- | ------------------------------------- | -------------------------------------------- |
| **How it works** | Queue runs inside your process        | Standalone server, clients connect via TCP   |
| **Setup**        | `bun add bunqueue`                    | `docker run` or `bunqueue start`             |
| **Performance**  | 286K ops/sec                          | 149K ops/sec                                 |
| **Best for**     | Single-process apps, CLIs, serverless | Multiple workers, separate producer/consumer |
| **Scaling**      | Same process only                     | Multiple clients across machines             |

### Embedded Mode

Everything runs in your process. No server, no network, no setup.

```typescript
import { Queue, Worker } from 'bunqueue/client';

const queue = new Queue('emails', { embedded: true });

const worker = new Worker(
  'emails',
  async (job) => {
    console.log('Processing:', job.data);
    return { sent: true };
  },
  { embedded: true }
);

await queue.add('welcome', { to: 'user@example.com' });
```

### Server Mode (TCP)

Run bunqueue as a standalone server. Multiple workers and producers connect via TCP.

```bash
# Start with persistent data
docker run -d -p 6789:6789 -p 6790:6790 \
  -v bunqueue-data:/app/data \
  ghcr.io/egeominotti/bunqueue:latest
```

Connect from your app:

```typescript
import { Queue, Worker } from 'bunqueue/client';

const queue = new Queue('tasks', { connection: { host: 'localhost', port: 6789 } });

const worker = new Worker(
  'tasks',
  async (job) => {
    return { done: true };
  },
  { connection: { host: 'localhost', port: 6789 } }
);

await queue.add('process', { data: 'hello' });
```

## One Queue, Any Language (SDKs)

The server does all the heavy lifting — retries, priorities, scheduling, DLQ.
Official client SDKs speak the native TCP protocol (msgpack, pipelined) with
full feature parity, so producers and workers can live anywhere in your stack:

| Where your code runs | Install | Status |
| -------------------- | ------- | ------ |
| **Node.js ≥ 20, Deno ≥ 2, Bun, Cloudflare Workers** | [`npm install bunqueue-client`](https://www.npmjs.com/package/bunqueue-client) | 58/58 e2e per runtime + 11/11 inside workerd |
| **Python ≥ 3.9** | PyPI coming soon — today: `pip install "git+https://github.com/egeominotti/bunqueue.git#subdirectory=sdk/python"` | 44/44 e2e |

```typescript
// Node.js / Deno / Cloudflare Workers
import { Queue, Worker } from 'bunqueue-client';

const queue = new Queue('emails', { host: 'localhost', port: 6789 });
await queue.add('welcome', { to: 'user@example.com' });

new Worker('emails', async (job) => ({ sent: true }), { concurrency: 10 });
```

```python
# Python
from bunqueue import Queue, Worker

queue = Queue("emails", host="localhost", port=6789)
queue.add("welcome", {"to": "user@example.com"})

Worker("emails", lambda job: {"sent": True}, concurrency=10).run()
```

Mix and match: a Next.js API adds jobs, a Python service processes them.
Docs: [bunqueue.dev/guide/sdks](https://bunqueue.dev/guide/sdks/) · Sources: [`sdk/`](./sdk/)

### Simple Mode

One object. Queue + Worker + Routes + Middleware + Cron. Zero boilerplate.

```typescript
import { Bunqueue } from 'bunqueue/client';

const app = new Bunqueue('notifications', {
  embedded: true,

  // Route jobs by name
  routes: {
    'send-email': async (job) => {
      console.log(`Email to ${job.data.to}`);
      return { sent: true };
    },
    'send-sms': async (job) => {
      console.log(`SMS to ${job.data.to}`);
      return { sent: true };
    },
  },
  concurrency: 10,
});

// Middleware — wraps every job (logging, timing, error recovery)
app.use(async (job, next) => {
  const start = Date.now();
  const result = await next();
  console.log(`${job.name} took ${Date.now() - start}ms`);
  return result;
});

// Cron — scheduled jobs
await app.cron('daily-report', '0 9 * * *', { type: 'summary' });
await app.every('healthcheck', 30000, { type: 'ping' });

// Events
app.on('completed', (job, result) => console.log(result));
app.on('failed', (job, err) => console.error(err));

// Add jobs
await app.add('send-email', { to: 'alice@example.com' });
await app.add('send-sms', { to: '+1234567890' });

// Graceful shutdown
await app.close();
```

Works with both embedded and TCP mode. [Simple Mode docs →](https://bunqueue.dev/guide/simple-mode/)

## Performance

SQLite handles surprisingly high throughput for single-node deployments:

| Mode     | Peak Throughput | Use Case            |
| -------- | --------------- | ------------------- |
| Embedded | 286K ops/sec    | Same process        |
| TCP      | 149K ops/sec    | Distributed workers |

> Run `bun run bench` to verify on your hardware. [Full benchmark methodology →](https://bunqueue.dev/guide/benchmarks/)

## Monitoring

```bash
# Start with Prometheus + Grafana
docker compose --profile monitoring up -d
```

- **Grafana**: http://localhost:3000 (admin/bunqueue)
- **Prometheus**: http://localhost:9090

## Documentation

**[Read the full documentation →](https://bunqueue.dev/)**

- [Quick Start](https://bunqueue.dev/guide/quickstart/)
- [Workflow Engine](https://bunqueue.dev/guide/workflow/)
- [MCP Server (AI Agents)](https://bunqueue.dev/guide/mcp/)
- [Simple Mode](https://bunqueue.dev/guide/simple-mode/)
- [Queue API](https://bunqueue.dev/guide/queue/)
- [Worker API](https://bunqueue.dev/guide/worker/)
- [Server Mode](https://bunqueue.dev/guide/server/)
- [Benchmarks](https://bunqueue.dev/guide/benchmarks/)
- [CLI Reference](https://bunqueue.dev/guide/cli/)

## License

MIT
