---
title: "Quick Start: Your First Bun Job Queue in Minutes"
description: "Get started with bunqueue in minutes. Create queues, add jobs, and process them with Workers. Includes SQLite persistence setup."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/quickstart.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · quickstart</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Working queue in <em>a minute.</em></h1>
  <p class="bq-hero-sub">Create a queue, add jobs, process them with a Worker, and turn on persistence. Everything on this page runs in a single Bun process with zero configuration.</p>
</div>

## The smallest working queue

Install bunqueue (`bun add bunqueue`), save this as `app.ts`, run `bun run app.ts`:

```typescript
import { Queue, Worker } from 'bunqueue/client';

// The queue: where you put jobs
const queue = new Queue('emails', { embedded: true });

// The worker: pulls jobs and runs your function on each one
const worker = new Worker('emails', async (job) => {
  console.log(`Sending "${job.data.subject}" to ${job.data.to}`);
  return { sent: true };
}, { embedded: true });

// Add a job
await queue.add('welcome', { to: 'user@example.com', subject: 'Welcome!' });
```

`embedded: true` means the queue runs inside your process, no server needed. That is the mode this page uses.

:::danger[The one mistake everyone makes]
`Queue` and `Worker` must use the **same mode**. If one has `embedded: true` and the other doesn't, the other tries to connect to a TCP server that isn't running and fails with a "Command timeout" error.

```typescript
// ✅ Both embedded
const queue = new Queue('tasks', { embedded: true });
const worker = new Worker('tasks', handler, { embedded: true });

// ❌ Mixed modes = timeout error
const queue = new Queue('tasks', { embedded: true });
const worker = new Worker('tasks', handler);  // Missing embedded: true!
```
:::

## Add jobs with options

```typescript
// Typed queue: job.data is type-checked
interface EmailJob {
  to: string;
  subject: string;
}
const emailQueue = new Queue<EmailJob>('emails', { embedded: true });

// Priority, delay, retries
await emailQueue.add('send-email', { to: 'a@test.com', subject: 'Hi' }, {
  priority: 10,   // Higher = processed first
  delay: 5000,    // Wait 5 seconds before processing
  attempts: 3,    // Retry up to 3 times if the processor throws
  backoff: 1000,  // Wait 1 second between retries (grows on each attempt)
});

// Many jobs at once (one optimized batch)
await emailQueue.addBulk([
  { name: 'send-email', data: { to: 'a@test.com', subject: 'Hi' } },
  { name: 'send-email', data: { to: 'b@test.com', subject: 'Hi' } },
]);
```

All options are in the [Queue guide](/guide/queue/).

## Do more inside the processor

```typescript
const worker = new Worker<EmailJob>('emails', async (job) => {
  await job.updateProgress(50, 'Sending email...');  // Report progress
  await sendEmail(job.data);                          // Do the work
  await job.log('Email sent successfully');           // Attach a log line
  return { sent: true, timestamp: Date.now() };       // Result, stored and queryable
}, {
  embedded: true,
  concurrency: 5,  // Process 5 jobs in parallel
});
```

## React to events

```typescript
worker.on('completed', (job, result) => {
  console.log(`Job ${job.id} completed:`, result);
});

worker.on('failed', (job, error) => {
  console.error(`Job ${job.id} failed:`, error.message);
});

worker.on('progress', (job, progress) => {
  console.log(`Job ${job.id} progress: ${progress}%`);
});
```

The full event list is in the [Worker guide](/guide/worker/).

## Turn on persistence

Without a data path, jobs live in memory and disappear on restart. Point bunqueue at a SQLite file to survive restarts:

```typescript
// Option 1: dataPath option (recommended)
const queue = new Queue('tasks', { embedded: true, dataPath: './data/bunqueue.db' });
const worker = new Worker('tasks', processor, { embedded: true, dataPath: './data/bunqueue.db' });

// Option 2: environment variable
// DATA_PATH=./data/bunqueue.db bun run app.ts
```

## Shut down cleanly

```typescript
import { shutdownManager } from 'bunqueue/client';

process.on('SIGINT', async () => {
  await worker.close();   // Finish active jobs
  shutdownManager();      // Flush pending writes, close SQLite
  process.exit(0);
});
```

## Need more than one process?

Everything above runs in a single process. When multiple services need to share one queue, run bunqueue as a standalone server instead:

| | Embedded mode | Server mode |
|---|---------------|-------------|
| **Best for** | Single-process apps, serverless | Multi-process, microservices |
| **Setup** | `embedded: true` | Run `bunqueue start`, drop the option |
| **Clients** | Bun only (in process) | Node.js, Deno, Bun, Python, Cloudflare Workers |

See the [Server guide](/guide/server/). For non-Bun clients, install [`bunqueue-client`](https://www.npmjs.com/package/bunqueue-client) and use the same API, see the [SDK guide](/guide/sdks/).

## Where to go next

**Less boilerplate.** `Bunqueue` (Simple Mode) wraps Queue + Worker in one object with routes, middleware, and cron:

```typescript
import { Bunqueue } from 'bunqueue/client';

const app = new Bunqueue('notifications', {
  embedded: true,
  routes: {
    'send-email': async (job) => ({ sent: true }),
    'send-sms': async (job) => ({ sent: true }),
  },
  concurrency: 10,
});

await app.add('send-email', { to: 'alice@example.com' });
await app.cron('daily-report', '0 9 * * *', { type: 'summary' });
```

See the [Simple Mode guide](/guide/simple-mode/).

**Watch it live.** The web dashboard shows queues, jobs, failures, crons, and workers. One command:

```bash
bunx bunqueue-dashboard
```

Try the [live demo](https://egeominotti.github.io/bunqueue-dashboard/) without installing anything.

**Connect AI agents.** bunqueue ships an MCP server with 73 tools, so agents like Claude can add jobs, manage crons, and monitor queues via natural language:

```bash
bun add bunqueue @modelcontextprotocol/sdk
claude mcp add bunqueue -- bunx bunqueue-mcp
```

Setup for Claude Desktop, Cursor, and Windsurf is in the [MCP guide](/guide/mcp/).

**Orchestrate multi-step processes.** The built-in workflow engine handles branching, parallel steps, rollback on failure, and human approvals:

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const flow = new Workflow('order')
  .step('validate', async (ctx) => ({ ok: true }))
  .step('charge', async (ctx) => ({ txId: 'tx_123' }))
  .waitFor('manager-approval')  // Pauses until you send a signal
  .step('ship', async (ctx) => ({ shipped: true }));

const engine = new Engine({ embedded: true });
engine.register(flow);
await engine.start('order', { orderId: 'ORD-1' });
```

See the [Workflow Engine guide](/guide/workflow/).

## Next steps

- [Queue API](/guide/queue/), all job options and queue operations
- [Worker API](/guide/worker/), concurrency, events, error handling
- [Server Mode](/guide/server/), run bunqueue as a standalone server
- [Client SDKs](/guide/sdks/), use the queue from Node.js, Deno, Python
- [Code Examples & Recipes](/examples/), complete examples
