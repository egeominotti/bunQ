---
title: "Quick Start — Build Your First Bun Job Queue in 5 Minutes"
description: "Get started with bunqueue in 5 minutes. Create queues, add jobs, and process them with Workers. Includes SQLite persistence setup."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/getting-started.png
---


This guide will get you up and running with bunqueue in 5 minutes.

## Choose Your Mode

bunqueue supports two deployment modes:

| | Embedded Mode | TCP Server Mode |
|---|---------------|-----------------|
| **Best for** | Single-process apps, serverless | Multi-process, microservices |
| **Setup** | Zero config | Run `bunqueue start` first |
| **Option needed** | `embedded: true` | None (default) |
| **Persistence** | `DATA_PATH` env var | `--data-path` flag |

**This guide covers Embedded Mode** (most common). For TCP Server Mode, see [Server Guide](/guide/server/).

:::danger[Common Mistake]
If `Queue` has `embedded: true` but `Worker` doesn't (or vice versa), the Worker will try to connect to a non-existent TCP server and **timeout with "Command timeout" error**.

**Both must have the same mode!**
```typescript
// ✅ Correct - both embedded
const queue = new Queue('tasks', { embedded: true });
const worker = new Worker('tasks', handler, { embedded: true });

// ✅ Correct - both TCP (server must be running)
const queue = new Queue('tasks');
const worker = new Worker('tasks', handler);

// ❌ Wrong - mixed modes = timeout error
const queue = new Queue('tasks', { embedded: true });
const worker = new Worker('tasks', handler);  // Missing embedded: true!
```
:::

## Create a Queue

```typescript
import { Queue } from 'bunqueue/client';

// Create a typed queue
interface EmailJob {
  to: string;
  subject: string;
  body: string;
}

const emailQueue = new Queue<EmailJob>('emails', { embedded: true });
```

## Add Jobs

```typescript
// Add a single job
const job = await emailQueue.add('send-email', {
  to: 'user@example.com',
  subject: 'Welcome!',
  body: 'Thanks for signing up.'
});

console.log(`Job created: ${job.id}`);

// Add with options
await emailQueue.add('send-email', data, {
  priority: 10,        // Higher = processed first
  delay: 5000,         // Wait 5 seconds before processing
  attempts: 3,         // Retry up to 3 times
  backoff: 1000,       // Wait 1 second between retries
});

// Add multiple jobs (batch optimized)
await emailQueue.addBulk([
  { name: 'send-email', data: { to: 'a@test.com', subject: 'Hi', body: '...' } },
  { name: 'send-email', data: { to: 'b@test.com', subject: 'Hi', body: '...' } },
]);
```

## Create a Worker

```typescript
import { Worker } from 'bunqueue/client';

const worker = new Worker<EmailJob>('emails', async (job) => {
  console.log(`Processing: ${job.name}`);

  // Update progress
  await job.updateProgress(50, 'Sending email...');

  // Do the work
  await sendEmail(job.data);

  // Log messages
  await job.log('Email sent successfully');

  // Return a result
  return { sent: true, timestamp: Date.now() };
}, {
  embedded: true,  // Required for embedded mode
  concurrency: 5,  // Process 5 jobs in parallel
});
```

## Handle Events

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

worker.on('active', (job) => {
  console.log(`Job ${job.id} started`);
});
```

## Full Example

```typescript
import { Queue, Worker, shutdownManager } from 'bunqueue/client';

interface EmailJob {
  to: string;
  subject: string;
}

// Producer - must have embedded: true
const queue = new Queue<EmailJob>('emails', { embedded: true });

// Add some jobs
await queue.add('welcome', { to: 'new@user.com', subject: 'Welcome!' });
await queue.add('newsletter', { to: 'sub@user.com', subject: 'News' });

// Consumer - must have embedded: true
const worker = new Worker<EmailJob>('emails', async (job) => {
  console.log(`Sending ${job.data.subject} to ${job.data.to}`);
  await job.updateProgress(100);
  return { sent: true };
}, { embedded: true, concurrency: 3 });

worker.on('completed', (job) => {
  console.log(`✓ ${job.id}`);
});

// Graceful shutdown
process.on('SIGINT', async () => {
  await worker.close();
  shutdownManager();
  process.exit(0);
});
```

## With Persistence (SQLite)

To persist jobs across restarts, pass `dataPath` in the constructor or set `DATA_PATH` before importing:

```typescript
import { Queue, Worker } from 'bunqueue/client';

// Option 1: Pass dataPath directly (recommended)
const queue = new Queue('tasks', { embedded: true, dataPath: './data/bunqueue.db' });
const worker = new Worker('tasks', processor, { embedded: true, dataPath: './data/bunqueue.db' });

// Option 2: Environment variable
// DATA_PATH=./data/bunqueue.db bun run app.ts
```

:::note
Without `dataPath` or `DATA_PATH`, bunqueue runs in-memory (no persistence). For server mode, you can also use a [configuration file](/guide/configuration/).
:::

## Simple Mode (All-in-One)

Want less boilerplate? `Bunqueue` wraps Queue + Worker in a single object with routes, middleware, cron, and more:

```typescript
import { Bunqueue } from 'bunqueue/client';

const app = new Bunqueue('notifications', {
  embedded: true,
  routes: {
    'send-email': async (job) => {
      await sendEmail(job.data.to);
      return { sent: true };
    },
    'send-sms': async (job) => {
      await sendSMS(job.data.to);
      return { sent: true };
    },
  },
  concurrency: 10,
});

// Middleware (wraps every job)
app.use(async (job, next) => {
  const start = Date.now();
  const result = await next();
  console.log(`${job.name}: ${Date.now() - start}ms`);
  return result;
});

// Cron jobs
await app.cron('daily-report', '0 9 * * *', { type: 'summary' });

// Add jobs
await app.add('send-email', { to: 'alice@example.com' });

// Graceful shutdown
await app.close();
```

Simple Mode also includes circuit breaker, batch processing, TTL, priority aging, deduplication, and debouncing. See [Simple Mode guide](/guide/simple-mode/) for the full reference.

## Connect AI Agents (MCP)

bunqueue includes a native MCP server with 73 tools. AI agents can schedule tasks, manage pipelines, and monitor queues via natural language — no code needed.

```bash
# Claude Code — bunqueue-mcp is a binary bundled with bunqueue, so install it first
bun add bunqueue
claude mcp add bunqueue -- bunx bunqueue-mcp
```

```json
// Claude Desktop / Cursor / Windsurf — --package=bunqueue resolves the bundled binary, no install needed
{
  "mcpServers": {
    "bunqueue": {
      "command": "bunx",
      "args": ["--package=bunqueue", "bunqueue-mcp"]
    }
  }
}
```

Once connected, agents can add jobs, manage crons, retry failures, set rate limits, and monitor everything. See [MCP Server guide](/guide/mcp/) for the full reference.

## Workflow Engine

Need to orchestrate multi-step processes? bunqueue includes a built-in **Workflow Engine** with branching, saga compensation, and human-in-the-loop signals:

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const flow = new Workflow('order-pipeline')
  .step('validate', async (ctx) => {
    const { orderId } = ctx.input as { orderId: string };
    return { orderId };
  })
  .step('charge', async (ctx) => {
    return { txId: 'tx_123' };
  }, {
    compensate: async () => {
      // Auto-rollback if a later step fails
      await refundPayment('tx_123');
    },
  })
  .waitFor('manager-approval')  // Pauses until signal received
  .step('ship', async (ctx) => {
    const approval = ctx.signals['manager-approval'];
    return { shipped: true };
  });

const engine = new Engine({ embedded: true });
engine.register(flow);

const run = await engine.start('order-pipeline', { orderId: 'ORD-1' });

// Later, when the manager approves:
await engine.signal(run.id, 'manager-approval', { approved: true });
```

Built on top of bunqueue's Queue and Worker — no new infrastructure. [Workflow Engine guide](/guide/workflow/) for the full reference.

## Next Steps

- [Workflow Engine](/guide/workflow/) - Multi-step orchestration with branching and saga compensation
- [Simple Mode](/guide/simple-mode/) - All-in-one Queue + Worker with routes, middleware, cron
- [Queue API](/guide/queue/) - Full queue operations
- [Worker API](/guide/worker/) - Worker configuration
- [MCP Server](/guide/mcp/) - Connect AI agents (Claude, Cursor, Windsurf)
- [Server Mode](/guide/server/) - Run bunqueue as a standalone server
- [Code Examples & Recipes](/examples/) - More complete examples
