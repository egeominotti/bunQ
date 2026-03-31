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

To persist jobs across restarts, set `DATA_PATH` before importing bunqueue:

```typescript
// Set DATA_PATH FIRST
import { mkdirSync } from 'fs';
mkdirSync('./data', { recursive: true });
process.env.DATA_PATH = './data/bunqueue.db';

// Then import
import { Queue, Worker } from 'bunqueue/client';

const queue = new Queue('tasks', { embedded: true });
const worker = new Worker('tasks', processor, { embedded: true });
```

:::note
Without `DATA_PATH`, bunqueue runs in-memory (no persistence).
:::

## Next Steps

- [Queue API](/guide/queue/) - Full queue operations
- [Worker API](/guide/worker/) - Worker configuration
- [Stall Detection](/guide/stall-detection/) - Handle unresponsive jobs
- [DLQ](/guide/dlq/) - Dead letter queue management

:::tip[Next Steps]
- [Code Examples & Recipes](/examples/) - More complete examples
- [Production Use Cases](/guide/use-cases/) - Real-world patterns
- [Server Mode](/guide/server/) - Run bunqueue as a standalone server
- [MCP Server](/guide/mcp/) - Connect AI agents (Claude, Cursor, Windsurf) to manage queues via natural language
:::
