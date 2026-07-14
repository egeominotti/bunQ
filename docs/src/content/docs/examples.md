---
title: "bunqueue Code Examples: Copy-Paste Recipes for Bun"
description: "Short, working bunqueue examples: retries, scheduled jobs, deduplication, server mode, events, graceful shutdown, and workflows."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/examples.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">reference · examples</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Code examples you copy and <em>ship.</em></h1>
  <p class="bq-hero-sub">Short recipes for the tasks you hit first: retries, schedules, dedup, events, shutdown and workflows. Each one links to the guide that covers it in depth.</p>
</div>

This page is a set of small, working snippets. For end-to-end scenarios like email pipelines, webhooks and payments, see [use cases](/guide/use-cases/).

:::note[Persistence]
Examples use embedded mode, meaning the queue runs inside your process with no separate server. Pass `dataPath` so jobs are saved to a SQLite file, otherwise everything is in-memory and lost on restart:

```typescript
const queue = new Queue('tasks', { embedded: true, dataPath: './data/bunq.db' });
```
:::

## Minimal queue and worker

The smallest complete setup: add a job, process it in the background.

```typescript
import { Queue, Worker } from 'bunqueue/client';

const queue = new Queue('tasks', { embedded: true, dataPath: './data/bunq.db' });

const worker = new Worker('tasks', async (job) => {
  console.log('processing', job.data);
  return { done: true };
}, { embedded: true, concurrency: 5 });

await queue.add('hello', { message: 'world' });
```

More in the [quickstart](/guide/quickstart/).

## Retries and the dead letter queue

A thrown error retries the job with backoff, a growing delay between attempts. Jobs that run out of attempts land in the dead letter queue (DLQ), a holding area you can inspect and retry.

```typescript
await queue.add('flaky-call', { url: 'https://api.example.com' }, {
  attempts: 5,   // try up to 5 times
  backoff: 2000, // wait 2s, 4s, 8s... between tries
});

// After all attempts fail:
const failed = queue.getDlq();      // inspect what died and why
queue.retryDlq();                   // send everything back for another run
```

Details and auto-retry config in the [DLQ guide](/guide/dlq/).

## Scheduled and repeating jobs

Attach a `repeat` option, or use `upsertJobScheduler()` for named schedules. Both persist in SQLite and survive restarts.

```typescript
// Cron expression: every day at 6 AM
await queue.add('daily-report', { type: 'sales' }, {
  repeat: { pattern: '0 6 * * *' },
});

// Plain interval: every 30 minutes
await queue.add('health-check', {}, {
  repeat: { every: 1_800_000 },
});

// Named, updatable schedule
await queue.upsertJobScheduler('cleanup', { pattern: '0 3 * * *' }, {
  data: { olderThanDays: 30 },
});
```

Timezones and schedule management in the [cron guide](/guide/cron/).

## Deduplicate jobs with jobId

Adding a job with a `jobId` that already exists returns the existing job instead of creating a duplicate. Useful for "exactly one welcome email per user" and safe re-runs after a restart.

```typescript
const a = await queue.add('notify', { userId: 'u1' }, { jobId: 'welcome-u1' });
const b = await queue.add('notify', { userId: 'u1' }, { jobId: 'welcome-u1' });

console.log(a.id === b.id); // true, same job
```

## Distributed mode (server + TCP)

Run one bunqueue server, connect producers and workers from any number of processes or machines.

```bash
bunqueue start --tcp-port 6789 --data-path ./data/tasks.db
```

```typescript
// producer.ts
import { Queue } from 'bunqueue/client';
const queue = new Queue('tasks', { connection: { host: 'localhost', port: 6789 } });
await queue.addBulk(items.map((i) => ({ name: 'process', data: i })));

// worker.ts (run as many copies as you want)
import { Worker } from 'bunqueue/client';
new Worker('tasks', async (job) => {
  return { processed: job.data.id };
}, { connection: { host: 'localhost', port: 6789 }, concurrency: 50 });
```

Server setup, auth and TLS in the [server guide](/guide/server/).

## Watch job events

`QueueEvents` streams lifecycle events for a queue, and workers emit their own events.

```typescript
import { QueueEvents } from 'bunqueue/client';

const events = new QueueEvents('tasks');

events.on('completed', ({ jobId, returnvalue }) => console.log('done', jobId, returnvalue));
events.on('failed', ({ jobId, failedReason }) => console.error('failed', jobId, failedReason));
events.on('progress', ({ jobId, data }) => console.log('progress', jobId, data));

worker.on('completed', (job, result) => console.log('worker finished', job.id));
worker.on('failed', (job, error) => console.error('worker error', error.message));
```

Dashboards, metrics and Prometheus in the [monitoring guide](/guide/monitoring/).

## Graceful shutdown

On SIGTERM, stop pulling new jobs, let active ones finish, then close.

```typescript
async function shutdown() {
  worker.pause();          // stop accepting new jobs
  await worker.close();    // wait for active jobs (worker.close(true) forces a stop)
  await queue.close();
  process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
```

The full production pattern, including timeouts and the embedded manager, is in the [production guide](/guide/production/).

## Workflow: automatic rollback on failure

The workflow engine runs multi-step processes where each step can declare a `compensate` function, code that undoes the step if a later one fails. This is the saga pattern: charge succeeded but shipping failed, so the charge is refunded automatically.

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const orderFlow = new Workflow('order')
  .step('reserve-stock', async (ctx) => {
    await inventory.reserve((ctx.input as { orderId: string }).orderId);
    return { reserved: true };
  }, {
    compensate: async () => { await inventory.release(); }, // runs if a later step fails
  })
  .step('charge', async (ctx) => {
    const txId = await stripe.charge((ctx.input as { amount: number }).amount);
    return { txId };
  }, {
    compensate: async () => { await stripe.refund(); },
  })
  .step('confirm', async (ctx) => {
    const { txId } = ctx.steps['charge'] as { txId: string };
    await mailer.send('order-confirm', { txId });
    return { done: true };
  });

const engine = new Engine({ embedded: true });
engine.register(orderFlow);
await engine.start('order', { orderId: 'ORD-1', amount: 99.99 });
```

## Workflow: wait for a human decision

`waitFor()` pauses the workflow until someone calls `engine.signal()`, hours or days later.

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const expenseFlow = new Workflow('expense')
  .step('submit', async (ctx) => {
    await slack.notify('#approvals', `New expense: ${JSON.stringify(ctx.input)}`);
    return { submitted: true };
  })
  .waitFor('manager-decision')
  .step('process', async (ctx) => {
    const decision = ctx.signals['manager-decision'] as { approved: boolean };
    return { status: decision.approved ? 'paid' : 'rejected' };
  });

const engine = new Engine({ embedded: true });
engine.register(expenseFlow);
const run = await engine.start('expense', { amount: 500 });

// Later, when the manager clicks approve:
await engine.signal(run.id, 'manager-decision', { approved: true });
```

Branching, parallel steps, loops, sub-workflows and schema validation are all in the [workflow guide](/guide/workflow/).

:::tip[Where next]
- [Use cases](/guide/use-cases/), end-to-end patterns for emails, webhooks, images and payments
- [Queue guide](/guide/queue/), every job option explained
- [Worker guide](/guide/worker/), concurrency, heartbeats and batching
- [Migration from BullMQ](/guide/migration/), the API is intentionally close
:::
