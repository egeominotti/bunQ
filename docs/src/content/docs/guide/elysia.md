---
title: "Elysia: Validated Background Jobs for Bun"
description: "Add background jobs to an Elysia app with bunqueue: validated enqueue routes with t.Object, a shared queue plugin, DLQ monitoring, and clean shutdown."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/elysia.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · elysia</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Background jobs for <em>Elysia.</em></h1>
  <p class="bq-hero-sub"><a href="https://elysiajs.com">Elysia</a> validates request bodies before your handler runs, so jobs enter the queue already type-checked. This guide shows the Elysia-specific pieces: validated routes, the plugin pattern, and failure monitoring.</p>
</div>

This guide wires bunqueue into Elysia. Everything runs in one process using **embedded mode**, which stores jobs in a local SQLite file, no separate queue server. The general patterns (status endpoints, separate worker processes, shutdown) are the same in every framework and live in the [Hono guide](/guide/hono/) and the [Integrations overview](/guide/integrations/); this page keeps to what Elysia does differently.

## Minimal working app

Copy, run with `bun run app.ts`:

```typescript
import { Elysia, t } from 'elysia';
import { Queue, Worker } from 'bunqueue/client';

interface EmailJob { to: string; subject: string; body: string }

// Queue: where jobs wait. Worker: runs your function on each job.
const emails = new Queue<EmailJob>('emails', { embedded: true });

new Worker<EmailJob>('emails', async (job) => {
  console.log('sending to', job.data.to);
  // await sendEmail(job.data);
  return { sent: true };
}, { embedded: true, concurrency: 3 }); // 3 jobs in parallel

new Elysia()
  .post('/emails', async ({ body }) => {
    const job = await emails.add('send', body);
    return { jobId: job.id, status: 'queued' };
  }, {
    // Elysia validates the body BEFORE your handler runs,
    // so `body` is already typed and bad payloads never reach the queue.
    body: t.Object({
      to: t.String({ format: 'email' }),
      subject: t.String({ minLength: 1 }),
      body: t.String(),
    }),
  })
  .listen(3000);
```

The route responds immediately; the worker processes the job in the background and retries on failure (3 attempts by default).

:::caution[Embedded mode required]
Every example uses `embedded: true`. Without it, bunqueue tries to connect to a TCP server. Create each `Queue` once at module level, not inside a handler.
:::

## Common tasks

### Job status endpoint

```typescript
.get('/jobs/:id', async ({ params }) => {
  const job = await emails.getJob(params.id);
  if (!job) return { error: 'Job not found' };
  return {
    id: job.id,
    progress: job.progress,          // 0-100, set by the worker
    result: job.returnvalue ?? null, // what the worker returned
    error: job.failedReason ?? null, // last error message
  };
}, { params: t.Object({ id: t.String() }) })
```

### Share queues with a plugin

Elysia's idiom for shared state is a plugin. `decorate` puts the queues on the context, `derive` adds a small typed helper:

```typescript
import { Elysia } from 'elysia';
import { Queue } from 'bunqueue/client';

export const queuePlugin = new Elysia({ name: 'queue' })
  .decorate('queues', {
    emails: new Queue('emails', { embedded: true }),
    reports: new Queue('reports', { embedded: true }),
  })
  .derive(({ queues }) => ({
    enqueue: <T>(queue: keyof typeof queues, name: string, data: T) =>
      queues[queue].add(name, data),
  }));

// Usage
const app = new Elysia()
  .use(queuePlugin)
  .post('/api/notify', async ({ body, enqueue }) => {
    const job = await enqueue('emails', 'send', body);
    return { jobId: job.id };
  });
```

### Monitor failed jobs (DLQ)

The DLQ (dead letter queue) holds jobs that failed all their retries, with the error preserved. Expose it so you can see and replay failures:

```typescript
.get('/dlq/emails', () => ({
  stats: emails.getDlqStats(),
  entries: emails.getDlq().slice(0, 10).map((e) => ({
    jobId: e.job.id,
    error: e.error,
    enteredAt: new Date(e.enteredAt).toISOString(),
  })),
}))

.post('/dlq/emails/retry', () => ({
  retried: emails.retryDlq(), // re-queues every DLQ entry
}))
```

You can also let bunqueue retry the DLQ on a schedule:

```typescript
emails.setDlqConfig({
  autoRetry: true,
  autoRetryInterval: 300_000, // try again every 5 minutes
  maxAutoRetries: 3,
});
```

See the [Dead Letter Queue guide](/guide/dlq/) for the full options.

### Health check with queue counts

`getJobCounts()` is synchronous in embedded mode:

```typescript
.get('/health', () => ({
  status: 'ok',
  queues: { emails: emails.getJobCounts() },
}))
```

### Shut down cleanly

Same as every framework: close workers (each waits for its active jobs), then release the embedded manager:

```typescript
import { shutdownManager } from 'bunqueue/client';

process.on('SIGTERM', async () => {
  await worker.close();
  shutdownManager();
  process.exit(0);
});
```

## Gotchas

- **Validate at the edge, trust in the worker.** With `t.Object` on the route, your worker can assume `job.data` matches the schema. Without it, validate inside the processor too, a bad payload will fail all retries and land in the DLQ.
- **One `Queue` instance per queue name**, created at startup. New instances per request waste memory.
- **CPU-heavy processors block the event loop**, the single thread that serves all requests. See [CPU-Intensive Workers](/guide/cpu-intensive-workers/).

:::tip[Related]
- [Hono Integration](/guide/hono/) - Same pattern, plus separate worker processes and progress reporting
- [Integrations Overview](/guide/integrations/) - Shared patterns for any framework
- [Queue API](/guide/queue/) - Every queue method explained
:::
