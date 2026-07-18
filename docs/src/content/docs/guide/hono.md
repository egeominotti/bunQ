---
title: "Bunqueue + Hono: Background Jobs in a Bun Web App"
description: "Add background jobs to a Hono app with bunqueue: enqueue from routes, process in a worker, expose job status endpoints, and shut down cleanly."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/hono.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · hono</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Background jobs for <em>Hono.</em></h1>
  <p class="bq-hero-sub">Return the HTTP response now, do the slow work later. This guide wires bunqueue into <a href="https://hono.dev">Hono</a>: enqueue from routes, process in a worker, check job status, shut down cleanly.</p>
</div>

This guide shows how to run background jobs, work your server does after the HTTP response is sent, inside a Hono app. Everything runs in one process using bunqueue's **embedded mode**, which stores jobs in a local SQLite file instead of a separate queue server.

:::note[Runtime]
This guide runs Hono on Bun with the Bun `bunqueue` package; embedded mode is Bun-only. Running Hono on Node.js or Deno? Start a bunqueue server and use `bunqueue-client` instead, the route and worker code is otherwise the same (see [SDKs](/guide/sdks/)).
:::

## Minimal working app

Copy, run with `bun run app.ts`, done:

```typescript
import { Hono } from 'hono';
import { Queue, Worker, shutdownManager } from 'bunqueue/client';

// Queue: where jobs wait. Worker: runs your function on each job.
const emails = new Queue('emails', { embedded: true });

new Worker('emails', async (job) => {
  console.log('sending to', job.data.to);
  // await sendEmail(job.data);
  return { sent: true };
}, { embedded: true, concurrency: 3 }); // 3 jobs in parallel

const app = new Hono();

app.post('/api/send-email', async (c) => {
  const body = await c.req.json();
  const job = await emails.add('send', body, {
    attempts: 3,   // retry up to 3 times on failure
    backoff: 5000, // wait 5s (then longer) between retries
  });
  return c.json({ queued: true, jobId: job.id });
});

process.on('SIGINT', () => {
  shutdownManager();
  process.exit(0);
});

export default app;
```

The route responds immediately. The worker sends the email in the background and retries automatically if it throws.

:::caution[Embedded mode required]
Every example uses `embedded: true`. Without it, bunqueue tries to connect to a TCP server. Also create each `Queue` once at module level, not inside a request handler.
:::

## Common tasks

### Let clients check job status

Return the job id from the enqueue route, then expose a status endpoint. `job.progress` is a 0-100 number your worker sets, `returnvalue` is what your worker returned, `failedReason` is the last error message:

```typescript
app.get('/api/jobs/:id', async (c) => {
  const job = await emails.getJob(c.req.param('id'));
  if (!job) return c.json({ error: 'Job not found' }, 404);

  return c.json({
    id: job.id,
    name: job.name,
    progress: job.progress,
    result: job.returnvalue ?? null,
    error: job.failedReason ?? null,
  });
});
```

### Report progress from the worker

```typescript
new Worker('reports', async (job) => {
  await job.updateProgress(10, 'Fetching data');
  const data = await fetchData(job.data);
  await job.updateProgress(80, 'Rendering PDF');
  const url = await renderPdf(data);
  return { url };
}, { embedded: true });
```

### Expose queue stats

`getJobCounts()` is synchronous in embedded mode and returns counts per state (`waiting`, `active`, `completed`, `failed`, `delayed`):

```typescript
app.get('/api/queues/emails/stats', (c) => c.json(emails.getJobCounts()));
```

### React to job results

```typescript
const worker = new Worker('emails', processor, { embedded: true });

worker.on('completed', (job, result) => console.log('done', job.id));
worker.on('failed', (job, err) => console.error('failed', job.id, err.message));
```

### Share queues through typed middleware

For larger apps, put queues on Hono's context so every route gets them typed:

```typescript
import { Hono } from 'hono';
import type { MiddlewareHandler } from 'hono';
import { Queue } from 'bunqueue/client';

const queues = {
  emails: new Queue('emails', { embedded: true }),
  reports: new Queue('reports', { embedded: true }),
};

type Env = { Variables: { queues: typeof queues } };

const queueMiddleware: MiddlewareHandler<Env> = async (c, next) => {
  c.set('queues', queues);
  await next();
};

const app = new Hono<Env>();
app.use('*', queueMiddleware);

app.post('/api/reports', async (c) => {
  const job = await c.get('queues').reports.add('generate', await c.req.json());
  return c.json({ jobId: job.id });
});
```

### Run workers in a separate process

In production you often want the web server and the workers to scale and restart independently. Both processes point at the same SQLite file via `dataPath`:

```typescript
// worker-process.ts (run with: bun run worker-process.ts)
import { Worker, shutdownManager } from 'bunqueue/client';

const worker = new Worker('emails', async (job) => {
  // ... process job
  return { success: true };
}, { embedded: true, concurrency: 5 });

process.on('SIGTERM', async () => {
  await worker.close(); // waits for active jobs to finish
  shutdownManager();
  process.exit(0);
});
```

:::note
Give both processes the same `dataPath` (or the same `BUNQUEUE_DATA_PATH` env var) so they share one database. For many producers and workers across machines, use [server mode](/guide/server/) instead.
:::

### Shut down cleanly

Close workers first (each `close()` waits for its active jobs), then release the embedded manager:

```typescript
import { shutdownManager } from 'bunqueue/client';

async function shutdown() {
  await Promise.all(workers.map((w) => w.close()));
  shutdownManager();
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
```

## Gotchas

- **One `Queue` instance per queue name, created at startup.** Creating queues inside handlers works but wastes memory and setup time on every request.
- **Long jobs need a `timeout`.** The default processing timeout comes from the job options; set `timeout: 300_000` for a 5 minute report job so it is not killed early.
- **CPU-heavy processors block the event loop**, the single thread Bun uses for all I/O. See [CPU-Intensive Workers](/guide/cpu-intensive-workers/) for yield patterns.

:::tip[Related]
- [Elysia Integration](/guide/elysia/) - Same pattern with schema validation
- [Integrations Overview](/guide/integrations/) - All integrations
- [Worker API](/guide/worker/) - Every worker option explained
:::
