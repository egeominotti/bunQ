---
title: "Bunqueue + Hono Integration: Background Jobs for Bun"
description: Add background job queues to Hono with bunqueue. Middleware patterns, worker setup, job status endpoints, and graceful shutdown for Bun apps.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/hono.png
---

[Hono](https://hono.dev) is an ultrafast web framework for the Edge. Here's how to integrate bunqueue.

:::caution[Embedded Mode Required]
All examples use `embedded: true` for in-process queues. Without it, bunqueue tries to connect to a TCP server.
:::

## Setup

```typescript
import { Hono } from 'hono';
import { Queue, Worker } from 'bunqueue/client';

const app = new Hono();

// Initialize queues in embedded mode
const emailQueue = new Queue('emails', { embedded: true });
const notificationQueue = new Queue('notifications', { embedded: true });
```

## API Routes

```typescript
// Add job endpoint
app.post('/api/jobs/:queue', async (c) => {
  const queueName = c.req.param('queue');
  const body = await c.req.json();

  const queue = new Queue(queueName, { embedded: true });
  const job = await queue.add(body.name, body.data, body.opts);

  return c.json({
    success: true,
    jobId: job.id
  });
});

// Get job status
app.get('/api/jobs/:queue/:id', async (c) => {
  const { queue: queueName, id } = c.req.param();

  const queue = new Queue(queueName, { embedded: true });
  const job = await queue.getJob(id);

  if (!job) {
    return c.json({ error: 'Job not found' }, 404);
  }

  return c.json({
    id: job.id,
    name: job.name,
    progress: job.progress,
    data: job.data,
    result: job.returnvalue,
    error: job.failedReason,
  });
});

// Queue stats
app.get('/api/queues/:name/stats', async (c) => {
  const queueName = c.req.param('name');
  const queue = new Queue(queueName, { embedded: true });

  const counts = queue.getJobCounts(); // Synchronous
  return c.json(counts);
});
```

## Background Workers

```typescript
// workers.ts - Run separately or in the same process
import { Worker } from 'bunqueue/client';

const emailWorker = new Worker('emails', async (job) => {
  const { to, subject, body } = job.data;

  await job.updateProgress(10, 'Preparing email');

  // Send email logic
  await sendEmail({ to, subject, body });

  await job.updateProgress(100, 'Email sent');
  return { sent: true, timestamp: Date.now() };
}, { embedded: true, concurrency: 3 });

emailWorker.on('completed', (job, result) => {
  console.log(`Email sent: ${job.id}`);
});

emailWorker.on('failed', (job, error) => {
  console.error(`Email failed: ${job.id}`, error.message);
});
```

## Complete Example

```typescript
import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { logger } from 'hono/logger';
import { Queue, Worker, shutdownManager } from 'bunqueue/client';

const app = new Hono();

// Middleware
app.use('*', logger());
app.use('/api/*', cors());

// Queues (embedded mode)
const queues = {
  emails: new Queue('emails', { embedded: true }),
  reports: new Queue('reports', { embedded: true }),
  webhooks: new Queue('webhooks', { embedded: true }),
};

// Enqueue job
app.post('/api/send-email', async (c) => {
  const { to, subject, template, data } = await c.req.json();

  const job = await queues.emails.add('send', {
    to,
    subject,
    template,
    data,
  }, {
    attempts: 3,
    backoff: 5000,
    removeOnComplete: true,
  });

  return c.json({ queued: true, jobId: job.id });
});

// Generate report (long-running task)
app.post('/api/reports/generate', async (c) => {
  const { type, filters, format } = await c.req.json();

  const job = await queues.reports.add('generate', {
    type,
    filters,
    format,
    requestedBy: c.req.header('X-User-ID'),
  }, {
    timeout: 300000, // 5 minutes
    priority: 10,
  });

  return c.json({
    jobId: job.id,
    statusUrl: `/api/jobs/reports/${job.id}`,
  });
});

// Poll job status
app.get('/api/jobs/:queue/:id/poll', async (c) => {
  const { queue: queueName, id } = c.req.param();
  const queue = new Queue(queueName, { embedded: true });
  const job = await queue.getJob(id);

  if (!job) {
    return c.json({ error: 'Not found' }, 404);
  }

  return c.json({
    id: job.id,
    name: job.name,
    progress: job.progress,
    result: job.returnvalue ?? null,
    error: job.failedReason ?? null,
  });
});

// Graceful shutdown
process.on('SIGINT', () => {
  shutdownManager();
  process.exit(0);
});

export default app;
```

## Workers in Separate Process

For production, run workers in a separate process:

```typescript
// worker-process.ts
import { Worker, shutdownManager } from 'bunqueue/client';

const emailWorker = new Worker('emails', async (job) => {
  await job.updateProgress(10, 'Preparing...');
  // ... process job
  await job.updateProgress(100, 'Done');
  return { success: true };
}, { embedded: true, concurrency: 5 });

const reportWorker = new Worker('reports', async (job) => {
  // ... generate report
  return { url: `/reports/${job.id}.pdf` };
}, { embedded: true, concurrency: 2 });

// Graceful shutdown
async function shutdown() {
  console.log('Shutting down workers...');
  await Promise.all([
    emailWorker.close(),
    reportWorker.close(),
  ]);
  shutdownManager();
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

console.log('Workers started');
```

## Middleware Pattern

Create a middleware to inject queues:

```typescript
import { Hono } from 'hono';
import { Queue } from 'bunqueue/client';

// Types
type QueueName = 'emails' | 'reports' | 'notifications';

type Env = {
  Variables: {
    queues: Record<QueueName, Queue<any>>;
  };
};

// Middleware
const queueMiddleware = () => {
  const queues = {
    emails: new Queue('emails', { embedded: true }),
    reports: new Queue('reports', { embedded: true }),
    notifications: new Queue('notifications', { embedded: true }),
  };

  return async (c: any, next: any) => {
    c.set('queues', queues);
    await next();
  };
};

// App with typed context
const app = new Hono<Env>();

app.use('*', queueMiddleware());

app.post('/api/notify', async (c) => {
  const queues = c.get('queues');
  const body = await c.req.json();

  const job = await queues.notifications.add('send', body);
  return c.json({ jobId: job.id });
});
```

:::tip[Related Integrations]
- [Elysia Framework Integration](/guide/elysia/) - Alternative framework integration
- [Framework Integrations Overview](/guide/integrations/) - All supported frameworks
:::
