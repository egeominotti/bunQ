---
title: "Bunqueue + Elysia Integration: Background Jobs for Bun"
description: Add background job queues to Elysia with bunqueue. Typed routes, DLQ monitoring, worker setup, and plugin pattern examples for Bun apps.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/elysia.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · elysia</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Background jobs for <em>Elysia.</em></h1>
  <p class="bq-hero-sub"><a href="https://elysiajs.com">Elysia</a> is an ergonomic framework for building backend servers. This guide shows the bunqueue integration: typed queues, workers, DLQ monitoring, and graceful shutdown, all embedded in one process.</p>
</div>

:::caution[Embedded Mode Required]
All examples use `embedded: true` for in-process queues. Without it, bunqueue tries to connect to a TCP server.
:::

## Basic Setup

```typescript
import { Elysia } from 'elysia';
import { Queue, Worker } from 'bunqueue/client';

// Create typed queues in embedded mode
interface EmailJob {
  to: string;
  subject: string;
  body: string;
}

const emailQueue = new Queue<EmailJob>('emails', { embedded: true });

const app = new Elysia()
  .post('/emails', async ({ body }) => {
    const job = await emailQueue.add('send', body as EmailJob);
    return { jobId: job.id, status: 'queued' };
  })
  .listen(3000);
```

## Complete Real-World Example

This example demonstrates a production-ready REST API with multiple queues, workers, DLQ monitoring, and graceful shutdown.

```typescript
import { Elysia } from 'elysia';
import { Queue, Worker, shutdownManager } from 'bunqueue/client';

// ============================================
// Job Types
// ============================================

interface EmailJob {
  to: string;
  subject: string;
  body: string;
}

interface ReportJob {
  type: 'daily' | 'weekly' | 'monthly';
  userId: string;
}

interface WebhookJob {
  url: string;
  payload: Record<string, unknown>;
}

// ============================================
// Queues (Embedded Mode with Persistence)
// ============================================

const emailQueue = new Queue<EmailJob>('emails', {
  embedded: true,
  dataPath: './data/app.db',
  defaultJobOptions: {
    attempts: 3,
    backoff: 1000,
  }
});

const reportQueue = new Queue<ReportJob>('reports', {
  embedded: true,
  dataPath: './data/app.db',
  defaultJobOptions: {
    attempts: 2,
    timeout: 60000,
  }
});

const webhookQueue = new Queue<WebhookJob>('webhooks', {
  embedded: true,
  dataPath: './data/app.db',
  defaultJobOptions: {
    attempts: 5,
    backoff: 2000,
  }
});

// Configure DLQ with auto-retry for webhooks
webhookQueue.setDlqConfig({
  autoRetry: true,
  autoRetryInterval: 300000, // 5 minutes
  maxAutoRetries: 3,
});

// ============================================
// Workers
// ============================================

const emailWorker = new Worker<EmailJob>('emails', async (job) => {
  await job.updateProgress(10, 'Validating email...');

  // Validate email format
  if (!job.data.to.includes('@')) {
    throw new Error('Invalid email address');
  }

  await job.updateProgress(50, 'Sending email...');
  await job.log(`Sending to: ${job.data.to}`);

  // Simulate sending
  await Bun.sleep(Math.random() * 500 + 100);

  await job.updateProgress(100, 'Sent!');
  return {
    messageId: `msg-${Date.now()}`,
    sentAt: new Date().toISOString(),
  };
}, { embedded: true, concurrency: 3 });

const reportWorker = new Worker<ReportJob>('reports', async (job) => {
  await job.log(`Generating ${job.data.type} report for ${job.data.userId}`);

  // Progress updates
  for (let i = 0; i <= 100; i += 20) {
    await job.updateProgress(i, `Processing... ${i}%`);
    await Bun.sleep(100);
  }

  return {
    reportUrl: `/reports/${job.data.type}-${job.data.userId}.pdf`,
    generatedAt: new Date().toISOString(),
  };
}, { embedded: true, concurrency: 2 });

const webhookWorker = new Worker<WebhookJob>('webhooks', async (job) => {
  await job.log(`Calling webhook: ${job.data.url}`);

  // Actual HTTP call
  const response = await fetch(job.data.url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(job.data.payload),
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  return {
    status: response.status,
    deliveredAt: new Date().toISOString(),
  };
}, { embedded: true, concurrency: 5 });

// ============================================
// Event Logging
// ============================================

emailWorker.on('completed', (job, result) => {
  console.log(`Email sent: ${job.data.to}`);
});

emailWorker.on('failed', (job, err) => {
  console.log(`Email failed: ${job.data.to} - ${err.message}`);
});

reportWorker.on('completed', (job, result) => {
  console.log(`Report ready: ${result.reportUrl}`);
});

webhookWorker.on('failed', (job, err) => {
  console.log(`Webhook failed: ${job.data.url} - ${err.message}`);
});

// ============================================
// Elysia API
// ============================================

const app = new Elysia()

  // Health check with queue stats
  .get('/health', () => ({
    status: 'ok',
    queues: {
      emails: emailQueue.getJobCounts(),
      reports: reportQueue.getJobCounts(),
      webhooks: webhookQueue.getJobCounts(),
    },
  }))

  // ---- Email Jobs ----

  .post('/emails', async ({ body }) => {
    const { to, subject, body: content } = body as EmailJob;
    const job = await emailQueue.add('send', { to, subject, body: content });
    return { jobId: job.id, status: 'queued' };
  })

  .post('/emails/priority', async ({ body }) => {
    const { to, subject, body: content } = body as EmailJob;
    const job = await emailQueue.add('send', { to, subject, body: content }, {
      priority: 10,
    });
    return { jobId: job.id, status: 'queued', priority: 'high' };
  })

  .post('/emails/scheduled', async ({ body }) => {
    const { to, subject, body: content, delayMs } = body as EmailJob & { delayMs: number };
    const job = await emailQueue.add('send', { to, subject, body: content }, {
      delay: delayMs || 5000,
    });
    return {
      jobId: job.id,
      status: 'scheduled',
      willRunAt: new Date(Date.now() + (delayMs || 5000)).toISOString(),
    };
  })

  // ---- Report Jobs ----

  .post('/reports', async ({ body }) => {
    const { type, userId } = body as ReportJob;
    const job = await reportQueue.add(`generate-${type}`, { type, userId });
    return { jobId: job.id, status: 'queued' };
  })

  // ---- Webhook Jobs ----

  .post('/webhooks', async ({ body }) => {
    const { url, payload } = body as WebhookJob;
    const job = await webhookQueue.add('deliver', { url, payload });
    return { jobId: job.id, status: 'queued' };
  })

  .post('/webhooks/bulk', async ({ body }) => {
    const { webhooks } = body as { webhooks: WebhookJob[] };
    const jobs = await webhookQueue.addBulk(
      webhooks.map(w => ({ name: 'deliver', data: w }))
    );
    return {
      jobIds: jobs.map(j => j.id),
      count: jobs.length,
      status: 'queued',
    };
  })

  // ---- Job Status ----

  .get('/jobs/:queue/:id', async ({ params }) => {
    const { queue, id } = params;

    let q: Queue<unknown>;
    switch (queue) {
      case 'emails': q = emailQueue; break;
      case 'reports': q = reportQueue; break;
      case 'webhooks': q = webhookQueue; break;
      default: return { error: 'Unknown queue' };
    }

    const job = await q.getJob(id);
    if (!job) return { error: 'Job not found' };

    return {
      id: job.id,
      name: job.name,
      data: job.data,
      progress: job.progress,
      attemptsMade: job.attemptsMade,
    };
  })

  // ---- DLQ Monitoring ----

  .get('/dlq/:queue', ({ params }) => {
    const { queue } = params;

    let q: Queue<unknown>;
    switch (queue) {
      case 'emails': q = emailQueue; break;
      case 'reports': q = reportQueue; break;
      case 'webhooks': q = webhookQueue; break;
      default: return { error: 'Unknown queue' };
    }

    return {
      stats: q.getDlqStats(),
      entries: q.getDlq().slice(0, 10).map(e => ({
        jobId: e.job.id,
        reason: e.reason,
        error: e.error,
        attempts: e.attempts.length,
        enteredAt: new Date(e.enteredAt).toISOString(),
      })),
    };
  })

  .post('/dlq/:queue/retry', ({ params }) => {
    const { queue } = params;

    let q: Queue<unknown>;
    switch (queue) {
      case 'emails': q = emailQueue; break;
      case 'reports': q = reportQueue; break;
      case 'webhooks': q = webhookQueue; break;
      default: return { error: 'Unknown queue' };
    }

    const count = q.retryDlq();
    return { retriedCount: count };
  })

  // ---- Queue Control ----

  .post('/queues/:queue/pause', ({ params }) => {
    switch (params.queue) {
      case 'emails': emailQueue.pause(); break;
      case 'reports': reportQueue.pause(); break;
      case 'webhooks': webhookQueue.pause(); break;
      default: return { error: 'Unknown queue' };
    }
    return { status: 'paused', queue: params.queue };
  })

  .post('/queues/:queue/resume', ({ params }) => {
    switch (params.queue) {
      case 'emails': emailQueue.resume(); break;
      case 'reports': reportQueue.resume(); break;
      case 'webhooks': webhookQueue.resume(); break;
      default: return { error: 'Unknown queue' };
    }
    return { status: 'resumed', queue: params.queue };
  });

// ============================================
// Graceful Shutdown
// ============================================

async function shutdown() {
  console.log('Shutting down...');

  await Promise.all([
    emailWorker.close(),
    reportWorker.close(),
    webhookWorker.close(),
  ]);

  shutdownManager();
  console.log('Shutdown complete');
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// ============================================
// Start Server
// ============================================

app.listen(3000, () => {
  console.log('Server running at http://localhost:3000');
});
```

## API Endpoints Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Queue statistics |
| `POST` | `/emails` | Create email job |
| `POST` | `/emails/priority` | High priority email |
| `POST` | `/emails/scheduled` | Delayed email |
| `POST` | `/reports` | Generate report |
| `POST` | `/webhooks` | Send webhook |
| `POST` | `/webhooks/bulk` | Bulk webhooks |
| `GET` | `/jobs/:queue/:id` | Job status |
| `GET` | `/dlq/:queue` | DLQ entries |
| `POST` | `/dlq/:queue/retry` | Retry all DLQ |
| `POST` | `/queues/:queue/pause` | Pause queue |
| `POST` | `/queues/:queue/resume` | Resume queue |

## Integration Tests

Test your Elysia + bunqueue integration:

```typescript
const BASE_URL = 'http://localhost:3000';

async function request(path: string, options?: RequestInit) {
  const res = await fetch(`${BASE_URL}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  return res.json();
}

// Test: Health check
const health = await request('/health');
console.assert(health.status === 'ok');
console.assert(health.queues.emails !== undefined);

// Test: Create email job
const email = await request('/emails', {
  method: 'POST',
  body: JSON.stringify({
    to: 'test@example.com',
    subject: 'Test',
    body: 'Hello',
  }),
});
console.assert(email.jobId !== undefined);
console.assert(email.status === 'queued');

// Test: High priority email
const priority = await request('/emails/priority', {
  method: 'POST',
  body: JSON.stringify({
    to: 'vip@example.com',
    subject: 'VIP',
    body: 'Priority message',
  }),
});
console.assert(priority.priority === 'high');

// Test: Scheduled email
const scheduled = await request('/emails/scheduled', {
  method: 'POST',
  body: JSON.stringify({
    to: 'later@example.com',
    subject: 'Later',
    body: 'Send later',
    delayMs: 5000,
  }),
});
console.assert(scheduled.status === 'scheduled');

// Test: Bulk webhooks
const bulk = await request('/webhooks/bulk', {
  method: 'POST',
  body: JSON.stringify({
    webhooks: [
      { url: 'https://example.com/hook1', payload: { id: 1 } },
      { url: 'https://example.com/hook2', payload: { id: 2 } },
    ],
  }),
});
console.assert(bulk.count === 2);

// Test: Pause/Resume
await request('/queues/emails/pause', { method: 'POST' });
await request('/queues/emails/resume', { method: 'POST' });

// Test: Check DLQ
const dlq = await request('/dlq/emails');
console.assert(dlq.stats !== undefined);

console.log('All tests passed!');
```

Run the full example:

```bash
# Terminal 1 - Start server
bun run server.ts

# Terminal 2 - Run tests
bun test
```

## Features Demonstrated

| Feature | How It's Used |
|---------|---------------|
| **Embedded Mode** | `embedded: true` - no server needed |
| **Persistence** | `dataPath` option for SQLite |
| **Multiple Queues** | emails, reports, webhooks |
| **Concurrency** | Different per worker (3, 2, 5) |
| **Priority Jobs** | `priority: 10` for VIP emails |
| **Delayed Jobs** | `delay: ms` for scheduled sending |
| **Bulk Operations** | `addBulk()` for batch creation |
| **Progress Updates** | `job.updateProgress()` with message |
| **Job Logging** | `job.log()` for audit trail |
| **DLQ Config** | Auto-retry failed webhooks |
| **Queue Control** | Pause/resume without losing jobs |
| **Graceful Shutdown** | Wait for active jobs to complete |

## Plugin Pattern

For larger applications, use a plugin to share queues:

```typescript
import { Elysia } from 'elysia';
import { Queue } from 'bunqueue/client';

export const queuePlugin = new Elysia({ name: 'queue' })
  .decorate('queues', {
    emails: new Queue('emails', { embedded: true }),
    notifications: new Queue('notifications', { embedded: true }),
    analytics: new Queue('analytics', { embedded: true }),
  })
  .derive(({ queues }) => ({
    enqueue: async <T>(
      queue: keyof typeof queues,
      name: string,
      data: T,
      opts?: { priority?: number; delay?: number }
    ) => {
      return queues[queue].add(name, data, opts);
    },
  }));

// Usage
const app = new Elysia()
  .use(queuePlugin)
  .post('/api/notify', async ({ body, enqueue }) => {
    const job = await enqueue('notifications', 'send', body);
    return { jobId: job.id };
  });
```

## Type-Safe Routes with Validation

```typescript
import { Elysia, t } from 'elysia';
import { Queue } from 'bunqueue/client';

interface EmailJob {
  to: string;
  subject: string;
  body: string;
}

const emailQueue = new Queue<EmailJob>('emails', { embedded: true });

const app = new Elysia()
  .post('/emails', async ({ body }) => {
    const job = await emailQueue.add('send', body);
    return { jobId: job.id };
  }, {
    body: t.Object({
      to: t.String({ format: 'email' }),
      subject: t.String({ minLength: 1 }),
      body: t.String(),
    }),
  })
  .get('/jobs/:id', async ({ params }) => {
    const job = await emailQueue.getJob(params.id);
    if (!job) return { error: 'Not found' };
    return job;
  }, {
    params: t.Object({
      id: t.String(),
    }),
  });
```

:::tip[Related Integrations]
- [Hono Framework Integration](/guide/hono/) - Alternative framework integration
- [Framework Integrations Overview](/guide/integrations/) - All supported frameworks
:::
