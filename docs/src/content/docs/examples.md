---
title: "bunqueue Code Examples: Job Queue Patterns & Recipes for Bun"
description: "Production-ready bunqueue examples: email queues, image processing, scheduled reports, webhooks, ETL workflows, and common patterns."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/api-reference.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">reference · examples</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Copy, paste, <em>ship.</em></h1>
  <p class="bq-hero-sub">Practical, production-ready examples for common use cases: email queues, image pipelines, scheduled reports, webhooks, ETL stages and workflows. Every snippet runs in embedded mode with zero external services.</p>
</div>

:::note[Persistence Setup]
All examples below use embedded mode. For data persistence, pass `dataPath` in the constructor or set `DATA_PATH`:
```typescript
const queue = new Queue('tasks', { embedded: true, dataPath: './data/bunq.db' });
```
For server mode, use a [configuration file](/guide/configuration/). Without persistence config, data is in-memory only.
:::

## Email Queue

Send emails with retry and rate limiting.

```typescript
import { Queue, Worker } from 'bunqueue/client';

const emailQueue = new Queue('emails', { embedded: true });

// Add email job
await emailQueue.add('send', {
  to: 'user@example.com',
  subject: 'Welcome!',
  template: 'welcome',
  data: { name: 'John' }
}, {
  priority: 10,
  attempts: 3,
  backoff: 5000
});

// Process emails
const worker = new Worker('emails', async (job) => {
  const { to, subject, template, data } = job.data;

  await job.updateProgress(10, 'Loading template');
  const html = await loadTemplate(template, data);

  await job.updateProgress(50, 'Sending email');
  const result = await sendEmail({ to, subject, html });

  await job.updateProgress(100, 'Sent');
  return { messageId: result.id };
}, { embedded: true, concurrency: 10 });

worker.on('completed', (job, result) => {
  console.log(`Email sent to ${job.data.to}: ${result.messageId}`);
});

worker.on('failed', (job, error) => {
  console.error(`Failed to send to ${job.data.to}:`, error.message);
});
```

## Image Processing Pipeline

Process images with parent-child flow.

```typescript
import { Queue, Worker, FlowProducer } from 'bunqueue/client';

// Create queues (embedded mode)
const uploadQueue = new Queue('uploads', { embedded: true });
const resizeQueue = new Queue('resize', { embedded: true });
const thumbnailQueue = new Queue('thumbnails', { embedded: true });

// Flow producer for dependencies (embedded mode)
const flow = new FlowProducer({ embedded: true });

// Add image processing flow
await flow.addTree({
  name: 'process-image',
  queueName: 'uploads',
  data: { imageId: 'img-123', path: '/uploads/photo.jpg' },
  children: [
    {
      name: 'resize-large',
      queueName: 'resize',
      data: { size: 'large', width: 1920, height: 1080 }
    },
    {
      name: 'resize-medium',
      queueName: 'resize',
      data: { size: 'medium', width: 800, height: 600 }
    },
    {
      name: 'thumbnail',
      queueName: 'thumbnails',
      data: { size: 'thumb', width: 150, height: 150 }
    }
  ]
});

// Upload worker
new Worker('uploads', async (job) => {
  const { imageId, path } = job.data;

  // Validate image
  const metadata = await getImageMetadata(path);

  // Children complete first, so the parent can read their results
  const childResults = await job.getChildrenValues();
  // childResults maps child job IDs to their returned output paths

  // Update database with original path
  await db.images.update(imageId, {
    original: path,
    metadata
  });

  return { processed: true };
}, { embedded: true });

// Resize workers
new Worker('resize', async (job) => {
  const { size, width, height, path } = job.data;
  // Note: getParent() is not yet implemented
  // Pass path in job data or use a shared storage key

  const resized = await resizeImage(path, width, height);
  const outputPath = `/processed/${size}/${Date.now()}.jpg`;
  await saveImage(resized, outputPath);

  return outputPath;
}, { embedded: true, concurrency: 4 });

// Thumbnail worker
new Worker('thumbnails', async (job) => {
  const { width, height, path } = job.data;
  // Note: getParent() is not yet implemented
  // Pass path in job data or use a shared storage key

  const thumb = await createThumbnail(path, width, height);
  const outputPath = `/thumbnails/${Date.now()}.jpg`;
  await saveImage(thumb, outputPath);

  return outputPath;
}, { embedded: true });
```

## Scheduled Reports

Generate reports on a schedule.

```typescript
import { Queue, Worker } from 'bunqueue/client';

const reportsQueue = new Queue('reports', { embedded: true });

// Daily report at 6 AM
await reportsQueue.add('daily-sales', {
  type: 'sales',
  period: 'daily'
}, {
  repeat: { pattern: '0 6 * * *' }
});

// Weekly report every Monday at 9 AM
await reportsQueue.add('weekly-summary', {
  type: 'summary',
  period: 'weekly'
}, {
  repeat: { pattern: '0 9 * * 1' }
});

// Every 30 minutes
await reportsQueue.add('system-health', {
  type: 'health'
}, {
  repeat: { every: 1800000 } // 30 minutes in ms
});

// Report worker
new Worker('reports', async (job) => {
  const { type, period } = job.data;

  await job.log(`Generating ${type} report`);

  let data;
  switch (type) {
    case 'sales':
      data = await generateSalesReport(period);
      break;
    case 'summary':
      data = await generateSummaryReport();
      break;
    case 'health':
      data = await checkSystemHealth();
      break;
  }

  // Send via email or save
  await sendReport(type, data);

  return { generated: new Date().toISOString() };
}, { embedded: true });
```

## Webhook Delivery

Reliable webhook delivery with retries.

```typescript
import { Queue, Worker } from 'bunqueue/client';

const webhookQueue = new Queue('webhooks', { embedded: true });

// Configure stall detection
webhookQueue.setStallConfig({
  enabled: true,
  stallInterval: 30000,
  maxStalls: 2
});

// Add webhook job
async function deliverWebhook(url: string, event: string, payload: any) {
  await webhookQueue.add('deliver', {
    url,
    event,
    payload,
    timestamp: Date.now()
  }, {
    attempts: 5,
    backoff: 10000, // 10s base backoff
    removeOnComplete: true
  });
}

// Webhook worker with failure tracking
const webhookWorker = new Worker('webhooks', async (job) => {
  const { url, event, payload, timestamp } = job.data;

  const response = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Webhook-Event': event,
      'X-Webhook-Timestamp': String(timestamp),
      'X-Webhook-Signature': sign(payload)
    },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(30000)
  });

  if (!response.ok) {
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }

  return {
    status: response.status,
    delivered: Date.now()
  };
}, { embedded: true, concurrency: 10 });

webhookWorker.on('failed', async (job, error) => {
  if (job.attemptsMade >= 5) { // Use hardcoded max attempts
    // Max retries reached, notify admin
    await notifyAdmin({
      type: 'webhook_failed',
      url: job.data.url,
      error: error.message
    });
  }
});
```

## Data Pipeline

ETL pipeline with stages.

```typescript
import { Queue, Worker } from 'bunqueue/client';

// Stage queues (embedded mode)
const extractQueue = new Queue('extract', { embedded: true });
const transformQueue = new Queue('transform', { embedded: true });
const loadQueue = new Queue('load', { embedded: true });

// Extract worker - fetch data from source
new Worker('extract', async (job) => {
  const { source, query, batchId } = job.data;

  await job.updateProgress(0, 'Connecting to source');
  const connection = await connectToSource(source);

  await job.updateProgress(25, 'Executing query');
  const records = await connection.query(query);

  await job.updateProgress(50, 'Fetched records');

  // Send to transform stage
  await transformQueue.add('transform', {
    batchId,
    records,
    source
  });

  await job.updateProgress(100, 'Sent to transform');
  return { recordCount: records.length };
}, { embedded: true, concurrency: 2 });

// Transform worker - clean and enrich data
new Worker('transform', async (job) => {
  const { batchId, records, source } = job.data;

  const transformed = [];
  for (let i = 0; i < records.length; i++) {
    const record = records[i];

    // Clean data
    const cleaned = cleanRecord(record);

    // Enrich data
    const enriched = await enrichRecord(cleaned);

    transformed.push(enriched);

    // Update progress
    const progress = Math.round((i / records.length) * 100);
    await job.updateProgress(progress);
  }

  // Send to load stage
  await loadQueue.add('load', {
    batchId,
    records: transformed,
    source
  });

  return { transformedCount: transformed.length };
}, { embedded: true, concurrency: 4 });

// Load worker - write to destination
new Worker('load', async (job) => {
  const { batchId, records, source } = job.data;

  await job.updateProgress(0, 'Connecting to destination');
  const dest = await connectToDestination();

  // Batch insert
  const batchSize = 1000;
  for (let i = 0; i < records.length; i += batchSize) {
    const batch = records.slice(i, i + batchSize);
    await dest.insertMany(batch);

    const progress = Math.round((i / records.length) * 100);
    await job.updateProgress(progress, `Loaded ${i} records`);
  }

  await job.log(`Completed batch ${batchId} from ${source}`);

  return {
    loadedCount: records.length,
    batchId
  };
}, { embedded: true });
```

## Distributed Task Processing (Server Mode)

Multi-worker task distribution.

```bash
# Start bunqueue server (in terminal)
bunqueue start --tcp-port 6789 --http-port 6790 --data-path ./data/tasks.db
```

```typescript
// producer.ts - Add tasks
import { Queue } from 'bunqueue/client';

const taskQueue = new Queue('tasks', {
  connection: { host: 'localhost', port: 6789 }
});

// Add 10,000 tasks
const tasks = Array.from({ length: 10000 }, (_, i) => ({
  name: 'process',
  data: { taskId: i, payload: `data-${i}` }
}));

await taskQueue.addBulk(tasks);
console.log('Added 10,000 tasks');

// worker-1.ts, worker-2.ts, etc. - Process tasks
import { Worker } from 'bunqueue/client';

const worker = new Worker('tasks', async (job) => {
  const { taskId, payload } = job.data;

  // Simulate work
  await Bun.sleep(100);

  return { processed: taskId };
}, {
  connection: { host: 'localhost', port: 6789 },
  concurrency: 50
});

worker.on('completed', () => {
  console.log(`Worker processed job`);
});
```

## Graceful Shutdown

Handle shutdown properly.

```typescript
import { Queue, Worker } from 'bunqueue/client';

const queue = new Queue('tasks');

// Track paused state locally
let isShuttingDown = false;

const worker = new Worker('tasks', async (job) => {
  // Long-running task
  for (let i = 0; i < 100; i++) {
    await Bun.sleep(100);
    await job.updateProgress(i);

    // Check if we should stop
    if (isShuttingDown) {
      throw new Error('Worker shutting down');
    }
  }
  return { done: true };
});

// Handle shutdown signals
async function shutdown(signal: string) {
  console.log(`Received ${signal}, shutting down...`);

  // Mark as shutting down
  isShuttingDown = true;

  // Stop accepting new jobs (pause() returns void, not Promise)
  worker.pause();

  // Wait for current jobs to complete (max 30s)
  const timeout = setTimeout(() => {
    console.log('Timeout waiting for jobs, forcing exit');
    process.exit(1);
  }, 30000);

  // Wait for active jobs
  await worker.close();
  clearTimeout(timeout);

  // Close queue connection
  await queue.close();

  console.log('Shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
```

## Job with Timeout

Set timeouts for long-running jobs.

```typescript
import { Queue, Worker } from 'bunqueue/client';

const queue = new Queue('processing');

// Add job with 60-second timeout
await queue.add('long-task', {
  data: 'large-dataset'
}, {
  timeout: 60000,
  attempts: 2
});

// Worker with timeout handling
new Worker('processing', async (job) => {
  const controller = new AbortController();
  const timeoutMs = 60000; // Use hardcoded timeout or pass in job.data
  const timeoutId = setTimeout(() => {
    controller.abort();
  }, timeoutMs);

  try {
    await processData(job.data, { signal: controller.signal });
    return { success: true };
  } finally {
    clearTimeout(timeoutId);
  }
});
```

## Unique Jobs (Deduplication)

Prevent duplicate jobs using BullMQ-style idempotency.

```typescript
import { Queue } from 'bunqueue/client';

const queue = new Queue('notifications', { embedded: true });

// Add unique job
const job1 = await queue.add('notify', {
  userId: 'user-123',
  type: 'welcome'
}, {
  jobId: 'welcome-user-123'  // Unique identifier
});

// Same jobId returns existing job (BullMQ-style idempotency)
const job2 = await queue.add('notify', {
  userId: 'user-123',
  type: 'welcome'
}, {
  jobId: 'welcome-user-123'
});

console.log(job1.id === job2.id); // true - same job returned

// Useful for service restart recovery
async function restoreJobsOnStartup(savedJobs: any[]) {
  for (const saved of savedJobs) {
    // Existing jobs are returned, not duplicated
    await queue.add(saved.name, saved.data, {
      jobId: saved.customId
    });
  }
}
```

## Queue Groups

Group related queues together.

```typescript
import { Queue, Worker, QueueGroup } from 'bunqueue/client';

// Create group with namespace
const emailGroup = new QueueGroup('email');

// Get queues from group (creates queues with namespace prefix)
const welcomeQueue = emailGroup.getQueue('welcome');        // email:welcome
const notificationQueue = emailGroup.getQueue('notifications'); // email:notifications
const digestQueue = emailGroup.getQueue('digest');          // email:digest

// Pause all email queues at once
emailGroup.pauseAll();

// Resume all
emailGroup.resumeAll();

// Get individual queue stats
const welcomeCounts = welcomeQueue.getJobCounts();
const notificationCounts = notificationQueue.getJobCounts();
console.log('Welcome pending:', welcomeCounts.waiting);
console.log('Notification pending:', notificationCounts.waiting);
```

## Monitoring with Events

Comprehensive event monitoring.

```typescript
import { Queue, Worker, QueueEvents } from 'bunqueue/client';

const queue = new Queue('tasks');
const events = new QueueEvents('tasks');

// Job lifecycle events
events.on('waiting', ({ jobId }) => {
  console.log(`Job ${jobId} added to queue`);
});

events.on('active', ({ jobId }) => {
  console.log(`Job ${jobId} started processing`);
});

events.on('progress', ({ jobId, progress }) => {
  console.log(`Job ${jobId}: ${progress}%`);
});

events.on('completed', ({ jobId, returnvalue }) => {
  console.log(`Job ${jobId} completed:`, returnvalue);
});

events.on('failed', ({ jobId, failedReason }) => {
  console.error(`Job ${jobId} failed:`, failedReason);
});

events.on('stalled', ({ jobId }) => {
  console.warn(`Job ${jobId} stalled`);
});

// Worker with events
const worker = new Worker('tasks', async (job) => {
  await job.updateProgress(50);
  return { done: true };
});

worker.on('error', (error) => {
  console.error('Worker error:', error);
});
```

## Workflow: Order Pipeline with Compensation

Multi-step order processing with automatic rollback on failure.

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const orderFlow = new Workflow('order-pipeline')
  .step('validate', async (ctx) => {
    const { orderId, amount } = ctx.input as { orderId: string; amount: number };
    if (amount <= 0) throw new Error('Invalid amount');
    return { orderId, validated: true };
  })
  .step('reserve-stock', async (ctx) => {
    const { orderId } = ctx.steps['validate'] as { orderId: string };
    await inventory.reserve(orderId);
    return { reserved: true };
  }, {
    compensate: async () => {
      await inventory.release(); // Auto-runs if charge or ship fails
    },
  })
  .step('charge-payment', async (ctx) => {
    const { amount } = ctx.input as { amount: number };
    const txId = await stripe.charge(amount);
    return { txId };
  }, {
    compensate: async () => {
      await stripe.refund(); // Auto-runs if ship fails
    },
  })
  .step('send-confirmation', async (ctx) => {
    const { txId } = ctx.steps['charge-payment'] as { txId: string };
    await mailer.send('order-confirm', { txId });
    return { emailSent: true };
  });

const engine = new Engine({ embedded: true });
engine.register(orderFlow);
await engine.start('order-pipeline', { orderId: 'ORD-1', amount: 99.99 });
```

## Workflow: Approval Gate with Human-in-the-Loop

Pause execution until a human approves.

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const expenseFlow = new Workflow('expense-approval')
  .step('submit', async (ctx) => {
    const { amount, description } = ctx.input as { amount: number; description: string };
    await slack.notify('#approvals', `New expense: $${amount} - ${description}`);
    return { submitted: true };
  })
  .waitFor('manager-decision')
  .step('process', async (ctx) => {
    const decision = ctx.signals['manager-decision'] as { approved: boolean };
    if (!decision.approved) return { status: 'rejected' };
    const { amount } = ctx.input as { amount: number };
    await accounting.reimburse(amount);
    return { status: 'paid' };
  });

const engine = new Engine({ embedded: true });
engine.register(expenseFlow);
const run = await engine.start('expense-approval', { amount: 500, description: 'Conference' });

// Later, when manager decides (could be hours later):
await engine.signal(run.id, 'manager-decision', { approved: true });
```

## Workflow: Branching by Risk Level

Route to different paths based on runtime conditions.

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const kycFlow = new Workflow('kyc')
  .step('score', async (ctx) => {
    const risk = await riskEngine.assess(ctx.input);
    return { level: risk > 80 ? 'high' : risk > 50 ? 'medium' : 'low' };
  })
  .branch((ctx) => (ctx.steps['score'] as { level: string }).level)
  .path('low', (w) => w.step('auto-approve', async () => ({ method: 'auto' })))
  .path('medium', (w) =>
    w.step('request-docs', async () => ({ docsRequested: true }))
      .waitFor('docs-uploaded')
      .step('review', async () => ({ method: 'document-review' }))
  )
  .path('high', (w) =>
    w.step('flag', async () => ({ flagged: true }))
      .waitFor('compliance-decision')
      .step('decide', async (ctx) => {
        const d = ctx.signals['compliance-decision'] as { approved: boolean };
        if (!d.approved) throw new Error('Rejected by compliance');
        return { method: 'compliance' };
      })
  )
  .step('activate', async () => ({ active: true }));
```

## Workflow: Parallel Data Enrichment

Fetch data from multiple sources concurrently, then merge.

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const enrichFlow = new Workflow('user-enrichment')
  .step('lookup-user', async (ctx) => {
    const { userId } = ctx.input as { userId: string };
    return await db.users.find(userId);
  })
  .parallel((w) => w
    .step('fetch-orders', async (ctx) => {
      const user = ctx.steps['lookup-user'] as { id: string };
      return await db.orders.findByUser(user.id);
    })
    .step('fetch-preferences', async (ctx) => {
      const user = ctx.steps['lookup-user'] as { id: string };
      return await db.preferences.get(user.id);
    })
    .step('fetch-analytics', async (ctx) => {
      const user = ctx.steps['lookup-user'] as { id: string };
      return await analytics.getRecent(user.id);
    })
  )
  .step('build-profile', async (ctx) => {
    return {
      user: ctx.steps['lookup-user'],
      orders: ctx.steps['fetch-orders'],
      prefs: ctx.steps['fetch-preferences'],
      activity: ctx.steps['fetch-analytics'],
    };
  });
```

## Workflow: Nested Sub-Workflow

Compose workflows by calling child workflows from a parent.

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

// Reusable payment workflow
const paymentFlow = new Workflow('payment')
  .step('validate', async (ctx) => {
    const { amount } = ctx.input as { amount: number };
    if (amount <= 0) throw new Error('Invalid amount');
    return { valid: true };
  })
  .step('charge', async (ctx) => {
    return { txId: `tx_${Date.now()}` };
  }, {
    compensate: async () => { await payments.refund(); },
  });

// Parent order workflow calls payment as sub-workflow
const orderFlow = new Workflow('order')
  .step('create', async (ctx) => {
    const { amount } = ctx.input as { amount: number };
    return { orderId: `ORD-${Date.now()}`, total: amount };
  })
  .subWorkflow('payment', (ctx) => ({
    amount: (ctx.steps['create'] as { total: number }).total,
  }))
  .step('confirm', async (ctx) => {
    const payment = ctx.steps['sub:payment'] as Record<string, unknown>;
    return { confirmed: true, payment };
  });

const engine = new Engine({ embedded: true });
engine.register(paymentFlow);
engine.register(orderFlow);
await engine.start('order', { amount: 99.99 });
```

## Workflow: Retry with Observability

Resilient API calls with monitoring.

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const apiFlow = new Workflow('api-sync')
  .step('fetch-data', async () => {
    const res = await fetch('https://api.external.com/data');
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  }, { retry: 5, timeout: 10000 })
  .step('process', async (ctx) => {
    const data = ctx.steps['fetch-data'] as Record<string, unknown>;
    await db.sync(data);
    return { synced: true };
  });

const engine = new Engine({ embedded: true });

// Monitor retries and failures
engine.on('step:retry', (e) => {
  const { stepName, attempt, error } = e as StepEvent;
  console.warn(`Retrying ${stepName} (attempt ${attempt}): ${error}`);
});
engine.on('workflow:failed', (e) => {
  alerting.send(`Workflow ${e.workflowName} failed: ${e.executionId}`);
});

engine.register(apiFlow);
await engine.start('api-sync');
```

## Workflow: forEach with Map Aggregation

Process a list of files, transform the results, and produce a summary:

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const batchFlow = new Workflow<{ files: string[] }>('file-processor')
  // Iterate over each file
  .forEach(
    (ctx) => (ctx.input as { files: string[] }).files,
    'process-file',
    async (ctx) => {
      const file = ctx.steps.__item as string;
      const index = ctx.steps.__index as number;
      console.log(`Processing file ${index}: ${file}`);
      const size = await getFileSize(file);
      return { file, size, processed: true };
    },
    { retry: 2, timeout: 30_000 }
  )

  // Aggregate results from all files
  .map('totals', (ctx) => {
    const results: { file: string; size: number }[] = [];
    let i = 0;
    while (ctx.steps[`process-file:${i}`]) {
      results.push(ctx.steps[`process-file:${i}`] as { file: string; size: number });
      i++;
    }
    return {
      fileCount: results.length,
      totalSize: results.reduce((sum, r) => sum + r.size, 0),
    };
  })

  // Send report
  .step('report', async (ctx) => {
    const totals = ctx.steps['totals'] as { fileCount: number; totalSize: number };
    await notify(`Processed ${totals.fileCount} files (${totals.totalSize} bytes)`);
    return { reported: true };
  });

const engine = new Engine({ embedded: true });
engine.register(batchFlow);
await engine.start('file-processor', { files: ['a.csv', 'b.csv', 'c.csv'] });
```

## Workflow: Polling Loop with doUntil

Wait for an external resource to be ready using a poll loop:

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const deployFlow = new Workflow<{ deployId: string }>('deploy-and-wait')
  .step('trigger', async (ctx) => {
    const { deployId } = ctx.input as { deployId: string };
    await cloudProvider.triggerDeploy(deployId);
    return { deployId, triggered: true };
  })
  .doUntil(
    (ctx) => (ctx.steps['poll'] as { ready: boolean })?.ready === true,
    (w) => w.step('poll', async (ctx) => {
      const id = (ctx.steps['trigger'] as { deployId: string }).deployId;
      const status = await cloudProvider.getStatus(id);
      await new Promise((r) => setTimeout(r, 5000)); // wait between polls
      return { ready: status === 'running', currentStatus: status };
    }, { retry: 1, timeout: 30_000 }),
    { maxIterations: 60 } // max 5 minutes
  )
  .step('verify', async (ctx) => {
    return { verified: true };
  });

const engine = new Engine({ embedded: true });
engine.register(deployFlow);
await engine.start('deploy-and-wait', { deployId: 'deploy-123' });
```

## Workflow: Schema Validation with Subscribe

Validate data at each step and monitor execution in real-time:

```typescript
import { z } from 'zod';
import { Workflow, Engine } from 'bunqueue/workflow';

const UserInput = z.object({ email: z.string().email(), name: z.string() });
const AccountResult = z.object({ userId: z.string(), created: z.boolean() });

const onboardFlow = new Workflow('onboard')
  .step('create-account', async (ctx) => {
    const { email, name } = ctx.input as { email: string; name: string };
    const userId = await accounts.create(email, name);
    return { userId, created: true };
  }, {
    inputSchema: UserInput,      // Validates ctx.input before handler
    outputSchema: AccountResult, // Validates return value after handler
  })
  .step('send-welcome', async (ctx) => {
    const { userId } = ctx.steps['create-account'] as { userId: string };
    await emails.sendWelcome(userId);
    return { sent: true };
  });

const engine = new Engine({ embedded: true });
engine.register(onboardFlow);

const run = await engine.start('onboard', { email: 'alice@example.com', name: 'Alice' });

// Subscribe to this specific execution's events
const unsub = engine.subscribe(run.id, (event) => {
  if (event.type === 'step:completed') console.log(`Step done: ${(event as any).stepName}`);
  if (event.type === 'step:failed') console.error(`Step failed: ${(event as any).error}`);
  if (event.type === 'workflow:completed') {
    console.log('Onboarding complete!');
    unsub(); // cleanup
  }
});
```

:::tip[Related]
- [Workflow Engine Guide](/guide/workflow/) - Full API reference and competitor comparison
- [Quick Start Tutorial](/guide/quickstart/) - Getting started basics
- [Production Use Cases](/guide/use-cases/) - Real-world patterns
- [Migration Guide from BullMQ](/guide/migration/) - Moving from BullMQ
:::
