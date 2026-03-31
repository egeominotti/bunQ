---
title: "bunqueue Code Examples: Job Queue Patterns & Recipes for Bun"
description: "Production-ready bunqueue examples: email queues, image processing, scheduled reports, webhooks, ETL workflows, and common patterns."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/api-reference.png
---


Practical examples for common use cases.

:::note[Persistence Setup]
All examples below use embedded mode. For data persistence, set `DATA_PATH` before running:
```bash
export DATA_PATH=./data/bunq.db
```
Without this, data is stored in-memory only and will be lost on restart.
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

  // Note: getChildrenValues() is not yet implemented
  // For now, handle child results through events or polling
  // const childResults = await job.getChildrenValues();

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

:::tip[Related]
- [Quick Start Tutorial](/guide/quickstart/) - Getting started basics
- [Production Use Cases](/guide/use-cases/) - Real-world patterns
- [Migration Guide from BullMQ](/guide/migration/) - Moving from BullMQ
:::
