---
title: "Bunqueue QueueGroup API for Namespace Isolation"
description: Use bunqueue QueueGroup to organize queues with namespace prefixes. Ideal for multi-tenant Bun apps and microservice domain isolation.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/client-sdk.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · queue-group</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Many queues, one <em>namespace.</em></h1>
  <p class="bq-hero-sub">QueueGroup gives related queues namespace isolation behind a shared prefix. Ideal for multi-tenant apps and per-domain microservice queues.</p>
</div>

## Basic Usage

```typescript
import { QueueGroup } from 'bunqueue/client';

// Create a group with namespace
const billing = new QueueGroup('billing');

// Get queues (automatically prefixed, pass embedded: true for each)
const invoices = billing.getQueue('invoices', { embedded: true });   // → "billing:invoices"
const payments = billing.getQueue('payments', { embedded: true });   // → "billing:payments"

// Add jobs
await invoices.add('create', { amount: 100 });
await payments.add('process', { orderId: '123' });
```

## Creating Workers

```typescript
// Create worker for a queue in the group
const invoiceWorker = billing.getWorker('invoices', async (job) => {
  console.log('Processing invoice:', job.data);
  return { processed: true };
}, { embedded: true });

const paymentWorker = billing.getWorker('payments', async (job) => {
  console.log('Processing payment:', job.data);
  return { processed: true };
}, { embedded: true });
```

## Listing Queues

```typescript
// List all queues in the group (without prefix)
const queues = billing.listQueues();
// ['invoices', 'payments']
```

## Bulk Operations

Perform operations on all queues in the group at once:

```typescript
// Pause all queues in the group
billing.pauseAll();

// Resume all queues in the group
billing.resumeAll();

// Drain all queues (remove waiting jobs)
billing.drainAll();

// Obliterate all queues (remove all data)
billing.obliterateAll();
```

:::caution[Embedded mode only]
`listQueues()` and the bulk operations (`pauseAll`, `resumeAll`, `drainAll`,
`obliterateAll`) operate on the **in-process embedded manager**. They do not
send commands to a remote server, so in TCP mode they are no-ops for the
server's queues; call `pause()` / `resume()` / `drain()` / `obliterate()` on
each `Queue` instance instead. `getQueue()` and `getWorker()` work in both
modes (pass `connection` options for TCP).
:::

## Options

Pass options when creating queues or workers:

```typescript
const billing = new QueueGroup('billing');

// Queue with options (embedded: true required for in-process mode)
const invoices = billing.getQueue<InvoiceData>('invoices', {
  embedded: true,
  defaultJobOptions: {
    attempts: 5,
    backoff: 2000,
  }
});

// Worker with options
const worker = billing.getWorker('invoices', processor, {
  embedded: true,
  concurrency: 10,
});
```

## Use Cases

### Multi-Tenant Applications

```typescript
// Create a group per tenant
const tenantA = new QueueGroup('tenant-a');
const tenantB = new QueueGroup('tenant-b');

// Each tenant has isolated queues (pass embedded: true to each queue)
const tasksA = tenantA.getQueue('tasks', { embedded: true });
const tasksB = tenantB.getQueue('tasks', { embedded: true });

// Jobs are isolated
await tasksA.add('process', { tenantId: 'a' });
await tasksB.add('process', { tenantId: 'b' });
```

### Microservice Domains

```typescript
// Group queues by domain
const orders = new QueueGroup('orders');
const notifications = new QueueGroup('notifications');
const analytics = new QueueGroup('analytics');

// Each domain has its own queues (pass embedded: true to each)
const orderQueue = orders.getQueue('process', { embedded: true });
const emailQueue = notifications.getQueue('email', { embedded: true });
const eventQueue = analytics.getQueue('events', { embedded: true });
```

### Environment Separation

```typescript
const env = process.env.NODE_ENV || 'development';
const group = new QueueGroup(`${env}-tasks`);

const queue = group.getQueue('jobs', { embedded: true });
// → "development-tasks:jobs" or "production-tasks:jobs"
```

## Methods Reference

| Method | Description |
|--------|-------------|
| `getQueue(name, opts?)` | Get a queue within the group (embedded or TCP) |
| `getWorker(name, processor, opts?)` | Create a worker for a queue in the group (embedded or TCP) |
| `listQueues()` | List all queue names in the group, without prefix (embedded only) |
| `pauseAll()` | Pause all queues in the group (embedded only) |
| `resumeAll()` | Resume all queues in the group (embedded only) |
| `drainAll()` | Remove waiting jobs from all queues (embedded only) |
| `obliterateAll()` | Remove all data from all queues (embedded only) |

## Complete Example

```typescript
import { QueueGroup, shutdownManager } from 'bunqueue/client';

interface OrderData {
  orderId: string;
  amount: number;
}

interface NotificationData {
  userId: string;
  message: string;
}

// Create groups
const orders = new QueueGroup('orders');
const notifications = new QueueGroup('notifications');

// Create queues
const orderQueue = orders.getQueue<OrderData>('process', { embedded: true });
const emailQueue = notifications.getQueue<NotificationData>('email', { embedded: true });

// Create workers
const orderWorker = orders.getWorker<OrderData>('process', async (job) => {
  console.log(`Processing order: ${job.data.orderId}`);

  // Create notification after order
  await emailQueue.add('order-confirmation', {
    userId: 'user-123',
    message: `Order ${job.data.orderId} confirmed!`,
  });

  return { processed: true };
}, { embedded: true, concurrency: 5 });

const emailWorker = notifications.getWorker<NotificationData>('email', async (job) => {
  console.log(`Sending email to: ${job.data.userId}`);
  return { sent: true };
}, { embedded: true, concurrency: 3 });

// Add an order
await orderQueue.add('new-order', { orderId: 'ORD-001', amount: 99.99 });

// Check queues in each group
console.log('Order queues:', orders.listQueues());
console.log('Notification queues:', notifications.listQueues());

// Graceful shutdown
process.on('SIGINT', async () => {
  await orderWorker.close();
  await emailWorker.close();
  shutdownManager();
  process.exit(0);
});
```
