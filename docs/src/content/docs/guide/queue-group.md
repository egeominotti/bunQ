---
title: "QueueGroup: Namespace Related Queues"
description: Group related bunqueue queues under a shared prefix. Useful for multi-tenant apps and per-domain queue organization.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/queue-group.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · queue-group</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Many queues, one <em>namespace.</em></h1>
  <p class="bq-hero-sub">QueueGroup prefixes a set of queues with a shared name, so "invoices" inside the "billing" group becomes "billing:invoices". Handy for multi-tenant apps and keeping domains apart.</p>
</div>

A QueueGroup is a thin organizer: it creates normal `Queue` and `Worker` instances whose names carry the group prefix, and it can pause, resume, or clear all of them at once.

## Quick Start

```typescript
import { QueueGroup } from 'bunqueue/client';

const billing = new QueueGroup('billing');

// Queues are automatically prefixed
const invoices = billing.getQueue('invoices', { embedded: true });   // "billing:invoices"
const payments = billing.getQueue('payments', { embedded: true });   // "billing:payments"

await invoices.add('create', { amount: 100 });
await payments.add('process', { orderId: '123' });

// Workers use the same prefixed names
const invoiceWorker = billing.getWorker('invoices', async (job) => {
  console.log('Processing invoice:', job.data);
  return { processed: true };
}, { embedded: true });
```

`getQueue` and `getWorker` accept the same options as `Queue` and `Worker`, so you can pass `defaultJobOptions`, `concurrency`, or `connection` settings for TCP mode.

## Common Tasks

### Operate on the whole group

```typescript
billing.listQueues();      // ['invoices', 'payments'] (names without prefix)
billing.pauseAll();        // pause every queue in the group
billing.resumeAll();       // resume them
billing.drainAll();        // remove all waiting jobs
billing.obliterateAll();   // remove ALL data from every queue
```

:::caution[Embedded mode only]
`listQueues()` and the bulk operations (`pauseAll`, `resumeAll`, `drainAll`, `obliterateAll`) operate on the **in-process embedded manager**. In TCP mode they are no-ops for the server's queues; call `pause()` / `resume()` / `drain()` / `obliterate()` on each `Queue` instance instead. `getQueue()` and `getWorker()` work in both modes.
:::

### Isolate tenants

```typescript
const tenantA = new QueueGroup('tenant-a');
const tenantB = new QueueGroup('tenant-b');

const tasksA = tenantA.getQueue('tasks', { embedded: true });  // "tenant-a:tasks"
const tasksB = tenantB.getQueue('tasks', { embedded: true });  // "tenant-b:tasks"
```

### Separate environments

```typescript
const env = process.env.NODE_ENV || 'development';
const group = new QueueGroup(`${env}-tasks`);
const queue = group.getQueue('jobs', { embedded: true });
// "development-tasks:jobs" or "production-tasks:jobs"
```

## Methods Reference

| Method | Description |
|--------|-------------|
| `getQueue(name, opts?)` | Get a queue within the group (embedded or TCP) |
| `getWorker(name, processor, opts?)` | Create a worker for a queue in the group (embedded or TCP) |
| `listQueues()` | List queue names in the group, without prefix (embedded only) |
| `pauseAll()` | Pause all queues in the group (embedded only) |
| `resumeAll()` | Resume all queues in the group (embedded only) |
| `drainAll()` | Remove waiting jobs from all queues (embedded only) |
| `obliterateAll()` | Remove all data from all queues (embedded only) |

:::tip[Related Guides]
- [Queue API](/guide/queue/) - Options accepted by `getQueue`
- [Worker API](/guide/worker/) - Options accepted by `getWorker`
:::
