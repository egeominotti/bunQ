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

:::note[Availability]
`QueueGroup` ships in the Bun package (`bunqueue/client`). From the other SDKs, create the queues individually with the prefixed name (for example `new Queue('billing:invoices')`): the prefix is just part of the queue name on the server, so grouped and non-grouped clients interoperate on the same queues.
:::

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

// Awaitable forms are authoritative in embedded and TCP modes
await billing.pauseAllAsync();
await billing.resumeAllAsync();
const removed = await billing.drainAllAsync();
await billing.obliterateAllAsync();
```

:::note[Synchronous and awaitable forms]
`listQueues()` and the synchronous bulk operations use the in-process embedded
manager. For TCP queues, use `listQueuesAsync`, `pauseAllAsync`,
`resumeAllAsync`, `drainAllAsync`, and `obliterateAllAsync`; they operate on
every queue created through the group and wait for completion.
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
| `listQueuesAsync()` | List tracked group queues in either runtime |
| `pauseAllAsync()` / `resumeAllAsync()` | Await group control in either runtime |
| `drainAllAsync()` | Drain all tracked queues and return the aggregate count |
| `obliterateAllAsync()` | Await removal of all tracked queue data |

:::tip[Related Guides]
- [Queue API](/guide/queue/) - Options accepted by `getQueue`
- [Worker API](/guide/worker/) - Options accepted by `getWorker`
:::
