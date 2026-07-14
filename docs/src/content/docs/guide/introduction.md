---
title: "bunqueue, High-Performance Job Queue for Bun with SQLite & MCP"
description: "Discover bunqueue: the fastest Bun job queue with SQLite persistence, zero Redis, cron scheduling, and native MCP server for AI agents."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/introduction.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · introduction</span>
  <h1 class="bq-hero-h1 bq-bench-h1">The queue that is <em>a file.</em></h1>
  <p class="bq-hero-sub">bunqueue is a job queue for the Bun runtime. It stores jobs in SQLite, so there is no Redis and no extra server to run. Add a job now, a worker processes it in the background, and nothing is lost on restart.</p>
</div>

A job queue lets your app hand off slow work (sending emails, processing images, calling APIs) to run in the background instead of blocking a request. bunqueue gives you that with one `bun add`, no external infrastructure.

## See it in 10 lines

```typescript
import { Queue, Worker } from 'bunqueue/client';

const queue = new Queue('emails', { embedded: true });

const worker = new Worker('emails', async (job) => {
  console.log('Sending to', job.data.to);
  return { sent: true };
}, { embedded: true });

await queue.add('welcome', { to: 'user@example.com' });
```

Run it with `bun run app.ts`. That is a complete, working queue. The [Quick Start](/guide/quickstart/) walks through it step by step.

## Why bunqueue?

- **Zero infrastructure.** Jobs persist in SQLite, a single file on disk. No Redis, no broker. Only 2 runtime dependencies, a 5.5 MB install.
- **Native Bun.** Built on `bun:sqlite` for performance, around 100k jobs/sec in the default mode.
- **Familiar API.** If you know BullMQ, you already know most of bunqueue.
- **Production features.** Retries with backoff (automatic waiting between retry attempts), a dead letter queue (a holding area for jobs that keep failing), cron scheduling, rate limiting, stall detection, and S3 backups.
- **AI-agent ready.** A built-in MCP server with 73 tools lets agents like Claude add jobs, manage crons, and monitor queues via natural language.

## Two ways to run it

| | Embedded | Server |
|---|----------|--------|
| **What it is** | A library inside your process | A standalone `bunqueue` process |
| **Best for** | Single-process apps, scripts, serverless | Multiple services sharing one queue |
| **Setup** | Pass `embedded: true` | Run `bunqueue start`, then connect |
| **Persistence** | `dataPath` option | `--data-path` flag |

**Embedded mode** means the queue lives inside your app, like using SQLite instead of Postgres:

```typescript
import { Queue, Worker } from 'bunqueue/client';

// Both must have embedded: true
const queue = new Queue('tasks', { embedded: true });
const worker = new Worker('tasks', async (job) => { /* ... */ }, { embedded: true });
```

**Server mode** runs bunqueue as its own process, and any number of apps connect to it over TCP:

```bash
bunqueue start --data-path ./data/queue.db
```

```typescript
// No embedded option = connects to localhost:6789
const queue = new Queue('tasks');
const worker = new Worker('tasks', async (job) => { /* ... */ });
```

Server mode also unlocks clients in other runtimes: Node.js, Deno, Python, and Cloudflare Workers via the [client SDKs](/guide/sdks/).

## Compared to BullMQ

| Feature | bunqueue | BullMQ |
|---------|----------|--------|
| Runtime | Bun | Node.js |
| Storage | SQLite | Redis |
| External deps | None | Redis server |
| Priorities, delays, retries, cron | Yes | Yes |
| Rate limiting, stall detection, flows | Yes | Yes |
| Advanced DLQ (auto-retry, filters) | Yes | Basic |
| S3 backups | Yes | No |
| MCP server for AI agents | Yes (73 tools) | No |
| Built-in workflow engine | Yes | No |

Migrating? The API is intentionally close to BullMQ, see the [migration guide](/guide/migration/).

## Beyond jobs: workflows

For multi-step processes (validate an order, charge, notify, ship) bunqueue ships a workflow engine with retries, parallel steps, branching, rollback on failure, and human-in-the-loop signals:

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const flow = new Workflow('order')
  .step('validate', async (ctx) => ({ ok: true }))
  .step('charge', async (ctx) => ({ txId: 'tx_123' }), { retry: 3 })
  .waitFor('approval')
  .step('ship', async (ctx) => ({ shipped: true }));
```

No Temporal, no extra service. See the [Workflow Engine guide](/guide/workflow/).

## Next steps

- [Installation](/guide/installation/), get bunqueue installed
- [Quick Start](/guide/quickstart/), build your first queue in a minute
- [Server Mode](/guide/server/), run bunqueue as a standalone service
- [MCP Server](/guide/mcp/), connect AI agents to your queues
