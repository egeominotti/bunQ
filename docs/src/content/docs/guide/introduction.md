---
title: "bunqueue — High-Performance Job Queue for Bun with SQLite & MCP"
description: "Discover bunqueue: the fastest Bun job queue with SQLite persistence, zero Redis, cron scheduling, and native MCP server for AI agents."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/getting-started.png
---

**bunqueue** is a high-performance job queue written in TypeScript, designed specifically for the [Bun](https://bun.sh) runtime. Built for AI agents and agentic workflows with a native MCP server.

## Why bunqueue?

- **Native Bun** - Built from the ground up for Bun, leveraging `bun:sqlite` for maximum performance
- **Zero Redis** - No external infrastructure. SQLite provides persistence with WAL mode for concurrent access. Only 2 runtime dependencies (croner + msgpackr): `bun add bunqueue` installs 7 packages in 5.4 MB
- **BullMQ-Compatible API** - Familiar patterns if you're migrating from BullMQ
- **Production Ready** - Stall detection, DLQ, rate limiting, webhooks, and S3 backups
- **MCP Server for AI Agents** - 73 MCP tools included. AI agents can schedule tasks, manage pipelines, and monitor queues via natural language

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      bunqueue server                        │
├─────────────────────────────────────────────────────────────┤
│  HTTP API (Bun.serve)  │  TCP Protocol (Bun.listen)        │
├─────────────────────────────────────────────────────────────┤
│                     Core Engine                             │
│  ┌─────────┐ ┌─────────┐ ┌──────────┐ ┌─────────┐         │
│  │ Queues  │ │ Workers │ │ Scheduler│ │   DLQ   │         │
│  └─────────┘ └─────────┘ └──────────┘ └─────────┘         │
├─────────────────────────────────────────────────────────────┤
│   bun:sqlite (WAL mode)    │   S3 Backup (optional)        │
└─────────────────────────────────────────────────────────────┘
```

## Two Modes of Operation

| | Embedded | TCP Server |
|---|----------|------------|
| **Use case** | Single process apps | Multi-process / Microservices |
| **Setup** | Zero config | Run `bunqueue start` |
| **Option** | `embedded: true` | Default (no option) |
| **Persistence** | `DATA_PATH` env var | `--data-path` flag |

### Embedded Mode

Use bunqueue as a library directly in your application:

```typescript
import { Queue, Worker } from 'bunqueue/client';

// ⚠️ BOTH must have embedded: true
const queue = new Queue('tasks', { embedded: true });
const worker = new Worker('tasks', async (job) => {
  // Process job
}, { embedded: true });
```

Best for:
- Single-process applications
- Serverless functions
- Simple use cases

### Server Mode

Run bunqueue as a standalone server:

```bash
# Start the server
bunqueue start --data-path ./data/queue.db
```

Then connect from your application:

```typescript
import { Queue, Worker } from 'bunqueue/client';

// No embedded option = connects to localhost:6789
const queue = new Queue('tasks');
const worker = new Worker('tasks', async (job) => {
  // Process job
});
```

Best for:
- Multi-process workers
- Microservices architecture
- Language-agnostic clients (HTTP API)

## Feature Comparison with BullMQ

| Feature | bunqueue | BullMQ |
|---------|----------|--------|
| Runtime | Bun | Node.js |
| Storage | SQLite | Redis |
| External deps | None | Redis server |
| Priority queues | ✅ | ✅ |
| Delayed jobs | ✅ | ✅ |
| Retries with backoff | ✅ | ✅ |
| Cron/repeatable jobs | ✅ | ✅ |
| Rate limiting | ✅ | ✅ |
| Stall detection | ✅ | ✅ |
| Parent-child flows | ✅ | ✅ |
| Advanced DLQ | ✅ | Basic |
| S3 backups | ✅ | ❌ |
| Sandboxed workers | ✅ | ✅ |
| Durable writes | ✅ | ✅ (Redis AOF) |
| MCP server (AI agents) | ✅ (73 tools) | ❌ |
| Workflow engine | ✅ (saga, branching, parallel, retry, signals, nested, loops, forEach, map, schema validation, subscribe) | ❌ |

## Workflow Engine

bunqueue includes a built-in workflow engine for multi-step orchestration. Define workflows with a fluent TypeScript DSL — saga compensation, conditional branching, parallel steps, step retry with backoff, nested sub-workflows, signal timeouts, loops (doUntil/doWhile), forEach iteration, map transforms, schema validation (Zod-compatible), per-execution subscribe, typed observability events, and cleanup/archival. No Temporal, no Inngest, no cloud service required.

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const flow = new Workflow('order')
  .step('validate', async (ctx) => { /* ... */ })
  .step('charge', async (ctx) => { /* ... */ }, {
    compensate: async () => { /* auto-rollback on failure */ },
    retry: 3,
  })
  .parallel((w) => w
    .step('notify-warehouse', async () => { /* ... */ })
    .step('send-email', async () => { /* ... */ })
  )
  .forEach(
    (ctx) => (ctx.input as any).items,
    'process-item', async (ctx) => { /* ctx.steps.__item */ },
  )
  .map('summary', (ctx) => ({ total: /* aggregate */ 0 }))
  .waitFor('approval', { timeout: 86400000 })
  .subWorkflow('payment', (ctx) => ({ amount: 99 }))
  .step('ship', async (ctx) => { /* ... */ });

const engine = new Engine({ embedded: true });
engine.on('step:retry', (e) => console.warn(e));
engine.register(flow);
await engine.start('order', { orderId: 'ORD-1' });
```

See [Workflow Engine guide](/guide/workflow/) for full documentation.

## Next Steps

- [Installation](/guide/installation/) - Get bunqueue installed
- [Quick Start](/guide/quickstart/) - Create your first queue
- [Workflow Engine](/guide/workflow/) - Multi-step orchestration
- [MCP Server](/guide/mcp/) - Connect AI agents to your queues
