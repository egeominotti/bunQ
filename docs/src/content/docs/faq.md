---
title: "bunqueue FAQ: Bun Job Queue Questions Answered"
description: "Common questions about bunqueue answered: performance benchmarks, scaling, SQLite vs Redis, BullMQ migration, and production deployment tips."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/getting-started.png
  - tag: script
    attrs:
      type: application/ld+json
    content: |
      {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [
          {
            "@type": "Question",
            "name": "What is bunqueue?",
            "acceptedAnswer": {
              "@type": "Answer",
              "text": "bunqueue is a high-performance job queue for Bun that uses SQLite for persistence instead of Redis. It provides a BullMQ-compatible API, making migration easy."
            }
          },
          {
            "@type": "Question",
            "name": "Why SQLite instead of Redis?",
            "acceptedAnswer": {
              "@type": "Answer",
              "text": "SQLite offers simplicity (no external service), performance (Bun's native SQLite is fast), built-in persistence, zero hosting costs, and portability as a single file database."
            }
          },
          {
            "@type": "Question",
            "name": "Is bunqueue production-ready?",
            "acceptedAnswer": {
              "@type": "Answer",
              "text": "Yes. bunqueue includes stall detection, dead letter queues, automatic retries with backoff, S3 backups for disaster recovery, and rate limiting with concurrency control."
            }
          },
          {
            "@type": "Question",
            "name": "How many jobs can bunqueue handle?",
            "acceptedAnswer": {
              "@type": "Answer",
              "text": "On typical hardware (M2 Pro, 16GB RAM): Push 125,000 jobs/second, Pull 100,000 jobs/second, with 0.1-0.5ms p99 latency."
            }
          },
          {
            "@type": "Question",
            "name": "What's the difference between embedded and server mode?",
            "acceptedAnswer": {
              "@type": "Answer",
              "text": "Embedded mode runs the queue in the same process as your app with no network overhead, best for single-process apps. Server mode runs as a separate server where multiple workers connect via TCP, best for distributed systems."
            }
          },
          {
            "@type": "Question",
            "name": "What happens if a worker crashes?",
            "acceptedAnswer": {
              "@type": "Answer",
              "text": "With stall detection enabled, when a worker misses its heartbeat the job is marked as stalled and retried automatically. If max stalls are exceeded, the job is sent to the Dead Letter Queue."
            }
          },
          {
            "@type": "Question",
            "name": "Can I migrate from BullMQ?",
            "acceptedAnswer": {
              "@type": "Answer",
              "text": "Yes. bunqueue provides a BullMQ-compatible API. Key differences: no Redis connection needed, backoff configuration is simplified, and rate limiting is on the queue level instead of the worker."
            }
          },
          {
            "@type": "Question",
            "name": "Why doesn't bunqueue work with Node.js?",
            "acceptedAnswer": {
              "@type": "Answer",
              "text": "bunqueue uses Bun-specific APIs: bun:sqlite for database access, Bun.serve for HTTP server, and Bun.listen for TCP server. These APIs are not available in Node.js."
            }
          },
          {
            "@type": "Question",
            "name": "How many dependencies does bunqueue have?",
            "acceptedAnswer": {
              "@type": "Answer",
              "text": "bunqueue has only 2 runtime dependencies: croner and msgpackr. There is no Redis, MongoDB, or external infrastructure. Running 'bun add bunqueue' installs 7 packages totaling 5.4 MB and completes up to roughly 5x faster on a cold cache. As of v2.8.0 the MCP SDK is an optional peer dependency and Zod is no longer a direct dependency; both are only needed for the MCP server."
            }
          },
          {
            "@type": "Question",
            "name": "Do I need to install anything extra to use the MCP server?",
            "acceptedAnswer": {
              "@type": "Answer",
              "text": "Yes, one package. The bunqueue-mcp bin and the bunqueue/mcp export still ship with bunqueue, but as of v2.8.0 the MCP SDK is an optional peer dependency that is not downloaded automatically. Install it once with 'bun add @modelcontextprotocol/sdk'. If it is missing when you launch the server, the launcher prints an install message and exits. Queue-only users never need this."
            }
          },
          {
            "@type": "Question",
            "name": "What is the Workflow Engine?",
            "acceptedAnswer": {
              "@type": "Answer",
              "text": "The Workflow Engine is a built-in multi-step orchestration system. It supports saga compensation, conditional branching, parallel steps, step retry with exponential backoff, nested sub-workflows, signal timeouts, loops (doUntil/doWhile), forEach iteration, map transforms, schema validation (Zod-compatible), per-execution subscribe, typed observability events, and cleanup/archival — all without external services like Temporal or Inngest."
            }
          },
          {
            "@type": "Question",
            "name": "What's the difference between Flow and Workflow?",
            "acceptedAnswer": {
              "@type": "Answer",
              "text": "FlowProducer creates parent-child job dependencies (fan-out/fan-in). The Workflow Engine orchestrates multi-step processes with saga compensation, branching, parallel steps, retry, loops (doUntil/doWhile/forEach), map transforms, schema validation, nested workflows, and human approval gates. Use Flow for job DAGs, Workflow for business processes."
            }
          },
          {
            "@type": "Question",
            "name": "Does bunqueue support Postgres or MySQL?",
            "acceptedAnswer": {
              "@type": "Answer",
              "text": "No. bunqueue is SQLite-only by design and has no Postgres or MySQL backend. Bun's bun:sql client is async-only while bunqueue's storage layer is built on synchronous bun:sqlite, and swapping the database would not add clustering to what is a single-process engine. To run on serverless or ephemeral filesystems, mount a persistent volume and point BUNQUEUE_DATA_PATH at it, or use store-and-forward to a central durable bunqueue server."
            }
          }
        ]
      }
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">reference · faq</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Asked, <em>answered.</em></h1>
  <p class="bq-hero-sub">Straight answers on performance, scaling, SQLite versus Redis, BullMQ migration and production deployment. If your question is not here, GitHub Discussions is the next stop.</p>
</div>

## General

### What is bunqueue?

bunqueue is a high-performance job queue for Bun that uses SQLite for persistence instead of Redis. It provides a BullMQ-compatible API, making migration easy.

### Why SQLite instead of Redis?

- **Simplicity**: No external service to manage
- **Performance**: Bun's native SQLite is incredibly fast
- **Persistence**: Data survives restarts by default
- **Cost**: No Redis hosting costs
- **Portability**: Single file database, easy to backup

### Is bunqueue production-ready?

Yes. bunqueue includes:
- Stall detection for crashed workers
- Dead letter queues for failed jobs
- Automatic retries with backoff
- S3 backups for disaster recovery
- Rate limiting and concurrency control

### What are the system requirements?

- **Bun**: Version 1.3.9 or higher (enforced via `engines`)
- **OS**: macOS, Linux, Windows (WSL)
- **Memory**: Minimum 512MB recommended
- **Disk**: SSD recommended for best performance

## Installation

### Why doesn't it work with Node.js?

bunqueue uses Bun-specific APIs:
- `bun:sqlite` for database access
- `Bun.serve` for HTTP server
- `Bun.listen` for TCP server

These APIs are not available in Node.js.

### How do I install Bun?

```bash
# macOS/Linux
curl -fsSL https://bun.sh/install | bash

# Windows (PowerShell)
powershell -c "irm bun.sh/install.ps1 | iex"

# Homebrew
brew install oven-sh/bun/bun
```

### How many dependencies does bunqueue have?

bunqueue has only **2 runtime dependencies**: `croner` (cron parsing) and `msgpackr` (binary serialization). There's no Redis, no MongoDB, no external infrastructure. Running `bun add bunqueue` installs **7 packages totaling 5.4 MB** and completes **up to ~5× faster** on a cold install.

As of v2.8.0, the MCP SDK (`@modelcontextprotocol/sdk`) is an **optional peer dependency** and Zod is no longer a direct dependency, both are only needed if you use the MCP server. Queue/Worker/Workflow users install nothing extra.

### Do I need to install anything extra to use the MCP server?

Yes, one package. The `bunqueue-mcp` bin and the `bunqueue/mcp` export still ship with bunqueue, but as of v2.8.0 the MCP SDK is an optional peer dependency that isn't downloaded automatically. Install it once:

```bash
bun add @modelcontextprotocol/sdk
```

If it's missing when you launch the server, the launcher prints a message telling you to install it and exits. Queue-only users never need this.

## Architecture

### What's the difference between embedded and server mode?

**Embedded Mode:**
- Queue runs in the same process as your app
- Best for single-process applications
- No network overhead

**Server Mode:**
- Queue runs as a separate server
- Multiple workers can connect via TCP
- Best for distributed systems

### Can I use both modes together?

No. Each mode uses its own database file. You should choose one mode per deployment.

### How does job persistence work?

Jobs are stored in SQLite with WAL (Write-Ahead Logging) mode:
- Writes are fast and atomic
- Reads don't block writes
- Data survives process crashes
- Automatic checkpointing

### Does bunqueue support Postgres or MySQL?

No. bunqueue is SQLite-only by design (zero external infrastructure). Bun's `bun:sql` client is async-only while bunqueue's storage layer is built on synchronous `bun:sqlite`, and swapping the database would not add clustering to what is a single-process engine.

To run on serverless or ephemeral filesystems, mount a persistent volume and point `BUNQUEUE_DATA_PATH` at it, or use store-and-forward to a central durable server. See [Postgres, MySQL & Storage Backends](/guide/databases/).

## Performance

### How many jobs can bunqueue handle?

On typical hardware (M2 Pro, 16GB RAM):
- **Push**: 125,000 jobs/second
- **Pull**: 100,000 jobs/second
- **Latency**: 0.1-0.5ms p99

### How do I optimize throughput?

1. **Increase concurrency**
   ```typescript
   const worker = new Worker('queue', processor, {
     concurrency: 50
   });
   ```

2. **Use batch operations**
   ```typescript
   await queue.addBulk(jobs);
   await queue.ackBatch(jobIds);
   ```

3. **Enable WAL mode** (default)
   ```bash
   sqlite3 queue.db "PRAGMA journal_mode=WAL;"
   ```

### Why are my jobs slow?

Common causes:
- Low concurrency setting
- Slow job processor function
- Database on HDD instead of SSD
- Too many indexes

## Job Processing

### How does job deduplication work?

bunqueue uses BullMQ-style idempotent job creation with `jobId`:

```typescript
// First call creates the job
const job1 = await queue.add('task', data, { jobId: 'unique-123' });

// Second call with same jobId returns existing job
const job2 = await queue.add('task', data, { jobId: 'unique-123' });

console.log(job1.id === job2.id); // true
```

This is useful for:
- **Service restart recovery**: Restore jobs without duplicates
- **Webhook deduplication**: Safe handling of retried webhooks
- **Idempotent operations**: Multiple calls have the same effect as one

### What happens if a worker crashes?

With stall detection enabled:
1. Worker misses heartbeat
2. Job is marked as stalled
3. Job is retried automatically
4. If max stalls exceeded, sent to DLQ

### How do retries work?

```typescript
await queue.add('task', data, {
  attempts: 5,        // Max attempts
  backoff: 1000       // Base delay in ms (doubles each retry)
});

// Or with advanced (object) backoff
await queue.add('task', data, {
  attempts: 5,
  backoff: {
    type: 'exponential',  // or 'fixed'
    delay: 1000,
  }
});
```

Retry delays follow exponential backoff with **jitter** (±50%) to prevent thundering herd. Example base delays: `~1s → ~2s → ~4s → ~8s → ~16s` (actual values vary due to jitter). Delays are capped at 1 hour by default.

### What is the Dead Letter Queue?

The DLQ stores jobs that:
- Exceeded max retry attempts
- Had unrecoverable errors
- Exceeded max stalls

You can inspect, retry, or purge DLQ jobs.

### Can I process jobs in order?

Yes, use LIFO mode:
```typescript
await queue.add('task', data, { lifo: true });
```

Or use priority:
```typescript
await queue.add('high', data, { priority: 10 });
await queue.add('low', data, { priority: 1 });
```

## Scaling

### Can I run multiple workers?

Yes. In server mode, multiple workers can connect:

```typescript
// Worker 1
const worker1 = new Worker('queue', processor);

// Worker 2 (different process/machine)
const worker2 = new Worker('queue', processor);
```

### Does bunqueue support clustering?

Not built-in. For high availability:
1. Use S3 backups for failover
2. Run read replicas with SQLite replication
3. Use load balancer for multiple servers

### How do I handle high load?

1. **Horizontal scaling**: Add more workers
2. **Rate limiting**: Protect downstream services
3. **Priority queues**: Process important jobs first
4. **Batch processing**: Reduce overhead

## Data & Backup

### Where is data stored?

Default: `./data/bunq.db`

Configure with:
```bash
DATA_PATH=./data/production.db bunqueue start
```

### How do I backup the database?

**Option 1: S3 Automatic Backup**
```bash
S3_BACKUP_ENABLED=1 \
S3_BUCKET=my-bucket \
S3_ACCESS_KEY_ID=xxx \
S3_SECRET_ACCESS_KEY=xxx \
bunqueue start
```

**Option 2: Manual Backup**
```bash
sqlite3 queue.db ".backup backup.db"
```

### How do I restore from backup?

```bash
bunqueue backup list
bunqueue backup restore backups/2024-01-15/queue.db --force
```

## Troubleshooting

### "SQLITE_BUSY: database is locked"

Multiple writers are conflicting. Solutions:
1. Use server mode for multi-process
2. Ensure only one embedded instance
3. Check for stale lock files

### "Job not found"

The job was already:
- Completed and removed (`removeOnComplete: true`)
- Failed and removed (`removeOnFail: true`)
- Manually deleted

### High memory usage

Common causes:
1. Too many jobs in memory
2. DLQ accumulating failed jobs
3. Job data is too large

Solutions:
```typescript
// Remove completed jobs
await queue.add('task', data, { removeOnComplete: true });

// Purge old DLQ entries
queue.purgeDlq();

// Clean old jobs
queue.clean(3600000); // 1 hour
```

## Migration

### Can I migrate from BullMQ?

Yes. See the [Migration Guide](/guide/migration/).

Key differences:
- No Redis connection needed
- Backoff is simplified
- Rate limiting is on queue, not worker

### Can I migrate from other queues?

bunqueue uses a standard job format. Export your jobs as JSON and use:

```typescript
const jobs = loadJobsFromOldQueue();
await queue.addBulk(jobs.map(j => ({
  name: j.type,
  data: j.payload,
  opts: { priority: j.priority }
})));
```

## Workflow Engine

### What is the Workflow Engine?

The Workflow Engine is a built-in multi-step orchestration system for defining sequential processes with:
- **Saga compensation**, automatic rollback on failure
- **Conditional branching**, route execution based on runtime data
- **Parallel steps**, run independent steps concurrently via `.parallel()`
- **Step retry**, automatic retry with exponential backoff and jitter
- **Human-in-the-loop**, pause and wait for external signals, with optional timeout
- **Nested workflows**, compose workflows with `.subWorkflow()`
- **Loops**, `doUntil()` and `doWhile()` for conditional iteration with safety limits
- **forEach**, iterate over dynamic item lists with indexed step results
- **Map**, synchronous data transforms between steps
- **Schema validation**, validate step input/output with Zod, ArkType, or any `.parse()` schema
- **Subscribe**, monitor a specific execution's events in real-time
- **Observability**, typed event emitter with 11 event types
- **Cleanup & archival**, manage execution history with cleanup/archive
- **Step timeouts**, per-step timeout configuration

No Temporal, no Inngest, no cloud service required.

### What's the difference between Flow and Workflow?

They solve different problems:

| | FlowProducer | Workflow Engine |
|---|---|---|
| **Pattern** | Parent-child job DAG | Sequential/parallel step pipeline |
| **Use case** | Fan-out/fan-in, dependencies | Business processes, approvals |
| **Rollback** | No | Saga compensation |
| **Branching** | No | Conditional paths |
| **Parallel** | Via job tree | `.parallel()` with `Promise.allSettled` |
| **Retry** | Job-level | Step-level with exponential backoff |
| **Human input** | No | `waitFor` signals with timeout |
| **Loops** | No | `doUntil()` / `doWhile()` / `forEach()` |
| **Data transform** | No | `.map()` (synchronous) |
| **Schema validation** | No | `inputSchema` / `outputSchema` (Zod, ArkType, etc.) |
| **Composition** | Nested trees | `.subWorkflow()` |
| **Observability** | Queue events | 11 typed workflow events + `subscribe(id)` |

Use `FlowProducer` when you need parallel job trees with dependencies. Use `Workflow` when you need ordered steps with rollback, branching, or human decisions.

### Can I use the Workflow Engine with TCP server mode?

Yes. The Engine constructor accepts the same connection options as Queue:

```typescript
const engine = new Engine({
  connection: { host: 'localhost', port: 6789 }
});
```

### How is workflow state persisted?

Execution state (current step, step results, received signals) is stored in SQLite via the `workflow_executions` table. This means workflows survive process restarts and can be inspected or resumed at any time.

## Contributing

### How can I contribute?

1. Report bugs on [GitHub Issues](https://github.com/egeominotti/bunqueue/issues)
2. Submit PRs for bug fixes
3. Propose features in [Discussions](https://github.com/egeominotti/bunqueue/discussions)
4. Improve documentation

### What's the development setup?

```bash
git clone https://github.com/egeominotti/bunqueue
cd bunqueue
bun install
bun test
bun run dev
```

:::tip[Related]
- [Troubleshooting](/troubleshooting/) - Debug common issues
- [Installation & Setup](/guide/installation/) - Getting started
- [bunqueue vs BullMQ](/guide/comparison/) - Feature comparison
- [Workflow Engine](/guide/workflow/) - Multi-step orchestration guide
:::
