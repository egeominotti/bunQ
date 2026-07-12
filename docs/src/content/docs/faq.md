---
title: "bunqueue FAQ: Bun Job Queue Questions Answered"
description: "Common questions about bunqueue answered: performance benchmarks, scaling, SQLite vs Redis, BullMQ migration, and production deployment tips."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/getting-started.png
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

bunqueue has only **2 runtime dependencies**: `croner` (cron parsing) and `msgpackr` (binary serialization). There's no Redis, no MongoDB, no external infrastructure. Running `bun add bunqueue` installs **7 packages totaling 5.5 MB** and completes **about 3.5× faster** on a cold install.

As of v2.8.1, the MCP SDK (`@modelcontextprotocol/sdk`) is an **optional peer dependency** and Zod is no longer a direct dependency, both are only needed if you use the MCP server. Queue/Worker/Workflow users install nothing extra.

### Do I need to install anything extra to use the MCP server?

Yes, one package. The `bunqueue-mcp` bin and the `bunqueue/mcp` export still ship with bunqueue, but as of v2.8.1 the MCP SDK is an optional peer dependency that isn't downloaded automatically. Install it once:

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
- **Push (TCP)**: up to ~90,000 ops/second with 100 concurrent batched adds
- **Bulk push (TCP)**: 85,700 ops/second, 3.5x faster than BullMQ (24,800)
- **Bulk push (embedded)**: up to 630,000 ops/second
- **Single push**: on par with BullMQ

See the [benchmarks page](/guide/benchmarks/) for methodology and full results.

### How do I optimize throughput?

1. **Increase concurrency**
   ```typescript
   const worker = new Worker('queue', processor, {
     concurrency: 50
   });
   ```

2. **Use batch operations**
   ```typescript
   await queue.addBulk(jobs); // single round-trip for many jobs
   // Workers batch pulls/acks automatically; tune with batchSize
   const worker = new Worker('queue', processor, { batchSize: 100 });
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

Retry delays follow exponential backoff with **jitter** (±50%) to prevent thundering herd. With a 1000ms base delay the retries wait roughly `~2s → ~4s → ~8s → ~16s` (formula: `delay * 2^attempts`, actual values vary due to jitter). Delays are capped at 1 hour by default.

### What is the Dead Letter Queue?

The DLQ stores jobs that:
- Exceeded max retry attempts
- Had unrecoverable errors
- Exceeded max stalls

You can inspect, retry, or purge DLQ jobs.

### Can I process jobs in order?

Yes. FIFO (first in, first out) is the default: jobs with the same priority are processed in the order they were added, so you get in-order processing without any extra options.

If you need newest-first processing instead, use LIFO mode:
```typescript
await queue.add('task', data, { lifo: true });
```

Note: LIFO ordering applies among LIFO jobs. Mixing LIFO and FIFO jobs in the same queue does not put a LIFO job ahead of already-queued FIFO jobs.

Or use priority to control ordering explicitly:
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

There is no default database file: without a data path, jobs are kept in-memory only and are lost on restart.

Set a path to enable persistence (priority: `BUNQUEUE_DATA_PATH` > `BQ_DATA_PATH` > `DATA_PATH` > `SQLITE_PATH`):
```bash
BUNQUEUE_DATA_PATH=./data/production.db bunqueue start
# or: bunqueue start --data-path ./data/production.db
```

In embedded mode you can also pass it programmatically: `new Queue('q', { embedded: true, dataPath: './data/q.db' })`.

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

// Clean old jobs (grace period in ms, max jobs to remove)
queue.clean(3600000, 1000); // 1 hour
```

## Migration

### Can I migrate from BullMQ?

Yes. See the [Migration Guide](/guide/migration/).

Key differences:
- No Redis connection needed
- Backoff is simplified
- Per-worker `limiter: { max, duration }` works as in BullMQ; a queue-level rate limit (`setGlobalRateLimit`) is also available

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
