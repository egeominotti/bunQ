<p align="center">
  <a href="https://bunqueue.dev/">
    <img src=".github/banner.svg" alt="bunqueue" width="400" />
  </a>
</p>

<p align="center">
  <a href="https://www.npmjs.com/package/bunqueue"><img src="https://img.shields.io/npm/v/bunqueue?style=flat-square" alt="npm version"></a>
  <a href="https://www.npmjs.com/package/bunqueue"><img src="https://img.shields.io/npm/dm/bunqueue?style=flat-square" alt="npm downloads"></a>
  <a href="https://github.com/egeominotti/bunqueue/actions"><img src="https://img.shields.io/github/actions/workflow/status/egeominotti/bunqueue/ci.yml?style=flat-square&label=CI" alt="CI"></a>
  <a href="https://github.com/egeominotti/bunqueue/stargazers"><img src="https://img.shields.io/github/stars/egeominotti/bunqueue?style=flat-square" alt="GitHub Stars"></a>
  <a href="https://github.com/egeominotti/bunqueue/blob/main/LICENSE"><img src="https://img.shields.io/github/license/egeominotti/bunqueue?style=flat-square" alt="License"></a>
</p>

<p align="center">
  High-performance job queue for Bun. Built for AI agents and automation.<br/>
  Zero external dependencies. MCP-native. TypeScript-first.
</p>

<p align="center">
  <a href="https://bunqueue.dev/">Documentation</a> &middot;
  <a href="https://bunqueue.dev/guide/benchmarks/">Benchmarks</a> &middot;
  <a href="https://www.npmjs.com/package/bunqueue">npm</a>
</p>

---

<p align="center">
  <strong>bunqueue Dashboard</strong><br/>
  <sub>A visual interface for managing queues, jobs, workers and monitoring in real time. Currently in beta.</sub>
</p>

https://github.com/user-attachments/assets/e8a8d38e-b4a6-4dc8-8360-876c0f24d116

<p align="center">
  <sub>Want early access? Reach out at <b>egeominotti@gmail.com</b></sub>
</p>

---

## Why bunqueue?

| Library      | Requires    |
| ------------ | ----------- |
| BullMQ       | Redis       |
| Agenda       | MongoDB     |
| pg-boss      | PostgreSQL  |
| **bunqueue** | **Nothing** |

- **BullMQ-compatible API** — Same `Queue`, `Worker`, `QueueEvents`
- **Zero dependencies** — No Redis, no MongoDB
- **SQLite persistence** — Survives restarts, WAL mode for concurrent access
- **Up to 286K ops/sec** — [Verified benchmarks](https://bunqueue.dev/guide/benchmarks/)
- **MCP server included** — AI agents get full control: scheduling, monitoring, DLQ, cron, rate limits

## When to use bunqueue

**Great for:**

- **AI agents that need a scheduler** — cron jobs, delayed tasks, retries, all via MCP
- **Agentic workflows** — agents push jobs, workers process, agents monitor results
- Single-server deployments
- Prototypes and MVPs
- Moderate to high workloads (up to 286K ops/sec)
- Teams that want to avoid Redis operational overhead
- Embedded use cases (CLI tools, edge functions, serverless)

**Not ideal for:**

- Multi-region distributed systems requiring HA
- Workloads that need automatic failover today
- Systems already running Redis with existing infrastructure

## Why not just use BullMQ?

If you're already running Redis, BullMQ is great — battle-tested and feature-rich.

bunqueue is for when you **don't want to run Redis**. SQLite with WAL mode handles surprisingly high throughput for single-node deployments (tested up to 286K ops/sec). You get persistence, priorities, delays, retries, cron jobs, and DLQ — without the operational overhead of another service.

## Install

```bash
bun add bunqueue
```

> Requires [Bun](https://bun.sh) runtime. Node.js is not supported.

## Two Modes

bunqueue runs in two modes depending on your architecture:

|                  | Embedded                              | Server (TCP)                                 |
| ---------------- | ------------------------------------- | -------------------------------------------- |
| **How it works** | Queue runs inside your process        | Standalone server, clients connect via TCP   |
| **Setup**        | `bun add bunqueue`                    | `docker run` or `bunqueue start`             |
| **Performance**  | 286K ops/sec                          | 149K ops/sec                                 |
| **Best for**     | Single-process apps, CLIs, serverless | Multiple workers, separate producer/consumer |
| **Scaling**      | Same process only                     | Multiple clients across machines             |

### Embedded Mode

Everything runs in your process. No server, no network, no setup.

```typescript
import { Queue, Worker } from 'bunqueue/client';

const queue = new Queue('emails', { embedded: true });

const worker = new Worker(
  'emails',
  async (job) => {
    console.log('Processing:', job.data);
    return { sent: true };
  },
  { embedded: true }
);

await queue.add('welcome', { to: 'user@example.com' });
```

### Server Mode (TCP)

Run bunqueue as a standalone server. Multiple workers and producers connect via TCP.

```bash
# Start with persistent data
docker run -d -p 6789:6789 -p 6790:6790 \
  -v bunqueue-data:/app/data \
  ghcr.io/egeominotti/bunqueue:latest
```

Connect from your app:

```typescript
import { Queue, Worker } from 'bunqueue/client';

const queue = new Queue('tasks', { connection: { host: 'localhost', port: 6789 } });

const worker = new Worker(
  'tasks',
  async (job) => {
    return { done: true };
  },
  { connection: { host: 'localhost', port: 6789 } }
);

await queue.add('process', { data: 'hello' });
```

### Simple Mode

One object. Queue + Worker + Routes + Middleware + Cron. Zero boilerplate.

```typescript
import { Bunqueue } from 'bunqueue/client';

const app = new Bunqueue('notifications', {
  embedded: true,

  // Route jobs by name
  routes: {
    'send-email': async (job) => {
      console.log(`Email to ${job.data.to}`);
      return { sent: true };
    },
    'send-sms': async (job) => {
      console.log(`SMS to ${job.data.to}`);
      return { sent: true };
    },
  },
  concurrency: 10,
});

// Middleware — wraps every job (logging, timing, error recovery)
app.use(async (job, next) => {
  const start = Date.now();
  const result = await next();
  console.log(`${job.name} took ${Date.now() - start}ms`);
  return result;
});

// Cron — scheduled jobs
await app.cron('daily-report', '0 9 * * *', { type: 'summary' });
await app.every('healthcheck', 30000, { type: 'ping' });

// Events
app.on('completed', (job, result) => console.log(result));
app.on('failed', (job, err) => console.error(err));

// Add jobs
await app.add('send-email', { to: 'alice@example.com' });
await app.add('send-sms', { to: '+1234567890' });

// Graceful shutdown
await app.close();
```

Works with both embedded and TCP mode. [Simple Mode docs →](https://bunqueue.dev/guide/simple-mode/)

## Performance

SQLite handles surprisingly high throughput for single-node deployments:

| Mode     | Peak Throughput | Use Case            |
| -------- | --------------- | ------------------- |
| Embedded | 286K ops/sec    | Same process        |
| TCP      | 149K ops/sec    | Distributed workers |

> Run `bun run bench` to verify on your hardware. [Full benchmark methodology →](https://bunqueue.dev/guide/benchmarks/)

## Built for AI Agents (MCP Server)

<p align="center">
  <img src=".github/mcp-flow.svg" alt="HTTP Handler Flow: Cron/Add Job → Queue → Embedded Worker → HTTP API → Job Result" width="700" />
</p>

bunqueue is the **first job queue with native MCP support**. AI agents get a full-featured scheduler, task queue, and monitoring system — no glue code needed.

**HTTP Handlers** solve a fundamental problem: an AI agent can schedule jobs and manage queues, but it cannot run a persistent worker. When the agent registers an HTTP handler, bunqueue spawns an embedded Worker that continuously pulls jobs and calls your HTTP endpoint. Responses are saved as results. Failed calls retry automatically via DLQ.

**What AI agents can do with bunqueue:**

- **Schedule tasks** — cron jobs, delayed execution, recurring workflows
- **Manage job pipelines** — push jobs, monitor progress, retry failures
- **Full pull/ack/fail cycle** — agents can consume and process jobs directly
- **Monitor everything** — stats, memory, Prometheus metrics, logs, DLQ
- **Control flow** — pause/resume queues, set rate limits, manage concurrency
- **73 MCP tools + 5 resources + 3 prompts** — complete control over every feature
- **HTTP handlers** — register a URL, bunqueue auto-processes jobs via HTTP calls

```bash
# One command to connect Claude Code
claude mcp add bunqueue -- bunx bunqueue-mcp
```

```json
// Claude Desktop / Cursor / Windsurf — add to MCP config
{
  "mcpServers": {
    "bunqueue": {
      "command": "bunx",
      "args": ["bunqueue-mcp"]
    }
  }
}
```

**Example agent interactions:**

- *"Schedule a cleanup job every day at 3 AM"*
- *"Add 500 email jobs to the queue with priority 10"*
- *"Show me all failed jobs and retry them"*
- *"Set rate limit to 50/sec on the api-calls queue"*
- *"What's the memory usage and queue throughput?"*

Supports **embedded** (local SQLite) and **TCP** (remote server) modes. [Full MCP documentation →](https://bunqueue.dev/guide/mcp/)

## Monitoring

```bash
# Start with Prometheus + Grafana
docker compose --profile monitoring up -d
```

- **Grafana**: http://localhost:3000 (admin/bunqueue)
- **Prometheus**: http://localhost:9090

## Documentation

**[Read the full documentation →](https://bunqueue.dev/)**

- [Quick Start](https://bunqueue.dev/guide/quickstart/)
- [Simple Mode](https://bunqueue.dev/guide/simple-mode/)
- [Queue API](https://bunqueue.dev/guide/queue/)
- [Worker API](https://bunqueue.dev/guide/worker/)
- [Server Mode](https://bunqueue.dev/guide/server/)
- [MCP Server (AI Agents)](https://bunqueue.dev/guide/mcp/)
- [Benchmarks](https://bunqueue.dev/guide/benchmarks/)
- [CLI Reference](https://bunqueue.dev/guide/cli/)

## License

MIT
