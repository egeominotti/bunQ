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
  High-performance job queue for Bun. Memory or one-file SQLite; PostgreSQL 15–18 multi-broker when you scale.<br/>
  DLQ, cron, SQLite S3 backups, and native MCP. Built for AI agents and automation. No Redis.
</p>

<p align="center">
  <a href="https://bunqueue.dev/">Documentation</a> &middot;
  <a href="https://bunqueue.dev/guide/quickstart/">Quick Start</a> &middot;
  <a href="https://bunqueue.dev/guide/benchmarks/">Benchmarks</a> &middot;
  <a href="https://www.npmjs.com/package/bunqueue">npm</a>
</p>

---

## Quickstart

```bash
bun add bunqueue
```

```typescript
import { Bunqueue } from 'bunqueue/client';

const app = new Bunqueue('emails', {
  embedded: true,
  dataPath: './data/emails.db', // omit to run in-memory (lost on restart)
  processor: async (job) => {
    console.log(`Sending to ${job.data.to}`);
    return { sent: true };
  },
});

await app.add('send', { to: 'alice@example.com' });
```

That's it. Queue + Worker in one object, persisted to a single SQLite file.
No Redis, no config, no setup. `msgpackr` is the only runtime dependency;
cron, SQLite, S3, HTTP and WebSocket use Bun's built-ins.

### Not on Bun? Run the server, connect from anywhere

The queue also runs as a standalone server. Memory is the zero-configuration
default; SQLite is the zero-infrastructure persistent option:

```bash
# in-memory without --data-path; pass it to persist jobs to SQLite
bunx bunqueue start --data-path ./data/bunq.db   # TCP :6789, HTTP :6790

# or, with no runtime at all (the volume persists /app/data):
docker run -d -p 6789:6789 -p 6790:6790 \
  -v bunqueue-data:/app/data \
  ghcr.io/egeominotti/bunqueue:latest
```

For multiple active brokers, the repository includes a topology pinned to
PostgreSQL 18.6:

```bash
POSTGRES_PASSWORD='replace-me' \
  BUNQUEUE_POSTGRES_URL='postgres://bunqueue:replace-me@postgres:5432/bunqueue' \
  docker compose -f docker-compose.postgres.yml up --build -d
```

It starts two brokers against one database/namespace. PostgreSQL is server-only;
embedded mode keeps using memory/SQLite. Supply both Compose values when the
password changes, percent-encoding reserved characters in the URL only. MySQL
is not supported. CI validates PostgreSQL 15, 16, 17, and the pinned/recommended
18.6 release. See the
[storage guide](https://bunqueue.dev/guide/databases/).

Every completed release publishes `ghcr.io/egeominotti/bunqueue` with the exact
version tag alongside `latest`, the commit SHA and a build timestamp, so pin the
version tag for reproducible deployments. Confirm the tag exists before pinning
it, because a release whose pipeline does not complete pushes no image;
`docker buildx imagetools inspect ghcr.io/egeominotti/bunqueue:<tag>` resolves
the digest.

Then produce and process from the language you already use:

```bash
npm install bunqueue-client    # Node.js ≥ 20, Deno ≥ 2, Bun, Cloudflare Workers
```

```typescript
import { Queue, Worker } from 'bunqueue-client';

const queue = new Queue('emails'); // localhost:6789 by default
await queue.add('welcome', { to: 'user@example.com' });

new Worker('emails', async (job) => ({ sent: true }), { concurrency: 10 });
```

Python, PHP, Go, Rust and Elixir clients speak the same protocol — see
[One Queue, Any Language](#one-queue-any-language-sdks).

> Only the server and embedded mode are **Bun-only** (`bun >= 1.4.0`,
> [bun.sh](https://bun.sh)); producers and workers can run anywhere.

[Quick Start guide →](https://bunqueue.dev/guide/quickstart/)

## Why bunqueue?

| Library      | Requires                                   | AI-native |
| ------------ | ------------------------------------------ | --------- |
| BullMQ       | Redis                                      | No        |
| Agenda       | MongoDB                                    | No        |
| pg-boss      | PostgreSQL                                 | No        |
| **bunqueue** | **Nothing (memory/SQLite) · PostgreSQL optional** | **Yes**   |

- **Zero external infrastructure by default** — memory by default; one SQLite
  file when local persistence is configured
- **PostgreSQL 15–18 multi-broker mode** — PostgreSQL 18.6 is recommended;
  database-authoritative claims, fenced
  leases, shared limits, cron, workers, job-state/lifecycle metrics, and failover state
  ([tested three-broker Docker example](https://bunqueue.dev/examples/postgres-multibroker/))
- **BullMQ-compatible API** — same `Queue`, `Worker`, `QueueEvents`; [migrating takes minutes](https://bunqueue.dev/guide/migration/)
- **MCP server included** — 73 tools; AI agents get full queue control out of the box
- **Everything server-side** — retries with backoff, priorities, cron, rate limits, dead letter queue
- **Measured, operation-specific performance** — 729K jobs/sec internal
  in-memory batch push, 186K jobs/sec public on-disk Embedded `addBulk`, and
  159K jobs/sec TCP `PUSHB`; [methodology and distributions](https://bunqueue.dev/guide/benchmarks/)

**Great for:** embedded and single-server deployments, PostgreSQL-backed broker
fleets, AI agents that need a scheduler, edge/serverless spooling, and teams
that don't want to operate Redis.

**Not ideal for:** multi-region consensus or deployments that require MySQL as
the queue store. If you already run Redis and BullMQ works for you, keep it.

[When to choose bunqueue →](https://bunqueue.dev/guide/comparison/)

## Two Modes

|                  | Embedded                                                       | Server (TCP)                                             |
| ---------------- | -------------------------------------------------------------- | -------------------------------------------------------- |
| **How it works** | Queue runs inside your process                                 | Standalone server, clients connect via TCP               |
| **Setup**        | `bun add bunqueue`                                             | `docker run` or `bunqueue start`                         |
| **Performance**  | 186K jobs/sec on-disk `addBulk`; 729K internal in-memory batch | SQLite: 159K TCP `PUSHB`; 17K jobs/sec worker drain      |
| **Best for**     | Single-process apps, CLIs, serverless                          | Multiple workers, separate producer/consumer             |
| **Scaling**      | Same process only                                              | Multiple clients; multiple brokers with PostgreSQL 15–18 |

### Embedded

Everything in your process. Without a data path the queue is in-memory: pass
`dataPath` (or set `BUNQUEUE_DATA_PATH`) to persist jobs.

```typescript
import { Queue, Worker } from 'bunqueue/client';

const queue = new Queue('emails', { embedded: true, dataPath: './data/app.db' });

const worker = new Worker(
  'emails',
  async (job) => {
    return { sent: true };
  },
  { embedded: true }
);

await queue.add('welcome', { to: 'user@example.com' });
```

### Server (TCP)

```bash
docker run -d -p 6789:6789 -p 6790:6790 \
  -v bunqueue-data:/app/data \
  ghcr.io/egeominotti/bunqueue:latest
```

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

[Running the server →](https://bunqueue.dev/guide/server/) ·
[Deployment guide →](https://bunqueue.dev/guide/deployment/)

## One Queue, Any Language (SDKs)

The server does all the heavy lifting. Official client SDKs share the
protocol-conformant core Queue, Worker, and Flow surface, so producers and
workers can live anywhere in your stack — add a job from TypeScript, process it
from Python. Language-specific capabilities are tracked in the
[SDK guide](https://bunqueue.dev/guide/sdks/).

| Where your code runs                                | Install                                                                                |
| --------------------------------------------------- | -------------------------------------------------------------------------------------- |
| **Node.js ≥ 20, Deno ≥ 2, Bun, Cloudflare Workers** | [`npm install bunqueue-client`](https://www.npmjs.com/package/bunqueue-client)         |
| **Python ≥ 3.9**                                    | [`pip install bunqueue-client`](https://pypi.org/project/bunqueue-client/)             |
| **PHP ≥ 8.1**                                       | [`composer require bunqueue/client`](https://packagist.org/packages/bunqueue/client)   |
| **Go ≥ 1.26.5**                                     | `go get github.com/egeominotti/bunqueue/sdk/go`                                        |
| **Rust ≥ 1.85**                                     | [`cargo add bunqueue-client`](https://crates.io/crates/bunqueue-client)                |
| **Elixir ≥ 1.15**                                   | Hex coming soon — today: use [`sdk/elixir`](./sdk/elixir) as a path dependency         |

```typescript
// Node.js / Deno / Cloudflare Workers
import { Queue, Worker } from 'bunqueue-client';

const queue = new Queue('emails', { host: 'localhost', port: 6789 });
await queue.add('welcome', { to: 'user@example.com' });

new Worker('emails', async (job) => ({ sent: true }), { concurrency: 10 });
```

```python
# Python
from bunqueue import Queue, Worker

queue = Queue("emails", host="localhost", port=6789)
queue.add("welcome", {"to": "user@example.com"})

Worker("emails", lambda job: {"sent": True}, concurrency=10).run()
```

Every SDK is certified against the same public
[wire protocol](https://github.com/egeominotti/bunqueue/blob/main/docs/protocol.md) and conformance suite.

### Atomic flows, in every SDK

Every official `FlowProducer` resolves all job IDs and reciprocal dependency
edges locally, then sends one `PUSHF` command. The broker validates the complete
graph and commits it atomically, so a worker cannot observe a leaf from a
partially-created flow.

```typescript
import { FlowProducer } from 'bunqueue-client';

const flows = new FlowProducer({ host: 'localhost', port: 6789 });
const root = await flows.add({
  name: 'publish-release',
  queueName: 'release',
  data: { version: 'candidate-42' },
  children: [
    { name: 'unit-tests', queueName: 'checks', data: { suite: 'unit' } },
    { name: 'sdk-tests', queueName: 'checks', data: { suite: 'sdk' } },
  ],
});

console.log(
  root.job.id,
  root.children?.map(({ job }) => job.id)
);
await flows.close();
```

The repository records the contracts and the test strategy beside each
implementation:

| SDK                                      | Runtime invariants                         | Generated tests | Mutation engine |
| ---------------------------------------- | ------------------------------------------ | --------------- | --------------- |
| [TypeScript](./sdk/typescript/README.md) | [contract](./sdk/typescript/INVARIANTS.md) | fast-check      | none¹           |
| [Python](./sdk/python/README.md)         | [contract](./sdk/python/INVARIANTS.md)     | Hypothesis      | mutmut          |
| [PHP](./sdk/php/README.md)               | [contract](./sdk/php/INVARIANTS.md)        | Eris            | Infection       |
| [Go](./sdk/go/README.md)                 | [contract](./sdk/go/INVARIANTS.md)         | Rapid           | Gremlins        |
| [Rust](./sdk/rust/README.md)             | [contract](./sdk/rust/INVARIANTS.md)       | proptest        | cargo-mutants   |
| [Elixir](./sdk/elixir/README.md)         | [contract](./sdk/elixir/INVARIANTS.md)     | StreamData      | Muex            |

¹ The TypeScript SDK has no mutation engine. StrykerJS was removed because its
dependency graph produced every advisory the weekly audit had to answer for,
none of it reachable from the published client; the planners keep their
fast-check coverage.

Property campaigns run in the ordinary SDK gate with deterministic replay
seeds. Mutation campaigns run separately against the pure planners and
snapshot validators. Contributors can reproduce the complete isolated SDK
gate with `bun run test:sandbox:sdk`; language-specific commands live in each
SDK README and `AGENTS.md`.

[SDK guide (all six languages) →](https://bunqueue.dev/guide/sdks/)

## Simple Mode

`Bunqueue` bundles Queue + Worker + routes + middleware + cron in one object:

```typescript
import { Bunqueue } from 'bunqueue/client';

const app = new Bunqueue('notifications', {
  embedded: true,
  routes: {
    'send-email': async (job) => ({ sent: true }),
    'send-sms': async (job) => ({ sent: true }),
  },
  concurrency: 10,
  retry: { maxAttempts: 5, strategy: 'jitter' },
  circuitBreaker: { threshold: 5, resetTimeout: 30000 },
});

// Onion middleware around every job
app.use(async (job, next) => {
  const start = Date.now();
  const result = await next();
  console.log(`${job.name}: ${Date.now() - start}ms`);
  return result;
});

await app.cron('daily-report', '0 9 * * *', { type: 'summary' });
await app.add('send-email', { to: 'alice@example.com' });

app.on('completed', (job, result) => console.log(result));
await app.close();
```

Also included: batch processing, event triggers (job A completes → create job
B), job TTL, priority aging, deduplication, per-group rate limiting, DLQ with
auto-retry, graceful cancellation via `AbortController`.

[Simple Mode reference →](https://bunqueue.dev/guide/simple-mode/)

## Workflow Engine

Multi-step orchestration with saga compensation, branching, parallel steps and
human-in-the-loop signals — built on bunqueue, no new infrastructure:

```typescript
import { Workflow, Engine } from 'bunqueue/workflow';

const orderFlow = new Workflow('order-pipeline')
  .step(
    'reserve-stock',
    async () => {
      await inventory.reserve();
      return { reserved: true };
    },
    {
      compensate: async () => await inventory.release(), // auto-rollback on failure
    }
  )
  .step(
    'charge',
    async () => {
      return { txId: await payments.charge() };
    },
    {
      compensate: async () => await payments.refund(),
    }
  )
  .waitFor('manager-approval', { timeout: 86_400_000 }) // human-in-the-loop
  .step('confirm', async (ctx) => {
    return { txId: (ctx.steps['charge'] as { txId: string }).txId };
  });

const engine = new Engine({ embedded: true });
engine.register(orderFlow);
const run = await engine.start('order-pipeline', { orderId: 'ORD-1' });
await engine.signal(run.id, 'manager-approval', { approved: true });
```

|                       | **bunqueue**    | **Temporal**            | **Inngest**           | **Trigger.dev**         |
| --------------------- | --------------- | ----------------------- | --------------------- | ----------------------- |
| **Infrastructure**    | None (embedded) | PostgreSQL + 7 services | Cloud-only            | Redis + PostgreSQL      |
| **Saga compensation** | Built-in        | Manual                  | Manual                | Manual                  |
| **Human-in-the-loop** | `.waitFor()`    | Signals API             | `step.waitForEvent()` | Waitpoint tokens        |
| **Self-hosted**       | Zero-config     | Complex                 | No                    | Complex                 |
| **Pricing**           | Free (MIT)      | Free / Cloud $$         | Per-execution         | Free tier, then $50/mo+ |

Also included: nested workflows, `doUntil`/`doWhile` loops, `forEach` over
dynamic lists, schema validation (Zod, ArkType, Valibot or any `.parse()`),
step timeouts, typed events, SQLite-persisted execution state.

[Workflow Engine guide →](https://bunqueue.dev/guide/workflow/)

## Built for AI Agents (MCP Server)

bunqueue ships a native MCP server: 73 tools, 5 resources, 3 prompts. Agents
schedule cron jobs, push and process jobs, retry failures, set rate limits,
and read stats — no glue code. HTTP handlers let an agent register a URL and
have an embedded worker call it for every job.

```bash
bun add bunqueue @modelcontextprotocol/sdk   # the MCP SDK is an optional peer
claude mcp add bunqueue -- bunx bunqueue-mcp
```

```json
// Claude Desktop / Cursor / Windsurf
{
  "mcpServers": {
    "bunqueue": {
      "command": "bunx",
      "args": ["--package=bunqueue", "bunqueue-mcp"]
    }
  }
}
```

Then just ask: _"Schedule a cleanup job every day at 3 AM"_ · _"Show me all
failed jobs and retry them"_ · _"Set rate limit to 50/sec on api-calls"_.

[MCP guide →](https://bunqueue.dev/guide/mcp/)

## Dashboard

A web dashboard that fully drives your server — queues, jobs, DLQ, cron,
webhooks, workers, live activity, SQLite inspector and an AI copilot. Open
source, currently in beta:

```bash
bunx bunqueue-dashboard
```

https://github.com/user-attachments/assets/e8a8d38e-b4a6-4dc8-8360-876c0f24d116

[Live demo](https://egeominotti.github.io/bunqueue-dashboard/) ·
[User guide](https://egeominotti.github.io/bunqueue-dashboard/docs/user-guide) ·
[GitHub](https://github.com/egeominotti/bunqueue-dashboard)

## Performance

Native Ryzen 9 9950X3D, Bun 1.3.14; medians from repeated fresh processes:

| Workload                             | Mode           |                      Median | Persistence                     |
| ------------------------------------ | -------------- | --------------------------: | ------------------------------- |
| Internal batched push, 1M jobs       | Embedded       |            729,395 jobs/sec | In-memory, no `dataPath`        |
| Public sustained `addBulk`, 50K cell | Embedded       |            186,384 jobs/sec | On-disk buffered SQLite         |
| `PUSHB`, fresh 50K sample            | TCP            |            158,779 jobs/sec | On-disk buffered SQLite         |
| No-work worker drain, concurrency 50 | TCP            |             17,256 jobs/sec | Full pull/process/ACK           |
| Linear Workflow Engine               | Embedded / TCP | 2,700 / 3,187 workflows/sec | Workflow SQLite + 3 queue nodes |

These operations do different work; the internal in-memory result is not an
SQLite or public-API claim. Run `bun run bench`, `bun run bench:tcp`, or
`bun run bench:workflow` on your hardware.
[Benchmark methodology →](https://bunqueue.dev/guide/benchmarks/) ·
[full engineering report](docs/benchmarks/native-engineering-2026-07-30.md)

## Documentation

**[bunqueue.dev →](https://bunqueue.dev/)**

- [Quick Start](https://bunqueue.dev/guide/quickstart/) — install to working queue in under a minute
- [Queue API](https://bunqueue.dev/guide/queue/) · [Worker API](https://bunqueue.dev/guide/worker/) — every option explained
- [Simple Mode](https://bunqueue.dev/guide/simple-mode/) — routes, middleware, triggers, TTL, dedup
- [Workflow Engine](https://bunqueue.dev/guide/workflow/) — sagas, branching, signals
- [SDKs](https://bunqueue.dev/guide/sdks/) — TypeScript, Python, PHP, Go, Rust, Elixir
- [MCP Server](https://bunqueue.dev/guide/mcp/) — AI agent integration
- [Server & Deployment](https://bunqueue.dev/guide/deployment/) — Docker, TLS, auth, monitoring
- [CLI Reference](https://bunqueue.dev/guide/cli/) — run and manage from the terminal
- [Migrate from BullMQ](https://bunqueue.dev/guide/migration/)

## License

MIT
