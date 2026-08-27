---
title: 'Integrations: Web Frameworks, Databases, AI Agents'
description: 'How bunqueue fits your stack: background jobs in Hono and Elysia, storage options, and AI agent control over MCP. Start here, then follow the detailed guides.'
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/integrations.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · integrations</span>
  <h1 class="bq-hero-h1 bq-bench-h1">bunqueue in <em>your stack.</em></h1>
  <p class="bq-hero-sub">One pattern works everywhere: create a queue, add jobs from your HTTP handlers, process them in a worker. This page shows the smallest version, then points you to the detailed guides.</p>
</div>

This page is the hub for integrations. It shows the one pattern every integration shares, then links to the framework, storage, and AI agent guides.

## The smallest integration

bunqueue in **embedded mode** runs inside your app's process, backed by a local SQLite file, so there is no queue server to install or run. This works in any Bun app, whatever framework you use:

```typescript
import { Queue, Worker } from 'bunqueue/client';

// The queue: where jobs wait
const emails = new Queue('emails', { embedded: true });

// The worker: runs your function on each job
new Worker(
  'emails',
  async (job) => {
    await sendEmail(job.data);
  },
  { embedded: true }
);

// Anywhere in your app (an HTTP handler, for example):
await emails.add('welcome', { to: 'user@example.com' });
```

The HTTP response returns immediately; the email is sent in the background, with automatic retries if it fails.

:::caution[Embedded mode required for in-process queues]
All framework examples use `embedded: true`. Without it, bunqueue tries to connect to a TCP server instead. If you do run a [standalone server](/guide/server/), drop `embedded: true` and pass `connection: { host, port }`.
:::

## Web frameworks

The pattern above plus each framework's idioms (typed context, validation, plugins):

| Framework                      | What the guide adds                                 | Guide                                |
| ------------------------------ | --------------------------------------------------- | ------------------------------------ |
| [Hono](https://hono.dev)       | Routes, job status endpoints, typed middleware      | [Hono Integration](/guide/hono/)     |
| [Elysia](https://elysiajs.com) | Schema validation with `t.Object()`, plugin pattern | [Elysia Integration](/guide/elysia/) |

Using another framework? The smallest example above works as is; only the routing syntax changes.

## Databases

bunqueue needs no external database in its default memory/SQLite modes. For a
standalone fleet, PostgreSQL 15–18 is an optional database-authoritative backend
that coordinates multiple brokers; MySQL is not supported. See
[Storage backends](/guide/databases/) for both topologies and ephemeral-host
patterns.

## AI agents (MCP)

bunqueue ships an MCP server, so AI agents like Claude can add jobs, manage crons, retry failures, and monitor queues directly:

```bash
claude mcp add bunqueue -- bunx --package=bunqueue bunqueue-mcp
```

The same command shape works for Claude Desktop, Cursor, Windsurf, and any MCP client over stdio. Setup for each client, plus the full tool list, is in the [MCP Server guide](/guide/mcp/).

:::note
`bunqueue-mcp` is a binary inside the `bunqueue` package, not a separate npm package. The MCP SDK is an optional peer dependency: run `bun add @modelcontextprotocol/sdk` once before starting the MCP server.
:::

## Shared patterns

These apply to any framework.

### Define queues in one module

Create each queue once at startup and import it where needed. Do not create a `new Queue(...)` inside a request handler.

```typescript
// queues.ts
import { Queue } from 'bunqueue/client';

export const queues = {
  emails: new Queue('emails', {
    embedded: true,
    defaultJobOptions: { attempts: 3, backoff: 5000 },
  }),
  reports: new Queue('reports', {
    embedded: true,
    defaultJobOptions: { timeout: 300_000 },
  }),
} as const;
```

`defaultJobOptions` sets the retry and timeout defaults for every job added to that queue; per-job options override them.

### Graceful shutdown

On shutdown, close workers first (they wait for active jobs to finish), then release the embedded queue manager:

```typescript
import { shutdownManager } from 'bunqueue/client';

async function shutdown() {
  await Promise.all(workers.map((w) => w.close()));
  shutdownManager();
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
```

## Next steps

- [Hono Integration](/guide/hono/) - Routes, workers, and status endpoints
- [Elysia Integration](/guide/elysia/) - Validation and the plugin pattern
- [Storage: SQLite by Design](/guide/databases/) - Postgres questions answered
- [MCP Server](/guide/mcp/) - Full AI agent setup
- [CPU-Intensive Workers](/guide/cpu-intensive-workers/) - Heavy jobs without dropped connections
