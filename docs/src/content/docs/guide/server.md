---
title: 'Server Mode: SQLite or PostgreSQL over TCP & HTTP'
description: 'Deploy bunqueue as one SQLite broker or a PostgreSQL 15–18 broker fleet, with TCP/HTTP APIs, token auth, Docker, and graceful shutdown.'
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/server.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">server · standalone</span>
  <h1 class="bq-hero-h1 bq-bench-h1">One server API. <em>One or many brokers.</em></h1>
  <p class="bq-hero-sub">Run bunqueue as a standalone service so multiple apps can share a queue. Keep one memory/SQLite broker, or point several brokers at PostgreSQL 15–18; producers and workers use the same TCP API either way.</p>
</div>

Embedded mode ties the queue to one Bun process. Server mode runs bunqueue standalone: your API adds jobs from one service, workers process them from another, and all six external SDKs join over the wire. Every broker listens on two ports: **6789** (TCP, the fast binary protocol clients use) and **6790** (HTTP, REST API and metrics).

## Start the server

```bash
# Defaults: TCP 6789, HTTP 6790, in-memory storage
bunqueue

# With persistence and custom ports
bunqueue start \
  --tcp-port 6789 \
  --http-port 6790 \
  --data-path ./data/queue.db

# PostgreSQL: use a unique BUNQUEUE_BROKER_ID in every active process
BUNQUEUE_POSTGRES_URL='postgres://bunqueue:secret@postgres:5432/bunqueue' \
BUNQUEUE_POSTGRES_NAMESPACE=production \
BUNQUEUE_BROKER_ID=broker-a \
bunqueue start
```

Always select durable storage in production: set a SQLite `--data-path`, or configure `BUNQUEUE_POSTGRES_URL` for the server-only multi-broker backend. Without either, jobs live in memory and are lost on restart. PostgreSQL 15–18 is tested in CI and 18.6 is recommended; see [Storage backends](/guide/databases/).

## Connect from your app

Drop the `embedded` option and clients connect to `localhost:6789` automatically:

```typescript
import { Queue, Worker } from 'bunqueue/client';

const queue = new Queue('tasks');
const worker = new Worker('tasks', async (job) => {
  console.log('Processing:', job.data);
  return { success: true };
});

await queue.add('my-job', { foo: 'bar' });
```

For a remote server, pass a connection:

```typescript
const queue = new Queue('tasks', {
  connection: {
    host: '192.168.1.100',
    port: 6789,
    token: 'my-secret-token', // Required if the server sets AUTH_TOKENS
  },
});
```

Not on Bun? Use the [client SDKs](/guide/sdks/) for TypeScript on Node.js/Deno/Cloudflare Workers, Python, PHP, Go, Rust, or Elixir.

## Add authentication

Without auth, anyone who can reach the port can control your queues. Set one or more tokens on the server:

```bash
AUTH_TOKENS=secret1,secret2 bunqueue start --data-path ./data/queue.db
```

Every client then needs a matching `token` in its connection options. More hardening tips in [Security](/security/).

## Configure it

The recommended way is a typed `bunqueue.config.ts` file in your project root, auto-discovered by `bunqueue start`:

```typescript
import { defineConfig } from 'bunqueue';

export default defineConfig({
  server: { tcpPort: 6789, httpPort: 6790 },
  auth: { tokens: ['my-secret-token'] },
  storage: { dataPath: './data/queue.db' },
});
```

See [Configuration File](/guide/configuration/) for every option. Environment variables work too, as a fallback:

| Variable                      | Default   | Description                                              |
| ----------------------------- | --------- | -------------------------------------------------------- |
| `TCP_PORT`                    | `6789`    | TCP server port                                          |
| `HTTP_PORT`                   | `6790`    | HTTP server port                                         |
| `HOST`                        | `0.0.0.0` | Bind address                                             |
| `BUNQUEUE_STORAGE_DRIVER`     | inferred  | `memory`, `sqlite`, or `postgres`                        |
| `BUNQUEUE_DATA_PATH`          | (memory)  | SQLite database path                                     |
| `BUNQUEUE_POSTGRES_URL`       | (none)    | PostgreSQL URL; selects `postgres` when no driver is set |
| `BUNQUEUE_POSTGRES_NAMESPACE` | `default` | Isolates a bunqueue installation in one database         |
| `BUNQUEUE_BROKER_ID`          | generated | Unique stable identity for each active PostgreSQL broker |
| `AUTH_TOKENS`                 | (none)    | Comma-separated auth tokens                              |
| `LOG_FORMAT`                  | `text`    | Log format (`text` / `json`)                             |

Priority when the same option is set in more than one place: CLI flags > config file > environment variables > defaults. Full list in [Environment Variables](/guide/env-vars/).

## Run it in Docker

Every completed release publishes the multi-arch image with the exact version
tag alongside `latest`, the commit SHA and a build timestamp. Pin the version
tag when the server and client must move together:

```bash
docker run -d -p 6789:6789 -p 6790:6790 \
  -v bunqueue-data:/app/data \
  ghcr.io/egeominotti/bunqueue:2.9.2
```

PostgreSQL storage needs a 2.9 image or newer: a 2.8.x image ignores the
variables above and starts in memory or SQLite mode without an error. Confirm
the tag you pin exists — a release whose pipeline did not complete pushes no
image, and `2.9.0` is one such gap. `docker buildx imagetools inspect
ghcr.io/egeominotti/bunqueue:<tag>` prints the digest a rollout should pin.

To build an application-specific image instead:

```dockerfile
FROM oven/bun:1.4.0-alpine
WORKDIR /app
COPY package.json bun.lock* ./
RUN bun install --production
COPY . .
EXPOSE 6789 6790
CMD ["bun", "run", "src/main.ts"]
```

```bash
docker build -t bunqueue .
docker run -p 6789:6789 -p 6790:6790 \
  -v ./data:/app/data \
  -e DATA_PATH=/app/data/queue.db \
  bunqueue
```

More deployment recipes (systemd, Kubernetes, Fly.io) in the [deployment guide](/guide/deployment/).

## Graceful shutdown

On `SIGINT` or `SIGTERM` the server:

1. Stops accepting new connections
2. Waits for active jobs to finish (30s timeout, configurable via `SHUTDOWN_TIMEOUT_MS`)
3. Flushes SQLite writes or drains admitted PostgreSQL operations and maintenance
4. Exits cleanly

## Connect AI agents (MCP)

AI agents can drive a running server through the bundled MCP server, which talks to bunqueue over TCP:

```bash
bunqueue start --data-path ./data/queue.db

# In another terminal
bun add bunqueue @modelcontextprotocol/sdk
claude mcp add bunqueue -- bunx bunqueue-mcp
```

Point the MCP server at your instance with `BUNQUEUE_MODE=tcp`, `BUNQUEUE_HOST`, `BUNQUEUE_PORT`, and `BUNQUEUE_TOKEN` (when auth is on). Agents get 73 tools to add jobs, manage queues, schedule crons, and monitor everything. Full setup, including Claude Desktop, Cursor, and Windsurf config, in the [MCP guide](/guide/mcp/).

:::tip[Related Guides]

- [Environment Variables](/guide/env-vars/), all server configuration options
- [CLI Commands](/guide/cli/), manage the server from the terminal
- [Security Best Practices](/security/), secure your deployment
- [Monitoring & Prometheus Metrics](/guide/monitoring/), watch server health
  :::
