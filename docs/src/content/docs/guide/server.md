---
title: "Server Mode: Run bunqueue as a Standalone TCP & HTTP Service"
description: "Deploy bunqueue as a standalone server with TCP and HTTP APIs. Multi-client support, token auth, Docker deployment, and graceful shutdown."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/server.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">server · standalone</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Server mode, one process for <em>all.</em></h1>
  <p class="bq-hero-sub">Run bunqueue as its own process so multiple apps and services can share one queue. Producers and workers connect over TCP, with token auth, Docker deployment, and graceful shutdown built in.</p>
</div>

Embedded mode ties the queue to one process. Server mode runs bunqueue standalone: your API adds jobs from one service, workers process them from another, and non-Bun clients (Node.js, Python) join over the wire. The server listens on two ports: **6789** (TCP, the fast binary protocol clients use) and **6790** (HTTP, REST API and metrics).

## Start the server

```bash
# Defaults: TCP 6789, HTTP 6790, in-memory storage
bunqueue

# With persistence and custom ports
bunqueue start \
  --tcp-port 6789 \
  --http-port 6790 \
  --data-path ./data/queue.db
```

Always set `--data-path` in production. Without it, jobs live in memory and are lost on restart.

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
    token: 'my-secret-token',  // Required if the server sets AUTH_TOKENS
  }
});
```

Not on Bun? Use the [client SDKs](/guide/sdks/) for Node.js, Deno, Python, and Cloudflare Workers.

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

| Variable | Default | Description |
|----------|---------|-------------|
| `TCP_PORT` | `6789` | TCP server port |
| `HTTP_PORT` | `6790` | HTTP server port |
| `HOST` | `0.0.0.0` | Bind address |
| `BUNQUEUE_DATA_PATH` | (memory) | SQLite database path |
| `AUTH_TOKENS` | (none) | Comma-separated auth tokens |
| `LOG_FORMAT` | `text` | Log format (`text` / `json`) |

Priority when the same option is set in more than one place: CLI flags > config file > environment variables > defaults. Full list in [Environment Variables](/guide/env-vars/).

## Run it in Docker

```dockerfile
FROM oven/bun:latest
WORKDIR /app
COPY package.json bun.lockb ./
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
3. Flushes data to disk
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
