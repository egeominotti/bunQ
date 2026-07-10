---
title: "Server Mode: Run bunqueue as a Standalone TCP & HTTP Service"
description: "Deploy bunqueue as a standalone server with TCP and HTTP APIs. Multi-client support, token auth, Docker deployment, and graceful shutdown."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/server-mode.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">server · standalone</span>
  <h1 class="bq-hero-h1 bq-bench-h1">One process serves them <em>all.</em></h1>
  <p class="bq-hero-sub">Run bunqueue as a standalone server with TCP and HTTP APIs. Any number of producers and workers connect over the wire, with token auth, Docker deployment, and graceful shutdown built in.</p>

  <div class="bq-proof">
    <span><b>:6789</b> TCP, msgpack protocol</span>
    <span><b>:6790</b> HTTP, REST and metrics</span>
    <span><b>73</b> MCP tools for AI agents</span>
    <span><b>30s</b> graceful shutdown window</span>
  </div>
</div>

## Starting the Server

```bash
# Default ports (TCP: 6789, HTTP: 6790)
bunqueue

# With custom configuration
bunqueue start \
  --tcp-port 6789 \
  --http-port 6790 \
  --data-path ./data/queue.db

# With authentication
AUTH_TOKENS=secret1,secret2 bunqueue
```

## Configuration File

The recommended way to configure bunqueue is with a `bunqueue.config.ts` file in your project root:

```typescript
import { defineConfig } from 'bunqueue';

export default defineConfig({
  server: { tcpPort: 6789, httpPort: 6790 },
  auth: { tokens: ['my-secret-token'] },
  storage: { dataPath: './data/queue.db' },
});
```

Then just run `bunqueue start`, the config file is auto-discovered. See [Configuration File](/guide/configuration/) for the full reference.

:::tip[Priority Order]
CLI flags > config file > environment variables > defaults
:::

## Environment Variables

Environment variables still work as fallback when no config file is present.

| Variable | Default | Description |
|----------|---------|-------------|
| `TCP_PORT` | `6789` | TCP server port |
| `HTTP_PORT` | `6790` | HTTP server port |
| `HOST` | `0.0.0.0` | Server hostname |
| `BUNQUEUE_DATA_PATH` | (memory) | SQLite database path (aliases, in priority order: `BQ_DATA_PATH`, `DATA_PATH`, `SQLITE_PATH`) |
| `AUTH_TOKENS` | (none) | Comma-separated auth tokens |
| `CORS_ALLOW_ORIGIN` | `(none)` | CORS allowed origins |
| `LOG_FORMAT` | `text` | Log format (text/json) |

See [Environment Variables](/guide/env-vars/) for the complete reference.

## Docker

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

## Connecting from Client

When the server is running, clients connect automatically via TCP:

```typescript
import { Queue, Worker } from 'bunqueue/client';

// No embedded option = TCP mode (connects to localhost:6789)
const queue = new Queue('tasks');
const worker = new Worker('tasks', async (job) => {
  console.log('Processing:', job.data);
  return { success: true };
});

// Add jobs
await queue.add('my-job', { foo: 'bar' });
```

### Custom Connection

```typescript
const queue = new Queue('tasks', {
  connection: {
    host: '192.168.1.100',
    port: 6789,
    token: 'my-secret-token',  // If AUTH_TOKENS is set on server
  }
});

const worker = new Worker('tasks', handler, {
  connection: {
    host: '192.168.1.100',
    port: 6789,
    token: 'my-secret-token',
  }
});
```

:::tip[Embedded vs Server Mode]
- **Embedded Mode**: Use `embedded: true` - no server needed, runs in-process
- **Server Mode**: No option needed - connects to bunqueue server via TCP

See [Quick Start](/guide/quickstart/) for a comparison.
:::

## Graceful Shutdown

The server handles `SIGINT` and `SIGTERM`:

1. Stops accepting new connections
2. Waits for active jobs to complete (30s timeout, configurable via `SHUTDOWN_TIMEOUT_MS`)
3. Flushes data to disk
4. Exits cleanly

## Connecting AI Agents (MCP)

AI agents connect to a running bunqueue server via the MCP server. The MCP server runs as a separate process and communicates with your server over TCP.

```bash
# Start bunqueue server
bunqueue start --data-path ./data/queue.db

# In another terminal, install bunqueue (ships the bunqueue-mcp binary) then connect Claude Code
bun add bunqueue
bun add @modelcontextprotocol/sdk   # optional peer dependency, required only for the MCP server
claude mcp add bunqueue -- bunx bunqueue-mcp
```

:::note
Since v2.8.1, `@modelcontextprotocol/sdk` is an **optional peer dependency**, queue-only installs skip it (7 packages and 5.5 MB instead of 117 and 93 MB, a 94% smaller install). To run the MCP server, install it once with `bun add @modelcontextprotocol/sdk`; `bunx --package=bunqueue` won't pull it in automatically.
:::

```json
// Claude Desktop / Cursor / Windsurf: --package=bunqueue resolves the bundled binary, no install needed
{
  "mcpServers": {
    "bunqueue": {
      "command": "bunx",
      "args": ["--package=bunqueue", "bunqueue-mcp"]
    }
  }
}
```

With authentication:

```bash
AUTH_TOKENS=my-secret bunqueue start
```

The MCP server picks up the connection settings from environment variables: `BUNQUEUE_MODE=tcp` (default: `embedded`), `BUNQUEUE_HOST`, `BUNQUEUE_PORT`, and `BUNQUEUE_TOKEN` when the server has `AUTH_TOKENS` set. Agents get 73 tools to add jobs, manage queues, schedule crons, retry failures, set rate limits, and monitor everything.

For HTTP handlers (agent-only feature), agents register a URL endpoint and bunqueue auto-processes jobs via HTTP calls, no Worker deployment needed. See [MCP Server guide](/guide/mcp/) for the full reference.

:::tip[Related Guides]
- [MCP Server](/guide/mcp/) - Full AI agent integration guide
- [Environment Variables](/guide/env-vars/) - All server configuration options
- [CLI Commands](/guide/cli/) - Manage the server via CLI
- [Security Best Practices](/security/) - Secure your deployment
- [Monitoring & Prometheus Metrics](/guide/monitoring/) - Monitor server health
:::
