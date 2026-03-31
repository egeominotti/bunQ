---
title: "Server Mode — Run bunqueue as a Standalone TCP & HTTP Service"
description: "Deploy bunqueue as a standalone server with TCP and HTTP APIs. Multi-client support, token auth, Docker deployment, and graceful shutdown."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/server-mode.png
---


Run bunqueue as a standalone server with HTTP and TCP APIs.

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

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TCP_PORT` | `6789` | TCP server port |
| `HTTP_PORT` | `6790` | HTTP server port |
| `HOST` | `0.0.0.0` | Server hostname |
| `DATA_PATH` | (memory) | SQLite database path |
| `AUTH_TOKENS` | (none) | Comma-separated auth tokens |
| `CORS_ALLOW_ORIGIN` | `(none)` | CORS allowed origins |
| `LOG_FORMAT` | `text` | Log format (text/json) |

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
2. Waits for active jobs to complete (30s timeout)
3. Flushes data to disk
4. Exits cleanly

:::tip[Related Guides]
- [Environment Variables](/guide/env-vars/) - All server configuration options
- [CLI Commands](/guide/cli/) - Manage the server via CLI
- [Security Best Practices](/security/) - Secure your deployment
- [Monitoring & Prometheus Metrics](/guide/monitoring/) - Monitor server health
:::
