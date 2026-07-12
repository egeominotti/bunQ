---
title: "bunqueue.config.ts: Typed Server Configuration File"
description: "Centralize every bunqueue server setting in one typed bunqueue.config.ts: ports, auth tokens, storage, CORS, S3 backups, and timeouts with full IntelliSense."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/server-mode.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">server · configuration</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Server configuration in one typed <em>file.</em></h1>
  <p class="bq-hero-sub">Configure your entire bunqueue server from a single typed bunqueue.config.ts, no more scattered environment variables. Every option has IntelliSense, every section is optional.</p>

  <div class="bq-proof">
    <span><b>1</b> typed file, auto-discovered</span>
    <span><b>9</b> config sections, all optional</span>
    <span><b>4</b>-level priority: CLI, file, env, defaults</span>
  </div>
</div>

## Quick Start

Create a `bunqueue.config.ts` in your project root:

```typescript
import { defineConfig } from 'bunqueue';

export default defineConfig({
  server: {
    tcpPort: 6789,
    httpPort: 6790,
  },
  storage: {
    dataPath: './data/queue.db',
  },
});
```

Then start normally:

```bash
bunqueue start
```

The config file is **auto-discovered**, no flags needed.

:::tip[Type Safety]
`defineConfig()` provides full TypeScript IntelliSense. Every option is typed and documented, no more guessing environment variable names.
:::

## Priority Order

Configuration values are resolved in this order (first wins):

1. **CLI flags**, `bunqueue start --tcp-port 8000`
2. **Config file**, `bunqueue.config.ts`
3. **Environment variables**, `TCP_PORT=8000`
4. **Defaults**, built-in defaults

This means you can use the config file as your baseline and override specific values per-environment with env vars or CLI flags.

## Explicit Config Path

```bash
# Use a specific config file
bunqueue start --config ./config/production.config.ts

# Short form
bunqueue start -c ./config/staging.config.ts
```

## Supported File Formats

bunqueue looks for these files in your project root (in order):

1. `bunqueue.config.ts`
2. `bunqueue.config.js`
3. `bunqueue.config.mjs`

## Full Configuration Reference

Every section is **optional**. Only specify what you need.

### `server`

TCP and HTTP server settings.

```typescript
defineConfig({
  server: {
    tcpPort: 6789,           // TCP server port (default: 6789)
    httpPort: 6790,           // HTTP/REST API port (default: 6790)
    host: '0.0.0.0',         // Bind address (default: 0.0.0.0)
    tcpSocketPath: undefined, // reserved, not applied yet: TCP always binds host:port
    httpSocketPath: undefined, // Unix socket for HTTP (overrides host/port)
    tlsCertFile: undefined,   // PEM certificate, enables native TLS on TCP + HTTP (with tlsKeyFile)
    tlsKeyFile: undefined,    // PEM private key (set both or neither, partial config is a startup error)
  },
});
```

### `auth`

Authentication and security.

```typescript
defineConfig({
  auth: {
    tokens: ['my-secret-token'],   // Auth tokens for TCP/HTTP
    requireAuthForMetrics: false,   // Require auth for the /prometheus endpoint (env: METRICS_AUTH)
  },
});
```

### `storage`

Database persistence.

```typescript
defineConfig({
  storage: {
    dataPath: './data/queue.db',  // SQLite database path (undefined = in-memory)
  },
});
```

### `cors`

Cross-Origin Resource Sharing for the HTTP API.

```typescript
defineConfig({
  cors: {
    origins: ['https://myapp.com', 'https://admin.myapp.com'],
  },
});
```

:::note[Secrets]
Use `process.env.*` for sensitive values like API keys. The config file is code, don't hardcode secrets that get committed to git.
:::

### `backup`

S3-compatible backup settings.

```typescript
defineConfig({
  backup: {
    enabled: true,
    bucket: 'my-bunqueue-backups',
    accessKeyId: process.env.S3_ACCESS_KEY_ID,
    secretAccessKey: process.env.S3_SECRET_ACCESS_KEY,
    region: 'eu-west-1',              // Default: us-east-1
    endpoint: undefined,              // Custom S3 endpoint (MinIO, R2, etc.)
    interval: 6 * 60 * 60 * 1000,    // Backup interval in ms (default: 6h)
    retention: 7,                     // Backups to keep (default: 7)
    prefix: 'backups/',               // S3 key prefix (default: 'backups/')
  },
});
```

### `timeouts`

Timeout settings for various operations.

```typescript
defineConfig({
  timeouts: {
    shutdown: 30000,   // Graceful shutdown timeout in ms (default: 30000)
    stats: 300000,     // Stats logging interval in ms (default: 300000)
    worker: 30000,     // Worker timeout (default: 30000)
    lock: 5000,        // Lock timeout (default: 5000)
  },
});
```

### `webhooks`

Webhook delivery settings.

```typescript
defineConfig({
  webhooks: {
    maxRetries: 3,       // Max delivery retries (default: 3)
    retryDelay: 1000,    // Retry delay in ms (default: 1000)
  },
});
```

### `logging`

Log output configuration.

```typescript
defineConfig({
  logging: {
    level: 'info',     // 'debug' | 'info' | 'warn' | 'error'
    format: 'json',    // 'text' | 'json'
  },
});
```

## Complete Examples

### Development

```typescript
import { defineConfig } from 'bunqueue';

export default defineConfig({
  server: {
    tcpPort: 6789,
    httpPort: 6790,
  },
  storage: {
    dataPath: './data/dev.db',
  },
  logging: {
    level: 'debug',
  },
});
```

### Production

```typescript
import { defineConfig } from 'bunqueue';

export default defineConfig({
  server: {
    tcpPort: 6789,
    httpPort: 6790,
    host: '0.0.0.0',
  },
  auth: {
    tokens: [process.env.BUNQUEUE_AUTH_TOKEN!],
    requireAuthForMetrics: true,
  },
  storage: {
    dataPath: '/data/bunqueue/queue.db',
  },
  cors: {
    origins: [process.env.FRONTEND_URL!],
  },
  backup: {
    enabled: true,
    bucket: process.env.S3_BUCKET!,
    accessKeyId: process.env.S3_ACCESS_KEY_ID,
    secretAccessKey: process.env.S3_SECRET_ACCESS_KEY,
    region: 'eu-west-1',
    interval: 3600000,  // Every hour
    retention: 30,
  },
  logging: {
    level: 'info',
    format: 'json',
  },
  timeouts: {
    shutdown: 60000,
  },
});
```

### Docker / Kubernetes

When deploying with containers, you can mix the config file with environment variables:

```typescript
// bunqueue.config.ts, static settings in the image
import { defineConfig } from 'bunqueue';

export default defineConfig({
  server: { host: '0.0.0.0' },
  logging: { format: 'json' },
  backup: {
    enabled: true,
    region: 'eu-west-1',
  },
});
```

```bash
# Dynamic settings from environment (fill values the config file leaves unset;
# the config file wins when both define the same option)
docker run \
  -e TCP_PORT=6789 \
  -e S3_BUCKET=my-bucket \
  -e S3_ACCESS_KEY_ID=xxx \
  -e S3_SECRET_ACCESS_KEY=xxx \
  my-bunqueue-image
```

## Importing `defineConfig`

Available from both package exports:

```typescript
// From the main package
import { defineConfig } from 'bunqueue';

// From the client package
import { defineConfig } from 'bunqueue/client';
```

## bunqueue Cloud

:::caution[Beta Coming Soon]
bunqueue Cloud is launching in beta soon. Once the dashboard is live, you'll be able to connect your instances with the `cloud` section in the config file. No code changes needed.
:::

When bunqueue Cloud is available, this is how you'll configure it:

```typescript
defineConfig({
  cloud: {
    url: 'https://cloud.bunqueue.io',
    apiKey: process.env.BUNQUEUE_CLOUD_API_KEY,
    instanceId: process.env.BUNQUEUE_CLOUD_INSTANCE_ID,
  },
});
```

These three fields are all you need. Everything else is managed automatically by the cloud dashboard.

:::tip[Related Guides]
- [Environment Variables](/guide/env-vars/), full env var reference (still supported as fallback)
- [Running the Server](/guide/server/), server startup guide
- [S3 Backup](/guide/backup/), backup configuration details
:::
