---
title: 'bunqueue.config.ts: Typed Server Configuration File'
description: 'Centralize every bunqueue server setting in one typed bunqueue.config.ts: ports, auth, SQLite or PostgreSQL 15–18 storage, CORS, backups, and timeouts.'
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/configuration.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">server · configuration</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Server configuration in one typed <em>file.</em></h1>
  <p class="bq-hero-sub">Configure the whole bunqueue server from a single typed bunqueue.config.ts instead of scattered environment variables. Every option has IntelliSense, every section is optional.</p>
</div>

This page is about configuring the **standalone server** ([Server Mode](/guide/server/)). Embedded mode needs no config file, it takes options directly in the `Queue`/`Worker` constructors.

## Quick start

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

The config file is **auto-discovered**, no flags needed. `defineConfig()` gives you full TypeScript IntelliSense, so you never have to guess an option name.

## Priority order

When the same option is set in more than one place, the first of these wins:

1. **CLI flags**, `bunqueue start --tcp-port 8000`
2. **Config file**, `bunqueue.config.ts`
3. **Environment variables**, `TCP_PORT=8000`
4. **Built-in defaults**

Use the config file as your baseline and override per environment with env vars or flags.

## Picking a config file

bunqueue looks in your project root for `bunqueue.config.ts`, then `bunqueue.config.js`, then `bunqueue.config.mjs`. To use a specific file:

```bash
bunqueue start --config ./config/production.config.ts
# Short form
bunqueue start -c ./config/staging.config.ts
```

## Full configuration reference

Every section is **optional**. Only specify what you need.

### `server`

TCP and HTTP server settings.

```typescript
defineConfig({
  server: {
    tcpPort: 6789, // TCP server port (default: 6789)
    httpPort: 6790, // HTTP/REST API port (default: 6790)
    host: '0.0.0.0', // Bind address (default: 0.0.0.0)
    tcpSocketPath: undefined, // Reserved, not applied yet: TCP always binds host:port
    httpSocketPath: undefined, // Unix socket for HTTP (overrides host/port)
    tlsCertFile: undefined, // PEM certificate, enables native TLS on TCP + HTTP (with tlsKeyFile)
    tlsKeyFile: undefined, // PEM private key (set both or neither, partial config is a startup error)
  },
});
```

### `auth`

Authentication tokens for clients. Set this on any server reachable from a network.

```typescript
defineConfig({
  auth: {
    tokens: ['my-secret-token'], // Auth tokens for TCP/HTTP
    requireAuthForMetrics: false, // Require auth for /prometheus (env: METRICS_AUTH)
  },
});
```

:::note[Secrets]
The config file is code, don't hardcode secrets that get committed to git. Use `process.env.*` for sensitive values.
:::

### `storage`

Where jobs persist. Memory and SQLite remain the defaults; PostgreSQL is an
optional standalone-server backend for multiple active brokers.

```typescript
defineConfig({
  storage: {
    driver: 'sqlite', // 'memory' | 'sqlite' | 'postgres'
    dataPath: './data/queue.db', // required for explicit SQLite
    maxCompletedJobs: 50_000, // completed-job hot cache/recovery window
    completedRetentionMs: 7 * 24 * 60 * 60 * 1000, // optional durable retention
  },
});
```

`maxCompletedJobs` bounds the in-memory completed-job projection; it does not
delete SQLite rows. Set `completedRetentionMs` to opt into age-based durable
cleanup (up to 1,000 oldest eligible rows per 10-second cleanup tick). The
default is `null`, so completed rows remain until `queue.clean(...)`,
`obliterate`, or another explicit policy removes them. Results still needed by
live dependency consumers are protected until the consumer leaves the graph.
Finite non-negative values are rounded down to whole milliseconds. Negative,
non-finite, and unsafe integer values disable automatic retention (`null`) in
both server configuration and direct embedded `QueueManager` construction.

The server CLI equivalents are `--max-completed-jobs` and
`--completed-retention-ms`; environment equivalents are documented in the
[environment reference](/guide/env-vars/).

Without `driver`, a PostgreSQL URL selects PostgreSQL, a data path selects
SQLite, and neither selects in-memory storage. PostgreSQL configuration:

```typescript
defineConfig({
  storage: {
    driver: 'postgres',
    url: process.env.BUNQUEUE_POSTGRES_URL!,
    namespace: 'production', // isolates installations sharing one database
    brokerId: process.env.HOSTNAME, // unique per active broker; auto-generated if omitted
    poolSize: 4, // default 4, runtime minimum 2
    leaseDurationMs: 30_000, // default 30s, runtime minimum 1s
    pollIntervalMs: 250, // durable event/cron fallback, minimum 25ms
    statementTimeoutMs: 30_000,
    lockTimeoutMs: 5_000,
    idleTransactionTimeoutMs: 30_000,
    maxConcurrentOperations: 16,
    maxQueuedOperations: 128,
    maxSnapshotJobs: 100_000,
    maxSnapshotPayloadBytes: 256 * 1024 * 1024,
  },
});
```

Do not combine `url` with `dataPath`. PostgreSQL support is server-only and is
validated against PostgreSQL 15, 16, 17, and 18.6; embedded queues continue to
use memory/SQLite. MySQL is not supported. See
[Storage backends](/guide/databases/).

### `telemetry`

Bound labelled Prometheus output independently from the exact global totals:

```typescript
defineConfig({
  telemetry: {
    maxPrometheusQueues: 100, // 0 disables per-queue label series
  },
});
```

The environment equivalent is `METRICS_MAX_QUEUES`. The default is `100`;
invalid or negative values fall back to the default.

### `cors`

Allowed origins for browser access to the HTTP API.

```typescript
defineConfig({
  cors: {
    origins: ['https://myapp.com', 'https://admin.myapp.com'],
  },
});
```

### `backup`

Automatic snapshots of the SQLite database to any S3-compatible storage (AWS, MinIO, Cloudflare R2). See [S3 Backup](/guide/backup/).

```typescript
defineConfig({
  backup: {
    enabled: true,
    bucket: 'my-bunqueue-backups',
    accessKeyId: process.env.S3_ACCESS_KEY_ID,
    secretAccessKey: process.env.S3_SECRET_ACCESS_KEY,
    sessionToken: process.env.S3_SESSION_TOKEN, // Temporary credentials
    region: 'eu-west-1', // Default: us-east-1
    endpoint: undefined, // Custom S3 endpoint (MinIO, R2, etc.)
    virtualHostedStyle: undefined, // Force bucket-in-host addressing
    interval: 6 * 60 * 60 * 1000, // Backup interval in ms (default: 6h)
    retention: 7, // Backups to keep (default: 7)
    prefix: 'backups/', // S3 key prefix (default: 'backups/')
  },
});
```

The server also needs `storage.dataPath` (or a data-path environment variable);
automatic backup is unavailable in in-memory and PostgreSQL modes.

### `timeouts`

```typescript
defineConfig({
  timeouts: {
    shutdown: 30000, // Graceful shutdown timeout in ms (default: 30000)
    stats: 300000, // Stats logging interval in ms (default: 300000)
  },
});
```

Only `shutdown` and `stats` are read from the config file. The type also
accepts `worker` and `lock`, but those values are currently ignored — set the
`WORKER_TIMEOUT_MS` and `LOCK_TIMEOUT_MS` environment variables instead.

### `webhooks`

Delivery retries for [webhooks](/guide/webhooks/) are configured via the
`WEBHOOK_MAX_RETRIES` (default: 3) and `WEBHOOK_RETRY_DELAY_MS` (default: 1000)
environment variables. The config-file type accepts a `webhooks` key for
forward compatibility, but its values are currently ignored.

### `logging`

```typescript
defineConfig({
  logging: {
    level: 'info', // 'debug' | 'info' | 'warn' | 'error'
    format: 'json', // 'text' | 'json'
  },
});
```

## Complete examples

### Development

```typescript
import { defineConfig } from 'bunqueue';

export default defineConfig({
  storage: { dataPath: './data/dev.db' },
  logging: { level: 'debug' },
});
```

### Production

```typescript
import { defineConfig } from 'bunqueue';

export default defineConfig({
  server: { tcpPort: 6789, httpPort: 6790, host: '0.0.0.0' },
  auth: {
    tokens: [process.env.BUNQUEUE_AUTH_TOKEN!],
    requireAuthForMetrics: true,
  },
  storage: { dataPath: '/data/bunqueue/queue.db' },
  telemetry: { maxPrometheusQueues: 100 },
  cors: { origins: [process.env.FRONTEND_URL!] },
  backup: {
    enabled: true,
    bucket: process.env.S3_BUCKET!,
    accessKeyId: process.env.S3_ACCESS_KEY_ID,
    secretAccessKey: process.env.S3_SECRET_ACCESS_KEY,
    region: 'eu-west-1',
    interval: 3600000, // Every hour
    retention: 30,
  },
  logging: { level: 'info', format: 'json' },
  timeouts: { shutdown: 60000 },
});
```

### Docker / Kubernetes

Mix the config file (static settings baked into the image) with environment variables (per-deployment values). Remember: when both define the same option, the config file wins.

```typescript
// bunqueue.config.ts, static settings in the image
import { defineConfig } from 'bunqueue';

export default defineConfig({
  server: { host: '0.0.0.0' },
  logging: { format: 'json' },
  backup: { enabled: true, region: 'eu-west-1' },
});
```

```bash
# Dynamic settings fill what the config file leaves unset
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
import { defineConfig } from 'bunqueue';
// or
import { defineConfig } from 'bunqueue/client';
```

## bunqueue Cloud

:::caution[Beta Coming Soon]
bunqueue Cloud is launching in beta soon. Once the dashboard is live, you'll connect instances with the `cloud` section, no code changes needed.
:::

```typescript
defineConfig({
  cloud: {
    url: 'https://cloud.bunqueue.io',
    apiKey: process.env.BUNQUEUE_CLOUD_API_KEY,
    instanceId: process.env.BUNQUEUE_CLOUD_INSTANCE_ID,
  },
});
```

:::tip[Related Guides]

- [Environment Variables](/guide/env-vars/), full env var reference (still supported as fallback)
- [Running the Server](/guide/server/), server startup guide
- [S3 Backup](/guide/backup/), backup configuration details
  :::
