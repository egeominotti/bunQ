---
title: "bunqueue Environment Variables Reference"
description: Complete environment variable reference for bunqueue. TCP/HTTP ports, SQLite path, auth tokens, S3 backup, timeouts, and logging options.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/env-vars.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">server · environment</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Every environment variable, <em>one page.</em></h1>
  <p class="bq-hero-sub">The complete environment variable reference for the bunqueue server and CLI: ports, storage, auth, TLS, S3 backup, timeouts, and logging.</p>
</div>

:::tip[Prefer a config file?]
A typed `bunqueue.config.ts` can replace most of these, with IntelliSense and everything in one place. See [Configuration File](/guide/configuration/). Environment variables still work as a fallback (priority: CLI flags > config file > env vars > defaults).
:::

## Server & storage

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `TCP_PORT` | number | `6789` | TCP server port for client connections |
| `HTTP_PORT` | number | `6790` | HTTP server port for REST API and metrics |
| `HOST` | string | `0.0.0.0` | Bind address (`127.0.0.1` for local-only) |
| `BUNQUEUE_DATA_PATH` | string | (in-memory) | SQLite database path. Without it, jobs are lost on restart |
| `HTTP_SOCKET_PATH` | string | (none) | Unix socket for the HTTP server, replaces `HTTP_PORT` |
| `TCP_SOCKET_PATH` | string | (none) | **Reserved, not functional yet** (see below) |
| `TLS_CERT_FILE` | string | (none) | PEM certificate, enables native TLS on TCP + HTTP |
| `TLS_KEY_FILE` | string | (none) | PEM private key matching `TLS_CERT_FILE` |

```bash
BUNQUEUE_DATA_PATH=/var/lib/queue.db TCP_PORT=6789 bunqueue start
```

**Data path aliases.** Four names are read for the SQLite path, in priority order: `BUNQUEUE_DATA_PATH` > `BQ_DATA_PATH` > `DATA_PATH` > `SQLITE_PATH`. They are equivalent; prefer `BUNQUEUE_DATA_PATH`.

**TLS.** Set both `TLS_CERT_FILE` and `TLS_KEY_FILE` or neither, setting only one is a startup error (fail fast, never silent plaintext). See the [TLS guide](/guide/tls/).

:::caution[`TCP_SOCKET_PATH` is not functional yet]
The variable is accepted and shown in the startup banner, but the TCP listener always binds `HOST:TCP_PORT` today. Use `HTTP_SOCKET_PATH` for Unix-socket access (HTTP API), or bind to `HOST=127.0.0.1` for local-only access.
:::

## Authentication & security

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `AUTH_TOKENS` | string | (none) | Comma-separated auth tokens. When set, every TCP and HTTP request needs a valid token |
| `BQ_TOKEN` / `BUNQUEUE_TOKEN` | string | (none) | Default token for CLI client commands (avoids `--token` on every command) |
| `METRICS_AUTH` | boolean | `false` | Require auth for `/prometheus`. Only the literal value `true` enables it |
| `CORS_ALLOW_ORIGIN` | string | (none) | Comma-separated allowed CORS origins for the HTTP API |

```bash
# Server side
AUTH_TOKENS=secret-token-1,secret-token-2 bunqueue start

# Client side, every request must carry a token
bunqueue push emails '{"to":"test@example.com"}' --token secret-token-1
curl -H "Authorization: Bearer secret-token-1" http://localhost:6790/queues

# Or set it once for the CLI (priority: --token flag > BQ_TOKEN > BUNQUEUE_TOKEN)
export BQ_TOKEN=secret-token-1
bunqueue stats
```

The JSON `/metrics` endpoint is already covered by the general `AUTH_TOKENS` check; `METRICS_AUTH` adds the same requirement to `/prometheus`.

## Logging

| Variable | Type | Default | Values |
|----------|------|---------|--------|
| `LOG_LEVEL` | string | `info` | `debug`, `info`, `warn`, `error` |
| `LOG_FORMAT` | string | `text` | `text`, `json` |

```bash
LOG_LEVEL=debug LOG_FORMAT=json bunqueue start
```

JSON output looks like:

```json
{"level":"info","msg":"Server started","tcp":6789,"http":6790,"ts":"2024-01-15T10:30:00Z"}
```

## S3 backup

Automatic snapshots of the SQLite database to any S3-compatible storage. Full guide: [S3 Backup](/guide/backup/).

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `S3_BACKUP_ENABLED` | boolean | `false` | Enable automated backups (`1` / `true`) |
| `S3_BUCKET` | string | (none) | Bucket name (alias: `AWS_BUCKET`) |
| `S3_ACCESS_KEY_ID` | string | (none) | Access key (alias: `AWS_ACCESS_KEY_ID`) |
| `S3_SECRET_ACCESS_KEY` | string | (none) | Secret key (alias: `AWS_SECRET_ACCESS_KEY`) |
| `S3_REGION` | string | `us-east-1` | Region (alias: `AWS_REGION`) |
| `S3_ENDPOINT` | string | (none) | Custom endpoint for non-AWS providers (alias: `AWS_ENDPOINT`) |
| `S3_BACKUP_INTERVAL` | number | `21600000` (6h) | Interval between backups in ms |
| `S3_BACKUP_RETENTION` | number | `7` | Number of backups to keep |
| `S3_BACKUP_PREFIX` | string | `backups/` | Key prefix for backup files |

```bash
# Cloudflare R2
S3_ENDPOINT=https://abc123.r2.cloudflarestorage.com S3_BACKUP_ENABLED=1 bunqueue start

# MinIO
S3_ENDPOINT=http://localhost:9000 S3_BACKUP_ENABLED=1 bunqueue start
```

## Timeouts & limits

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SHUTDOWN_TIMEOUT_MS` | number | `30000` | How long graceful shutdown waits for active jobs |
| `STATS_INTERVAL_MS` | number | `300000` | Stats logging interval |
| `WORKER_TIMEOUT_MS` | number | `30000` | Worker-registration freshness window. Older heartbeats mark a worker stale; cleanup removes it after 3× this value |
| `LOCK_TIMEOUT_MS` | number | `5000` | Timeout for acquiring internal locks |
| `WORKER_CLEANUP_INTERVAL_MS` | number | `60000` | Interval for removing inactive worker registrations |
| `TCP_IDLE_TIMEOUT_MS` | number | `60000` | Slowloris mitigation: close a connection that starts a frame but makes no progress within this window. Idle connections with no partial frame are never affected. `0` disables |
| `TCP_MAX_WRITE_QUEUE_BYTES` | number | `67108864` (64 MB) | Max bytes buffered per connection's outbound queue before it is dropped (protects against clients that stop reading). `0` disables |

## Webhooks

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `WEBHOOK_MAX_RETRIES` | number | `3` | Max delivery retry attempts |
| `WEBHOOK_RETRY_DELAY_MS` | number | `1000` | Delay between delivery retries |

## Server rate limiting

Protects the server itself from misbehaving clients (per TCP connection or HTTP client IP). Unrelated to per-queue job rate limiting, which is set via the [Queue API](/guide/rate-limiting/).

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `RATE_LIMIT_MAX_REQUESTS` | number | `10000` | Max requests per client within the window |
| `RATE_LIMIT_WINDOW_MS` | number | `60000` | Window duration |
| `RATE_LIMIT_CLEANUP_MS` | number | `60000` | Cleanup interval for tracking data |

## Monitoring thresholds

These control the real-time monitoring events (`queue:idle`, `queue:threshold`, `worker:overloaded`, `server:memory-warning`, `storage:size-warning`) delivered over WebSocket/SSE. See the [HTTP API events reference](/api/http/#explicit-subscription-events-86).

| Variable | Default | Description |
|----------|---------|-------------|
| `QUEUE_IDLE_THRESHOLD_MS` | `30000` | Emit `queue:idle` when a queue is empty with no active jobs for this long. `0` disables |
| `QUEUE_SIZE_THRESHOLD` | `0` (disabled) | Emit `queue:threshold` when a queue's waiting count reaches this size |
| `WORKER_OVERLOAD_THRESHOLD_MS` | `30000` | Emit `worker:overloaded` when a worker stays at max concurrency for this long |
| `MEMORY_WARNING_MB` | `0` (disabled) | Emit `server:memory-warning` when heap usage exceeds this many MB |
| `STORAGE_WARNING_MB` | `0` (disabled) | Emit `storage:size-warning` when the SQLite database exceeds this many MB |

## bunqueue Cloud

Telemetry agent for the bunqueue Cloud dashboard. Cloud mode activates only when `BUNQUEUE_CLOUD_URL`, `BUNQUEUE_CLOUD_API_KEY`, **and** `BUNQUEUE_CLOUD_INSTANCE_ID` are all set.

| Variable | Default | Description |
|----------|---------|-------------|
| `BUNQUEUE_CLOUD_URL` | (none) | Cloud dashboard URL. Required for cloud mode |
| `BUNQUEUE_CLOUD_API_KEY` | (none) | API key. Required for cloud mode |
| `BUNQUEUE_CLOUD_INSTANCE_ID` | (none) | Unique instance identifier. Required for cloud mode |
| `BUNQUEUE_CLOUD_INSTANCE_NAME` | hostname | Display name for this instance |
| `BUNQUEUE_CLOUD_SIGNING_SECRET` | (none) | HMAC signing secret for payloads |
| `BUNQUEUE_CLOUD_INTERVAL_MS` | `15000` | Snapshot upload interval in ms |
| `BUNQUEUE_CLOUD_INCLUDE_JOB_DATA` | `true` | Include job payloads in telemetry. Set `false` for metadata only |
| `BUNQUEUE_CLOUD_REDACT_FIELDS` | (none) | Comma-separated payload fields to redact |
| `BUNQUEUE_CLOUD_EVENTS` | (all) | Comma-separated event filter |
| `BUNQUEUE_CLOUD_BUFFER_SIZE` | `720` | Snapshot buffer size while offline |
| `BUNQUEUE_CLOUD_CIRCUIT_BREAKER_THRESHOLD` | `5` | Consecutive failures before the circuit breaker opens |
| `BUNQUEUE_CLOUD_CIRCUIT_BREAKER_RESET_MS` | `60000` | Circuit breaker reset window in ms |
| `BUNQUEUE_CLOUD_USE_WEBSOCKET` | `true` | Stream via WebSocket. Set `false` to disable |
| `BUNQUEUE_CLOUD_USE_HTTP` | `true` | Upload via HTTP. Set `false` to disable |
| `BUNQUEUE_CLOUD_REMOTE_COMMANDS` | `true` | Allow remote commands from the dashboard. Set `false` to disable |

## Client & CLI

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `BUNQUEUE_MODE` | string | `embedded` | Connection mode for the MCP server (`embedded` or `tcp`) |
| `BUNQUEUE_HOST` | string | `localhost` | Server host for the MCP server in TCP mode; also a CLI fallback for `--host` |
| `BUNQUEUE_PORT` | number | `6789` | Server port for the MCP server in TCP mode |
| `BUNQUEUE_POOL_SIZE` | number | `2` | Connection pool size for the MCP server in TCP mode |
| `BUNQUEUE_EMBEDDED` | string | (none) | Set to `1` to force embedded mode for the client library |
| `NO_COLOR` | string | (none) | Set to `1` to disable colored CLI output |

```bash
# Point the MCP server at a remote bunqueue instance
BUNQUEUE_MODE=tcp BUNQUEUE_HOST=your-server.com BUNQUEUE_PORT=7000 bunx bunqueue-mcp
```

The MCP server also reads `BUNQUEUE_TOKEN` for authentication.

**CLI port fallback.** When `--port` is not passed, the CLI reads, in priority order: `TCP_PORT` > `BUNQUEUE_TCP_PORT` > `BQ_TCP_PORT`. Using `TCP_PORT` means the same variable that binds the server also routes the client in the same shell:

```bash
export TCP_PORT=7000
bunqueue stats   # connects to localhost:7000
```

**CLI host fallback.** When `--host` is not passed: `HOST` > `BUNQUEUE_HOST` > `BQ_HOST`.

## Complete examples

### Development

```bash
# .env.development
TCP_PORT=6789
HTTP_PORT=6790
DATA_PATH=./data/dev.db
LOG_LEVEL=debug
LOG_FORMAT=text
```

### Production

```bash
# .env.production
TCP_PORT=6789
HTTP_PORT=6790
DATA_PATH=/var/lib/production.db
LOG_LEVEL=info
LOG_FORMAT=json
AUTH_TOKENS=prod-token-abc123,prod-token-xyz789

# S3 Backup
S3_BACKUP_ENABLED=1
S3_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
S3_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
S3_BUCKET=company-bunqueue-backups
S3_REGION=us-east-1
S3_BACKUP_INTERVAL=3600000
S3_BACKUP_RETENTION=30
S3_BACKUP_PREFIX=production/
```

### Docker Compose

```yaml
services:
  bunqueue:
    image: bunqueue:latest
    ports:
      - "6789:6789"
      - "6790:6790"
    volumes:
      - bunqueue-data:/data
    environment:
      - DATA_PATH=/data/queue.db
      - LOG_FORMAT=json
      - AUTH_TOKENS=${AUTH_TOKENS}
      - S3_BACKUP_ENABLED=1
      - S3_ACCESS_KEY_ID=${S3_ACCESS_KEY_ID}
      - S3_SECRET_ACCESS_KEY=${S3_SECRET_ACCESS_KEY}
      - S3_BUCKET=${S3_BUCKET}
      - S3_REGION=${S3_REGION}

volumes:
  bunqueue-data:
```

Kubernetes manifests and more deployment recipes are in the [deployment guide](/guide/deployment/).

## Precedence

When the same setting comes from several sources:

1. Command-line arguments (highest)
2. Configuration file
3. Environment variables
4. Default values (lowest)

```bash
# Command-line wins
TCP_PORT=6789 bunqueue start --tcp-port 7000
# Uses port 7000
```
