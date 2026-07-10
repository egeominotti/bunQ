---
title: "Bunqueue Environment Variables Reference"
description: Complete environment variable reference for bunqueue. TCP/HTTP ports, SQLite path, auth tokens, S3 backup, timeouts, and logging options.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/server-mode.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">server · environment</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Every variable, <em>one page.</em></h1>
  <p class="bq-hero-sub">The complete environment variable reference for bunqueue: server ports, SQLite path, auth tokens, TLS, S3 backup, timeouts, rate limiting, and logging, each with type, default, and a runnable example.</p>
</div>

:::tip[Prefer a config file?]
You can use a typed `bunqueue.config.ts` file instead of environment variables. Full IntelliSense, all options in one place. See [Configuration File](/guide/configuration/). Environment variables still work as fallback.
:::

## Server Configuration

### `TCP_PORT`

TCP server port for client connections.

| Type | Default | Example |
|------|---------|---------|
| number | `6789` | `6789` |

```bash
TCP_PORT=6789 bunqueue start
```

### `HTTP_PORT`

HTTP server port for REST API and metrics.

| Type | Default | Example |
|------|---------|---------|
| number | `6790` | `6790` |

```bash
HTTP_PORT=6790 bunqueue start
```

### `HOST`

Hostname to bind servers to.

| Type | Default | Example |
|------|---------|---------|
| string | `0.0.0.0` | `127.0.0.1` |

```bash
# Bind to localhost only
HOST=127.0.0.1 bunqueue start

# Bind to all interfaces (default)
HOST=0.0.0.0 bunqueue start
```

### `TCP_SOCKET_PATH`

Unix socket path for TCP server (alternative to TCP_PORT).

| Type | Default | Example |
|------|---------|---------|
| string | (none) | `/var/run/bunqueue.sock` |

```bash
TCP_SOCKET_PATH=/var/run/bunqueue.sock bunqueue start
```

:::note[Unix Socket vs TCP Port]
When `TCP_SOCKET_PATH` is set, the TCP server binds to the Unix socket instead of a TCP port. Unix sockets offer lower latency for local connections.
:::

### `HTTP_SOCKET_PATH`

Unix socket path for HTTP server (alternative to HTTP_PORT).

| Type | Default | Example |
|------|---------|---------|
| string | (none) | `/var/run/bunqueue-http.sock` |

```bash
HTTP_SOCKET_PATH=/var/run/bunqueue-http.sock bunqueue start
```

### `TLS_CERT_FILE`

Path to a PEM certificate file. Together with `TLS_KEY_FILE`, enables native
TLS on both the TCP and HTTP servers. Setting only one of the two is a
startup error (fail fast, never silent plaintext).

| Type | Default | Example |
|------|---------|---------|
| string | (none) | `/etc/bunqueue/cert.pem` |

```bash
TLS_CERT_FILE=./cert.pem TLS_KEY_FILE=./key.pem bunqueue start
```

### `TLS_KEY_FILE`

Path to the PEM private key file matching `TLS_CERT_FILE`.

| Type | Default | Example |
|------|---------|---------|
| string | (none) | `/etc/bunqueue/key.pem` |

See the [TLS guide](/guide/tls/) for client options and self-signed setup.

### `DATA_PATH`

Path to SQLite database file.

| Type | Default | Example |
|------|---------|---------|
| string | `in-memory` | `/var/lib/queue.db` |

```bash
DATA_PATH=/var/lib/queue.db bunqueue start
```

### `AUTH_TOKENS`

Comma-separated list of authentication tokens.

| Type | Default | Example |
|------|---------|---------|
| string | (none) | `token1,token2,token3` |

```bash
AUTH_TOKENS=secret-token-1,secret-token-2 bunqueue start
```

When set, all TCP and HTTP requests must include a valid token:

```bash
# TCP client
bunqueue push emails '{"to":"test@example.com"}' --token secret-token-1

# HTTP API
curl -H "Authorization: Bearer secret-token-1" http://localhost:6790/api/queues
```

### `BQ_TOKEN` / `BUNQUEUE_TOKEN`

CLI auth token for client commands. Avoids repeating `--token` on every command.

| Variable | Type | Default |
|----------|------|---------|
| `BQ_TOKEN` | string | (none) |
| `BUNQUEUE_TOKEN` | string | (none) |

Priority: `--token` flag > `BQ_TOKEN` > `BUNQUEUE_TOKEN`.

```bash
export BQ_TOKEN=secret-token-1
bunqueue stats              # no --token needed
bunqueue push emails '{}'   # uses BQ_TOKEN automatically
```

## Logging

### `LOG_LEVEL`

Minimum log level to output.

| Type | Default | Values |
|------|---------|--------|
| string | `info` | `debug`, `info`, `warn`, `error` |

```bash
LOG_LEVEL=debug bunqueue start
```

### `LOG_FORMAT`

Log output format.

| Type | Default | Values |
|------|---------|--------|
| string | `text` | `text`, `json` |

```bash
LOG_FORMAT=json bunqueue start
```

JSON format output:
```json
{"level":"info","msg":"Server started","tcp":6789,"http":6790,"ts":"2024-01-15T10:30:00Z"}
```

## S3 Backup Configuration

### `S3_BACKUP_ENABLED`

Enable automated S3 backups.

| Type | Default | Values |
|------|---------|--------|
| boolean | `false` | `0`, `1`, `false`, `true` |

```bash
S3_BACKUP_ENABLED=1 bunqueue start
```

### `S3_ACCESS_KEY_ID`

S3 access key for authentication.

| Type | Default | Aliases |
|------|---------|---------|
| string | (none) | `AWS_ACCESS_KEY_ID` |

```bash
S3_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE bunqueue start
```

### `S3_SECRET_ACCESS_KEY`

S3 secret key for authentication.

| Type | Default | Aliases |
|------|---------|---------|
| string | (none) | `AWS_SECRET_ACCESS_KEY` |

```bash
S3_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY bunqueue start
```

### `S3_BUCKET`

S3 bucket name for backups.

| Type | Default | Aliases |
|------|---------|---------|
| string | (none) | `AWS_BUCKET` |

```bash
S3_BUCKET=my-bunqueue-backups bunqueue start
```

### `S3_REGION`

AWS region for S3 bucket.

| Type | Default | Aliases |
|------|---------|---------|
| string | `us-east-1` | `AWS_REGION` |

```bash
S3_REGION=eu-west-1 bunqueue start
```

### `S3_ENDPOINT`

Custom S3 endpoint for non-AWS providers.

| Type | Default | Example |
|------|---------|---------|
| string | (none) | `https://account.r2.cloudflarestorage.com` |

```bash
# Cloudflare R2
S3_ENDPOINT=https://abc123.r2.cloudflarestorage.com bunqueue start

# MinIO
S3_ENDPOINT=http://localhost:9000 bunqueue start

# DigitalOcean Spaces
S3_ENDPOINT=https://nyc3.digitaloceanspaces.com bunqueue start
```

### `S3_BACKUP_INTERVAL`

Interval between automated backups (milliseconds).

| Type | Default | Example |
|------|---------|---------|
| number | `21600000` (6 hours) | `3600000` (1 hour) |

```bash
S3_BACKUP_INTERVAL=3600000 bunqueue start
```

### `S3_BACKUP_RETENTION`

Number of backups to keep.

| Type | Default | Example |
|------|---------|---------|
| number | `7` | `30` |

```bash
S3_BACKUP_RETENTION=30 bunqueue start
```

### `S3_BACKUP_PREFIX`

Prefix for backup files in S3.

| Type | Default | Example |
|------|---------|---------|
| string | `backups/` | `bunqueue/prod/` |

```bash
S3_BACKUP_PREFIX=bunqueue/production/ bunqueue start
```

## Timeouts & Limits

### `SHUTDOWN_TIMEOUT_MS`

Timeout for graceful shutdown in milliseconds.

| Type | Default | Example |
|------|---------|---------|
| number | `30000` | `60000` |

```bash
SHUTDOWN_TIMEOUT_MS=60000 bunqueue start
```

### `STATS_INTERVAL_MS`

Interval for stats logging in milliseconds.

| Type | Default | Example |
|------|---------|---------|
| number | `300000` (5 min) | `60000` |

```bash
STATS_INTERVAL_MS=60000 bunqueue start
```

### `WORKER_TIMEOUT_MS`

Default timeout for job processing in milliseconds.

| Type | Default | Example |
|------|---------|---------|
| number | `30000` | `60000` |

```bash
WORKER_TIMEOUT_MS=60000 bunqueue start
```

### `LOCK_TIMEOUT_MS`

Timeout for acquiring internal locks in milliseconds.

| Type | Default | Example |
|------|---------|---------|
| number | `5000` | `10000` |

```bash
LOCK_TIMEOUT_MS=10000 bunqueue start
```

### `WORKER_CLEANUP_INTERVAL_MS`

Interval for cleaning up inactive worker registrations.

| Type | Default | Example |
|------|---------|---------|
| number | `60000` | `120000` |

```bash
WORKER_CLEANUP_INTERVAL_MS=120000 bunqueue start
```

## Webhooks

### `WEBHOOK_MAX_RETRIES`

Maximum retry attempts for webhook deliveries.

| Type | Default | Example |
|------|---------|---------|
| number | `3` | `5` |

```bash
WEBHOOK_MAX_RETRIES=5 bunqueue start
```

### `WEBHOOK_RETRY_DELAY_MS`

Delay between webhook retry attempts in milliseconds.

| Type | Default | Example |
|------|---------|---------|
| number | `1000` | `5000` |

```bash
WEBHOOK_RETRY_DELAY_MS=5000 bunqueue start
```

## Rate Limiting (Server)

### `RATE_LIMIT_MAX_REQUESTS`

Maximum TCP requests per client within the rate limit window. Disabled when not set.

| Type | Default | Example |
|------|---------|---------|
| number | (none) | `1000` |

```bash
RATE_LIMIT_MAX_REQUESTS=1000 bunqueue start
```

### `RATE_LIMIT_WINDOW_MS`

Time window for rate limiting in milliseconds.

| Type | Default | Example |
|------|---------|---------|
| number | `60000` | `30000` |

```bash
RATE_LIMIT_WINDOW_MS=30000 bunqueue start
```

### `RATE_LIMIT_CLEANUP_MS`

Interval for cleaning up rate limit tracking data.

| Type | Default | Example |
|------|---------|---------|
| number | `60000` | `120000` |

```bash
RATE_LIMIT_CLEANUP_MS=120000 bunqueue start
```

## Security & Access

### `METRICS_AUTH`

Require authentication for metrics endpoints.

| Type | Default | Values |
|------|---------|--------|
| boolean | `false` | `true`, `false` |

```bash
METRICS_AUTH=true bunqueue start
```

### `CORS_ALLOW_ORIGIN`

Comma-separated list of allowed CORS origins.

| Type | Default | Example |
|------|---------|---------|
| string | `(none)` | `https://app.example.com` |

```bash
CORS_ALLOW_ORIGIN=https://app.example.com,https://admin.example.com bunqueue start
```

## Client & CLI

### `BUNQUEUE_MODE`

Connection mode for bunqueue client and MCP server.

| Type | Default | Values |
|------|---------|--------|
| string | `embedded` | `embedded`, `tcp` |

```bash
BUNQUEUE_MODE=tcp bun run worker.ts
```

### `BUNQUEUE_HOST`

Server host for TCP mode connections.

| Type | Default | Example |
|------|---------|---------|
| string | `localhost` | `your-server.com` |

```bash
BUNQUEUE_HOST=your-server.com bun run worker.ts
```

### `BUNQUEUE_PORT`

Server port for TCP mode connections.

| Type | Default | Example |
|------|---------|---------|
| number | `6789` | `7000` |

```bash
BUNQUEUE_PORT=7000 bun run worker.ts
```

### `BUNQUEUE_EMBEDDED`

Force embedded mode for client library.

| Type | Default | Values |
|------|---------|--------|
| string | (none) | `1` |

```bash
BUNQUEUE_EMBEDDED=1 bun run worker.ts
```

### `SQLITE_PATH`

Legacy alias for `DATA_PATH`.

| Type | Default | Example |
|------|---------|---------|
| string | (none) | `./data/queue.db` |

```bash
SQLITE_PATH=./data/queue.db bunqueue start
```

### `NO_COLOR`

Disable colored output in CLI.

| Type | Default | Values |
|------|---------|--------|
| string | (none) | `1` |

```bash
NO_COLOR=1 bunqueue stats
```

## Complete Examples

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
version: '3.8'

services:
  bunqueue:
    image: bunqueue:latest
    ports:
      - "6789:6789"
      - "6790:6790"
    volumes:
      - bunqueue-data:/data
    environment:
      - TCP_PORT=6789
      - HTTP_PORT=6790
      - DATA_PATH=/data/queue.db
      - LOG_LEVEL=info
      - LOG_FORMAT=json
      - AUTH_TOKENS=${AUTH_TOKENS}
      - S3_BACKUP_ENABLED=1
      - S3_ACCESS_KEY_ID=${S3_ACCESS_KEY_ID}
      - S3_SECRET_ACCESS_KEY=${S3_SECRET_ACCESS_KEY}
      - S3_BUCKET=${S3_BUCKET}
      - S3_REGION=${S3_REGION}
      - S3_BACKUP_INTERVAL=21600000
      - S3_BACKUP_RETENTION=7

volumes:
  bunqueue-data:
```

### Kubernetes

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: bunqueue-config
data:
  TCP_PORT: "6789"
  HTTP_PORT: "6790"
  DATA_PATH: "/data/queue.db"
  LOG_LEVEL: "info"
  LOG_FORMAT: "json"
  S3_BACKUP_ENABLED: "1"
  S3_REGION: "us-east-1"
  S3_BACKUP_INTERVAL: "21600000"
  S3_BACKUP_RETENTION: "7"
  S3_BACKUP_PREFIX: "kubernetes/"

---
apiVersion: v1
kind: Secret
metadata:
  name: bunqueue-secrets
type: Opaque
stringData:
  AUTH_TOKENS: "your-production-token"
  S3_ACCESS_KEY_ID: "your-access-key"
  S3_SECRET_ACCESS_KEY: "your-secret-key"
  S3_BUCKET: "your-bucket"

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bunqueue
spec:
  replicas: 1
  selector:
    matchLabels:
      app: bunqueue
  template:
    metadata:
      labels:
        app: bunqueue
    spec:
      containers:
        - name: bunqueue
          image: bunqueue:latest
          ports:
            - containerPort: 6789
            - containerPort: 6790
          envFrom:
            - configMapRef:
                name: bunqueue-config
            - secretRef:
                name: bunqueue-secrets
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: bunqueue-pvc
```

## Precedence

Environment variables take precedence in this order:

1. Command-line arguments (highest)
2. Environment variables
3. Configuration file
4. Default values (lowest)

```bash
# Command-line wins
TCP_PORT=6789 bunqueue start --tcp-port 7000
# Uses port 7000
```
