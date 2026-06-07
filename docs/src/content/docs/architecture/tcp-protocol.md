---
title: "bunqueue TCP Protocol Architecture: Wire Format & Pipelining"
description: "bunqueue TCP protocol deep dive: MessagePack wire format, 6x faster pipelining, connection pooling, and binary command architecture."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og-image.png
---

bunqueue uses a high-performance binary protocol over TCP with MessagePack serialization and optional pipelining.

## Wire Format

Each message is a **length-prefixed MessagePack frame**:

| Bytes | Content |
|-------|---------|
| 0-3 | Frame length (4 bytes, big-endian uint32) |
| 4-N | MessagePack payload |

**Maximum frame size:** 64 MB

## TCP Pipelining

Pipelining allows multiple commands to be sent without waiting for responses, dramatically improving throughput.

### Without Pipelining (Sequential)

```
Client                    Server
  │── PUSH job1 ────────────>│
  │<── { ok, id } ───────────│  wait ~1ms
  │── PUSH job2 ────────────>│
  │<── { ok, id } ───────────│  wait ~1ms
  │── PUSH job3 ────────────>│
  │<── { ok, id } ───────────│  wait ~1ms

  Total: 3 round-trips ≈ 3ms
  Throughput: ~20,000 ops/sec
```

### With Pipelining (Parallel)

```
Client                    Server
  │── PUSH job1 (reqId:1) ──>│
  │── PUSH job2 (reqId:2) ──>│  no wait
  │── PUSH job3 (reqId:3) ──>│  no wait
  │<── { ok, reqId:1 } ──────│
  │<── { ok, reqId:2 } ──────│
  │<── { ok, reqId:3 } ──────│

  Total: 1 round-trip ≈ 1ms
  Throughput: ~125,000 ops/sec
```

**Result: 6x faster** with pipelining enabled.

### How Pipelining Works

1. **Client sends commands** with unique `reqId` identifiers
2. **Server processes in parallel** (up to 50 concurrent per connection)
3. **Responses include `reqId`** for matching (may arrive out of order)
4. **Client matches responses** using a `Map<reqId, Promise>`

### Configuration

```typescript
const queue = new Queue('my-queue', {
  connection: {
    host: 'localhost',
    port: 6789,
    pipelining: true,        // Enable pipelining (default: true)
    maxInFlight: 100,        // Max concurrent commands (default: 100)
    poolSize: 32,            // Connection pool size
    commandTimeout: 30000,   // Timeout per command (ms)
    pingInterval: 30000,     // Health-check ping interval (ms, 0 disables)
    maxCommandTimeouts: 3    // Consecutive command timeouts → reconnect (0 disables)
  }
});
```

| Option | Default | Description |
|--------|---------|-------------|
| `pipelining` | `true` | Enable TCP pipelining |
| `maxInFlight` | `100` | Max commands in flight per connection |
| `poolSize` | `4` | Number of TCP connections |
| `commandTimeout` | `30000` | Command timeout (ms) |
| `pingInterval` | `30000` | Health-check ping interval (ms, `0` disables) |
| `maxCommandTimeouts` | `3` | Consecutive command timeouts (no intervening success) before the link is concluded dead and reconnect is forced (`0` disables) |

## Protocol Version Negotiation

On connect, client and server negotiate protocol version:

```typescript
// Client → Server
{ cmd: 'Hello', protocolVersion: 2, capabilities: ['pipelining'] }

// Server → Client
{ ok: true, protocolVersion: 2, capabilities: ['pipelining'] }
```

Protocol v2 supports pipelining. Older clients without `Hello` default to v1 (sequential).

## Connection Lifecycle

**States:**

1. **DISCONNECTED** → Initial state
2. **CONNECTING** → Socket.connect() in progress
3. **CONNECTED** → Ready for commands
4. **RECONNECTING** → Auto-reconnect with backoff

**Connect sequence:**

1. TCP socket connect
2. Send `Hello` (protocol negotiation)
3. Send `Auth` (if token configured)
4. Start ping timer
5. Ready for commands

**Reconnect strategy:**

- Base delay: 100ms
- Max delay: 30s
- Backoff: exponential (2x each attempt)
- Jitter: ±30%

**Dead-link detection (half-open sockets):**

A socket can go **half-open** — the peer vanishes with no FIN/RST (suspended host,
NAT/load-balancer silently dropping an idle connection). Writes still succeed and no
`close`/`error` event fires, so the client must detect it actively. Two independent
signals conclude the link is dead and trigger `forceReconnect()`:

1. **Health-check ping** — after `maxPingFailures` (3) consecutive failed pings.
2. **Command timeouts** — after `maxCommandTimeouts` (3) consecutive command timeouts
   with no intervening success. This is the path that recovers a worker whose `PULL`s
   keep timing out, without waiting for the slower ping cycle (and it works even when the
   ping is disabled). The counter resets on any successful response.

On detection the socket is torn down, all in-flight commands are rejected immediately
(`Connection lost`) so callers unblock at once, and the reconnect/backoff loop above
re-establishes a fresh connection. `SO_KEEPALIVE` is also enabled so the OS can surface a
dead peer on its own rather than lingering until `tcp_retries2` (~15 min).

For *fast* recovery, lower `pingInterval` / `commandTimeout` — e.g.
`{ pingInterval: 10000, commandTimeout: 5000 }` recovers in ~tens of seconds vs ~120s on
defaults (each default timeout is 30s, so timeout-based detection is inherently coarse).

## Authentication

If `AUTH_TOKENS` is configured on the server, clients must authenticate:

```typescript
// Client → Server
{ cmd: 'Auth', token: 'your-secret-token' }

// Server → Client
{ ok: true }  // or { ok: false, error: 'Invalid token' }
```

Token comparison uses constant-time algorithm to prevent timing attacks.

## Commands Reference

### Core Commands

| Command | Description | Request | Response |
|---------|-------------|---------|----------|
| `PUSH` | Add single job | `{ cmd, queue, data, priority?, delay? }` | `{ ok, id }` |
| `PUSHB` | Add batch | `{ cmd, queue, jobs }` | `{ ok, ids }` |
| `PULL` | Get single job | `{ cmd, queue, timeout? }` | `{ ok, job, token? }` |
| `PULLB` | Get batch | `{ cmd, queue, count, timeout? }` | `{ ok, jobs, tokens? }` |
| `ACK` | Complete job | `{ cmd, id, result?, token? }` | `{ ok }` |
| `ACKB` | Complete batch | `{ cmd, ids, results?, tokens? }` | `{ ok }` |
| `FAIL` | Fail job | `{ cmd, id, error?, token? }` | `{ ok }` |

### Query Commands

| Command | Description |
|---------|-------------|
| `GetJob` | Get job by ID |
| `GetJobByCustomId` | Get job by custom ID |
| `GetState` | Get job state |
| `GetResult` | Get job result |
| `GetJobs` | List jobs with filters |
| `GetJobCounts` | Queue statistics |
| `GetCountsPerPriority` | Counts grouped by priority |
| `GetProgress` | Get job progress |
| `Count` | Count jobs in queue |

### Control Commands

| Command | Description |
|---------|-------------|
| `Pause` | Stop processing queue |
| `Resume` | Resume processing |
| `IsPaused` | Check if queue is paused |
| `Drain` | Remove waiting jobs |
| `Obliterate` | Delete queue completely |
| `Clean` | Remove old jobs |
| `Cancel` | Cancel pending job |
| `Promote` | Move delayed job to waiting |
| `MoveToDelayed` | Move job to delayed state |
| `Progress` | Update job progress |
| `ListQueues` | List all queues |

### DLQ Commands

| Command | Description |
|---------|-------------|
| `Dlq` | List DLQ entries |
| `RetryDlq` | Retry failed jobs |
| `RetryCompleted` | Retry completed jobs |
| `PurgeDlq` | Clear DLQ |

### Cron Commands

| Command | Description |
|---------|-------------|
| `Cron` | Add scheduled job |
| `CronDelete` | Remove scheduled job |
| `CronList` | List all cron jobs |

### Monitoring Commands

| Command | Description |
|---------|-------------|
| `Stats` | Server statistics |
| `Metrics` | Queue metrics |
| `Prometheus` | Prometheus format |
| `Ping` | Health check |
| `Heartbeat` | Worker heartbeat |
| `JobHeartbeat` | Per-job heartbeat |
| `AddLog` | Add job log entry |
| `GetLogs` | Get job logs |
| `RegisterWorker` | Register worker with server |
| `UnregisterWorker` | Unregister worker |
| `ListWorkers` | List registered workers |

## Connection Pool

The client maintains a pool of TCP connections for load balancing:

```typescript
// Default: 4 connections, configurable via poolSize
const pool = new TcpConnectionPool({
  host: 'localhost',
  port: 6789,
  poolSize: 32  // 32 connections for high throughput
});
```

**Selection strategy:** Round-robin, preferring connected sockets.

**Features:**
- Automatic reconnection
- Health tracking (latency, errors)
- Shared pools (reference counted)

## Client Disconnect Handling

When a client disconnects, the server:

1. Identifies all jobs owned by client
2. Releases job locks (returns to queue)
3. Cleans up client tracking

Jobs with active locks are automatically requeued for other workers.

## Validation Limits

| Parameter | Limit |
|-----------|-------|
| Queue name | Max 256 chars, alphanumeric + `_-.:` |
| Job data | Max 10 MB JSON |
| Priority | -1,000,000 to +1,000,000 |
| Delay | 0 to 365 days |
| Timeout | 0 to 24 hours |
| Max attempts | 1 to 1,000 |
| Backoff | 0 to 24 hours |
| TTL | 0 to 365 days |

## HTTP Endpoints

bunqueue also exposes an HTTP API on port 6790:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health + memory stats |
| `/healthz` | GET | Kubernetes liveness |
| `/ready` | GET | Kubernetes readiness |
| `/prometheus` | GET | Prometheus metrics |
| `/stats` | GET | JSON statistics |
| `/queues/:queue/jobs` | POST | Add job |
| `/queues/:queue/jobs` | GET | Pull job |
| `/jobs/:id` | GET | Get job |
| `/jobs/:id/ack` | POST | Acknowledge |
| `/jobs/:id/fail` | POST | Fail |
| `/ws` | GET | WebSocket |
| `/events` | GET | Server-Sent Events |
