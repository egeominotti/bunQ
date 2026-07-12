---
title: "bunqueue HTTP REST API Reference: 83 Endpoints, 60 Events"
description: "Complete HTTP REST API reference for bunqueue on port 6790: 83 JSON endpoints, 60 real-time pub/sub events, WebSocket and SSE streams, Bearer auth."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/server-mode.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">api reference · http</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Every endpoint, <em>one page.</em></h1>
  <p class="bq-hero-sub">The bunqueue HTTP API runs on port <code>6790</code> by default, configurable via the <code>HTTP_PORT</code> environment variable. All request and response bodies use JSON (<code>Content-Type: application/json</code>) unless otherwise noted.</p>
</div>

**Response contract:** Every response includes an `ok` boolean field. Successful responses return `"ok": true` with operation-specific data. Failed responses return `"ok": false` with an `"error"` string describing the failure reason.

```json
// Success
{ "ok": true, "id": "019ce9d7-6983-7000-946f-48737be2b0f9" }

// Error
{ "ok": false, "error": "Job not found" }
```

---

## Authentication

When `AUTH_TOKENS` is configured, all endpoints (except health probes and CORS preflight) require a Bearer token in the `Authorization` header. Multiple tokens are supported, separated by commas.

```bash
# Server configuration (env var)
AUTH_TOKENS=secret-token-1,secret-token-2

# Or in bunqueue.config.ts:
# auth: { tokens: ['secret-token-1', 'secret-token-2'] }

# Client usage
curl -H "Authorization: Bearer secret-token-1" http://localhost:6790/stats
```

Token comparison uses **constant-time equality** (`crypto.timingSafeEqual` equivalent) to prevent timing attacks. Each token is compared against all configured tokens, ensuring no information leaks about token length or prefix.

**Endpoints that skip authentication:**

| Endpoint | Reason |
|---|---|
| `GET /health` | Load balancer health checks must work without credentials |
| `GET /healthz`, `GET /live` | Kubernetes liveness probes |
| `GET /ready` | Kubernetes readiness probes |
| `OPTIONS *` | CORS preflight must respond before auth headers are available |

The `GET /prometheus` endpoint optionally requires auth when `requireAuthForMetrics: true` is set in the server configuration. This allows Prometheus to scrape without credentials in trusted networks, while requiring auth in public-facing deployments.

**Unauthorized response** (`401`):

```json
{ "ok": false, "error": "Unauthorized" }
```

---

## CORS

Cross-Origin Resource Sharing is configured via the `CORS_ALLOW_ORIGIN` environment variable. By default it is unset, meaning no cross-origin access is granted. Set it explicitly, and only to the origins that need browser access (e.g., `CORS_ALLOW_ORIGIN=https://dashboard.example.com`). See the [Security guide](/security/) for hardening recommendations.

When CORS is configured, all JSON responses include the `Access-Control-Allow-Origin` header. Preflight (`OPTIONS`) requests return:

```
HTTP/1.1 204 No Content
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS
Access-Control-Allow-Headers: Content-Type, Authorization
Access-Control-Max-Age: 86400
```

The `Max-Age: 86400` (24 hours) means browsers cache the preflight response, avoiding repeated OPTIONS requests.

---

## Error Responses

All errors follow a consistent format with appropriate HTTP status codes:

| Code | Meaning | When |
|---|---|---|
| `200` | Success | Operation completed successfully |
| `400` | Bad Request | Invalid JSON, missing required fields, validation failure (e.g., queue name too long, priority out of range) |
| `401` | Unauthorized | Missing or invalid Bearer token |
| `404` | Not Found | Job, queue, cron, or webhook not found |
| `429` | Rate Limited | Client exceeded the configured request rate |
| `500` | Internal Error | Unexpected server error (logged server-side) |

**Error response body:**

```json
{ "ok": false, "error": "Queue name contains invalid characters" }
```

**Validation rules applied to all endpoints:**

- **Queue names**: 1-256 characters, alphanumeric + `-_.:`
- **Numeric fields**: Validated for type, range, and finiteness (e.g., `delay` must be 0 to 365 days, `priority` must be -1M to +1M)
- **Job data**: Max 10MB per job payload
- **Job IDs**: UUID v7 format (auto-generated) or custom string (via `jobId` field)

---

## Rate Limiting

HTTP requests are rate-limited per client IP using a **sliding window** algorithm. The client IP is resolved in order: `X-Forwarded-For` header (first IP) > `X-Real-IP` header > `"unknown"`.

| Variable | Default | Description |
|---|---|---|
| `RATE_LIMIT_WINDOW_MS` | `60000` | Sliding window duration in milliseconds |
| `RATE_LIMIT_MAX_REQUESTS` | `10000` | Maximum requests per window per IP. |
| `RATE_LIMIT_CLEANUP_MS` | `60000` | Interval for cleaning up expired rate limit entries |

When rate limited, the server responds with:

```json
{ "ok": false, "error": "Rate limit exceeded" }
```

Status code: `429`. The client should implement exponential backoff before retrying.

:::note
This is HTTP-level rate limiting per client IP. For per-queue job throughput limiting, use the [Queue Rate Limit](#set-rate-limit) endpoints.
:::

---

## Job Lifecycle

Understanding the job lifecycle is essential for using the API effectively. A job flows through these states:

<div class="bq-diag">
  <div class="bq-diag-head"><b>Job lifecycle</b><span>states and transitions</span></div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">push <i>priority = 0</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">waiting</div>
  </div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">push <i>priority &gt; 0</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">prioritized</div>
  </div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">push <i>delay &gt; 0</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">delayed</div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">waiting / prioritized <i>delay expires</i></div>
  </div>
  <div class="bq-diag-arrow">↓ pull</div>
  <div class="bq-diag-cell bq-diag-accent">active</div>
  <div class="bq-diag-arrow">↓</div>
  <div class="bq-diag-row">
    <div class="bq-diag-cell">completed <i>ack</i></div>
    <div class="bq-diag-cell">failed <i>fail (terminal)</i></div>
    <div class="bq-diag-cell">waiting / prioritized <i>retry</i></div>
  </div>
  <div class="bq-diag-group">
    <span class="bq-diag-group-label">flow dependencies</span>
    <div class="bq-diag-flow">
      <div class="bq-diag-cell">active</div>
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">waiting-children</div>
      <div class="bq-diag-arrow">→</div>
      <div class="bq-diag-cell">waiting <i>all children complete</i></div>
    </div>
  </div>
</div>

**States:**
- **waiting**, Job is queued with priority = 0
- **prioritized**, Job is queued with priority > 0 (processed before waiting jobs)
- **delayed**, Job waiting for its delay to expire, then moves to waiting/prioritized
- **active**, Job is being processed by a worker
- **completed**, Job finished successfully
- **failed**, Job failed after all retries (stored in DLQ with attempt history)
- **waiting-children**, Parent job waiting for child flow jobs to complete

**Delayed jobs:** When `delay > 0` is set at push time, the job enters `delayed` state and becomes `waiting` (or `prioritized` if priority > 0) after the delay expires. A delayed job can be promoted immediately via the Promote endpoint.

**Durable mode:** When `durable: true` is set, the job is written to SQLite synchronously before returning. Without it, jobs are buffered in memory (10ms write buffer) for ~10x higher throughput, with a small window of potential data loss on crash.

---

## Jobs

### Push a Job

Add a new job to a queue. The job enters `waiting` state (or `delayed` if `delay > 0`).

```
POST /queues/:queue/jobs
```

```bash
curl -X POST http://localhost:6790/queues/emails/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "data": {"to": "user@test.com", "subject": "Welcome"},
    "priority": 10,
    "delay": 5000
  }'
```

**Request body**, only `data` is required:

| Field | Type | Default | Description |
|---|---|---|---|
| `data` | `any` | *(required)* | Job payload. Any JSON-serializable value. Max 10MB. |
| `priority` | `number` | `0` | Higher value = processed sooner. Range: -1,000,000 to 1,000,000. |
| `delay` | `number` | `0` | Milliseconds before the job becomes available for processing. Max: 1 year. |
| `maxAttempts` | `number` | `3` | Maximum retry attempts before the job moves to the DLQ. Range: 1-1000. `attempts` is accepted as an alias. |
| `backoff` | `number` or `object` | `1000` | Base retry delay in milliseconds (exponential: `backoff * 2^attempt`, max: 1 day). Also accepts `{ "type": "fixed" \| "exponential", "delay": ms }`. |
| `ttl` | `number` | - | Time-to-live from creation in milliseconds. Job is discarded if not processed within this window. Max: 1 year. |
| `timeout` | `number` | - | Processing timeout in milliseconds. If a worker doesn't ACK within this time, the job is considered stalled. Max: 1 day. |
| `uniqueKey` | `string` | - | Deduplication key. If a job with the same `uniqueKey` already exists in the queue, the push is silently ignored. |
| `jobId` | `string` | - | Custom job ID. If a job with this ID already exists, the push is idempotent (returns the existing ID). |
| `tags` | `string[]` | `[]` | Metadata tags for filtering and querying. |
| `groupId` | `string` | - | Group identifier for per-group concurrency limiting. Jobs in the same group are processed sequentially. |
| `lifo` | `boolean` | `false` | Last-in-first-out ordering. When true, the job is processed before other jobs at the same priority. |
| `removeOnComplete` | `boolean` | `false` | Automatically remove the job from memory after completion. Saves memory for fire-and-forget jobs. |
| `removeOnFail` | `boolean` | `false` | Automatically remove the job after final failure (after all retries exhausted). |
| `durable` | `boolean` | `false` | Bypass the write buffer and persist to SQLite immediately. Slower (~10k/s vs ~100k/s) but zero data loss risk. |
| `dependsOn` | `string[]` | `[]` | Job IDs that must complete before this job becomes available. The job enters `waiting-children` state until all dependencies are met. |
| `repeat` | `object` | - | Repeat configuration: `{ every: ms, limit: n }` for interval-based, or `{ pattern: "cron expression" }` for cron-based (optional `tz`, `startDate`, `endDate`, `immediately`). |

**Success response** (`200`):

```json
{ "ok": true, "id": "019ce9d7-6983-7000-946f-48737be2b0f9" }
```

The `id` is a UUID v7 (time-ordered, sortable). If `jobId` was provided and a job with that ID already exists, the existing job's ID is returned (idempotent).

**Error responses:**

| Status | Error | Cause |
|---|---|---|
| `400` | `Invalid JSON body` | Request body is not valid JSON |
| `400` | `Queue name is required` | Empty queue name |
| `400` | `Queue name contains invalid characters` | Queue name has chars outside `a-zA-Z0-9_-.:` |
| `400` | `Job data too large (max 10MB)` | Serialized data exceeds 10MB |
| `400` | `priority must be an integer` | Non-integer priority |
| `400` | `delay must be at least 0` | Negative delay |

---

### Push Jobs in Bulk

Push multiple jobs to a queue in a single round-trip. More efficient than individual pushes, all jobs are inserted in a single batch operation.

```
POST /queues/:queue/jobs/bulk
```

```bash
curl -X POST http://localhost:6790/queues/emails/jobs/bulk \
  -H "Content-Type: application/json" \
  -d '{
    "jobs": [
      {"data": {"to": "user1@test.com"}, "priority": 5},
      {"data": {"to": "user2@test.com"}},
      {"data": {"to": "user3@test.com"}, "delay": 60000}
    ]
  }'
```

Each item in `jobs` supports all the same fields as a single push and is validated with the same rules (option bounds and `dependsOn` existence; a `dependsOn` entry may also reference the `customId` of an earlier job in the same batch). The operation is **atomic**, either all jobs are pushed or none are: if validation fails for any job, the whole batch is rejected with an error naming the offending index (`jobs[i]: ...`).

**Response** (`200`):

```json
{ "ok": true, "ids": ["id-1", "id-2", "id-3"] }
```

IDs are returned in the same order as the input jobs.

---

### Pull a Job

Pull the next available job from a queue for processing. The job transitions from `waiting` to `active` state. Respects priority ordering (higher priority first) and FIFO within the same priority.

```
GET /queues/:queue/jobs[?timeout=ms]
```

```bash
# Immediate return (no wait), returns null if queue is empty
curl http://localhost:6790/queues/emails/jobs

# Long-poll for up to 5 seconds, waits for a job to become available
curl http://localhost:6790/queues/emails/jobs?timeout=5000
```

| Parameter | Type | Default | Max | Description |
|---|---|---|---|---|
| `timeout` | `number` | `0` | `60000` | Long-poll timeout in ms. `0` = return immediately if no job available. |

**Response with job** (`200`):

```json
{
  "ok": true,
  "job": {
    "id": "019ce9d7-6983-7000-946f-48737be2b0f9",
    "queue": "emails",
    "data": {"to": "user@test.com", "subject": "Welcome"},
    "priority": 10,
    "createdAt": 1700000000000,
    "runAt": 1700000000000,
    "attempts": 0,
    "maxAttempts": 3,
    "backoff": 1000,
    "progress": 0,
    "tags": [],
    "lifo": false,
    "removeOnComplete": false,
    "removeOnFail": false
  }
}
```

**No job available** (`200`):

```json
{ "ok": true, "job": null }
```

**Behavior notes:**

- Paused queues return `null` even if jobs exist
- The pulled job is tracked for the duration of the HTTP request. If the client disconnects without ACKing, the stall detector will eventually return the job to `waiting` state
- Rate-limited queues may return `null` even if jobs exist (rate limit exceeded)
- Per-group concurrency: if the job's `groupId` has reached its concurrency limit, the next job from a different group is returned

---

### Pull Jobs in Batch

Pull multiple jobs at once. More efficient than individual pulls for high-throughput workers.

```
POST /queues/:queue/jobs/pull-batch
```

```bash
curl -X POST http://localhost:6790/queues/emails/jobs/pull-batch \
  -H "Content-Type: application/json" \
  -d '{"count": 10, "timeout": 5000}'
```

| Field | Type | Required | Range | Description |
|---|---|---|---|---|
| `count` | `number` | Yes | 1-1000 | Number of jobs to pull |
| `timeout` | `number` | No | 0-60000 | Long-poll timeout (ms), honored with or without `owner`. Default 0 (return immediately). |
| `owner` | `string` | No | - | Lock owner identifier for lock-based processing |
| `lockTtl` | `number` | No | - | Lock time-to-live (ms). Job is released if lock expires without ACK. |

**Response** (`200`):

```json
{
  "ok": true,
  "jobs": [
    {"id": "id-1", "queue": "emails", "data": {...}, "priority": 5, ...},
    {"id": "id-2", "queue": "emails", "data": {...}, "priority": 3, ...}
  ]
}
```

Returns fewer jobs than `count` if the queue doesn't have enough available jobs.

---

### Get a Job

Retrieve a job by ID. Returns the full job object regardless of state (waiting, active, delayed, completed).

```
GET /jobs/:id
```

```bash
curl http://localhost:6790/jobs/019ce9d7-6983-7000-946f-48737be2b0f9
```

**Response** (`200`):

```json
{
  "ok": true,
  "job": {
    "id": "019ce9d7-6983-7000-946f-48737be2b0f9",
    "queue": "emails",
    "data": {"to": "user@test.com"},
    "priority": 0,
    "createdAt": 1700000000000,
    "runAt": 1700000000000,
    "startedAt": 1700000001000,
    "completedAt": null,
    "attempts": 1,
    "maxAttempts": 3,
    "backoff": 1000,
    "progress": 50,
    "tags": ["onboarding"],
    "lifo": false,
    "removeOnComplete": false,
    "removeOnFail": false
  }
}
```

**Not found** (`404`): `{ "ok": false, "error": "Job not found" }`

:::note
Jobs with `removeOnComplete: true` or `removeOnFail: true` are permanently deleted after completion/failure and cannot be retrieved.
:::

---

### Get Job by Custom ID

Look up a job using the custom `jobId` that was set at push time. Useful for idempotent workflows where you generate your own IDs.

```
GET /jobs/custom/:customId
```

```bash
curl http://localhost:6790/jobs/custom/order-12345
```

Returns the same response format as `GET /jobs/:id`.

---

### Get Job State

```
GET /jobs/:id/state
```

```json
{ "ok": true, "id": "019ce9d7-...", "state": "active" }
```

Possible states: `waiting`, `prioritized`, `delayed`, `active`, `waiting-children`, `completed`, `failed`, `unknown` (job not found)

---

### Get Job Result

Retrieve the result stored when a job was acknowledged. Only available for completed jobs.

```
GET /jobs/:id/result
```

```json
{ "ok": true, "id": "019ce9d7-...", "result": {"sent": true, "messageId": "abc-123"} }
```

Results are stored in an LRU cache (max 5,000 entries). Oldest results are evicted when the cache is full. For permanent result storage, use the `result` field in your own database.

---

### Cancel a Job

Remove a job from the queue. Works on `waiting`, `delayed`, and `active` jobs.

```
DELETE /jobs/:id
```

```bash
curl -X DELETE http://localhost:6790/jobs/019ce9d7-...
```

**Response** (`200`): `{ "ok": true }`

If the job is `active`, it's removed from the processing queue and the worker's next heartbeat or ACK attempt will fail with "job not found". The job is not re-queued.

---

### Acknowledge a Job

Mark a job as successfully completed. The job transitions from `active` to `completed` state. Optionally store a result that can be retrieved later via `GET /jobs/:id/result`.

```
POST /jobs/:id/ack
```

```bash
curl -X POST http://localhost:6790/jobs/019ce9d7-.../ack \
  -H "Content-Type: application/json" \
  -d '{"result": {"sent": true, "messageId": "abc-123"}}'
```

**Request body** (optional):

| Field | Type | Description |
|---|---|---|
| `result` | `any` | Completion result. Stored in LRU cache (5,000 max). |
| `token` | `string` | Lock token (if using lock-based processing). |

**Response** (`200`): `{ "ok": true }`

**Error** (`400`): `{ "ok": false, "error": "Job not found or not active" }`

**What happens on ACK:**

1. Job is removed from the `active` processing queue
2. Result is stored in the LRU cache (if provided)
3. Completion counter incremented
4. `job:completed` event broadcast to all subscribers
5. `queue:counts` event broadcast with updated counts
6. Dependent jobs (via `dependsOn`) are checked and promoted if all dependencies are met
7. If `removeOnComplete: true`, the job is permanently deleted from memory

---

### Acknowledge Jobs in Batch

Acknowledge multiple jobs in a single round-trip.

```
POST /jobs/ack-batch
```

```bash
curl -X POST http://localhost:6790/jobs/ack-batch \
  -H "Content-Type: application/json" \
  -d '{"ids": ["id-1", "id-2", "id-3"], "results": [{"a": 1}, null, {"c": 3}]}'
```

| Field | Type | Required | Description |
|---|---|---|---|
| `ids` | `string[]` | Yes | Job IDs to acknowledge |
| `results` | `unknown[]` | No | Per-job results (positional, same order as `ids`) |
| `tokens` | `string[]` | No | Lock tokens (positional) |

---

### Fail a Job

Mark a job as failed. If retry attempts remain, the job is automatically re-queued with exponential backoff (`backoff * 2^attempt`). If all attempts are exhausted, the job moves to the Dead Letter Queue (DLQ).

```
POST /jobs/:id/fail
```

```bash
curl -X POST http://localhost:6790/jobs/019ce9d7-.../fail \
  -H "Content-Type: application/json" \
  -d '{"error": "SMTP connection refused"}'
```

| Field | Type | Description |
|---|---|---|
| `error` | `string` | Error message. Stored with the job for debugging. |
| `token` | `string` | Lock token (if using lock-based processing). |

**Retry behavior:**

<div class="bq-diag">
  <div class="bq-diag-head"><b>Retry behavior</b><span>exponential backoff</span></div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">Attempt 1 fails</div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">wait 1s <i>backoff</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">retry</div>
  </div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">Attempt 2 fails</div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">wait 2s <i>backoff * 2</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">retry</div>
  </div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">Attempt 3 fails</div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell">wait 4s <i>backoff * 4</i></div>
    <div class="bq-diag-arrow">→</div>
    <div class="bq-diag-cell bq-diag-accent">move to DLQ</div>
  </div>
</div>

The retry delay is calculated as `min(backoff * 2^attempt, 24 hours)`.

---

### Update Job Data

Edit the JSON payload of a job in-place. Works on jobs in `waiting`, `delayed`, or `active` state. Useful for modifying job parameters before processing or while a job is being retried.

```
PUT /jobs/:id/data
```

```bash
curl -X PUT http://localhost:6790/jobs/019ce9d7-.../data \
  -H "Content-Type: application/json" \
  -d '{"data": {"to": "new@email.com", "subject": "Updated subject"}}'
```

The entire `data` field is replaced (not merged). To update a single field, read the current data first, modify it, then PUT the full object.

**Broadcasts:** `job:data-updated` event.

---

### Change Job Priority

Change the priority of a job in `waiting` or `delayed` state. Higher priority = processed sooner.

```
PUT /jobs/:id/priority
```

```json
{ "priority": 100 }
```

The job is repositioned in the priority queue immediately. Does not work on `active` jobs (they're already being processed).

**Broadcasts:** `job:priority-changed` event with `{ jobId, newPriority }`.

---

### Promote a Delayed Job

Move a job from `delayed` to `waiting` state for immediate processing. The job becomes available for the next `PULL` operation.

```
POST /jobs/:id/promote
```

```bash
curl -X POST http://localhost:6790/jobs/019ce9d7-.../promote
```

**Error** (`400`): `{ "ok": false, "error": "Job not found or not delayed" }`, returned if the job doesn't exist, is already in `waiting` state, or is `active`.

**Broadcasts:** `job:promoted` event.

---

### Move to Waiting

Alias for Promote. Identical behavior.

```
POST /jobs/:id/move-to-wait
```

---

### Move to Delayed

Move an `active` job back to `delayed` state. Useful when a worker determines it can't process the job right now but doesn't want to fail it.

```
POST /jobs/:id/move-to-delayed
```

```json
{ "delay": 60000 }
```

The job will become `waiting` again after `delay` milliseconds.

---

### Change Delay

Update the delay of a `delayed` job. The job's `runAt` time is recalculated.

```
PUT /jobs/:id/delay
```

```json
{ "delay": 30000 }
```

**Broadcasts:** `job:delay-changed` event with `{ jobId, newDelay }`.

---

### Discard to DLQ

Move a job directly to the Dead Letter Queue, bypassing the normal retry mechanism. Works on `waiting`, `delayed`, and `active` jobs.

```
POST /jobs/:id/discard
```

```bash
curl -X POST http://localhost:6790/jobs/019ce9d7-.../discard
```

**Broadcasts:** `job:discarded` event.

---

### Wait for Job Completion

Long-poll until a job completes or the timeout expires. This is **event-driven** (not polling), the server subscribes to the job's completion event internally and resolves immediately when the job finishes.

```
POST /jobs/:id/wait
```

```bash
curl -X POST http://localhost:6790/jobs/019ce9d7-.../wait \
  -H "Content-Type: application/json" \
  -d '{"timeout": 30000}'
```

| Field | Type | Default | Description |
|---|---|---|---|
| `timeout` | `number` | `30000` | Maximum wait time in milliseconds (max: 600000) |

**Completed within timeout:**

```json
{ "ok": true, "completed": true, "result": {"sent": true} }
```

**Timed out:**

```json
{ "ok": true, "completed": false }
```

**Not found:**

```json
{ "ok": false, "error": "Job not found" }
```

If the job is already completed when the request arrives, the result is returned immediately without waiting.

---

### Get/Update Job Progress

Workers can report progress (0-100) during long-running jobs. The dashboard can display this as a progress bar.

**Get current progress:**

```
GET /jobs/:id/progress
```

```json
{ "ok": true, "progress": 75, "message": "Processing attachments..." }
```

**Update progress:**

```
POST /jobs/:id/progress
```

```json
{ "progress": 75, "message": "Processing attachments..." }
```

Progress is stored on the job object and broadcast as a `job:progress` event to all WebSocket subscribers.

---

### Get Children Values

For jobs that use `dependsOn` (flow/pipeline), retrieve the results of all completed child jobs.

```
GET /jobs/:id/children
```

```json
{ "ok": true, "data": {"values": {"child-job-1": {"result": "..."}, "child-job-2": {"result": "..."}}} }
```

---

### Job Heartbeat

Send a heartbeat to prevent the stall detector from marking the job as stalled. Workers should send heartbeats at regular intervals (default: every 10 seconds) for long-running jobs.

```
POST /jobs/:id/heartbeat
```

```json
{ "token": "lock-token", "duration": 30000 }
```

Both fields are optional. If the job doesn't exist or isn't active, returns an error.

**Batch heartbeat:**

```
POST /jobs/heartbeat-batch
```

```json
{ "ids": ["id-1", "id-2"], "tokens": ["tok-1", "tok-2"] }
```

---

### Extend Lock

Extend the lock TTL on an active job. Used in lock-based processing where a worker holds a lock on a job and needs more time.

```
POST /jobs/:id/extend-lock
```

```json
{ "duration": 30000, "token": "lock-token" }
```

**Batch extend:**

```
POST /jobs/extend-locks
```

```json
{ "ids": ["id-1", "id-2"], "tokens": ["tok-1", "tok-2"], "durations": [30000, 60000] }
```

---

### Job Logs

Structured logging attached to individual jobs. Useful for debugging failed jobs, each log entry has a level and message.

**Add a log entry:**

```
POST /jobs/:id/logs
```

```bash
curl -X POST http://localhost:6790/jobs/019ce9d7-.../logs \
  -H "Content-Type: application/json" \
  -d '{"message": "Connecting to SMTP server...", "level": "info"}'
```

| Field | Type | Required | Description |
|---|---|---|---|
| `message` | `string` | Yes | Log message |
| `level` | `string` | No | `info` (default), `warn`, or `error` |

Logs are stored in an LRU cache (max 100 entries per job, 10,000 jobs total).

**Get all logs:**

```
GET /jobs/:id/logs
```

**Clear logs:**

```
DELETE /jobs/:id/logs
```

---

## Queues

### List All Queues

Returns all queue names that have had at least one job pushed to them. Queue names persist until the queue is obliterated.

```
GET /queues
```

```json
{ "ok": true, "queues": ["emails", "notifications", "reports"] }
```

---

### Queues Summary

All queues with paused state and per-state counts in a single call (one round-trip instead of N).

```
GET /queues/summary
```

```json
[
  {
    "name": "emails",
    "paused": false,
    "counts": {"waiting": 125, "active": 5, "completed": 10234, "failed": 23, "delayed": 2}
  }
]
```

Note: this endpoint returns a bare JSON array (no `ok` wrapper).

---

### List Workers for a Queue

Workers currently registered for a specific queue.

```
GET /queues/:queue/workers
```

```json
{
  "ok": true,
  "workers": [
    {"id": "w-1", "name": "email-worker", "queues": ["emails"], "concurrency": 5, "registeredAt": 1700000000000, "lastSeen": 1700000010000, "activeJobs": 3, "processedJobs": 1500, "failedJobs": 12}
  ]
}
```

---

### List Jobs by State

Paginated listing of jobs in a specific queue, filtered by state.

```
GET /queues/:queue/jobs/list[?status=waiting&limit=10&offset=0]
```

```bash
curl "http://localhost:6790/queues/emails/jobs/list?status=waiting&limit=20&offset=0"
# multiple states (comma-separated or repeated):
curl "http://localhost:6790/queues/emails/jobs/list?status=failed,completed"
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `status` | `string` | all | State filter: `waiting`, `prioritized`, `delayed`, `active`, `completed`, `failed`, `waiting-children`. Aliases: `state`, `states`. Repeatable and comma-separated for multiple states. |
| `limit` | `number` | all | Max jobs to return |
| `offset` | `number` | `0` | Skip first N jobs |

**Response** (`200`):

```json
{
  "ok": true,
  "jobs": [
    {"id": "...", "queue": "emails", "data": {...}, "priority": 5, "createdAt": 1700000000000, "runAt": 1700000000000, "attempts": 0, "progress": 0}
  ]
}
```

Jobs are returned in priority order (highest first).

---

### Get Job Counts

Returns the number of jobs in each state for a specific queue.

```
GET /queues/:queue/counts
```

```json
{
  "ok": true,
  "counts": {"waiting": 150, "prioritized": 0, "active": 12, "delayed": 30, "completed": 5000, "failed": 3, "waiting-children": 4, "paused": 0}
}
```

:::tip
For real-time count updates without polling, subscribe to `queue:counts` via [WebSocket pub/sub](#websocket-pubsub). This event fires on every job state change.
:::

---

### Get Total Count

Returns the total number of jobs (all states) in a queue.

```
GET /queues/:queue/count
```

```json
{ "ok": true, "count": 192 }
```

---

### Get Counts per Priority

Returns a breakdown of jobs by priority level. Useful for dashboards showing priority distribution.

```
GET /queues/:queue/priority-counts
```

```json
{ "ok": true, "queue": "emails", "counts": {"0": 100, "5": 30, "10": 12} }
```

---

### Check If Paused

```
GET /queues/:queue/paused
```

```json
{ "ok": true, "paused": false }
```

---

### Pause a Queue

Stop processing new jobs from this queue. Active jobs continue to completion, only new pulls are blocked.

```
POST /queues/:queue/pause
```

**Broadcasts:** `queue:paused` event.

---

### Resume a Queue

Resume processing after a pause.

```
POST /queues/:queue/resume
```

**Broadcasts:** `queue:resumed` event.

---

### Drain a Queue

Remove **all** `waiting` and `delayed` jobs from a queue. Active jobs are not affected, they continue processing normally. This is useful for clearing a backlog without affecting in-progress work.

```
POST /queues/:queue/drain
```

```json
{ "ok": true, "count": 150 }
```

**Broadcasts:** `queue:drained` event with `{ queue, count }`.

---

### Obliterate a Queue

Completely destroy a queue and all its jobs (waiting, delayed, and metadata). Active jobs continue but their ACK/FAIL will be no-ops.

```
POST /queues/:queue/obliterate
```

:::caution
This is **irreversible**. All jobs in the queue are permanently deleted. The queue name is removed from the queue list.
:::

**Broadcasts:** `queue:obliterated` event.

---

### Clean a Queue

Remove jobs older than a grace period, optionally filtered by state. Useful for maintenance, cleaning up old waiting/delayed jobs that are no longer relevant.

```
POST /queues/:queue/clean
```

```bash
curl -X POST http://localhost:6790/queues/emails/clean \
  -H "Content-Type: application/json" \
  -d '{"grace": 86400000, "state": "waiting", "limit": 500}'
```

| Field | Type | Default | Description |
|---|---|---|---|
| `grace` | `number` | `0` | Only remove jobs older than this many milliseconds. `0` = remove all. |
| `state` | `string` | queued | `waiting`/`delayed`/`prioritized`/`paused` (all clean the queued set, the default), `completed`, or `failed`. |
| `limit` | `number` | `1000` | Max jobs to remove per call. |

**Response** (`200`):

```json
{ "ok": true, "count": 42, "ids": ["019ce9d7-...", "..."] }
```

Uses a temporal index for efficient O(log n + k) cleanup instead of full queue scan.

**Broadcasts:** `queue:cleaned` event with `{ queue, state, count }`.

---

### Promote All Delayed Jobs

Move all (or up to N) delayed jobs in a queue to `waiting` state immediately.

```
POST /queues/:queue/promote-jobs
```

```json
{ "count": 50 }
```

Omit `count` to promote all delayed jobs.

---

### Retry Completed Jobs

Re-queue completed jobs for reprocessing. Useful for replaying jobs after a bug fix.

```
POST /queues/:queue/retry-completed
```

```json
{ "id": "specific-job-id" }
```

Omit `id` to retry all completed jobs in the queue.

---

## Dead Letter Queue (DLQ)

Jobs that exhaust all retry attempts or are explicitly discarded land in the DLQ. Each queue has its own DLQ. DLQ entries include the original job data, failure reason, and timestamp.

### List DLQ Jobs

```
GET /queues/:queue/dlq[?limit=100&offset=0]
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `limit` | `number` | all | Max entries to return |
| `offset` | `number` | `0` | Skip first N entries |

Returns full DLQ entries (original job + failure metadata) under `entries`, plus the `total` count for pagination:

```json
{
  "ok": true,
  "entries": [
    {
      "job": {"id": "...", "data": {}, "attempts": 3},
      "enteredAt": 1700000000000,
      "reason": "max_attempts_exceeded",
      "error": "SMTP timeout",
      "attempts": [
        {"attempt": 1, "startedAt": 1700000000000, "failedAt": 1700000001000, "reason": "explicit_fail", "error": "SMTP timeout", "duration": 1000}
      ],
      "retryCount": 0
    }
  ],
  "total": 1
}
```

Omit `limit`/`offset` to return all entries.

---

### DLQ Stats

Aggregated DLQ statistics for a queue.

```
GET /queues/:queue/dlq/stats
```

```json
{
  "ok": true,
  "stats": {
    "total": 12,
    "byReason": {"explicit_fail": 4, "max_attempts_exceeded": 6, "timeout": 1, "stalled": 1, "ttl_expired": 0, "worker_lost": 0, "unknown": 0},
    "byQueue": {"emails": 12},
    "pendingRetry": 0,
    "expired": 0,
    "oldestEntry": 1700000000000,
    "newestEntry": 1700003600000
  }
}
```

---

### Retry DLQ Jobs

Re-queue jobs from the DLQ back to the main queue for reprocessing. The job's attempt counter is reset.

```
POST /queues/:queue/dlq/retry
```

```json
{ "jobId": "specific-job-id" }
```

Omit `jobId` to retry **all** DLQ jobs. Returns `{ "ok": true, "count": 5 }`.

**Broadcasts:** `dlq:retried` (single) or `dlq:retry-all` (all) event.

---

### Purge DLQ

Remove all jobs from the DLQ permanently. This is irreversible.

```
POST /queues/:queue/dlq/purge
```

```json
{ "ok": true, "count": 12 }
```

**Broadcasts:** `dlq:purged` event with `{ queue, count }`.

---

## Rate Limiting & Concurrency

Per-queue controls for throughput and parallelism. These are queue-level settings, independent of HTTP rate limiting.

### Set Rate Limit

Limit the number of jobs that can be processed per second from a queue.

```
PUT /queues/:queue/rate-limit
```

```json
{ "limit": 100 }
```

When the rate limit is hit, workers pulling from this queue receive `null` until the next window opens.

**Broadcasts:** `ratelimit:set` event.

### Clear Rate Limit

```
DELETE /queues/:queue/rate-limit
```

**Broadcasts:** `ratelimit:cleared` event.

### Set Concurrency Limit

Limit the number of jobs that can be processed simultaneously from a queue.

```
PUT /queues/:queue/concurrency
```

```json
{ "concurrency": 5 }
```

Accepts either `concurrency` (natural for this endpoint) or `limit`. A non-numeric value is rejected.

**Broadcasts:** `concurrency:set` event.

### Clear Concurrency Limit

```
DELETE /queues/:queue/concurrency
```

**Broadcasts:** `concurrency:cleared` event.

---

## Queue Configuration

### Stall Detection

Stall detection identifies jobs that a worker started processing but never acknowledged. This can happen when a worker crashes, hangs, or loses network connectivity.

**Get current config:**

```
GET /queues/:queue/stall-config
```

**Update config:**

```
PUT /queues/:queue/stall-config
```

```json
{
  "config": {
    "stallInterval": 30000,
    "maxStalls": 3,
    "gracePeriod": 5000
  }
}
```

| Field | Default | Description |
|---|---|---|
| `stallInterval` | `30000` | How often to check for stalled jobs (ms) |
| `maxStalls` | `3` | Max times a job can stall before moving to DLQ |
| `gracePeriod` | `5000` | Grace period after job starts before stall detection kicks in |

**Broadcasts:** `config:stall-changed` event.

### DLQ Configuration

**Get current config:**

```
GET /queues/:queue/dlq-config
```

**Update config:**

```
PUT /queues/:queue/dlq-config
```

```json
{
  "config": {
    "autoRetry": true,
    "maxAge": 604800000,
    "maxEntries": 10000
  }
}
```

| Field | Default | Description |
|---|---|---|
| `autoRetry` | `false` | Automatically retry DLQ entries after a delay |
| `maxAge` | `604800000` | Max age of DLQ entries in ms (default: 7 days). Older entries are removed. |
| `maxEntries` | `10000` | Max DLQ entries per queue. Oldest are evicted when full. |

**Broadcasts:** `config:dlq-changed` event.

---

## Cron Jobs

Schedule recurring jobs using cron expressions or fixed intervals.

### List All Crons

```
GET /crons
```

```json
{
  "ok": true,
  "crons": [
    {
      "name": "daily-cleanup",
      "queue": "maintenance",
      "schedule": "0 2 * * *",
      "repeatEvery": null,
      "nextRun": 1700100000000,
      "executions": 42,
      "maxLimit": null,
      "timezone": "UTC"
    }
  ]
}
```

---

### Add a Cron Job

```
POST /crons
```

```bash
curl -X POST http://localhost:6790/crons \
  -H "Content-Type: application/json" \
  -d '{
    "name": "daily-cleanup",
    "queue": "maintenance",
    "data": {"task": "cleanup-stale-sessions"},
    "schedule": "0 2 * * *",
    "timezone": "America/New_York"
  }'
```

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | `string` | Yes | Unique identifier. Re-using a name updates the existing cron. |
| `queue` | `string` | Yes | Target queue for the generated jobs. |
| `data` | `any` | Yes | Job payload pushed on each execution. |
| `schedule` | `string` | * | Cron expression (`"*/5 * * * *"`, `"0 2 * * *"`). |
| `repeatEvery` | `number` | * | Interval in ms (alternative to cron expression). |
| `timezone` | `string` | No | IANA timezone (default: `UTC`). Affects cron scheduling. |
| `priority` | `number` | No | Priority for generated jobs. |
| `maxLimit` | `number` | No | Max total executions. Cron is removed after reaching this count. |
| `immediately` | `boolean` | No | Fire once on creation, then continue on schedule (default `false`). |
| `skipIfNoWorker` | `boolean` | No | Skip a tick when no worker is registered for the queue (default `false`). |
| `preventOverlap` | `boolean` | No | Deduplicate overlapping runs, a tick is skipped while the previous generated job is still pending/active (default `true`). |
| `jobOptions` | `object` | No | Per-job options applied to every generated job: `maxAttempts`, `backoff`, `timeout`, `delay`, `stallTimeout`, `removeOnComplete`, `removeOnFail`. |

\* Either `schedule` or `repeatEvery` is required (not both).

**Broadcasts:** `cron:created` event.

---

### Get a Cron Job

```
GET /crons/:name
```

---

### Delete a Cron Job

```
DELETE /crons/:name
```

**Broadcasts:** `cron:deleted` event.

---

## Webhooks

Register HTTP endpoints to be called when specific job events occur. Webhooks are delivered with exponential backoff on failure (3 retries, 1s base delay).

### List All Webhooks

```
GET /webhooks
```

---

### Add a Webhook

```
POST /webhooks
```

```bash
curl -X POST http://localhost:6790/webhooks \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://example.com/hooks/bunqueue",
    "events": ["completed", "failed"],
    "queue": "emails",
    "secret": "whsec_abc123"
  }'
```

| Field | Type | Required | Description |
|---|---|---|---|
| `url` | `string` | Yes | HTTPS endpoint URL. Validated against SSRF (localhost, private IPs, cloud metadata blocked). |
| `events` | `string[]` | Yes | Event types to subscribe to (`completed`, `failed`, `pushed`, `started`). |
| `queue` | `string` | No | Filter to specific queue. Omit for all queues. |
| `secret` | `string` | No | HMAC signing secret for verifying webhook authenticity. |

**Response** (`200`):

```json
{ "ok": true, "data": {"webhookId": "wh-abc123", "url": "https://...", "events": ["completed", "failed"], "createdAt": 1700000000000} }
```

**Broadcasts:** `webhook:added` event.

---

### Remove a Webhook

```
DELETE /webhooks/:id
```

**Broadcasts:** `webhook:removed` event.

---

### Enable/Disable a Webhook

```
PUT /webhooks/:id/enabled
```

```json
{ "enabled": false }
```

Disabled webhooks stop receiving deliveries but retain their configuration.

---

## Workers

### List All Workers

```
GET /workers
```

```json
{
  "ok": true,
  "data": {
    "workers": [
      {"id": "w-1", "name": "email-worker", "queues": ["emails"], "lastSeen": 1700000000000, "activeJobs": 3, "processedJobs": 1500, "failedJobs": 12}
    ],
    "stats": {"total": 4, "active": 3}
  }
}
```

---

### Register a Worker

```
POST /workers
```

```json
{ "name": "email-worker-1", "queues": ["emails", "notifications"] }
```

**Broadcasts:** `worker:connected` event with `{ workerId, name, queues }`.

---

### Unregister a Worker

```
DELETE /workers/:id
```

**Broadcasts:** `worker:disconnected` event with `{ workerId }`.

---

### Worker Heartbeat

Keep a worker's registration alive. Workers that stop sending heartbeats are eventually marked as disconnected.

```
POST /workers/:id/heartbeat
```

---

## Monitoring

### Health Check

Comprehensive health information for load balancers and monitoring systems. No authentication required.

```
GET /health
```

```json
{
  "ok": true,
  "status": "healthy",
  "uptime": 86400,
  "version": "2.8.30",
  "queues": {"waiting": 150, "active": 12, "delayed": 30, "completed": 50000, "dlq": 3},
  "connections": {"tcp": 8, "ws": 4, "sse": 2},
  "memory": {"heapUsed": 45, "heapTotal": 64, "rss": 82}
}
```

Memory values in MB. Uptime in seconds. Returns `"status": "degraded"` when disk is full.

---

### Liveness / Readiness Probes

```
GET /healthz    # Returns "OK" (text/plain, 200)
GET /live       # Returns "OK" (text/plain, 200)
GET /ready      # Returns { "ok": true, "ready": true }
```

No authentication required. Designed for Kubernetes probe configuration.

---

### Ping

```
GET /ping
```

```json
{ "ok": true, "data": {"pong": true, "time": 1700000000000} }
```

---

### Stats

Server statistics with throughput counters, memory usage, and internal collection sizes.

```
GET /stats
```

```json
{
  "ok": true,
  "stats": {
    "waiting": 150, "active": 12, "delayed": 30, "completed": 50000, "dlq": 3,
    "totalPushed": 100000, "totalPulled": 99500, "totalCompleted": 98000, "totalFailed": 200,
    "uptime": 86400
  },
  "memory": {"heapUsed": 45, "heapTotal": 64, "rss": 82, "external": 2, "arrayBuffers": 1},
  "collections": {"jobIndex": 1500, "completedJobs": 5000, "processingTotal": 12, "queuedTotal": 150, "temporalIndexTotal": 30}
}
```

:::tip
For real-time stats without polling, subscribe to `stats:snapshot` via [WebSocket pub/sub](#websocket-pubsub). Pushed every 5 seconds.
:::

---

### Metrics (JSON)

```
GET /metrics
```

```json
{ "ok": true, "metrics": {"totalPushed": 100000, "totalPulled": 99500, "totalCompleted": 98000, "totalFailed": 200} }
```

---

### Prometheus Metrics

```
GET /prometheus
```

Returns `text/plain; version=0.0.4` format for Prometheus scraping. Includes per-queue gauges, throughput counters, and latency histograms. Optionally requires auth (`requireAuthForMetrics`).

---

### Storage Status

```
GET /storage
```

```json
{ "ok": true, "data": {"diskFull": false, "error": null, "since": null} }
```

When `diskFull: true`, the server stops accepting durable writes. In-memory operations continue.

---

### Force Garbage Collection

```
POST /gc
```

Triggers Bun GC and internal memory compaction (`compactMemory()`). Returns before/after heap stats in MB.

```json
{
  "ok": true,
  "before": {"heapUsed": 52, "heapTotal": 64, "rss": 90},
  "after": {"heapUsed": 45, "heapTotal": 64, "rss": 85}
}
```

---

### Heap Stats

```
GET /heapstats
```

Detailed V8/JSC heap breakdown for debugging memory leaks. Returns top 20 object types by count, internal collection sizes, and heap metrics.

---

## Dashboard Endpoints

Aggregated read-only snapshots designed for dashboards (fewer round-trips than composing the individual endpoints).

### Overview

```
GET /dashboard
```

Single call returning `stats` (global counts + totals + uptime), `throughput` (per-second rates), `latency` (averages + percentiles, nested per operation: `push`, `pull`, `ack`), `memory`, `collections`, `workers` (stats + list, capped at 100 with a `truncated` flag), `crons` (total + list, capped at 100), `storage`, and `timestamp`.

### Queues (paginated)

```
GET /dashboard/queues[?limit=100&offset=0]
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `limit` | `number` | `100` | Max queues to return (1-500) |
| `offset` | `number` | `0` | Skip first N queues |

```json
{
  "ok": true,
  "queues": [
    {"name": "emails", "waiting": 125, "delayed": 2, "active": 5, "dlq": 3, "paused": false}
  ],
  "total": 3,
  "limit": 100,
  "offset": 0,
  "timestamp": 1700000000000
}
```

### Queue Detail

```
GET /dashboard/queues/:queue[?includeJobs=true]
```

Returns `counts` (all 8 states, paused-aware), `paused`, `priorityCounts`, a `dlqPreview` (up to 10 entries), and, with `includeJobs=true`, up to 10 job summaries per state (`waiting`, `active`, `delayed`, `paused`).

---

## Real-time Events

bunqueue provides two real-time event channels: **Server-Sent Events (SSE)** for simple one-way streaming, and **WebSocket** with full pub/sub for interactive dashboards.

### Server-Sent Events (SSE)

```
GET /events
GET /events/queues/:queue
```

SSE broadcasts all job events in the legacy format (`{ eventType, queue, jobId, ... }`). For authenticated SSE, use `@microsoft/fetch-event-source` (native `EventSource` doesn't support custom headers).

```javascript
const events = new EventSource('http://localhost:6790/events');
events.onmessage = (e) => {
  const data = JSON.parse(e.data);
  if (data.connected) return;
  console.log(`[${data.eventType}] ${data.queue} ${data.jobId}`);
};
```

### WebSocket Pub/Sub

```
ws://localhost:6790/ws
ws://localhost:6790/ws/queues/:queue
```

WebSocket supports **pub/sub subscriptions** with **60 event types** across 10 categories. Clients subscribe to specific events and receive only matching data, **zero polling needed**.

#### Event Format

Every pub/sub event follows this structure:

```json
{
  "event": "job:completed",
  "ts": 1710000000000,
  "data": {
    "queue": "payments",
    "jobId": "abc-123"
  }
}
```

- `event`, event name (category:action)
- `ts`, unix timestamp in milliseconds
- `data`, event-specific payload

#### Subscribe / Unsubscribe

After connecting, send a `Subscribe` command to start receiving events:

```json
{ "cmd": "Subscribe", "events": ["job:*", "queue:counts", "stats:snapshot", "health:status"], "reqId": "1" }
```

**Response:**

```json
{ "ok": true, "subscribed": ["job:*", "queue:counts", "stats:snapshot", "health:status"], "reqId": "1" }
```

**Unsubscribe from specific events:**

```json
{ "cmd": "Unsubscribe", "events": ["job:progress"] }
```

**Unsubscribe from everything:**

```json
{ "cmd": "Unsubscribe", "events": [] }
```

#### Wildcards

| Pattern | Matches |
|---|---|
| `*` | All 60 events |
| `job:*` | All 15 job events |
| `queue:*` | All 10 queue events, including `queue:counts` |
| `flow:*` | Both flow events |
| `worker:*` | All 5 worker events |
| `dlq:*` | All 4 DLQ events |
| `cron:*` | All 6 cron events |
| `stats:*` | `stats:snapshot` |
| `health:*` | `health:status` |
| `storage:*` | `storage:status`, `storage:size-warning` |
| `config:*` | Both config events |
| `ratelimit:*` | All rate limit events |
| `concurrency:*` | All concurrency events |
| `webhook:*` | All 4 webhook events |
| `server:*` | `server:started`, `server:shutdown`, `server:memory-warning` |

#### Legacy Mode

Clients that never send `Subscribe` receive all job events in the **old format** (`{ eventType: "completed", queue, jobId, ... }`). This maintains backward compatibility with existing integrations.

#### Sending Commands

WebSocket clients can also send any TCP protocol command as JSON. This allows a dashboard to both receive events AND send commands (pause queue, retry job, etc.) over a single connection:

```javascript
// Send a command
ws.send(JSON.stringify({ cmd: 'Pause', queue: 'emails', reqId: '2' }));

// Response
{ "ok": true, "reqId": "2" }
```

#### Authentication

Two options:
1. **Header auth:** Send `Authorization: Bearer <token>` during the WebSocket handshake
2. **Command auth:** Send `{ "cmd": "Auth", "token": "my-secret" }` after connecting

#### Connection Cleanup

When a WebSocket disconnects, all jobs owned by that client (pulled but not ACKed) are automatically released back to the queue. This prevents jobs from being stuck when a worker disconnects unexpectedly.

#### Complete Dashboard Example

```javascript
const ws = new WebSocket('ws://localhost:6790/ws');

ws.onopen = () => {
  // Subscribe to everything a dashboard needs
  ws.send(JSON.stringify({
    cmd: 'Subscribe',
    events: [
      'job:*',           // All job lifecycle events
      'queue:counts',    // Real-time count updates (eliminates N+1 polling)
      'stats:snapshot',  // Global stats every 5s
      'health:status',   // Health check every 10s
      'worker:*',        // Worker connect/disconnect
      'dlq:*',           // DLQ events
      'cron:*',          // Cron events
      'queue:paused',    // Queue state changes
      'queue:resumed',
    ]
  }));
};

ws.onmessage = (e) => {
  const msg = JSON.parse(e.data);

  // Pub/sub event
  if (msg.event) {
    switch (msg.event) {
      // Periodic snapshots (replace HTTP polling)
      case 'stats:snapshot':
        updateOverviewCards(msg.data);
        updateMetricsCharts(msg.data);
        break;
      case 'health:status':
        updateConnectionBanner(msg.data.ok);
        updateMemoryDisplay(msg.data.memory);
        break;

      // Queue counts (eliminates the N+1 problem)
      case 'queue:counts':
        updateQueueRow(msg.data.queue, msg.data);
        break;

      // Real-time activity feed
      case 'job:completed':
      case 'job:failed':
      case 'job:pushed':
        addToActivityFeed(msg);
        break;

      // Worker status
      case 'worker:connected':
        addWorkerRow(msg.data);
        break;
      case 'worker:disconnected':
        removeWorkerRow(msg.data.workerId);
        break;

      // DLQ alerts
      case 'dlq:added':
        incrementDlqCounter(msg.data.queue);
        showAlert(`Job ${msg.data.jobId} moved to DLQ: ${msg.data.reason}`);
        break;
    }
    return;
  }

  // Command response (for interactive operations)
  if (msg.reqId) {
    handleCommandResponse(msg);
  }
};

// Interactive: pause a queue from the dashboard
function pauseQueue(queue) {
  ws.send(JSON.stringify({ cmd: 'Pause', queue, reqId: `pause-${queue}` }));
}
```

### All Events (60 total)

#### Job Lifecycle (15 events)

| Event | Payload | Description |
|---|---|---|
| `job:pushed` | `queue, jobId` | Job added to queue |
| `job:active` | `queue, jobId` | Worker picked up job |
| `job:completed` | `queue, jobId` | Job finished successfully |
| `job:failed` | `queue, jobId, error` | Job errored |
| `job:removed` | `queue, jobId` | Job cancelled/deleted |
| `job:promoted` | `jobId` | Delayed job moved to waiting |
| `job:progress` | `queue, jobId, progress` | Worker reported progress (0-100) |
| `job:delayed` | `queue, jobId, delay` | Job moved to delayed state |
| `job:stalled` | `queue, jobId` | Stall detected (no heartbeat) |
| `job:retried` | `queue, jobId` | Failed job retried |
| `job:discarded` | `jobId` | Job sent to DLQ via discard |
| `job:priority-changed` | `jobId, newPriority` | Priority updated |
| `job:data-updated` | `jobId` | Job payload modified |
| `job:delay-changed` | `jobId, newDelay` | Delay modified |
| `job:expired` | `queue, jobId, ttl, age` | Job TTL expired (distinguished from fail) |

#### Queue (10 events)

| Event | Payload | Description |
|---|---|---|
| `queue:counts` | `queue, waiting, active, completed, failed, delayed` | **Fired on every job state change.** Eliminates N+1 polling. |
| `queue:paused` | `queue` | Queue paused |
| `queue:resumed` | `queue` | Queue resumed |
| `queue:drained` | `queue, count` | All waiting/delayed jobs removed |
| `queue:cleaned` | `queue, state, count` | Jobs cleaned by state |
| `queue:obliterated` | `queue` | Queue destroyed |
| `queue:created` | `queue` | First job pushed to new queue |
| `queue:removed` | `queue` | Queue removed |
| `queue:idle` | `queue, idleSeconds` | Queue empty with no active jobs for N seconds. Configure via `QUEUE_IDLE_THRESHOLD_MS` (default: 30000). |
| `queue:threshold` | `queue, size, threshold` | Queue size exceeds threshold. Configure via `QUEUE_SIZE_THRESHOLD` (default: 0 = disabled). |

#### Flow (2 events)

| Event | Payload | Description |
|---|---|---|
| `flow:completed` | `parentJobId, queue, childrenCount` | All children of a flow completed successfully |
| `flow:failed` | `parentJobId, failedChildId, queue, error` | A child in a flow failed permanently (moved to DLQ) |

#### DLQ (4 events)

| Event | Payload | Description |
|---|---|---|
| `dlq:added` | `queue, jobId, reason` | Job moved to DLQ |
| `dlq:retried` | `queue, jobId` | Single DLQ entry retried |
| `dlq:retry-all` | `queue, count` | All DLQ entries retried |
| `dlq:purged` | `queue, count` | DLQ emptied |

#### Cron (6 events)

| Event | Payload | Description |
|---|---|---|
| `cron:created` | `name, queue, pattern?, every?, nextRun` | Cron added |
| `cron:deleted` | `name` | Cron removed |
| `cron:fired` | `name, queue` | Cron triggered, job pushed |
| `cron:updated` | `name, queue, nextRun` | Cron modified |
| `cron:missed` | `name, queue, error` | Cron missed execution window |
| `cron:skipped` | `name, queue, reason` | Cron skipped due to overlap (previous instance still within interval) |

#### Worker (5 events)

| Event | Payload | Description |
|---|---|---|
| `worker:connected` | `workerId, name, queues` | Worker registered |
| `worker:disconnected` | `workerId` | Worker gone |
| `worker:heartbeat` | `workerId` | Worker alive signal |
| `worker:overloaded` | `workerId, name, activeJobs, concurrency, overloadedSeconds` | Worker at max concurrency for N seconds. Configure via `WORKER_OVERLOAD_THRESHOLD_MS` (default: 30000). |
| `worker:error` | `workerId, name, failedJobs, processedJobs, failureRate` | Worker failure rate is high (emitted at thresholds: 5, 10, 25, 50, 100 failures) |

#### Rate Limiting & Concurrency (5 events)

| Event | Payload | Description |
|---|---|---|
| `ratelimit:set` | `queue, max` | Rate limit configured |
| `ratelimit:cleared` | `queue` | Rate limit removed |
| `ratelimit:hit` | `queue, jobId` | Job throttled by rate limit |
| `concurrency:set` | `queue, concurrency` | Concurrency limit configured |
| `concurrency:cleared` | `queue` | Concurrency limit removed |

#### Webhook (4 events)

| Event | Payload | Description |
|---|---|---|
| `webhook:added` | `id, url, events` | Webhook created |
| `webhook:removed` | `id` | Webhook deleted |
| `webhook:fired` | `id, event, statusCode` | Webhook delivered |
| `webhook:failed` | `id, event, error` | Webhook delivery failed |

#### System (7 events)

| Event | Payload | Description |
|---|---|---|
| `stats:snapshot` | `waiting, active, completed, dlq, totalPushed, totalCompleted, totalFailed, pushPerSec, pullPerSec, uptime, queues, workers, cronJobs` | Every 5s |
| `health:status` | `ok, uptime, memory: { rss, heapUsed }, connections` | Every 10s |
| `storage:status` | `collections, diskFull` | Every 30s |
| `server:started` | `version, startedAt` | Server boot |
| `server:shutdown` | `reason` | Graceful shutdown |
| `server:memory-warning` | `heapUsedMB, thresholdMB, rssMB` | Heap exceeds threshold. Configure via `MEMORY_WARNING_MB` (default: 0 = disabled). |
| `storage:size-warning` | `sizeMB, thresholdMB` | SQLite DB exceeds threshold. Configure via `STORAGE_WARNING_MB` (default: 0 = disabled). |

#### Config (2 events)

| Event | Payload | Description |
|---|---|---|
| `config:stall-changed` | `queue, config` | Stall detection config updated |
| `config:dlq-changed` | `queue, config` | DLQ config updated |

### The `queue:counts` Event

This is the most impactful event for dashboards. It fires automatically on **every job state change** and provides the current counts for the affected queue:

```json
{
  "event": "queue:counts",
  "ts": 1710000000000,
  "data": {
    "queue": "payments",
    "waiting": 15,
    "active": 2,
    "completed": 100,
    "failed": 0,
    "delayed": 3
  }
}
```

**Without `queue:counts`:** A dashboard with 20 queues needs to poll `GET /queues/:q/counts` for each queue every few seconds = 200+ HTTP requests per minute.

**With `queue:counts`:** Subscribe once, receive real-time updates only when counts change. Zero polling, instant UI updates.

---

## Endpoint Summary

### Jobs (30 endpoints)

| Method | Path | Description |
|---|---|---|
| `POST` | `/queues/:q/jobs` | Push a job |
| `POST` | `/queues/:q/jobs/bulk` | Push jobs in bulk |
| `GET` | `/queues/:q/jobs` | Pull a job |
| `POST` | `/queues/:q/jobs/pull-batch` | Pull jobs in batch |
| `GET` | `/jobs/:id` | Get job by ID |
| `GET` | `/jobs/custom/:customId` | Get job by custom ID |
| `DELETE` | `/jobs/:id` | Cancel a job |
| `POST` | `/jobs/:id/ack` | Acknowledge a job |
| `POST` | `/jobs/ack-batch` | Acknowledge batch |
| `POST` | `/jobs/:id/fail` | Fail a job |
| `GET` | `/jobs/:id/state` | Get job state |
| `GET` | `/jobs/:id/result` | Get job result |
| `GET` | `/jobs/:id/progress` | Get progress |
| `POST` | `/jobs/:id/progress` | Update progress |
| `PUT` | `/jobs/:id/data` | Update job data |
| `PUT` | `/jobs/:id/priority` | Change priority |
| `POST` | `/jobs/:id/promote` | Promote delayed job |
| `POST` | `/jobs/:id/move-to-wait` | Move to waiting |
| `POST` | `/jobs/:id/move-to-delayed` | Move to delayed |
| `PUT` | `/jobs/:id/delay` | Change delay |
| `POST` | `/jobs/:id/discard` | Discard to DLQ |
| `POST` | `/jobs/:id/wait` | Wait for completion |
| `GET` | `/jobs/:id/children` | Get children values |
| `POST` | `/jobs/:id/heartbeat` | Job heartbeat |
| `POST` | `/jobs/heartbeat-batch` | Job heartbeat batch |
| `POST` | `/jobs/:id/extend-lock` | Extend lock |
| `POST` | `/jobs/extend-locks` | Extend locks batch |
| `GET` | `/jobs/:id/logs` | Get logs |
| `POST` | `/jobs/:id/logs` | Add log |
| `DELETE` | `/jobs/:id/logs` | Clear logs |

### Queues (15 endpoints)

| Method | Path | Description |
|---|---|---|
| `GET` | `/queues` | List all queues |
| `GET` | `/queues/summary` | All queues with paused + counts |
| `GET` | `/queues/:q/workers` | Workers for a queue |
| `GET` | `/queues/:q/jobs/list` | List jobs by state |
| `GET` | `/queues/:q/counts` | Job counts per state |
| `GET` | `/queues/:q/count` | Total job count |
| `GET` | `/queues/:q/priority-counts` | Counts per priority |
| `GET` | `/queues/:q/paused` | Check if paused |
| `POST` | `/queues/:q/pause` | Pause queue |
| `POST` | `/queues/:q/resume` | Resume queue |
| `POST` | `/queues/:q/drain` | Drain queue |
| `POST` | `/queues/:q/obliterate` | Obliterate queue |
| `POST` | `/queues/:q/clean` | Clean old jobs |
| `POST` | `/queues/:q/promote-jobs` | Promote delayed jobs |
| `POST` | `/queues/:q/retry-completed` | Retry completed jobs |

### DLQ (4 endpoints)

| Method | Path | Description |
|---|---|---|
| `GET` | `/queues/:q/dlq` | List DLQ jobs |
| `GET` | `/queues/:q/dlq/stats` | DLQ statistics |
| `POST` | `/queues/:q/dlq/retry` | Retry DLQ jobs |
| `POST` | `/queues/:q/dlq/purge` | Purge DLQ |

### Rate Limiting & Concurrency (4 endpoints)

| Method | Path | Description |
|---|---|---|
| `PUT` | `/queues/:q/rate-limit` | Set rate limit |
| `DELETE` | `/queues/:q/rate-limit` | Clear rate limit |
| `PUT` | `/queues/:q/concurrency` | Set concurrency |
| `DELETE` | `/queues/:q/concurrency` | Clear concurrency |

### Configuration (4 endpoints)

| Method | Path | Description |
|---|---|---|
| `GET/PUT` | `/queues/:q/stall-config` | Stall detection config |
| `GET/PUT` | `/queues/:q/dlq-config` | DLQ config |

### Crons (4 endpoints)

| Method | Path | Description |
|---|---|---|
| `GET` | `/crons` | List crons |
| `POST` | `/crons` | Add cron |
| `GET` | `/crons/:name` | Get cron |
| `DELETE` | `/crons/:name` | Delete cron |

### Webhooks (4 endpoints)

| Method | Path | Description |
|---|---|---|
| `GET` | `/webhooks` | List webhooks |
| `POST` | `/webhooks` | Add webhook |
| `DELETE` | `/webhooks/:id` | Remove webhook |
| `PUT` | `/webhooks/:id/enabled` | Toggle webhook |

### Workers (4 endpoints)

| Method | Path | Description |
|---|---|---|
| `GET` | `/workers` | List workers |
| `POST` | `/workers` | Register worker |
| `DELETE` | `/workers/:id` | Unregister worker |
| `POST` | `/workers/:id/heartbeat` | Worker heartbeat |

### Monitoring (11 endpoints)

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/health` | No | Health check |
| `GET` | `/healthz` | No | Liveness probe |
| `GET` | `/live` | No | Liveness probe |
| `GET` | `/ready` | No | Readiness probe |
| `GET` | `/ping` | Yes | Ping/pong |
| `GET` | `/stats` | Yes | Server statistics |
| `GET` | `/metrics` | Yes | Throughput metrics |
| `GET` | `/prometheus` | Optional | Prometheus metrics |
| `GET` | `/storage` | Yes | Storage health |
| `POST` | `/gc` | Yes | Force GC + compact |
| `GET` | `/heapstats` | Yes | Heap statistics |

### Dashboard (3 endpoints)

| Method | Path | Description |
|---|---|---|
| `GET` | `/dashboard` | Aggregated overview |
| `GET` | `/dashboard/queues` | Paginated queues with stats |
| `GET` | `/dashboard/queues/:q` | Single queue detail |

### Real-time (4 channels, 60 pub/sub events)

| Protocol | Path | Description |
|---|---|---|
| SSE | `/events` | All events (legacy format) |
| SSE | `/events/queues/:q` | Queue-filtered events |
| WebSocket | `/ws` | Pub/sub + commands (60 events, wildcards) |
| WebSocket | `/ws/queues/:q` | Queue-filtered pub/sub |

:::tip[Related]
- [TCP Protocol Reference](/api/tcp/), the same operations over the binary msgpack protocol, with its own command list
- [TypeScript Types](/api/types/), type definitions for all APIs
- [Server Mode](/guide/server/), run the HTTP API server
:::
