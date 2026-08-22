---
title: "TCP Protocol Reference: Binary MessagePack Commands"
description: "TCP protocol spec for bunqueue: MessagePack wire format, pipelining, length-prefixed framing, and full command reference for all operations."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/api/tcp.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">api reference · tcp</span>
  <h1 class="bq-hero-h1 bq-bench-h1">The wire protocol, <em>documented.</em></h1>
  <p class="bq-hero-sub">A high-performance binary protocol on port <code>6789</code> by default. All messages use MessagePack encoding with length-prefixed framing, and pipelining lets the server process commands concurrently.</p>
</div>

## Wire Format

Every message (request and response) is wrapped in a length-prefixed frame:

<div class="bq-diag">
  <div class="bq-diag-head"><b>Frame layout</b><span>request and response</span></div>
  <div class="bq-diag-flow">
    <div class="bq-diag-cell">payload length <i>4 bytes, big-endian unsigned 32-bit</i></div>
    <div class="bq-diag-cell bq-diag-accent">MessagePack payload <i>N bytes</i></div>
  </div>
</div>

The framing protocol works as follows:

1. The first 4 bytes are a big-endian unsigned 32-bit integer indicating the length of the MessagePack payload.
2. The next N bytes are the MessagePack-encoded command or response object.
3. Maximum frame size is **64 MB**. Frames exceeding this limit cause the connection to be terminated.

### Encoding Example

```typescript
import { pack, unpack } from 'msgpackr';

// Encode a command into a framed message
function frameCommand(cmd: object): Uint8Array {
  const payload = pack(cmd);
  const frame = new Uint8Array(4 + payload.length);
  // Write length prefix (big-endian u32)
  frame[0] = (payload.length >> 24) & 0xff;
  frame[1] = (payload.length >> 16) & 0xff;
  frame[2] = (payload.length >> 8) & 0xff;
  frame[3] = payload.length & 0xff;
  frame.set(payload, 4);
  return frame;
}

// Decode a framed response
function decodeFrame(frame: Uint8Array): object {
  return unpack(frame);
}
```

## Connection

```typescript
import { pack, unpack } from 'msgpackr';

const socket = await Bun.connect({
  hostname: 'localhost',
  port: 6789,
  socket: {
    data(socket, data) {
      // Parse frames from data, then unpack each frame with msgpackr
    },
  },
});

// Send a command
const cmd = pack({ cmd: 'Ping' });
const frame = new Uint8Array(4 + cmd.length);
frame[0] = (cmd.length >> 24) & 0xff;
frame[1] = (cmd.length >> 16) & 0xff;
frame[2] = (cmd.length >> 8) & 0xff;
frame[3] = cmd.length & 0xff;
frame.set(cmd, 4);
socket.write(frame);
```

## Protocol Negotiation (Hello)

Clients should send a `Hello` command after connecting to report their protocol revision and discover server capabilities.

**Request:**

```typescript
{ cmd: 'Hello', protocolVersion: 3, capabilities: ['pipelining', 'separate-job-name'] }
```

**Response:**

```typescript
{
  ok: true,
  protocolVersion: 3,
  capabilities: ['pipelining', 'separate-job-name'],
  server: 'bunqueue',
  version: 'x.y.z'  // Installed server package version
}
```

The current protocol version is **3**. It supports `pipelining` and
`separate-job-name`. Revision 3 places job metadata in top-level `job.name`
and preserves `job.data` exactly as supplied. The server still accepts legacy
inputs with no top-level `name`: at that inbound boundary only, a string
`data.name` is decoded as the old embedded-name envelope.

## Pipelining

The server supports **pipelining**: clients can send multiple commands without waiting for each response. The server processes frames in parallel with a concurrency limit of **50 commands per connection**, controlled by a semaphore.

To correlate responses with requests when pipelining, include a `reqId` field in each command. The server echoes `reqId` back in the corresponding response.

```typescript
// Send two commands simultaneously
socket.write(frameCommand({ cmd: 'PUSH', queue: 'emails', data: { to: 'a@b.com' }, reqId: '1' }));
socket.write(frameCommand({ cmd: 'PUSH', queue: 'emails', data: { to: 'c@d.com' }, reqId: '2' }));

// Responses may arrive in any order - match by reqId
// { ok: true, id: 'abc-123', reqId: '1' }
// { ok: true, id: 'def-456', reqId: '2' }
```

## Authentication

When the server is configured with `AUTH_TOKENS`, all connections must authenticate before sending other commands. The `Auth` command is always permitted regardless of authentication state.

**Request:**

```typescript
{ cmd: 'Auth', token: 'your-secret-token' }
```

**Response (success):**

```typescript
{ ok: true }
```

**Response (failure):**

```typescript
{ ok: false, error: 'Invalid token' }
```

If auth tokens are configured and a client sends any command before authenticating, the server responds with:

```typescript
{ ok: false, error: 'Not authenticated' }
```

## Response Format

All responses include an `ok` boolean field. On success `ok` is `true` with command-specific data. On failure `ok` is `false` with an `error` string.

```typescript
// Success
{ ok: true, ...data, reqId?: string }

// Error
{ ok: false, error: 'Error message', reqId?: string }
```

### Queue event frames

`SubscribeEvents` selects one queue for the current connection;
`UnsubscribeEvents` clears it without closing the socket. Both commands require
normal authentication and return a regular `reqId`-correlated response.

```typescript
{ cmd: 'SubscribeEvents', queue: 'tasks', reqId: 'events-1' }
{ ok: true, reqId: 'events-1' }

// Later, independently of command responses:
{ type: 'event', event: { eventType: 'completed', queue: 'tasks', jobId: '...', timestamp: 0, data: { ok: true } } }

{ cmd: 'UnsubscribeEvents', reqId: 'events-2' }
```

The unsolicited event envelope has no `reqId`. Pipelined clients must recognize
`type: 'event'` before correlating command responses. A new subscription on the
same connection replaces the previous queue. Slow subscribers are subject to
the normal per-connection write-buffer limit.

## Connection Lifecycle

When a TCP connection closes, the server automatically releases all jobs that were being processed by that client back to their queues. This uses retry logic with exponential backoff (up to 3 attempts) to ensure jobs are not left in an inconsistent state.

## Rate Limiting

Each connection is subject to server-side rate limiting. If exceeded, the server responds with:

```typescript
{ ok: false, error: 'Rate limit exceeded' }
```

---

## Command Reference

Every command object must include a `cmd` field. An optional `reqId` field can be included for request-response correlation (required for pipelining).

### Core Commands

#### PUSH

Add a single job to a queue.

**Request:**

```typescript
{
  cmd: 'PUSH',
  queue: string,          // Queue name (required, max 256 chars, alphanumeric/underscore/dash/dot/colon)
  name: string,           // Job name metadata (required for protocol v3 clients)
  data: any,              // Untouched user payload (required, max 10 MB)
  priority?: number,      // Higher = processed sooner (default: 0, range: -1000000 to 1000000)
  delay?: number,         // Delay in ms before processing (default: 0, max: 1 year)
  maxAttempts?: number,   // Max retry attempts (default: 3, range: 1-1000)
  backoff?: number,       // Retry backoff delay in ms (default: 1000, max: 1 day)
  ttl?: number,           // Time-to-live in ms (max: 1 year)
  timeout?: number,       // Processing timeout in ms (max: 1 day)
  uniqueKey?: string,     // Deduplication key
  jobId?: string,         // Custom job ID (idempotent)
  dependsOn?: string[],   // Job IDs this job depends on
  tags?: string[],        // Metadata tags
  groupId?: string,       // Job group identifier
  lifo?: boolean,         // Last-in-first-out (default: false)
  removeOnComplete?: boolean, // Auto-remove on completion (default: false)
  removeOnFail?: boolean,     // Auto-remove on failure (default: false)
  durable?: boolean,      // Force immediate disk write, bypassing write buffer (default: false)
  repeat?: {              // Repeat configuration
    every?: number,       //   Repeat interval in ms
    pattern?: string,     //   Cron expression (alternative to every)
    limit?: number,       //   Max repetitions
    count?: number,       //   Current count
    startDate?: number,   //   Don't fire before this timestamp
    endDate?: number,     //   Don't fire after this timestamp
    tz?: string,          //   IANA timezone for pattern
    immediately?: boolean //   Fire once on creation
  },
  // Flow / parent-child (used by FlowProducer):
  parentId?: string,      // Parent job ID
  childrenIds?: string[], // Child job IDs (flow parent)
  failParentOnFailure?: boolean,
  removeDependencyOnFailure?: boolean,
  ignoreDependencyOnFailure?: boolean,
  continueParentOnFailure?: boolean,
  // Advanced options:
  stallTimeout?: number,  // Stall detection timeout in ms (max: 1 day)
  stackTraceLimit?: number, // Cap on stored stack trace lines
  keepLogs?: number,      // Cap on stored log entries
  sizeLimit?: number,     // Max serialized data size for this job
  dedup?: { ttl?: number, extend?: boolean, replace?: boolean }, // Dedup options (uniqueKey carries the id)
  debounceId?: string,    // Debounce identifier
  debounceTtl?: number,   // Debounce window in ms
  timestamp?: number      // Explicit creation timestamp
}
```

The `backoff` field also accepts an object form: `{ type: 'fixed' | 'exponential', delay: number }`.

**Response:**

```typescript
{ ok: true, id: string }  // The generated job ID (UUIDv7)
```

---

#### PUSHB

Batch push multiple jobs to a queue.

**Request:**

```typescript
{
  cmd: 'PUSHB',
  queue: string,
  jobs: Array<{
    name: string,
    data: any,
    priority?: number,
    delay?: number,
    maxAttempts?: number,
    backoff?: number,
    ttl?: number,
    timeout?: number,
    uniqueKey?: string,
    customId?: string,
    dependsOn?: string[],
    tags?: string[],
    groupId?: string,
    lifo?: boolean,
    removeOnComplete?: boolean,
    removeOnFail?: boolean,
    durable?: boolean
  }>
}
```

Each job is validated with the same rules as `PUSH` (option bounds and
`dependsOn` existence). A `dependsOn` entry may also reference the `customId`
of any job in the same batch, so order-independent intra-batch chains work. On
violation the whole batch is rejected with an error naming the offending index
(`jobs[i]: ...`).

**Response:**

```typescript
{ ok: true, ids: string[] }  // Array of generated job IDs
```

---

#### PUSHF

Atomically commit a fully resolved, potentially multi-queue FlowProducer graph.
This is the command used by the Bun package and all six current official SDKs.
Previously published clients may still compose legacy `PUSH`/`UpdateParent`
calls.

**Request:**

```typescript
{
  cmd: 'PUSHF',
  jobs: Array<{
    id: string,             // Final ID; non-empty, no colon
    queue: string,
    input: {
      name: string,
      data: unknown,        // untouched user payload
      dependsOn?: string[],
      parentId?: string,
      childrenIds?: string[],
      // supported ordinary scheduling/retry/failure options
    }
  }>
}
```

The complete graph is validated before mutation: strict runtime types,
duplicate/missing/asymmetric edges, cycles, policy conflicts, 10,000 jobs,
10 MB per job and 64 MB aggregate data. With configured SQLite, all job rows
commit in one immediate transaction before any leaf becomes visible. In
memory-only mode, publication is still atomic but not crash-durable.

**Response:**

```typescript
{ ok: true, data: { jobs: Job[] } }
```

The returned array has exactly one authoritative committed snapshot per input
ID. Any validation, ownership or persistence error returns `{ ok: false,
error }` and publishes no job.

---

#### PULL

Pull the next available job from a queue. Supports optional long polling and lock-based ownership.

**Request:**

```typescript
{
  cmd: 'PULL',
  queue: string,
  timeout?: number,    // Long poll timeout in ms (0-60000, default: 0)
  owner?: string,      // Client identifier for lock-based pull
  lockTtl?: number,    // Lock TTL in ms (default: 30000)
  detach?: boolean     // Don't auto-release the job when this connection closes (CLI usage)
}
```

**Response (without owner):**

```typescript
{ ok: true, job: Job | null }
```

**Response (with owner, includes lock token):**

```typescript
{ ok: true, job: Job | null, token: string | null }
```

The `token` must be passed to `ACK` or `FAIL` to verify ownership.

---

#### PULLB

Batch pull multiple jobs from a queue.

**Request:**

```typescript
{
  cmd: 'PULLB',
  queue: string,
  count: number,       // Number of jobs to pull (1-1000)
  timeout?: number,    // Long poll timeout in ms (0-60000, default: 0), with or without owner
  owner?: string,      // Client identifier for lock-based pull
  lockTtl?: number     // Lock TTL in ms (default: 30000)
}
```

**Response (without owner):**

```typescript
{ ok: true, jobs: Job[] }
```

Results are ordered by `createdAt` ascending (oldest first), with the job ID as
a deterministic tie-breaker, before `offset` and `limit` are applied.

**Response (with owner, includes lock tokens):**

```typescript
{ ok: true, jobs: Job[], tokens: string[] }
```

---

#### ACK

Acknowledge a job as completed.

**Request:**

```typescript
{
  cmd: 'ACK',
  id: string,           // Job ID
  result?: any,         // Optional result data
  token?: string        // Lock token (required if pulled with owner)
}
```

**Response:**

```typescript
{ ok: true }
```

If an exact timeout or retired cron generation already finalized before the
ACK claimed it, the response is a successful no-op rather than a retryable
transport error:

```typescript
{ ok: true, data: { applied: false, reason: 'already-finalized' } }
```

---

#### ACKB

Batch acknowledge multiple jobs.

**Request:**

```typescript
{
  cmd: 'ACKB',
  ids: string[],          // Job IDs
  results?: any[],        // Optional results (same order as ids; if provided, length must match ids)
  tokens?: string[]       // Lock tokens (same order/length as ids; required for leased jobs)
}
```

The broker validates every token before completing any item. A missing or
incorrect token rejects the whole batch and leaves all jobs, locks, and results
unchanged.

**Response:**

```typescript
{ ok: true }
```

A timeout may win after the batch's lease preflight. Live positions still
apply and the broker reports the exact ignored input positions in order:

```typescript
{
  ok: true,
  data: {
    ignoredIds: ['job-id'],
    ignoredIndices: [2]
  }
}
```

Clients must use `ignoredIndices` when IDs repeat. Wrong/missing tokens and
ordinary missing/completed jobs remain errors.

---

#### FAIL

Mark a job as failed. The job will be retried with exponential backoff if it has remaining attempts, otherwise it is moved to the dead-letter queue.

**Request:**

```typescript
{
  cmd: 'FAIL',
  id: string,            // Job ID
  error?: string,        // Error message
  stack?: string[],      // Failure stack trace lines, persisted server-side, capped at job.stackTraceLimit (#74)
  unrecoverable?: boolean, // Skip all remaining retries and fail terminally (straight to DLQ)
  token?: string         // Lock token (required if pulled with owner)
}
```

The optional `stack` is stored on the job and surfaced by `GetJob` and on DLQ entries, so a failed job's stack trace survives a restart.

**Response:**

```typescript
{ ok: true }
```

An exact late generation uses the same successful no-op envelope as `ACK`:

```typescript
{ ok: true, data: { applied: false, reason: 'already-finalized' } }
```

---

### Query Commands

#### GetJob

Retrieve a job by its internal ID.

**Request:**

```typescript
{ cmd: 'GetJob', id: string }
```

**Response:**

```typescript
{ ok: true, job: Job }
```

Returns an error if the job is not found.

---

#### GetState

Get the current state of a job.

**Request:**

```typescript
{ cmd: 'GetState', id: string }
```

**Response:**

```typescript
{ ok: true, id: string, state: string }
```

Possible states: `waiting`, `prioritized`, `delayed`, `active`, `waiting-children`, `completed`, `failed`, or `unknown` (job not found).

---

#### GetResult

Get the stored result of a completed job.

**Request:**

```typescript
{ cmd: 'GetResult', id: string }
```

**Response:**

```typescript
{ ok: true, id: string, result: any }
```

The `result` field is the value passed via `ACK`. It may be `null` or `undefined` if no result was stored or if the result has been evicted from the LRU cache.

---

#### GetJobs

List jobs with filtering and pagination.

**Request:**

```typescript
{
  cmd: 'GetJobs',
  queue: string,
  state?: JobState | JobState[],  // e.g. 'waiting', 'delayed', 'active', 'completed', 'failed', or an array
  limit?: number,        // Max results (default: 100)
  offset?: number,       // Skip N results (default: 0)
  asc?: boolean          // createdAt/id order (default: true)
}
```

**Response:**

```typescript
{ ok: true, jobs: Job[] }
```

Ordering is applied before pagination. Send the same `asc` value on every
request when traversing multiple offset pages.

---

#### GetJobCounts

Get job counts grouped by state for a specific queue.

**Request:**

```typescript
{ cmd: 'GetJobCounts', queue: string }
```

**Response:**

```typescript
{
  ok: true,
  counts: {
    waiting: number,
    prioritized: number,
    delayed: number,
    active: number,
    completed: number,
    failed: number,
    'waiting-children': number,
    paused: number
  }
}
```

When the queue is paused, ready jobs are reported under `paused` instead of `waiting`/`prioritized` (BullMQ semantics).

---

#### GetCountsPerPriority

Get job counts grouped by priority level for a specific queue.

**Request:**

```typescript
{ cmd: 'GetCountsPerPriority', queue: string }
```

**Response:**

```typescript
{ ok: true, queue: string, counts: Record<number, number> }
```

---

#### GetJobByCustomId

Look up a job by its custom ID (the `jobId` field from PUSH).

**Request:**

```typescript
{ cmd: 'GetJobByCustomId', customId: string }
```

**Response:**

```typescript
{ ok: true, job: Job }
```

Returns an error if no job with that custom ID exists.

---

#### Count

Get the total number of jobs in a queue (all states).

**Request:**

```typescript
{ cmd: 'Count', queue: string }
```

**Response:**

```typescript
{ ok: true, count: number }
```

---

#### GetProgress

Get the progress of an active job.

**Request:**

```typescript
{ cmd: 'GetProgress', id: string }
```

**Response:**

```typescript
{ ok: true, progress: number, message: string | null }
```

---

#### GetChildrenValues

Get the return values from all child jobs of a parent job. Used with FlowProducer workflows to retrieve results from completed children.

**Request:**

```typescript
{ cmd: 'GetChildrenValues', id: string }
```

**Response:**

```typescript
{ ok: true, data: { values: Record<string, any> } }
```

Returns an empty `values` object if the job has no children or if an error occurs.

---

### Control Commands

#### Cancel

Cancel a waiting or delayed job.

**Request:**

```typescript
{ cmd: 'Cancel', id: string }
```

**Response:**

```typescript
{ ok: true }
```

---

#### Progress

Update the progress of an active job.

**Request:**

```typescript
{
  cmd: 'Progress',
  id: string,
  progress: number,       // 0-100
  message?: string        // Optional progress message
}
```

**Response:**

```typescript
{ ok: true }
```

---

#### Update

Update the data payload of an existing job.

**Request:**

```typescript
{
  cmd: 'Update',
  id: string,
  data: any              // New job data
}
```

**Response:**

```typescript
{ ok: true }
```

---

#### ChangePriority

Change the priority of a queued job.

**Request:**

```typescript
{
  cmd: 'ChangePriority',
  id: string,
  priority: number,
  lifo?: boolean         // Tie-break ordering among same-priority jobs
}
```

**Response:**

```typescript
{ ok: true }
```

---

#### Promote

Move a delayed job to the waiting state immediately.

**Request:**

```typescript
{ cmd: 'Promote', id: string }
```

**Response:**

```typescript
{ ok: true }
```

---

#### MoveToDelayed

Move an active job back to the delayed state.

**Request:**

```typescript
{
  cmd: 'MoveToDelayed',
  id: string,
  delay: number,         // Delay in ms from now
  token?: string         // Required when the active job has a lock
}
```

**Response:**

```typescript
{ ok: true }
```

---

#### Discard

Discard a job by moving it to the dead-letter queue.

**Request:**

```typescript
{ cmd: 'Discard', id: string, token?: string }
```

When the job has an active lease, `token` must match the current delivery
token. For waiting or otherwise unlocked jobs, the field may be omitted for an
administrative discard.

**Response:**

```typescript
{ ok: true }
```

---

#### WaitJob

Wait for a job to complete. This is event-driven (no polling). Returns immediately if the job is already completed.

**Request:**

```typescript
{
  cmd: 'WaitJob',
  id: string,
  timeout?: number       // Max wait time in ms (default: 30000, max: 600000)
}
```

**Response:**

```typescript
{ ok: true, completed: boolean, result?: any }
```

---

#### Pause

Pause a queue. Workers will stop pulling new jobs.

**Request:**

```typescript
{ cmd: 'Pause', queue: string }
```

**Response:**

```typescript
{ ok: true }
```

---

#### Resume

Resume a paused queue.

**Request:**

```typescript
{ cmd: 'Resume', queue: string }
```

**Response:**

```typescript
{ ok: true }
```

---

#### IsPaused

Check whether a queue is currently paused.

**Request:**

```typescript
{ cmd: 'IsPaused', queue: string }
```

**Response:**

```typescript
{ ok: true, paused: boolean }
```

---

#### Drain

Remove all waiting jobs from a queue.

**Request:**

```typescript
{ cmd: 'Drain', queue: string }
```

**Response:**

```typescript
{ ok: true, count: number }  // Number of jobs removed
```

---

#### Obliterate

Remove all data for a queue (all jobs in all states).

**Request:**

```typescript
{ cmd: 'Obliterate', queue: string }
```

**Response:**

```typescript
{ ok: true }
```

---

#### Clean

Remove jobs older than a grace period, optionally filtered by state.

**Request:**

```typescript
{
  cmd: 'Clean',
  queue: string,
  grace: number,         // Grace period in ms - jobs older than this are removed
  state?: string,        // 'waiting'/'delayed'/'prioritized'/'paused' (queued jobs, the default), 'completed', or 'failed'
  limit?: number         // Max jobs to remove (default: 1000)
}
```

**Response:**

```typescript
{ ok: true, count: number, ids: string[] }  // IDs of the removed jobs
```

---

#### ListQueues

List the names of all known queues.

**Request:**

```typescript
{ cmd: 'ListQueues' }
```

**Response:**

```typescript
{ ok: true, queues: string[] }  // Queue names
```

For per-queue counts use `GetJobCounts` per queue, or the HTTP `GET /queues/summary` endpoint.

---

### DLQ Commands

#### Dlq

Retrieve jobs from the dead-letter queue.

**Request:**

```typescript
{
  cmd: 'Dlq',
  queue: string,
  count?: number,        // Max entries to return (optional)
  filter?: {
    reason?: string,
    olderThan?: number,
    newerThan?: number,
    retriable?: boolean,
    expired?: boolean,
    limit?: number,
    offset?: number
  }
}
```

**Response:**

```typescript
{ ok: true, jobs: Job[], entries: DlqEntry[] }
```

---

#### GetDlqStats

Read aggregate DLQ health for a queue.

```typescript
{ cmd: 'GetDlqStats', queue: string }

{ ok: true, data: { stats: DlqStats } }
```

---

#### RetryDlq

Retry jobs from the dead-letter queue (move them back to waiting).

**Request:**

```typescript
{
  cmd: 'RetryDlq',
  queue: string,
  jobId?: string,        // Retry a specific job (optional; omit to retry all)
  count?: number,        // Cap the number of entries retried (omit = retry all)
  filter?: DlqFilter     // Retry only matching entries
}
```

**Response:**

```typescript
{ ok: true, count: number }  // Number of jobs retried
```

---

#### PurgeDlq

Clear all jobs from the dead-letter queue.

**Request:**

```typescript
{ cmd: 'PurgeDlq', queue: string }
```

**Response:**

```typescript
{ ok: true, count: number }  // Number of jobs purged
```

---

#### RemoveDlqJob

Permanently delete one failed job without retrying it.

**Request:**

```typescript
{ cmd: 'RemoveDlqJob', queue: string, jobId: string }
```

**Response:**

```typescript
{ ok: true, data: { removed: boolean } }
```

`removed: false` is an idempotent miss. Persistence or handler failures return
the normal `{ ok: false, error }` response and must not be interpreted as a
missing entry.

---

#### RetryCompleted

Re-queue completed jobs back to waiting state.

**Request:**

```typescript
{
  cmd: 'RetryCompleted',
  queue: string,
  id?: string,           // Retry a specific job (optional; omit to retry all)
  count?: number,        // Non-negative cap
  timestamp?: number     // completedAt must be <= this epoch-ms cutoff
}
```

**Response:**

```typescript
{ ok: true, count: number }
```

---

### Cron Commands

#### Cron

Create or update a cron/repeating job schedule.

**Request:**

```typescript
{
  cmd: 'Cron',
  name: string,             // Unique cron job name
  jobName?: string,         // First-class name assigned to spawned jobs
  queue: string,            // Target queue
  data: any,                // Job data payload
  schedule?: string,        // Cron expression (e.g., '*/5 * * * *')
  repeatEvery?: number,     // Repeat interval in ms (alternative to schedule)
  priority?: number,        // Job priority
  maxLimit?: number,        // Max executions
  timezone?: string,        // IANA timezone (e.g., 'Europe/Rome', 'America/New_York')
  uniqueKey?: string,       // Deduplication key for cron-spawned jobs
  dedup?: { ttl?: number, extend?: boolean, replace?: boolean }, // Dedup options for spawned jobs
  skipMissedOnRestart?: boolean, // Skip missed runs on restart instead of executing them (default true)
  immediately?: boolean,    // Fire once on creation, then continue on schedule (default false)
  skipIfNoWorker?: boolean, // Skip a tick when no worker is registered (default false)
  preventOverlap?: boolean, // Skip a tick while the previous run is still pending/active (default true)
  jobOptions?: {            // Per-job options applied to every generated job
    maxAttempts?: number,
    backoff?: number | { type: 'fixed' | 'exponential', delay: number },
    timeout?: number,
    delay?: number,
    stallTimeout?: number,
    removeOnComplete?: boolean,
    removeOnFail?: boolean
  }
}
```

**Response:**

```typescript
{
  ok: true,
  cron: {
    name: string,
    jobName: string,
    queue: string,
    schedule: string | null,
    repeatEvery: number | null,
    nextRun: number,
    executions: number,
    maxLimit: number | null,
    timezone: string | null,
    priority: number
  }
}
```

---

#### CronDelete

Delete a cron job schedule by name.

**Request:**

```typescript
{ cmd: 'CronDelete', name: string }
```

**Response:**

```typescript
{ ok: true }
```

---

#### CronList

List all registered cron job schedules.

**Request:**

```typescript
{ cmd: 'CronList' }
```

**Response:**

```typescript
{
  ok: true,
  crons: Array<{
    name: string,
    jobName: string,
    queue: string,
    schedule: string | null,
    repeatEvery: number | null,
    nextRun: number,
    executions: number,
    maxLimit: number | undefined,
    timezone: string | undefined
  }>
}
```

---

#### CronGet

Get a single cron job by name.

**Request:**

```typescript
{ cmd: 'CronGet', name: string }
```

**Response:**

```typescript
{
  ok: true,
  cron: {
    name: string,
    jobName: string,
    queue: string,
    schedule: string | null,
    repeatEvery: number | null,
    nextRun: number,
    executions: number,
    maxLimit: number | undefined,
    timezone: string | undefined
  }
}
```

Returns an error if the cron job is not found.

---

### Monitoring Commands

#### Ping

Connection health check.

**Request:**

```typescript
{ cmd: 'Ping' }
```

**Response:**

```typescript
{ ok: true, data: { pong: true, time: number } }
```

---

#### Hello

Protocol version negotiation and server capability discovery. See the [Protocol Negotiation](#protocol-negotiation-hello) section above for details.

**Request:**

```typescript
{
  cmd: 'Hello',
  protocolVersion: number,
  capabilities?: Array<'pipelining' | 'separate-job-name'>
}
```

**Response:**

```typescript
{
  ok: true,
  protocolVersion: number,
  capabilities: Array<'pipelining' | 'separate-job-name'>,
  server: 'bunqueue',
  version: string
}
```

---

#### Stats

Get high-level server statistics.

**Request:**

```typescript
{ cmd: 'Stats' }
```

**Response:**

```typescript
{
  ok: true,
  stats: {
    waiting: number,      // Waiting jobs
    active: number,       // Active jobs
    delayed: number,      // Delayed jobs
    dlq: number,          // Dead-letter queue size
    completed: number,    // Completed count
    failed: number,       // Failed (totalFailed) count
    uptime: number,       // Server uptime in ms
    pushPerSec: number,   // Push throughput
    pullPerSec: number    // Pull throughput
  }
}
```

---

#### Metrics

Get detailed server metrics. The request without queue fields retains the
legacy broker-wide response shown below.

**Request:**

```typescript
{ cmd: 'Metrics' }
```

**Response:**

```typescript
{
  ok: true,
  metrics: {
    totalPushed: number,
    totalPulled: number,
    totalCompleted: number,
    totalFailed: number,
    avgLatencyMs: number,
    avgProcessingMs: number,
    memoryUsageMb: number,
    sqliteSizeMb: number,
    activeConnections: number
  }
}
```

For durable queue-scoped minute metrics, send:

```typescript
{
  cmd: 'Metrics',
  queue: 'emails',
  type: 'completed', // or 'failed'
  start: 0,          // newest bucket index
  end: -1            // through the oldest retained bucket
}
```

```typescript
{
  ok: true,
  data: {
    meta: { count: number, prevTS: number, prevCount: number },
    data: number[], // one-minute buckets, newest first
    count: number   // bucket count before pagination
  }
}
```

#### TrimEvents

Keep only the newest lifecycle events for one queue. The response reports the
exact removed count, so repeating the request at the same length returns zero.

```typescript
{ cmd: 'TrimEvents', queue: 'emails', maxLength: 1000 }
```

```typescript
{ ok: true, data: { removed: number } }
```

---

#### Prometheus

Get metrics in Prometheus text exposition format.

**Request:**

```typescript
{ cmd: 'Prometheus' }
```

**Response:**

```typescript
{ ok: true, data: { metrics: string } }
```

---

#### StorageStatus

Get the storage/disk health status. Reports whether the disk is full or has errors.

**Request:**

```typescript
{ cmd: 'StorageStatus' }
```

**Response:**

```typescript
{
  ok: true,
  data: {
    diskFull: boolean,         // Whether the disk is full
    error: string | null,      // Error message if any
    since: number | null       // Timestamp when the issue started (ms since epoch)
  }
}
```

---

#### Heartbeat

Send a heartbeat for a registered worker (keeps the worker registration alive).

**Request:**

```typescript
{
  cmd: 'Heartbeat',
  id: string,            // Worker ID
  activeJobs?: number,   // Optional stats update
  processed?: number,
  failed?: number
}
```

**Response:**

```typescript
{ ok: true, data: { ok: true } }
```

---

#### JobHeartbeat

Send a heartbeat for an active job (prevents stall detection from marking it as stalled). Also renews the lock if a token is provided.

**Request:**

```typescript
{
  cmd: 'JobHeartbeat',
  id: string,           // Job ID
  token?: string,       // Lock token for renewal
  duration?: number     // Lock renewal duration in ms (with token: extends the lock)
}
```

**Response:**

```typescript
{ ok: true, data: { ok: true } }
```

---

#### JobHeartbeatB

Batch job heartbeat for multiple active jobs.

**Request:**

```typescript
{
  cmd: 'JobHeartbeatB',
  ids: string[],         // Job IDs
  tokens?: string[]      // Lock tokens (same order as ids)
}
```

**Response:**

```typescript
{ ok: true, data: { ok: true, count: number } }
```

---

### Worker Commands

#### RegisterWorker

Register a worker with the server for monitoring.

**Request:**

```typescript
{
  cmd: 'RegisterWorker',
  name: string,
  queues: string[],      // Queues this worker processes
  concurrency?: number,
  workerId?: string,     // Reuse a stable worker ID across reconnects
  hostname?: string,
  pid?: number,
  startedAt?: number
}
```

**Response:**

```typescript
{
  ok: true,
  data: {
    workerId: string,
    name: string,
    queues: string[],
    concurrency: number,
    hostname: string | undefined,
    pid: number | undefined,
    status: 'active',
    registeredAt: number,
    lastSeen: number,
    activeJobs: number,
    processedJobs: number,
    failedJobs: number,
    currentJob: string | null
  }
}
```

The registration is tied to the TCP connection: the server auto-unregisters the worker when the connection closes.

---

#### UnregisterWorker

Remove a worker registration.

**Request:**

```typescript
{ cmd: 'UnregisterWorker', workerId: string }
```

**Response:**

```typescript
{ ok: true, data: { removed: true } }
```

---

#### ListWorkers

List all registered workers and their stats.

**Request:**

```typescript
{ cmd: 'ListWorkers' }
```

**Response:**

```typescript
{
  ok: true,
  data: {
    workers: Array<{
      id: string,
      name: string,
      queues: string[],
      concurrency: number,
      hostname: string | undefined,
      pid: number | undefined,
      status: 'active' | 'stale',   // stale = no heartbeat within WORKER_TIMEOUT_MS (default 30s)
      registeredAt: number,
      lastSeen: number,
      activeJobs: number,
      processedJobs: number,
      failedJobs: number,
      currentJob: string | null,
      uptime: number
    }>,
    stats: object          // Aggregated worker stats
  }
}
```

---

### Webhook Commands

#### AddWebhook

Register a webhook to receive event notifications. URLs are validated to prevent SSRF (localhost, private IPs, and cloud metadata endpoints are blocked).

**Request:**

```typescript
{
  cmd: 'AddWebhook',
  url: string,           // Webhook URL (https required for production)
  events: string[],      // Event types to subscribe to
  queue?: string,        // Filter by queue (optional)
  secret?: string        // Signing secret for payload verification
}
```

**Response:**

```typescript
{
  ok: true,
  data: {
    webhookId: string,
    url: string,
    events: string[],
    queue: string | undefined,
    createdAt: number
  }
}
```

---

#### RemoveWebhook

Remove a registered webhook.

**Request:**

```typescript
{ cmd: 'RemoveWebhook', webhookId: string }
```

**Response:**

```typescript
{ ok: true, data: { removed: true } }
```

---

#### ListWebhooks

List all registered webhooks.

**Request:**

```typescript
{ cmd: 'ListWebhooks' }
```

**Response:**

```typescript
{
  ok: true,
  data: {
    webhooks: Array<{
      id: string,
      url: string,
      events: string[],
      queue: string | undefined,
      createdAt: number,
      lastTriggered: number | null,
      successCount: number,
      failureCount: number,
      enabled: boolean
    }>,
    stats: object
  }
}
```

---

### Rate Limiting Commands

#### RateLimit

Set a rate limit on a queue: `limit` jobs per `duration` ms (default 1000, so jobs per second).

**Request:**

```typescript
{
  cmd: 'RateLimit',
  queue: string,
  limit: number,         // Max jobs per window
  duration?: number,     // Window in ms (default 1000)
  ttl?: number           // Auto-expiry in ms: the server clears the limit itself
}
```

Invalid `duration` or `ttl` values (non-finite or not positive) fall back to the defaults (1 second window, permanent limit) instead of failing. Servers older than 2.8.35 ignore both optional fields.

**Response:**

```typescript
{ ok: true }
```

---

#### RateLimitClear

Remove the rate limit from a queue.

**Request:**

```typescript
{ cmd: 'RateLimitClear', queue: string }
```

**Response:**

```typescript
{ ok: true }
```

---

#### SetConcurrency

Set a concurrency limit on a queue (max concurrent active jobs).

**Request:**

```typescript
{
  cmd: 'SetConcurrency',
  queue: string,
  limit: number
}
```

**Response:**

```typescript
{ ok: true }
```

---

#### ClearConcurrency

Remove the concurrency limit from a queue.

**Request:**

```typescript
{ cmd: 'ClearConcurrency', queue: string }
```

**Response:**

```typescript
{ ok: true }
```

---

#### GetQueueLimits

Read the live rate/concurrency configuration and saturation state.

```typescript
{ cmd: 'GetQueueLimits', queue: string, maxJobs?: number }

{
  ok: true,
  data: {
    limits: {
      rateLimit: { max: number, duration: number } | null,
      rateLimitTtl: number,          // -2 when no rate limit exists
      concurrencyLimit: number | null,
      maxed: boolean
    }
  }
}
```

---

#### Deduplication Introspection

```typescript
{ cmd: 'GetDeduplicationJobId', queue: string, deduplicationId: string }
// -> { ok: true, data: { jobId: string | null } }

{ cmd: 'RemoveDeduplicationKey', queue: string, deduplicationId: string }
// -> { ok: true, data: { count: number } }

{ cmd: 'RemoveJobDeduplicationKey', id: string }
// -> { ok: true, data: { removed: boolean } }
```

The job-owned form removes a key only when the requested job is still its
registered owner.

---

#### MoveToWaitingChildren

```typescript
{ cmd: 'MoveToWaitingChildren', id: string, token?: string }
// -> { ok: true, data: { moved: true } }
```

The job must be active. The transition releases its active resources and
persists the parked state. If the job has a lock, `token` must match it.

---

### Log Commands

#### AddLog

Add a log entry to a job.

**Request:**

```typescript
{
  cmd: 'AddLog',
  id: string,            // Job ID
  message: string,       // Log message
  level?: 'info' | 'warn' | 'error'  // Log level (default: 'info')
}
```

**Response:**

```typescript
{ ok: true, data: { added: true } }
```

---

#### GetLogs

Get all log entries for a job.

**Request:**

```typescript
{ cmd: 'GetLogs', id: string, start?: number, end?: number }  // start/end: inclusive pagination indexes
```

**Response:**

```typescript
{ ok: true, data: { logs: Array<{ message: string, level: string, timestamp: number }>, count: number } }
```

`count` is the total number of stored log entries (before pagination). Logs are capped at 100 entries per job.

---

### Lock Commands

#### ExtendLock

Extend the lock TTL on an active job (lock-based processing).

**Request:**

```typescript
{ cmd: 'ExtendLock', id: string, duration: number, token?: string }
```

**Response:** `{ ok: true }` or `{ ok: false, error: 'Lock not found or invalid token' }`

---

#### ExtendLocks

Batch variant of `ExtendLock` (positional arrays, same order).

**Request:**

```typescript
{ cmd: 'ExtendLocks', ids: string[], tokens: string[], durations: number[] }
```

**Response:**

```typescript
{ ok: true, count: number }  // Number of locks successfully extended
```

---

### More Job Commands

#### ChangeDelay

Change the delay of a delayed job (recomputes `runAt`).

**Request:** `{ cmd: 'ChangeDelay', id: string, delay: number, token?: string }`

`token` is required when the job is active and currently leased. Worker
processor Job objects forward their current delivery token automatically;
unlocked administrative transitions may omit it.

**Response:** `{ ok: true }`

---

#### MoveToWait

Move a job back to `waiting`, dispatching by current state: `active` is released back to the queue, `delayed` is promoted, `failed` is retried from the DLQ, `waiting`/`prioritized` is a no-op success.

**Request:** `{ cmd: 'MoveToWait', id: string, token?: string }`

**Response:** `{ ok: true }`

For an active locked job, `token` is required and must match the current lease.
An active job without a lock can still be moved administratively.

---

#### PromoteJobs

Promote all (or up to `count`) delayed jobs in a queue to waiting.

**Request:** `{ cmd: 'PromoteJobs', queue: string, count?: number }`

**Response:** `{ ok: true, count: number }`

---

#### ClearLogs

Clear a job's log entries, optionally keeping the most recent N.

**Request:** `{ cmd: 'ClearLogs', id: string, keepLogs?: number }`

**Response:** `{ ok: true }`

---

#### SetWebhookEnabled

Enable or disable a webhook without deleting it.

**Request:** `{ cmd: 'SetWebhookEnabled', id: string, enabled: boolean }`

**Response:** `{ ok: true }` or `{ ok: false, error: 'Webhook not found' }`

---

#### CompactMemory

Trigger internal memory compaction.

**Request:** `{ cmd: 'CompactMemory' }`

**Response:** `{ ok: true }`

---

### Flow Dependency Commands

Used by FlowProducer for parent/child job graphs.

#### UpdateParent

**Request:** `{ cmd: 'UpdateParent', childId: string, parentId: string }`

**Response:** `{ ok: true }`

This is a compatibility command for legacy multi-request flow creation. If the
parent already declares `childId`, only the child's temporary parent marker is
updated; the parent may be active or terminal and its state/topology is not
rewritten. A queued, active, completed, DLQ, or `removeOnComplete`-tombstoned
child is accepted when that declared edge is consistent. Persisted job/DLQ data
and any failure-outbox key move atomically. A genuinely new edge still requires
a queued parent; conflicting ownership, self-links, and undeclared missing
nodes fail.

#### GetFailedChildrenValues

**Request:** `{ cmd: 'GetFailedChildrenValues', id: string }`

**Response:** `{ ok: true, values: Record<string, any> }`

#### GetIgnoredChildrenFailures

**Request:** `{ cmd: 'GetIgnoredChildrenFailures', id: string }`

**Response:** `{ ok: true, values: Record<string, any> }`

#### RemoveChildDependency

**Request:** `{ cmd: 'RemoveChildDependency', id: string }`

**Response:** `{ ok: true, removed: boolean }`

#### RemoveUnprocessedChildren

**Request:** `{ cmd: 'RemoveUnprocessedChildren', id: string }`

**Response:** `{ ok: true }`

---

### Queue Config Commands

#### SetStallConfig / GetStallConfig

Per-queue stall detection configuration. Numeric fields: `stallInterval`, `maxStalls`, `gracePeriod` (numeric strings are coerced, non-numeric values are dropped).

**Request:**

```typescript
{ cmd: 'SetStallConfig', queue: string, config: { stallInterval?: number, maxStalls?: number, gracePeriod?: number } }
{ cmd: 'GetStallConfig', queue: string }
```

**Response:** `{ ok: true }` for set, `{ ok: true, config: {...} }` for get.

#### SetDlqConfig / GetDlqConfig

Per-queue DLQ configuration. Numeric fields: `autoRetryInterval`, `maxAutoRetries`, `maxAge`, `maxEntries`.

**Request:**

```typescript
{ cmd: 'SetDlqConfig', queue: string, config: { autoRetry?: boolean, autoRetryInterval?: number, maxAutoRetries?: number, maxAge?: number | null, maxEntries?: number } }
{ cmd: 'GetDlqConfig', queue: string }
```

**Response:** `{ ok: true }` for set, `{ ok: true, config: {...} }` for get.

---

### Dashboard Commands

Aggregated read-only snapshots for dashboards (same data as the HTTP `/dashboard` endpoints).

#### DashboardOverview

**Request:** `{ cmd: 'DashboardOverview' }`

**Response:** `{ ok: true, data: { stats, throughput, latency, memory, collections, workers, crons, storage, timestamp } }`

#### DashboardQueues

**Request:** `{ cmd: 'DashboardQueues' }`

**Response:** `{ ok: true, data: { queues: Array<{ name, waiting, prioritized, delayed, active, dlq, paused }>, timestamp } }`

#### DashboardQueue

**Request:** `{ cmd: 'DashboardQueue', queue: string, includeJobs?: boolean, jobsLimit?: number }` (`jobsLimit` default 10, max 50)

**Response:** `{ ok: true, data: { name, counts, paused, priorityCounts, dlqPreview, jobs?, timestamp } }`

---

## Queue Name Validation

Queue names must satisfy the following constraints:

- Not empty and at most 256 characters
- Only alphanumeric characters, underscores, dashes, dots, and colons: `[a-zA-Z0-9_\-.:]+`

## Job Data Limits

Job data payloads are limited to **10 MB** when serialized.

## Command Summary

| Category | Command | Description |
|----------|---------|-------------|
| **Core** | `PUSH` | Add a job to a queue |
| | `PUSHB` | Batch push multiple jobs |
| | `PULL` | Pull next job (supports long poll and locks) |
| | `PULLB` | Batch pull jobs |
| | `ACK` | Acknowledge job completion |
| | `ACKB` | Batch acknowledge |
| | `FAIL` | Mark job as failed |
| **Query** | `GetJob` | Get job by ID |
| | `GetState` | Get job state |
| | `GetResult` | Get job result |
| | `GetJobs` | List jobs with filtering |
| | `GetJobCounts` | Count jobs by state |
| | `GetCountsPerPriority` | Count jobs by priority |
| | `GetJobByCustomId` | Look up job by custom ID |
| | `Count` | Total job count for a queue |
| | `GetProgress` | Get job progress |
| | `GetChildrenValues` | Get child job return values |
| | `GetQueueLimits` | Read live queue rate/concurrency status |
| | `GetDeduplicationJobId` | Resolve a queue-scoped deduplication key |
| **Control** | `Cancel` | Cancel a job |
| | `Progress` | Update job progress |
| | `Update` | Update job data |
| | `ChangePriority` | Change job priority |
| | `Promote` | Move delayed job to waiting |
| | `MoveToDelayed` | Move active job to delayed |
| | `MoveToWaitingChildren` | Park an active job for children |
| | `ChangeDelay` | Change a delayed job's delay |
| | `MoveToWait` | Move a job back to waiting |
| | `PromoteJobs` | Promote all delayed jobs in a queue |
| | `Discard` | Move job to DLQ |
| | `WaitJob` | Wait for job completion |
| | `ExtendLock` | Extend a job lock |
| | `ExtendLocks` | Extend job locks (batch) |
| | `RemoveDeduplicationKey` | Release a queue-scoped deduplication key |
| | `RemoveJobDeduplicationKey` | Release only a job-owned key |
| | `Pause` | Pause a queue |
| | `Resume` | Resume a queue |
| | `IsPaused` | Check if queue is paused |
| | `Drain` | Remove all waiting jobs |
| | `Obliterate` | Remove all queue data |
| | `Clean` | Remove old jobs |
| | `ListQueues` | List all queues |
| **DLQ** | `Dlq` | Get DLQ entries |
| | `GetDlqStats` | Get aggregate DLQ statistics |
| | `RetryDlq` | Retry DLQ jobs |
| | `PurgeDlq` | Clear DLQ |
| | `RemoveDlqJob` | Permanently delete one DLQ job |
| | `RetryCompleted` | Re-queue completed jobs |
| **Cron** | `Cron` | Create/update cron schedule |
| | `CronDelete` | Delete cron schedule |
| | `CronList` | List cron schedules |
| | `CronGet` | Get cron schedule by name |
| **Monitoring** | `Ping` | Health check |
| | `Hello` | Protocol negotiation |
| | `Stats` | Server statistics |
| | `Metrics` | Detailed metrics |
| | `TrimEvents` | Trim one queue's lifecycle journal |
| | `Prometheus` | Prometheus-format metrics |
| | `StorageStatus` | Get storage/disk health status |
| | `Heartbeat` | Worker heartbeat |
| | `JobHeartbeat` | Job heartbeat (stall prevention) |
| | `JobHeartbeatB` | Batch job heartbeat |
| **Workers** | `RegisterWorker` | Register a worker |
| | `UnregisterWorker` | Unregister a worker |
| | `ListWorkers` | List workers |
| **Webhooks** | `AddWebhook` | Register a webhook |
| | `RemoveWebhook` | Remove a webhook |
| | `ListWebhooks` | List webhooks |
| | `SetWebhookEnabled` | Enable/disable a webhook |
| **Rate** | `RateLimit` | Set queue rate limit |
| | `RateLimitClear` | Clear queue rate limit |
| | `SetConcurrency` | Set queue concurrency limit |
| | `ClearConcurrency` | Clear concurrency limit |
| **Config** | `SetStallConfig` | Set per-queue stall config |
| | `GetStallConfig` | Get per-queue stall config |
| | `SetDlqConfig` | Set per-queue DLQ config |
| | `GetDlqConfig` | Get per-queue DLQ config |
| **Logs** | `AddLog` | Add job log entry |
| | `GetLogs` | Get job logs |
| | `ClearLogs` | Clear job logs |
| **Flow** | `UpdateParent` | Update a child's parent reference |
| | `GetFailedChildrenValues` | Failed children values |
| | `GetIgnoredChildrenFailures` | Ignored children failures |
| | `RemoveChildDependency` | Remove a child's parent dependency |
| | `RemoveUnprocessedChildren` | Remove unprocessed children |
| **Dashboard** | `DashboardOverview` | Aggregated dashboard snapshot |
| | `DashboardQueues` | All queues with stats |
| | `DashboardQueue` | Single queue detail |
| **System** | `CompactMemory` | Trigger memory compaction |
| **Auth** | `Auth` | Authenticate connection |

:::tip[Related]
- [HTTP API Reference](/api/http/) - REST API alternative
- [TypeScript Types](/api/types/) - Type definitions
- [TCP Protocol Architecture](/architecture/tcp-protocol/) - Protocol internals
:::
