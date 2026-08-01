# Store-and-Forward & BullMQ Compatibility

> **Category:** Edge / Integration · **Source:** `src/client/forwarder.ts`, `src/client/queue/bullmqCompat.ts`

## Purpose

This module groups two unrelated client-side concerns that share the "compatibility / integration" theme.

1. **Store-and-Forward** (`forwarder.ts`): drains jobs from a local (typically embedded SQLite) queue to a remote bunqueue server over TCP/TLS. Built for the edge/gateway pattern — a device buffers work locally while offline, then ships it to a central broker when the uplink is up. A remote failure does not lose work: the forward is modeled as a *local* job, so a failed push triggers the local retry/backoff/DLQ path. Re-forwards are idempotent thanks to a deterministic remote `jobId`.

2. **BullMQ v5 compatibility shims** (`bullmqCompat.ts`): a thin adapter exposing BullMQ-named read APIs (`getPrioritized`, `getWaitingChildren`, `getJobDependencies`, …) on the [Client SDK: Queue](./client-queue-sdk.md), so code written against BullMQ can read prioritized jobs, waiting-children, and parent/child dependency views with minimal changes. Several of these are partial or embedded-only — see [Edge Cases & Failure Modes](#edge-cases--failure-modes).

## Responsibilities & Scope

**Owns:**

- `Forwarder` — the lifecycle (worker + remote queue) that moves local jobs to a remote server, the deterministic remote `jobId` scheme, and `forwarded` / `error` event emission.
- BullMQ-compatible read shims: prioritized-job listing/count, waiting-children listing/count, and dependency (parent → children results) listing/count.

**Does NOT own (delegated elsewhere):**

- The actual local consumption loop, retry/backoff, and DLQ routing — owned by [Client SDK: Worker](./client-worker-sdk.md) and the [Dead Letter Queue](./dead-letter-queue.md). The Forwarder is just a `Worker` whose processor pushes to a remote queue.
- The remote push transport (pooling, framing, reconnect) — owned by [Client Transport](./client-transport.md) and a remote `Queue` instance injected as `RemoteQueueCtor`.
- Server-side custom-id dedupe that backs idempotency — owned by [Deduplication & Unique Jobs](./deduplication-and-unique.md).
- The underlying dependency/child-result storage read by the BullMQ shims — owned by [FlowProducer & Job Dependencies](./flow-producer.md) and the shared [Core Queue Engine](./core-queue-engine.md) (`getSharedManager()`).

## Dependencies

**Internal:**

- `./worker` (`Worker`) — the forwarder runs a single `Worker<unknown, ForwardedInfo>` over the source queue (`forwarder.ts:16`, `forwarder.ts:91`).
- `./types` — `ConnectionOptions`, `Job`, `QueueOptions` (`forwarder.ts:17`).
- `../manager` (`getSharedManager`) — embedded job/result lookups for the BullMQ shims (`bullmqCompat.ts:6`).
- `../types` (`toPublicJob`) and `../../domain/types/job` (`jobId`) — public-job conversion and id branding (`bullmqCompat.ts:9-10`).
- A `RemoteQueueCtor` (the `Queue` class itself) is injected by `Queue.forward()` to avoid an import cycle (`queue.ts:602`).

**External / runtime:** Node `events` (`EventEmitter`) for `Forwarder` (`forwarder.ts:15`). No other external runtime dependencies; the remote transport reuses Bun's TCP socket layer via the injected `Queue`.

## Public Interface

### Store-and-Forward (`forwarder.ts`)

Entry point is `Queue.forward()` (`queue.ts:591`), which constructs a `Forwarder`:

```typescript
// src/client/queue/queue.ts:591
forward(options: ForwardOptions): Forwarder
```

```typescript
// src/client/forwarder.ts:20
export interface ForwardOptions {
  to: ConnectionOptions;     // remote server connection (host/port/tls/token…)
  queue?: string;            // remote queue name (default: same as source)
  concurrency?: number;      // parallel forwards (default: 4)
  durable?: boolean;         // push to remote with durable:true (server-side fsync)
}
```

```typescript
// src/client/forwarder.ts:32
export interface ForwardSource {
  name: string;              // logical source queue name
  queueKey: string;          // prefixKey + name — used in the deterministic jobId
  prefixKey?: string;
  embedded: boolean;
  dataPath?: string;
  connection?: ConnectionOptions;
}

// src/client/forwarder.ts:54
export type RemoteQueueCtor = new (name: string, opts: QueueOptions) => RemoteQueueLike;

// src/client/forwarder.ts:57
export interface ForwardedInfo {
  id: string;        // local job id
  remoteId: string;  // deterministic remote id: fwd:<queueKey>:<id>
  name: string;      // job name
}

// src/client/forwarder.ts:70
export class Forwarder extends EventEmitter {
  constructor(source: ForwardSource, options: ForwardOptions, RemoteQueue: RemoteQueueCtor);
  async close(): Promise<void>;   // idempotent
}
```

`Forwarder` and `ForwardOptions` / `ForwardedInfo` are re-exported from the client entry (`src/client/index.ts:46-47`).

**Events emitted** (`forwarder.ts:71-72`):

- `forwarded` — `(info: ForwardedInfo)` — emitted after a successful remote push.
- `error` — `(error: Error)` — emitted only when at least one `error` listener is attached (see failure modes).

### BullMQ Compatibility (`bullmqCompat.ts`)

These are free functions taking a `BullMQContext` (`bullmqCompat.ts:12`); they are surfaced as `Queue` methods (`queue.ts:526-567`):

```typescript
// src/client/queue/bullmqCompat.ts
export function getPrioritized<T>(ctx, start = 0, end = -1): Promise<Job<T>[]>          // :38
export async function getPrioritizedCount<T>(ctx): Promise<number>                      // :43
export async function getWaitingChildren<T>(ctx, start = 0, end = -1): Promise<Job<T>[]> // :49
export async function getWaitingChildrenCount<T>(ctx): Promise<number>                   // :90
export function getDependencies(ctx, parentId, type?, start?, end?): Promise<{…}>        // :73
export async function getJobDependencies(ctx, id, opts?): Promise<JobDependencies>       // :112
export async function getJobDependenciesCount(ctx, id, opts?): Promise<JobDependenciesCount> // :143
```

Public `Queue` methods wiring to them: `getJobDependencies` / `getJobDependenciesCount` / `getDependencies` (`queue.ts:526-539`), `getPrioritized` / `getPrioritizedCount` (`queue.ts:542-561`), `getWaitingChildren` / `getWaitingChildrenCount` (`queue.ts:562-567`).

No HTTP endpoints or CLI commands are defined by this module. The forwarder
reuses the standard `PULL`/`ACK`/`PUSH` path through its `Worker` and remote
`Queue`; dependency reads reuse `GetJob`, `GetResult`, and `GetJobs`, while the
state transition uses `MoveToWaitingChildren`.

## Data Models

See [data-model](../data-model.md) for full definitions. Most relevant shapes:

```typescript
// src/client/types.ts:61
interface GetDependenciesOpts {
  processed?: { cursor?: number; count?: number };
  unprocessed?: { cursor?: number; count?: number };
}

// src/client/types.ts:67
interface JobDependencies {
  processed: Record<string, unknown>;   // keyed `${queue}:${childId}` → child result
  unprocessed: string[];                // child ids without a result yet
  nextProcessedCursor?: number;
  nextUnprocessedCursor?: number;
}

// src/client/types.ts:75
interface JobDependenciesCount { processed: number; unprocessed: number; }

// src/client/types.ts:403 — relevant fields for forward({ to })
interface ConnectionOptions {
  host?: string; port?: number; socketPath?: string;
  tls?: boolean | ClientTlsOptions; token?: string; poolSize?: number; /* … */
}
```

The deterministic remote id is plain string-formatted: `` `fwd:${source.queueKey}:${job.id}` `` (`forwarder.ts:94`). `queueKey` is `prefixKey + name` (`queue.ts:67`), so the remote id is namespaced by the source queue's full key.

## Business Logic / Control Flow

### Store-and-forward (`Forwarder` construction, `forwarder.ts:82-128`)

1. **Build the remote sink.** The constructor instantiates the injected `RemoteQueue` against `options.queue ?? source.name` with `embedded: false`, `connection: options.to`, and `autoBatch: { enabled: false }` — auto-batching is disabled so each forward is an individual push (`forwarder.ts:85-89`).
2. **Build the local consumer.** A `Worker` is created on `source.name` with the source's mode (`embedded`, `dataPath`, `connection`, `prefixKey`) and `concurrency: options.concurrency ?? 4` (`forwarder.ts:91`, `forwarder.ts:110-116`). This worker drains the local queue.
3. **Per-job processor** (`forwarder.ts:93-108`): compute `remoteId = fwd:<queueKey>:<localId>`, then `remote.add(job.name, job.data, { jobId: remoteId, …priority, …durable })`. `priority` is forwarded only when set; `durable: true` only when `options.durable` (`forwarder.ts:95-99`).
4. **Emit `forwarded`.** After a successful remote push, the processor emits `forwarded` inside a `try/catch` (`forwarder.ts:103-107`). A throwing user listener is swallowed: the forward already succeeded remotely, so letting the listener error propagate would fail an already-done local job and trigger a spurious local retry.
5. **Return `info`** — the worker stores `ForwardedInfo` as the local job result.

### Failure path

A failed `remote.add` throws inside the processor → the local `Worker` marks the local job failed → standard local retry/backoff applies, and on exhausting attempts the job goes to the local [DLQ](./dead-letter-queue.md). Nothing is lost while the uplink is down. The `Forwarder` separately listens to the worker's `failed` and `error` events purely for observability (`forwarder.ts:122-127`).

### BullMQ shims

- **`getPrioritized` / `getPrioritizedCount`** (`bullmqCompat.ts`): the list delegates to `getJobsAsync({ state: 'prioritized', … })`; `end=-1` exhausts every TCP page. The count reads the authoritative `prioritized` counter instead of deriving a total from a bounded list.
- **`getWaitingChildren`** queries the dedicated `waiting-children` state through `getJobsAsync`, so embedded and TCP paths share ordering and pagination.
- **`getDependencies`** resolves every child through `getFlowDependencies`, uses the child's actual queue in each key, sorts both result sets deterministically, applies `type` filtering, then slices the inclusive `start`/`end` range.
- **`getJobDependencies`** uses the same authoritative graph and independently applies `processed` and `unprocessed` cursor/count windows, returning a next cursor of `0` at the end.
- **`getJobDependenciesCount`** reads the same graph and returns total processed/unprocessed cardinalities.

## Concurrency & Locking

The Forwarder holds no locks of its own. Concurrency is the `Worker`'s `concurrency` (default 4, `forwarder.ts:115`): up to N local jobs forward in parallel, each an independent pull/process/ack cycle. Lock-based ownership, heartbeats, and stall detection are entirely the `Worker`'s responsibility — see [Concurrency & Locking](./concurrency-and-locking.md) and [Client SDK: Worker](./client-worker-sdk.md).

`close()` is guarded by a `closed` flag (`forwarder.ts:131-133`) so concurrent/duplicate calls are safe; it closes the worker first, then the remote queue (`forwarder.ts:134-135`).

## Edge Cases & Failure Modes

- **Idempotent re-forwards (bounded).** Every push carries `jobId = fwd:<queueKey>:<localId>`. The server dedupes custom job ids, so a re-forward after a crash/retry does not duplicate the job remotely (`forwarder.ts:10-12`, `forwarder.ts:94`). **Invariant/gotcha:** dedupe only holds *within the server's custom-id retention window* (the bounded `customIdMap` LRU, see [Deduplication & Unique Jobs](./deduplication-and-unique.md)). If the same `localId` is re-forwarded after eviction (or after `removeOnComplete` drops it), a duplicate remote job is possible. Local job-id reuse across DB resets would likewise collide.
- **Listener-error isolation.** A throwing `forwarded` listener is caught and ignored so it cannot fail an already-succeeded forward (`forwarder.ts:103-107`).
- **`error` event suppression.** Worker `failed`/`error` are only re-emitted as `Forwarder` `error` when `listenerCount('error') > 0` (`forwarder.ts:123,126`). This prevents `EventEmitter`'s default "throw on unhandled error" from crashing the process on a transient uplink failure — but it also means errors are silently dropped if you never attach an `error` listener. Failed forwards are still durable locally regardless.
- **Auto-batch disabled on the remote.** The remote queue forces `autoBatch:{ enabled:false }` (`forwarder.ts:88`); forwards are individual pushes, not coalesced.
- **Deterministic pagination:** dependency keys are sorted before slicing so repeated cursor reads cannot reorder cross-queue children. A missing parent or child is an explicit error instead of an empty-success sentinel.
- **Waiting-children state:** manually parked jobs and flow parents are read through the dedicated state, not inferred from user data. The TCP and embedded views therefore expose the same jobs.
- **Exact counts:** `getPrioritizedCount` and `getWaitingChildrenCount` read the broker's state counters and are not constrained by list-page size. See [Public API Completeness](./public-api-completeness.md) for the regression contract and the offset-pagination concurrency caveat.

## Configuration

This module is configured per call, not via env vars.

| Option | Where | Default |
| --- | --- | --- |
| `to` | `ForwardOptions` | required — remote `ConnectionOptions` (host/port/tls/token) |
| `queue` | `ForwardOptions` | source queue `name` |
| `concurrency` | `ForwardOptions` | `4` |
| `durable` | `ForwardOptions` | `false` (omitted → remote uses buffered write) |

The source side inherits its mode (`embedded`, `dataPath`, `connection`, `prefixKey`) from the originating `Queue` via `ForwardSource` (`queue.ts:593-600`). The remote side is always `embedded: false` with `autoBatch` disabled (`forwarder.ts:85-89`). Standard server env vars (`TCP_PORT`, `TLS_CERT_FILE`, `AUTH_TOKENS`, …) apply to whatever bunqueue server `to` points at — see [Configuration](./configuration.md). The BullMQ shims take no configuration.

## Related Docs

- [Client SDK: Queue](./client-queue-sdk.md) — exposes `forward()` and the BullMQ-compatible methods.
- [Client SDK: Worker](./client-worker-sdk.md) — the consumer the Forwarder is built on; owns retry/heartbeat/locks.
- [Client Transport](./client-transport.md) — TCP pool/reconnect/batching used by the remote `Queue`.
- [Dead Letter Queue](./dead-letter-queue.md) — where exhausted local forwards land.
- [Deduplication & Unique Jobs](./deduplication-and-unique.md) — server-side custom-id dedupe that backs forward idempotency.
- [FlowProducer & Job Dependencies](./flow-producer.md) — the parent/child model the dependency shims read.
- [Simple Mode (Bunqueue all-in-one)](./simple-mode.md) — common edge consumer pattern.
- [Security: TLS, Auth, CORS](./security-tls-auth.md) — TLS/token options for the `to` connection.
- [architecture](../architecture.md) · [data-model](../data-model.md)
