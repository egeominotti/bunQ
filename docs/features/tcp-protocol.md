# TCP Wire Protocol & Framing

> **Category:** Transport · **Source:** `src/infrastructure/server/tcp.ts`, `src/infrastructure/server/tcp/`, `src/infrastructure/server/protocol.ts`, `src/infrastructure/server/protocol/`, `src/infrastructure/server/socketWriteQueue.ts`, `src/domain/types/commands/`, `src/domain/types/responses/`, `src/domain/response/builders.ts`, `src/shared/msgpack.ts`, `src/shared/serialization.ts`

## Purpose

This module is the binary transport layer that carries every client/server interaction over a raw TCP socket (default `:6789`). Each message is a length-prefixed frame whose body is a MessagePack-encoded command or response; the server accepts these frames, decodes them into typed `Command` objects, and writes back framed `Response` objects. It owns frame assembly/disassembly, pipelined (parallel) command processing per connection, write-side backpressure handling, and the transport-level DoS mitigations (frame-size cap, slowloris stall timeout, write-queue bound). It does not interpret command semantics — that is delegated to the command handlers.

## Responsibilities & Scope

Owns:
- **Length-prefixed framing** — `FrameParser` (`protocol/frameParser.ts:13-71`) reassembles complete frames from arbitrary TCP segment boundaries, and `FrameParser.frame()` (`protocol/frameParser.ts:63-70`) prepends the 4-byte big-endian length prefix on the way out.
- **MessagePack (de)serialization of the wire body** — the hardened `encodeMessagePack`/`decodeMessagePack` helpers wrap `msgpackr` (`shared/msgpack.ts:68-76`); the server decodes inbound commands in `tcp.ts:58-64` and frames responses in `tcp/responses.ts:5-10`.
- **TCP listener lifecycle** — `createTcpServer()` (`tcp.ts:20-148`) wires `Bun.listen` with `open`/`data`/`close`/`error`/`drain` socket handlers, plus `broadcast()` and `stop()`.
- **Per-connection pipelining** — frames in one read are decoded and processed in parallel, bounded by a `Semaphore` of `MAX_CONCURRENT_PER_CONNECTION = 50` (`tcp/constants.ts:1`, `tcp/connections.ts:42`, `tcp.ts:85-103`).
- **Write-side backpressure** — `SocketWriteQueue` (`socketWriteQueue.ts:24`) buffers unwritten tails on short writes and flushes them on `drain`.
- **Transport DoS bounds** — 64MB max frame size (`protocol/frameParser.ts:1`), slowloris stall timer (`tcp/connections.ts:51-72`), and a write-queue byte cap (`tcp/constants.ts:8-11`, `tcp/connections.ts:74-85`).
- **Per-connection identity & cleanup** — assigns a `clientId` (uuid) when the connection state is initialized (`TcpConnectionRegistry.init`, `tcp/connections.ts:27-49`), releases that client's leased jobs on close (`tcp/connections.ts:87-111`).
- **Queue event streaming** — each connection may select one queue with
  `SubscribeEvents`; matching lifecycle events are emitted as unsolicited
  MessagePack frames through the same bounded write queue. The shared manager
  bridge exists only while at least one connection is subscribed.
- **The command/response type contract** — the `Command` and `Response` discriminated unions (`commands/union.ts:13-107`, `responses/model.ts:155-176`) plus the response builder functions (`domain/response/builders.ts:22-70`).

Does NOT own (delegated elsewhere):
- **Command dispatch / business logic** — `handleCommand()` routes to the handler groups; see [TCP Server Command Handlers](./tcp-server-handlers.md).
- **Authentication / authorization** — gated inside `handleCommand` (`Auth`, token check); see [Security: TLS, Auth, CORS](./security-tls-auth.md). This module only carries the `Auth`/`Hello` frames and supplies the `authTokens` set + `authenticated` flag in the handler context.
- **Rate limiting policy** — `getRateLimiter()` is consulted once per complete decoded frame, but the limiter itself lives in [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md).
- **TLS certificate loading** — `loadTlsOptions()` from `src/infrastructure/server/tls.ts`; see [Security: TLS, Auth, CORS](./security-tls-auth.md).
- **Client-side framing/reconnect/batching** — the client reuses the same `FrameParser` but owns its own connection logic; see [Client Transport](./client-transport.md).
- **HTTP/SSE/WebSocket transport** — see [HTTP / REST / SSE / WebSocket API](./http-api.md).

## Dependencies

Internal:
- `./handler` — `handleCommand`, `HandlerContext` (dispatch + auth/clientId context).
- `./protocol` — `FrameParser`, `FrameSizeError`, `createConnectionState`, `ConnectionState`, `MAX_FRAME_SIZE`.
- `./socketWriteQueue` — `SocketWriteQueue`.
- `./rateLimiter` — `getRateLimiter()`.
- `./tls` — `loadTlsOptions`, `TlsServerOptions`.
- `../../shared/semaphore` — `Semaphore`, `withSemaphore`.
- `../../shared/hash` — `uuid` (per-connection clientId).
- `../../shared/logger` — `tcpLog`.
- `../../application/queueManager` — `QueueManager` (target of every command; job release on disconnect).
- `../../domain/types/command`, `../../domain/types/response` — compatibility barrels for the split command and response wire types.

External / runtime:
- **`msgpackr`** (`pack`/`unpack`) — the only third-party runtime dependency in the hot path; everything else is Bun built-ins.
- **Bun** — `Bun.listen` (`TCPSocketListener`, `Socket`), `Bun.env`, `Bun.sleep`, `Bun.file` (TLS), `setTimeout`/`clearTimeout`.

## Public Interface

Exported from `tcp.ts` and `types/tcpServer.ts`:
```ts
export interface TcpServerConfig {
  port?: number;             // default 6789
  hostname?: string;         // default '0.0.0.0'
  authTokens?: string[];     // empty => auto-authenticated
  idleTimeoutMs?: number;    // slowloris stall timeout; default TCP_IDLE_TIMEOUT_MS (60000), 0 disables
  maxWriteQueueBytes?: number; // write-side memory bound; default 64MB, 0 disables
  tls?: TlsServerOptions;    // native TLS termination (cert/key PEM paths)
}

export function createTcpServer(
  queueManager: QueueManager,
  config: TcpServerConfig,
): {
  server: TCPSocketListener<ConnectionData>;
  connections: Map<string, Socket<ConnectionData>>;
  getConnectionCount(): number;
  broadcast(message: unknown): void;
  stop(): void;
};

export type TcpServer = ReturnType<typeof createTcpServer>;
```

Exported through `protocol.ts` from `protocol/frameParser.ts` (framing surface):
```ts
export const MAX_FRAME_SIZE = 64 * 1024 * 1024; // 64MB

export class FrameSizeError extends Error {
  readonly requestedSize: number;
  readonly maxSize: number;
}

export class FrameParser {
  constructor(maxFrameSize?: number);
  addData(data: Uint8Array): Uint8Array[]; // returns complete frame bodies, retains partial tail
  get bufferedBytes(): number;
  get hasPartialFrame(): boolean;
  clear(): void;
  static frame(data: Uint8Array): Uint8Array; // prepend 4-byte BE length prefix
}
```
`protocol.ts` also exports a **legacy JSON-text path** — `parseCommand`/`parseCommands`/`serializeResponse(string)`/`LineBuffer` (newline-delimited) from `protocol/commands.ts` and `protocol/lineBuffer.ts` — and validators `validateQueueName`, `validateJobData` (10MB cap, `protocol/validation.ts:8-19`), `validateNumericField`, `validateBackoffField` (number or `{ type, delay }` object form, `protocol/validation.ts:41-55`), `validateJobOptions`, plus `ConnectionState`/`createConnectionState`. The binary `FrameParser` path is the one the TCP server uses; the JSON-text helpers are not invoked by `createTcpServer`.

Exported from `socketWriteQueue.ts`:
```ts
export class SocketWriteQueue {
  constructor(maxBytes?: number);            // 0 => unbounded
  get isOverBudget(): boolean;
  write(socket: WritableSocket, data: Uint8Array): boolean; // false if socket closed
  flush(socket: WritableSocket): boolean;    // false when the socket is closed
  get bytesQueued(): number;
  get hasPending(): boolean;
  clear(): void;
}
export function queuedWrite<T extends { writeQueue: SocketWriteQueue }>(socket, data): void;
```

`serialization.ts` exports `serializeJob`/`serializeJobs` (BigInt id → string, `Record<string,unknown>`), `bigIntReplacer`, and `jsonStringify`. These produce **JSON-safe** shapes used by the JSON/HTTP layer; the binary TCP path relies on `msgpackr` (which encodes `bigint` natively) and does not call these.

**TCP commands carried (exact `cmd` names):** the full set is the `Command` union (`commands/union.ts:13-107`):
`PUSH`, `PUSHB`, `PUSHF`, `PULL`, `PULLB`, `ACK`, `ACKB`, `FAIL`, `GetJob`, `GetState`, `GetResult`, `GetJobs`, `GetJobCounts`, `GetCountsPerPriority`, `GetJobByCustomId`, `Count`, `GetProgress`, `GetQueueLimits`, `GetDeduplicationJobId`, `Cancel`, `Progress`, `Update`, `ChangePriority`, `Promote`, `WaitJob`, `MoveToDelayed`, `MoveToWaitingChildren`, `Discard`, `RemoveDeduplicationKey`, `RemoveJobDeduplicationKey`, `Pause`, `Resume`, `IsPaused`, `Drain`, `Obliterate`, `ListQueues`, `Clean`, `Dlq`, `GetDlqStats`, `RetryDlq`, `PurgeDlq`, `RetryCompleted`, `RateLimit`, `SetConcurrency`, `RateLimitClear`, `ClearConcurrency`, `SetStallConfig`, `GetStallConfig`, `SetDlqConfig`, `GetDlqConfig`, `Cron`, `CronDelete`, `CronList`, `AddLog`, `GetLogs`, `Heartbeat`, `JobHeartbeat`, `JobHeartbeatB`, `Ping`, `RegisterWorker`, `UnregisterWorker`, `ListWorkers`, `AddWebhook`, `RemoveWebhook`, `ListWebhooks`, `Stats`, `Metrics`, `TrimEvents`, `Prometheus`, `CronGet`, `GetChildrenValues`, `StorageStatus`, `ClearLogs`, `ExtendLock`, `ExtendLocks`, `ChangeDelay`, `SetWebhookEnabled`, `CompactMemory`, `UpdateParent`, `MoveToWait`, `PromoteJobs`, `DashboardOverview`, `DashboardQueues`, `DashboardQueue`, `Auth`, `GetFailedChildrenValues`, `GetIgnoredChildrenFailures`, `RemoveChildDependency`, `RemoveUnprocessedChildren`, `Hello`, `SubscribeEvents`, `UnsubscribeEvents`.

`Metrics` without queue fields retains the legacy broker-wide `{metrics}`
response. The Queue client sends `{cmd:'Metrics',queue,type,start,end}` and
receives `{data: QueueMetrics}`; `start`/`end` are inclusive newest-first bucket
indexes. `TrimEvents` sends `{queue,maxLength}` and returns
`{data:{removed:number}}`. Both validate the queue and safe-integer window or
length at the manager boundary.

`SubscribeEvents` carries `{ queue }` and replaces any prior subscription on
that connection. `UnsubscribeEvents` clears it without closing the connection;
subsequent request/response commands continue to work. Both commands are
authenticated, rate-limited, semaphore-bounded, and acknowledge with the
request `reqId`. Matching events arrive independently as
`{ type: 'event', event: JobEvent }`. The unsolicited envelope has no `reqId`
and must be dispatched before command correlation. Queue filtering happens
server-side, and slow subscribers are terminated through the existing
write-queue byte budget.

`Dlq` accepts `count?`/`filter?` and returns both `jobs` and full `entries`;
`GetDlqStats` returns `data.stats`. `RetryDlq` accepts `jobId?`, `count?`, or a
filter; `RetryCompleted` accepts `id?`, `count?`, and `timestamp?`.
`GetQueueLimits` returns the live limiter/concurrency status. The three
deduplication commands expose lookup, queue-key release, and generation-safe
job-owned release. `MoveToWaitingChildren` performs the real active-to-parked
state transition. `GetJobs.asc` is optional and defaults to `true`; `false`
selects descending createdAt/job-id order before offset/limit pagination.

`PUSHF` carries `{ cmd: 'PUSHF', jobs: AtomicFlowJobInput[] }`, where every
entry already has its final string `id`, queue, job input, and graph links. The
broker validates strict runtime types, symmetric parent/child metadata,
acyclicity, 10,000 jobs, 10 MB per job and 64 MB aggregate flow data before
mutation. Success is `{ ok: true, data: { jobs: Job[] } }`; the snapshots cover
exactly the requested IDs. With SQLite configured, all rows commit in one
immediate transaction before the graph is visible to workers. See
[FlowProducer & Job Dependencies](./flow-producer.md).

**Events emitted** (via `queueManager.emitDashboardEvent`, for the dashboard/cloud layer): `client:connected`, `client:disconnected` (both with `transport: 'tcp'`), `ratelimit:hit`.

## Data Models

Every request body is a `Command`, every solicited reply is a `Response`, and a
subscribed server may additionally send an `EventEnvelope`. The full field
definitions of `Job`, `JobInput`, and the per-job options carried on `JobInput`
live in `src/domain/types/jobs/model.ts` and are re-exported through
`src/domain/types/job.ts`.

**Frame layout (both directions, identical):**
```
┌──────────────────────────┬───────────────────────────────┐
│ length: u32 big-endian    │ body: msgpack(Command|Response|EventEnvelope) │
│ (4 bytes)                 │ (`length` bytes)                │  declared length ≤ 64MB
└──────────────────────────┴───────────────────────────────┘
```

**Command base** (`commands/base.ts:2-5`): `{ readonly cmd: string; readonly reqId?: string }`. `reqId` is the correlation id the client uses to match an out-of-order pipelined response to its request.

**Response base** (`responses/model.ts:6-9`): `{ readonly ok: boolean; readonly reqId?: string }`. Successful responses set `ok: true` and carry a payload-specific field (`id`, `ids`, `job`, `jobs`, `token`/`tokens`, `state`, `result`, `counts`, `stats`, `metrics`, `data`, …). Errors are `ErrorResponse` (`responses/model.ts:139-142`): `{ ok: false, error: string, reqId? }`. Builders live at `domain/response/builders.ts:22-70` (`ok`, `batch`, `job`, `pulledJob`, `pulledJobs`, `error`, `hello`, `data`, `counts`, `stats`, `metrics`).

**Event envelope:** `{ readonly type: 'event'; readonly event: JobEvent }` is a
server-initiated frame used only after `SubscribeEvents`. It is deliberately
outside the `Response` union because it does not answer a command.

**Protocol discovery:** `HelloCommand` carries `protocolVersion` and optional `ProtocolCapability[]`; the server replies via `handleHello` with `PROTOCOL_VERSION = 3` and `SUPPORTED_CAPABILITIES = ['pipelining', 'separate-job-name']`, plus server name `'bunqueue'` and package version. Revision 3 makes `name` a top-level job-envelope field while preserving `data` unchanged. The inbound handler alone decodes old requests that omit top-level `name` and embed it in object-shaped `data`.

## Business Logic / Control Flow

**Connection open (`TcpConnectionRegistry.init`, `tcp/connections.ts:27-49`)**: allocate `clientId = uuid()`, build `HandlerContext` with `authenticated = authTokens.size === 0` (auto-auth when no tokens configured), and attach per-connection `TcpConnectionData`: a fresh `FrameParser`, `Semaphore(50)`, `SocketWriteQueue(maxWriteQueueBytes)`, abort controller, null `stallTimer`, and null event queue. Register in `connections` map and emit `client:connected`. `init` is idempotent (returns early if `socket.data` exists) and is called from **both** the `open` handler and lazily at the top of the `data` handler (`tcp.ts:29-36`): under native TLS, Bun can deliver `data` before `open` has run, and the handler previously destructured a null `socket.data`, escalating a TypeError to the process-level unhandledRejection handler (a pre-auth remote DoS on an exposed TLS port, #108). Lazy init preserves that first frame instead of dropping it.

**Inbound data** (`tcp.ts:34-104`):
1. `registry.init(socket)`: ensure per-socket state exists (TLS data-before-open, #108).
2. `frameParser.addData(data)` — the Bun read `Buffer` is passed directly; the previous defensive `new Uint8Array(data)` copy was removed because `addData` copies its input into owned storage.
3. A thrown `FrameSizeError` clears the stall timer, writes `Frame too large: …`, and ends the socket.
4. `updateStallTimer(socket)` (re)arms the slowloris timer iff a partial frame is buffered.
5. For each complete frame, `processFrame` decodes MessagePack through
   `src/shared/msgpack.ts`, extracts a trustworthy string `reqId`, and then
   consumes exactly one protocol-rate token. Ordinary frames use msgpackr's
   fast decoder. Frames containing the byte sequence `__proto__` take a safe
   Map materialization path that defines own properties without invoking the
   JavaScript prototype setter. An overload emits `ratelimit:hit` and writes
   `Rate limit exceeded` with that `reqId`; partial TCP chunks consume no quota
   and coalesced frames consume one token each. Allowed frames continue through
   command validation and `withSemaphore` → `handleCommand`.
6. `await Promise.all(frames.map(processFrame))` (`tcp.ts:103`) — all frames in one read are processed concurrently (pipelining); the client correlates responses by `reqId`, so server-side ordering is not guaranteed.

**Frame parsing — `FrameParser.addData`** (`protocol/frameParser.ts:21-49`): concatenate the retained partial tail with new data into a single owned buffer (one copy), then walk it with an `offset` cursor. Each iteration reads the big-endian u32 length (`>>> 0` to force unsigned, `protocol/frameParser.ts:28-34`), rejects lengths > `maxFrameSize` (clears the buffer and throws `FrameSizeError`, `protocol/frameParser.ts:36-39`), breaks if the full body has not yet arrived (keeping the partial bytes buffered, `protocol/frameParser.ts:40`), else slices out the body (`protocol/frameParser.ts:41`) and advances the cursor by `4 + length`. Tail retention has three cases (`protocol/frameParser.ts:45-47`): fully drained → fresh empty buffer (don't pin the read's ArrayBuffer); nothing consumed → keep the concat buffer as-is (no extra copy); partial leftover after ≥1 frame → slice the small tail. The cursor makes the pass **O(total bytes) / O(F)** in the number of frames; the prior implementation resliced the tail after every frame, which was **O(F²)** when many frames arrived coalesced in one read.

**Outbound write & backpressure — `SocketWriteQueue`** (`socketWriteQueue.ts`): if a tail is already pending, append the new chunk (never write ahead of older bytes — preserves frame order). Otherwise `socket.write(data)`; a return `< 0` means the socket is closed (`write` returns `false`), and a short write buffers the unwritten `data.subarray(written)` tail. `flush()`, invoked from both server and client `drain` handlers, drains the pending queue in order, advancing `offset` within the current chunk on a partial write and stopping at zero or another short write. It returns `false` for a closed or throwing socket so the owner can terminate it. Server responses and reference-client commands use a queue owned by the individual physical socket; neither queue survives reconnect. This matters because Bun's `socket.write()` may write fewer bytes than supplied; silently dropping the tail would corrupt the length-prefixed stream.

**Connection close** (`tcp/connections.ts:87-111`): if the socket closed before its state was ever
initialized (e.g. an aborted TLS handshake), return immediately, nothing to
release (#108). Otherwise abort the connection-scoped signal first, cancelling
pending `PULL`/`PULLB` waiters before they can claim future jobs; then clear the
stall timer and buffered writes, remove connection/rate-limiter/worker state,
emit `client:disconnected`, and call `releaseClientJobsWithRetry` for jobs
already delivered to that client.

## Concurrency & Locking

- **Per-connection pipelining is bounded by a `Semaphore(50)`** (`MAX_CONCURRENT_PER_CONNECTION`, `tcp/constants.ts:1`; construction at `tcp/connections.ts:42`). All frames from a single `data` event are launched together (`tcp.ts:103`), but at most 50 commands per connection are in-flight at once; the rest await a permit. The semaphore (`shared/semaphore.ts`) is a simple FIFO permit queue.
- **No lock is taken in this module.** Frame parsing is synchronous per `data` callback (single-threaded JS event loop), and the per-connection `FrameParser`/`SocketWriteQueue`/`stallTimer` are touched only from that connection's own callbacks. Job-state locking (the `jobIndex → completedJobs → shards → processingShards` hierarchy) happens downstream inside `QueueManager`; see [Concurrency & Locking](./concurrency-and-locking.md).
- **Lease ownership** is keyed by the connection `clientId`: jobs pulled over a connection are released on disconnect. `PULL`/`PULLB` accept `owner`/`lockTtl`, and `detach: true` (CLI use) opts out of auto-release on disconnect. Lock-token mechanics belong to [Job Lifecycle](./job-lifecycle.md).
- **Token-bearing transitions** include `ACK`, `FAIL`, both `ACKB` forms,
  `MoveToWait`, `MoveToDelayed`, `MoveToWaitingChildren`, and `Discard`. If the target has
  a lease, its exact positional/current token is mandatory. `ACKB` validates
  array lengths and every current lease before extraction. A timeout can still
  win concurrently after validation; the response then carries ordered
  `ignoredIds` plus `ignoredIndices`, while all other live positions apply.
  Single `ACK`/`FAIL` use `{ applied:false, reason:'already-finalized' }` for
  that exact retired generation. Clients validate this evidence and do not
  retry it as a transport failure.
- **Pending pull ownership** is also connection-scoped. Disconnect aborts
  single/batch and owner/detached long-polls; a cancelled request cannot consume
  a later push, increment pulled counters, activate a group, consume a
  concurrency slot, or create a lock.
- **`SocketWriteQueue` ordering invariant:** once any tail is pending, every subsequent `write` must enqueue rather than write directly, or response bytes would interleave and corrupt the frame stream.

## Edge Cases & Failure Modes

- **Frame size cap (64MB):** a declared length over `MAX_FRAME_SIZE` throws `FrameSizeError`, clears the parser buffer (stops processing attacker bytes), sends an error, and ends the socket. Legal large partial frames are *not* dropped mid-assembly — only over-cap declarations are.
- **Slowloris mitigation:** a peer that starts a frame (buffers partial bytes) but stalls is closed after `idleTimeoutMs` (default 60s, `tcp/constants.ts:3-6`). The timer is armed **only while `hasPartialFrame` is true** and reset on each data event (`tcp/connections.ts:51-72`), so healthy idle-but-complete connections are never disturbed. Implemented as a manual `setTimeout` because Bun (1.3.x) has no socket `idleTimeout` and `socket.timeout()` resets on every byte (a 1-byte-per-window trickle would defeat it). `0` disables.
- **Write-side memory DoS:** if a client stops reading while the server keeps producing responses, `SocketWriteQueue.bytesQueued` grows; once it exceeds `maxWriteQueueBytes` (default 64MB, `tcp/constants.ts:8-11`), `isOverBudget` trips and the connection is `terminate()`d after clearing the queue (`tcp/connections.ts:74-85`, also enforced in `broadcast` at `tcp/connections.ts:114-124`). `0` disables.
- **Reference-client write bound:** commands use the same ordered queue with a
  fixed 64 MiB cap. Crossing it, a negative write result, or a thrown write
  terminates that physical connection; queued bytes are discarded rather than
  replayed on the next connection because command application may already have
  occurred remotely.
- **Malformed input:** every complete frame consumes one protocol-rate token. Non-msgpack bodies return `Invalid command format`; decoded bodies lacking `cmd` return `Invalid command` and preserve a valid string `reqId`. Neither closes the connection.
- **MessagePack map-key preservation:** JSON-like maps use string keys.
  `__proto__`, `__proto_`, `constructor`, and `prototype` are ordinary,
  distinct own data properties. They round-trip through TCP and SQLite without
  prototype mutation or key collision; encoding bytes remain compatible with
  non-JavaScript SDKs.
- **Error sanitization:** handler errors whose message contains `SQLITE` or `database` are rewritten to `Internal server error` before being framed back (`tcp.ts:93-99`, mirrored in `handler.ts:96-101`), so internal storage details never leak to clients.
- **Job release on disconnect with retry:** `releaseClientJobsWithRetry` (`tcp/clientRelease.ts:4-23`) retries up to 3× with exponential backoff (100/200/400ms). If all retries fail, `TcpConnectionRegistry.close` falls back to `forceReleaseClientJobs` (`tcp/connections.ts:103-110`), which unconditionally clears client tracking (prevents a `connections`/ownership Map leak) and resets heartbeats so the stall detector recovers any orphaned `active` jobs on its next tick — chosen over leaking the jobs.
- **Idempotency** is a command-level concern (`jobId`/`uniqueKey`/`dedup`), not a transport one; the transport delivers exactly the frames it parsed. `reqId` is purely for client-side response correlation and is echoed back verbatim, including on errors.
- **TLS startup invariant:** `loadTlsOptions` runs *before* `Bun.listen` binds the port (`tcp.ts:120-126`), so a bad cert/key path fails fast at startup instead of leaving a half-started listener.
- **`stop()`** terminates every connection through `TcpConnectionRegistry.closeAll()` and then stops the listener (`tcp.ts:141-144`); `broadcast()` frames the message once and fans it out through each connection's `SocketWriteQueue` (`tcp/connections.ts:114-124`).

## Configuration

| Env var / option | Default | Effect |
| --- | --- | --- |
| `TcpServerConfig.port` / `TCP_PORT` | `6789` | Listen port. |
| `TcpServerConfig.hostname` / `HOST` | `0.0.0.0` | Bind address. |
| `TCP_SOCKET_PATH` | — | **Reserved, currently inert for TCP**: it is resolved into config and shown in the startup banner (`bootstrap.ts:51-65`), but `bootstrap` never passes it to `createTcpServer` (`bootstrap.ts:131-140`) and `TcpServerConfig` has no socket-path field (`types/tcpServer.ts:8-15`), so the TCP listener always binds host/port. Only `HTTP_SOCKET_PATH` produces a Unix-socket bind (see [Configuration & Entrypoint](./configuration.md)). |
| `TcpServerConfig.authTokens` / `AUTH_TOKENS` | `[]` | If empty, connections are auto-authenticated; otherwise `Auth` is required. |
| `TcpServerConfig.idleTimeoutMs` / `TCP_IDLE_TIMEOUT_MS` | `60000` | Slowloris stall timeout (ms) for in-progress partial frames; `0` disables. |
| `TcpServerConfig.maxWriteQueueBytes` / `TCP_MAX_WRITE_QUEUE_BYTES` | `67108864` (64MB) | Max buffered outbound bytes before dropping the connection; `0` disables. |
| `TcpServerConfig.tls` / `TLS_CERT_FILE` + `TLS_KEY_FILE` | — | Native TLS termination (both files required). |
| `MAX_FRAME_SIZE` (constant) | 64MB | Hard cap on a single frame body; not env-tunable on the server (`FrameParser` constructor takes an override used in tests). |
| `MAX_CONCURRENT_PER_CONNECTION` (constant) | `50` | Per-connection pipelining concurrency. |

`PROTOCOL_VERSION = 3` and `SUPPORTED_CAPABILITIES = ['pipelining', 'separate-job-name']` are reported via the `Hello` handshake.

## Related Docs

- [TCP Server Command Handlers](./tcp-server-handlers.md) — what `handleCommand` does with each frame.
- [Client Transport](./client-transport.md) — the client side of this protocol (pool, reconnect, auto-batching).
- [Security: TLS, Auth, CORS](./security-tls-auth.md) — `Auth`, token comparison, TLS files.
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md) — the limiter consulted once per complete protocol frame.
- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md) — semantics of the core commands and lock tokens.
- [HTTP / REST / SSE / WebSocket API](./http-api.md) — the alternative transport.
- [Stats, Metrics & Monitoring](./stats-and-monitoring.md) — `Hello`, `Ping`, `Stats`, `Metrics`, `Prometheus`.
