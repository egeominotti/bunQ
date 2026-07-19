# TCP Wire Protocol & Framing

> **Category:** Transport · **Source:** `src/infrastructure/server/tcp.ts`, `src/infrastructure/server/protocol.ts`, `src/infrastructure/server/socketWriteQueue.ts`, `src/domain/types/command.ts`, `src/domain/types/response.ts`, `src/shared/serialization.ts`

## Purpose

This module is the binary transport layer that carries every client/server interaction over a raw TCP socket (default `:6789`). Each message is a length-prefixed frame whose body is a MessagePack-encoded command or response; the server accepts these frames, decodes them into typed `Command` objects, and writes back framed `Response` objects. It owns frame assembly/disassembly, pipelined (parallel) command processing per connection, write-side backpressure handling, and the transport-level DoS mitigations (frame-size cap, slowloris stall timeout, write-queue bound). It does not interpret command semantics — that is delegated to the command handlers.

## Responsibilities & Scope

Owns:
- **Length-prefixed framing** — `FrameParser` (`protocol.ts:216`) reassembles complete frames from arbitrary TCP segment boundaries, and `FrameParser.frame()` (`protocol.ts:309`) prepends the 4-byte big-endian length prefix on the way out.
- **MessagePack (de)serialization of the wire body** — `pack`/`unpack` from `msgpackr`, called in `tcp.ts:123` (`serializeResponse`), `tcp.ts:128` (`errorResponse`), and `tcp.ts:272` (decode inbound command).
- **TCP listener lifecycle** — `createTcpServer()` (`tcp.ts:135`) wires `Bun.listen` with `open`/`data`/`close`/`error`/`drain` socket handlers, plus `broadcast()` and `stop()`.
- **Per-connection pipelining** — frames in one read are decoded and processed in parallel, bounded by a `Semaphore` of `MAX_CONCURRENT_PER_CONNECTION = 50` (`tcp.ts:27`, `tcp.ts:284`, `tcp.ts:300`).
- **Write-side backpressure** — `SocketWriteQueue` (`socketWriteQueue.ts:24`) buffers unwritten tails on short writes and flushes them on `drain`.
- **Transport DoS bounds** — 64MB max frame size (`protocol.ts:202`), slowloris stall timer (`tcp.ts:156`), and a write-queue byte cap (`tcp.ts:49`).
- **Per-connection identity & cleanup** — assigns a `clientId` (uuid) when the connection state is initialized (`initConnection`, `tcp.ts:182`), releases that client's leased jobs on close (`tcp.ts:318`).
- **The command/response type contract** — the `Command` and `Response` discriminated unions (`command.ts:617`, `response.ts:201`) plus the response builder functions (`response.ts:225`+).

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
- `../../domain/types/command`, `../../domain/types/response` — wire types.

External / runtime:
- **`msgpackr`** (`pack`/`unpack`) — the only third-party runtime dependency in the hot path; everything else is Bun built-ins.
- **Bun** — `Bun.listen` (`TCPSocketListener`, `Socket`), `Bun.env`, `Bun.sleep`, `Bun.file` (TLS), `setTimeout`/`clearTimeout`.

## Public Interface

Exported from `tcp.ts`:
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

Exported from `protocol.ts` (framing surface):
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
`protocol.ts` also exports a **legacy JSON-text path** — `parseCommand`/`parseCommands`/`serializeResponse(string)`/`LineBuffer` (newline-delimited) — and validators `validateQueueName`, `validateJobData` (10MB cap, `protocol.ts:57`), `validateNumericField`, `validateBackoffField` (number or `{ type, delay }` object form, `protocol.ts:109`), `validateJobOptions`, plus `ConnectionState`/`createConnectionState`. The binary `FrameParser` path is the one the TCP server uses; the JSON-text helpers are not invoked by `createTcpServer`.

Exported from `socketWriteQueue.ts`:
```ts
export class SocketWriteQueue {
  constructor(maxBytes?: number);            // 0 => unbounded
  get isOverBudget(): boolean;
  write(socket: WritableSocket, data: Uint8Array): boolean; // false if socket closed
  flush(socket: WritableSocket): void;       // call from drain handler
  get bytesQueued(): number;
  get hasPending(): boolean;
  clear(): void;
}
export function queuedWrite<T extends { writeQueue: SocketWriteQueue }>(socket, data): void;
```

`serialization.ts` exports `serializeJob`/`serializeJobs` (BigInt id → string, `Record<string,unknown>`), `bigIntReplacer`, and `jsonStringify`. These produce **JSON-safe** shapes used by the JSON/HTTP layer; the binary TCP path relies on `msgpackr` (which encodes `bigint` natively) and does not call these.

**TCP commands carried (exact `cmd` names):** the full set is the `Command` union (`command.ts:617`):
`PUSH`, `PUSHB`, `PULL`, `PULLB`, `ACK`, `ACKB`, `FAIL`, `GetJob`, `GetState`, `GetResult`, `GetJobs`, `GetJobCounts`, `GetCountsPerPriority`, `GetJobByCustomId`, `Count`, `GetProgress`, `Cancel`, `Progress`, `Update`, `ChangePriority`, `Promote`, `WaitJob`, `MoveToDelayed`, `Discard`, `Pause`, `Resume`, `IsPaused`, `Drain`, `Obliterate`, `ListQueues`, `Clean`, `Dlq`, `RetryDlq`, `PurgeDlq`, `RetryCompleted`, `RateLimit`, `SetConcurrency`, `RateLimitClear`, `ClearConcurrency`, `SetStallConfig`, `GetStallConfig`, `SetDlqConfig`, `GetDlqConfig`, `Cron`, `CronDelete`, `CronList`, `AddLog`, `GetLogs`, `Heartbeat`, `JobHeartbeat`, `JobHeartbeatB`, `Ping`, `RegisterWorker`, `UnregisterWorker`, `ListWorkers`, `AddWebhook`, `RemoveWebhook`, `ListWebhooks`, `Stats`, `Metrics`, `Prometheus`, `CronGet`, `GetChildrenValues`, `StorageStatus`, `ClearLogs`, `ExtendLock`, `ExtendLocks`, `ChangeDelay`, `SetWebhookEnabled`, `CompactMemory`, `UpdateParent`, `MoveToWait`, `PromoteJobs`, `DashboardOverview`, `DashboardQueues`, `DashboardQueue`, `Auth`, `GetFailedChildrenValues`, `GetIgnoredChildrenFailures`, `RemoveChildDependency`, `RemoveUnprocessedChildren`, `Hello`.

**Events emitted** (via `queueManager.emitDashboardEvent`, for the dashboard/cloud layer): `client:connected`, `client:disconnected` (both with `transport: 'tcp'`), `ratelimit:hit`.

## Data Models

Every wire body is one of two discriminated unions. The full field definitions of `Job` (`src/domain/types/job.ts:81`), `JobInput` (`job.ts:171`), and the per-job options carried on `JobInput` live in `src/domain/types/job.ts`.

**Frame layout (both directions, identical):**
```
┌──────────────────────────┬───────────────────────────────┐
│ length: u32 big-endian    │ body: msgpack(Command|Response) │  length = body byte count
│ (4 bytes)                 │ (`length` bytes)                │  declared length ≤ 64MB
└──────────────────────────┴───────────────────────────────┘
```

**Command base** (`command.ts:9`): `{ readonly cmd: string; readonly reqId?: string }`. `reqId` is the correlation id the client uses to match an out-of-order pipelined response to its request.

**Response base** (`response.ts:9`): `{ readonly ok: boolean; readonly reqId?: string }`. Successful responses set `ok: true` and carry a payload-specific field (`id`, `ids`, `job`, `jobs`, `token`/`tokens`, `state`, `result`, `counts`, `stats`, `metrics`, `data`, …). Errors are `ErrorResponse` (`response.ts:179`): `{ ok: false, error: string, reqId? }`. Builders live at `response.ts:225`–`345` (`ok`, `batch`, `job`, `pulledJob`, `pulledJobs`, `error`, `hello`, `data`, `counts`, `stats`, `metrics`).

**Protocol negotiation:** `HelloCommand` carries `protocolVersion` and optional `capabilities: 'pipelining'[]`; the server replies via `handleHello` with `PROTOCOL_VERSION = 2` and `SUPPORTED_CAPABILITIES = ['pipelining']` (`src/infrastructure/server/handlers/monitoring.ts:123-129`), plus server name `'bunqueue'` and version.

## Business Logic / Control Flow

**Connection open (`initConnection`, `tcp.ts:182`)**: allocate `clientId = uuid()`, build `HandlerContext` with `authenticated = authTokens.size === 0` (auto-auth when no tokens configured), and attach per-connection `ConnectionData`: a fresh `FrameParser`, `Semaphore(50)`, `SocketWriteQueue(maxWriteQueueBytes)`, and a null `stallTimer`. Register in `connections` map and emit `client:connected`. `initConnection` is idempotent (returns early if `socket.data` exists) and is called from **both** the `open` handler and lazily at the top of the `data` handler: under native TLS, Bun can deliver `data` before `open` has run, and the handler previously destructured a null `socket.data`, escalating a TypeError to the process-level unhandledRejection handler (a pre-auth remote DoS on an exposed TLS port, #108). Lazy init preserves that first frame instead of dropping it.

**Inbound data** (`tcp.ts:211`):
1. `initConnection(socket)`: ensure per-socket state exists (TLS data-before-open, #108).
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
7. `await Promise.all(frames.map(processFrame))` (`tcp.ts:300`) — all frames in one read are processed concurrently (pipelining); the client correlates responses by `reqId`, so server-side ordering is not guaranteed.

**Frame parsing — `FrameParser.addData`** (`protocol.ts:227`): concatenate the retained partial tail with new data into a single owned buffer (one copy), then walk it with an `offset` cursor. Each iteration reads the big-endian u32 length (`>>> 0` to force unsigned, `protocol.ts:245`), rejects lengths > `maxFrameSize` (clears the buffer and throws `FrameSizeError`, `protocol.ts:258`), breaks if the full body has not yet arrived (keeping the partial bytes buffered, `protocol.ts:264`), else slices out the body (`protocol.ts:273`) and advances the cursor by `4 + length`. Tail retention has three cases (`protocol.ts:277`): fully drained → fresh empty buffer (don't pin the read's ArrayBuffer); nothing consumed → keep the concat buffer as-is (no extra copy); partial leftover after ≥1 frame → slice the small tail. The cursor makes the pass **O(total bytes) / O(F)** in the number of frames; the prior implementation resliced the tail after every frame, which was **O(F²)** when many frames arrived coalesced in one read.

**Outbound write & backpressure — `SocketWriteQueue`** (`socketWriteQueue.ts:55`): if a tail is already pending, append the new chunk (never write ahead of older bytes — preserves frame order). Otherwise `socket.write(data)`; a return `< 0` means the socket is closed (`write` returns `false`), and a short write buffers the unwritten `data.subarray(written)` tail. `flush()` (`socketWriteQueue.ts:92`), invoked from the socket `drain` handler (`tcp.ts:341`, guarded against uninitialized `socket.data` under TLS, #108), drains the pending queue in order, advancing `offset` within the current chunk on a partial write and stopping at the first short write. This matters because Bun's `socket.write()` may write fewer bytes than supplied; silently dropping the tail would corrupt the length-prefixed stream.

**Connection close** (`tcp.ts`): if the socket closed before its state was ever
initialized (e.g. an aborted TLS handshake), return immediately, nothing to
release (#108). Otherwise abort the connection-scoped signal first, cancelling
pending `PULL`/`PULLB` waiters before they can claim future jobs; then clear the
stall timer and buffered writes, remove connection/rate-limiter/worker state,
emit `client:disconnected`, and call `releaseClientJobsWithRetry` for jobs
already delivered to that client.

## Concurrency & Locking

- **Per-connection pipelining is bounded by a `Semaphore(50)`** (`MAX_CONCURRENT_PER_CONNECTION`, `tcp.ts:27`). All frames from a single `data` event are launched together (`Promise.all`), but at most 50 commands per connection are in-flight at once; the rest await a permit. The semaphore (`shared/semaphore.ts`) is a simple FIFO permit queue.
- **No lock is taken in this module.** Frame parsing is synchronous per `data` callback (single-threaded JS event loop), and the per-connection `FrameParser`/`SocketWriteQueue`/`stallTimer` are touched only from that connection's own callbacks. Job-state locking (the `jobIndex → completedJobs → shards → processingShards` hierarchy) happens downstream inside `QueueManager`; see [Concurrency & Locking](./concurrency-and-locking.md).
- **Lease ownership** is keyed by the connection `clientId`: jobs pulled over a connection are released on disconnect. `PULL`/`PULLB` accept `owner`/`lockTtl`, and `detach: true` (CLI use) opts out of auto-release on disconnect. Lock-token mechanics belong to [Job Lifecycle](./job-lifecycle.md).
- **Pending pull ownership** is also connection-scoped. Disconnect aborts
  single/batch and owner/detached long-polls; a cancelled request cannot consume
  a later push, increment pulled counters, activate a group, consume a
  concurrency slot, or create a lock.
- **`SocketWriteQueue` ordering invariant:** once any tail is pending, every subsequent `write` must enqueue rather than write directly, or response bytes would interleave and corrupt the frame stream.

## Edge Cases & Failure Modes

- **Frame size cap (64MB):** a declared length over `MAX_FRAME_SIZE` throws `FrameSizeError`, clears the parser buffer (stops processing attacker bytes), sends an error, and ends the socket. Legal large partial frames are *not* dropped mid-assembly — only over-cap declarations are.
- **Slowloris mitigation:** a peer that starts a frame (buffers partial bytes) but stalls is closed after `idleTimeoutMs` (default 60s, `tcp.ts:41`). The timer is armed **only while `hasPartialFrame` is true** and reset on each data event, so healthy idle-but-complete connections are never disturbed. Implemented as a manual `setTimeout` because Bun (1.3.x) has no socket `idleTimeout` and `socket.timeout()` resets on every byte (a 1-byte-per-window trickle would defeat it). `0` disables.
- **Write-side memory DoS:** if a client stops reading while the server keeps producing responses, `SocketWriteQueue.bytesQueued` grows; once it exceeds `maxWriteQueueBytes` (default 64MB, `tcp.ts:49`), `isOverBudget` trips and the connection is `terminate()`d after clearing the queue (`tcp.ts:253`, also enforced in `broadcast` at `tcp.ts:383`). `0` disables.
- **Malformed input:** every complete frame consumes one protocol-rate token. Non-msgpack bodies return `Invalid command format`; decoded bodies lacking `cmd` return `Invalid command` and preserve a valid string `reqId`. Neither closes the connection.
- **MessagePack map-key preservation:** JSON-like maps use string keys.
  `__proto__`, `__proto_`, `constructor`, and `prototype` are ordinary,
  distinct own data properties. They round-trip through TCP and SQLite without
  prototype mutation or key collision; encoding bytes remain compatible with
  non-JavaScript SDKs.
- **Error sanitization:** handler errors whose message contains `SQLITE` or `database` are rewritten to `Internal server error` before being framed back (`tcp.ts:290-292`, mirrored in `handler.ts:99-100`), so internal storage details never leak to clients.
- **Job release on disconnect with retry:** `releaseClientJobsWithRetry` (`tcp.ts:58`) retries up to 3× with exponential backoff (100/200/400ms). If all retries fail, it falls back to `forceReleaseClientJobs`, which unconditionally clears client tracking (prevents a `connections`/ownership Map leak) and resets heartbeats so the stall detector recovers any orphaned `active` jobs on its next tick — chosen over leaking the jobs.
- **Idempotency** is a command-level concern (`jobId`/`uniqueKey`/`dedup`), not a transport one; the transport delivers exactly the frames it parsed. `reqId` is purely for client-side response correlation and is echoed back verbatim, including on errors.
- **TLS startup invariant:** `loadTlsOptions` runs *before* `Bun.listen` binds the port (`tcp.ts:352`), so a bad cert/key path fails fast at startup instead of leaving a half-started listener.
- **`stop()`** stops the listener and `socket.end()`s every connection (clearing each stall timer first); `broadcast()` frames the message once and fans it out through each connection's `SocketWriteQueue`.

## Configuration

| Env var / option | Default | Effect |
| --- | --- | --- |
| `TcpServerConfig.port` / `TCP_PORT` | `6789` | Listen port. |
| `TcpServerConfig.hostname` / `HOST` | `0.0.0.0` | Bind address. |
| `TCP_SOCKET_PATH` | — | **Reserved, currently inert for TCP**: it is resolved into config and shown in the startup banner (`bootstrap.ts:35`), but `bootstrap` never passes it to `createTcpServer` (`bootstrap.ts:106-111`) and `TcpServerConfig` has no socket-path field, so the TCP listener always binds host/port. Only `HTTP_SOCKET_PATH` produces a Unix-socket bind (see [Configuration & Entrypoint](./configuration.md)). |
| `TcpServerConfig.authTokens` / `AUTH_TOKENS` | `[]` | If empty, connections are auto-authenticated; otherwise `Auth` is required. |
| `TcpServerConfig.idleTimeoutMs` / `TCP_IDLE_TIMEOUT_MS` | `60000` | Slowloris stall timeout (ms) for in-progress partial frames; `0` disables. |
| `TcpServerConfig.maxWriteQueueBytes` / `TCP_MAX_WRITE_QUEUE_BYTES` | `67108864` (64MB) | Max buffered outbound bytes before dropping the connection; `0` disables. |
| `TcpServerConfig.tls` / `TLS_CERT_FILE` + `TLS_KEY_FILE` | — | Native TLS termination (both files required). |
| `MAX_FRAME_SIZE` (constant) | 64MB | Hard cap on a single frame body; not env-tunable on the server (`FrameParser` constructor takes an override used in tests). |
| `MAX_CONCURRENT_PER_CONNECTION` (constant) | `50` | Per-connection pipelining concurrency. |

`PROTOCOL_VERSION = 2` and `SUPPORTED_CAPABILITIES = ['pipelining']` are reported via the `Hello` handshake.

## Related Docs

- [TCP Server Command Handlers](./tcp-server-handlers.md) — what `handleCommand` does with each frame.
- [Client Transport](./client-transport.md) — the client side of this protocol (pool, reconnect, auto-batching).
- [Security: TLS, Auth, CORS](./security-tls-auth.md) — `Auth`, token comparison, TLS files.
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md) — the limiter consulted once per complete protocol frame.
- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md) — semantics of the core commands and lock tokens.
- [HTTP / REST / SSE / WebSocket API](./http-api.md) — the alternative transport.
- [Stats, Metrics & Monitoring](./stats-and-monitoring.md) — `Hello`, `Ping`, `Stats`, `Metrics`, `Prometheus`.
