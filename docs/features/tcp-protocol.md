# TCP Wire Protocol & Framing

> **Category:** Transport · **Source:** `src/infrastructure/server/tcp.ts`, `src/infrastructure/server/protocol.ts`, `src/infrastructure/server/socketWriteQueue.ts`, `src/domain/types/command.ts`, `src/domain/types/response.ts`, `src/shared/serialization.ts`

## Purpose

This module is the binary transport layer that carries every client/server interaction over a raw TCP socket (default `:6789`). Each message is a length-prefixed frame whose body is a MessagePack-encoded command or response; the server accepts these frames, decodes them into typed `Command` objects, and writes back framed `Response` objects. It owns frame assembly/disassembly, pipelined (parallel) command processing per connection, write-side backpressure handling, and the transport-level DoS mitigations (frame-size cap, slowloris stall timeout, write-queue bound). It does not interpret command semantics — that is delegated to the command handlers.

## Responsibilities & Scope

Owns:
- **Length-prefixed framing** — `FrameParser` (`protocol.ts:194`) reassembles complete frames from arbitrary TCP segment boundaries, and `FrameParser.frame()` (`protocol.ts:287`) prepends the 4-byte big-endian length prefix on the way out.
- **MessagePack (de)serialization of the wire body** — `pack`/`unpack` from `msgpackr`, called in `tcp.ts:123` (`serializeResponse`), `tcp.ts:128` (`errorResponse`), and `tcp.ts:255` (decode inbound command).
- **TCP listener lifecycle** — `createTcpServer()` (`tcp.ts:135`) wires `Bun.listen` with `open`/`data`/`close`/`error`/`drain` socket handlers, plus `broadcast()` and `stop()`.
- **Per-connection pipelining** — frames in one read are decoded and processed in parallel, bounded by a `Semaphore` of `MAX_CONCURRENT_PER_CONNECTION = 50` (`tcp.ts:27`, `tcp.ts:267`, `tcp.ts:283`).
- **Write-side backpressure** — `SocketWriteQueue` (`socketWriteQueue.ts:24`) buffers unwritten tails on short writes and flushes them on `drain`.
- **Transport DoS bounds** — 64MB max frame size (`protocol.ts:180`), slowloris stall timer (`tcp.ts:156`), and a write-queue byte cap (`tcp.ts:49`).
- **Per-connection identity & cleanup** — assigns a `clientId` (uuid) on open, releases that client's leased jobs on close (`tcp.ts:298`).
- **The command/response type contract** — the `Command` and `Response` discriminated unions (`command.ts:598`, `response.ts:201`) plus the response builder functions (`response.ts:225`+).

Does NOT own (delegated elsewhere):
- **Command dispatch / business logic** — `handleCommand()` routes to the handler groups; see [TCP Server Command Handlers](./tcp-server-handlers.md).
- **Authentication / authorization** — gated inside `handleCommand` (`Auth`, token check); see [Security: TLS, Auth, CORS](./security-tls-auth.md). This module only carries the `Auth`/`Hello` frames and supplies the `authTokens` set + `authenticated` flag in the handler context.
- **Rate limiting policy** — `getRateLimiter()` is consulted at `tcp.ts:201`, but the limiter itself lives in [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md).
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
`protocol.ts` also exports a **legacy JSON-text path** — `parseCommand`/`parseCommands`/`serializeResponse(string)`/`LineBuffer` (newline-delimited) — and validators `validateQueueName`, `validateJobData` (10MB cap, `protocol.ts:57`), `validateNumericField`, `validateJobOptions`, plus `ConnectionState`/`createConnectionState`. The binary `FrameParser` path is the one the TCP server uses; the JSON-text helpers are not invoked by `createTcpServer`.

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

**TCP commands carried (exact `cmd` names):** the full set is the `Command` union (`command.ts:598`):
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

**Connection open** (`tcp.ts:172`): allocate `clientId = uuid()`, build `HandlerContext` with `authenticated = authTokens.size === 0` (auto-auth when no tokens configured), and attach per-connection `ConnectionData`: a fresh `FrameParser`, `Semaphore(50)`, `SocketWriteQueue(maxWriteQueueBytes)`, and a null `stallTimer`. Register in `connections` map and emit `client:connected`.

**Inbound data** (`tcp.ts:196`):
1. Rate-limit gate (`tcp.ts:201`): if `getRateLimiter().isAllowed(clientId)` is false, emit `ratelimit:hit`, write a framed `Rate limit exceeded` error, and return without parsing.
2. `frameParser.addData(data)` (`tcp.ts:212`) — the Bun read `Buffer` is passed directly; the previous defensive `new Uint8Array(data)` copy was removed (copy elision) because `addData` copies its input into its own owned buffer and never retains the caller's view.
3. A thrown `FrameSizeError` (`tcp.ts:214`) → clear stall timer, write a `Frame too large: …` error, `socket.end()`.
4. `updateStallTimer(socket)` (`tcp.ts:232`) — (re)arm the slowloris timer iff a partial frame is now buffered.
5. For each complete frame, `processFrame` (`tcp.ts:252`): `unpack` → `Command`; on decode failure write `Invalid command format`; on missing `cmd` write `Invalid command`; otherwise run under `withSemaphore` → `handleCommand(cmd, ctx)` → write the framed response (or a framed error carrying `cmd.reqId`). After each write, `dropForWriteOverflow()` checks the write-queue bound.
6. `await Promise.all(frames.map(processFrame))` (`tcp.ts:283`) — all frames in one read are processed concurrently (pipelining); the client correlates responses by `reqId`, so server-side ordering is not guaranteed.

**Frame parsing — `FrameParser.addData`** (`protocol.ts:205`): concatenate the retained partial tail with new data into a single owned buffer (one copy), then walk it with an `offset` cursor. Each iteration reads the big-endian u32 length (`>>> 0` to force unsigned, `protocol.ts:223`), rejects lengths > `maxFrameSize` (clears the buffer and throws `FrameSizeError`, `protocol.ts:236`), breaks if the full body has not yet arrived (keeping the partial bytes buffered, `protocol.ts:242`), else slices out the body (`protocol.ts:251`) and advances the cursor by `4 + length`. Tail retention has three cases (`protocol.ts:260`): fully drained → fresh empty buffer (don't pin the read's ArrayBuffer); nothing consumed → keep the concat buffer as-is (no extra copy); partial leftover after ≥1 frame → slice the small tail. The cursor makes the pass **O(total bytes) / O(F)** in the number of frames; the prior implementation resliced the tail after every frame, which was **O(F²)** when many frames arrived coalesced in one read (#103 perf fix).

**Outbound write & backpressure — `SocketWriteQueue`** (`socketWriteQueue.ts:55`): if a tail is already pending, append the new chunk (never write ahead of older bytes — preserves frame order). Otherwise `socket.write(data)`; a return `< 0` means the socket is closed (`write` returns `false`), and a short write buffers the unwritten `data.subarray(written)` tail. `flush()` (`socketWriteQueue.ts:92`), invoked from the socket `drain` handler (`tcp.ts:321`), drains the pending queue in order, advancing `offset` within the current chunk on a partial write and stopping at the first short write. This matters because Bun's `socket.write()` may write fewer bytes than supplied; silently dropping the tail would corrupt the length-prefixed stream.

**Connection close** (`tcp.ts:286`): clear stall timer, drop buffered writes (`writeQueue.clear()`), remove from `connections`, drop the rate-limiter entry, `unregisterWorkersByClientId`, emit `client:disconnected`, then `releaseClientJobsWithRetry` to return that client's leased jobs to their queues.

## Concurrency & Locking

- **Per-connection pipelining is bounded by a `Semaphore(50)`** (`MAX_CONCURRENT_PER_CONNECTION`, `tcp.ts:27`). All frames from a single `data` event are launched together (`Promise.all`), but at most 50 commands per connection are in-flight at once; the rest await a permit. The semaphore (`shared/semaphore.ts`) is a simple FIFO permit queue.
- **No lock is taken in this module.** Frame parsing is synchronous per `data` callback (single-threaded JS event loop), and the per-connection `FrameParser`/`SocketWriteQueue`/`stallTimer` are touched only from that connection's own callbacks. Job-state locking (the `jobIndex → completedJobs → shards → processingShards` hierarchy) happens downstream inside `QueueManager`; see [Concurrency & Locking](./concurrency-and-locking.md).
- **Lease ownership** is keyed by the connection `clientId`: jobs pulled over a connection are released on disconnect. `PULL`/`PULLB` accept `owner`/`lockTtl`, and `detach: true` (CLI use) opts out of auto-release on disconnect. Lock-token mechanics belong to [Job Lifecycle](./job-lifecycle.md).
- **`SocketWriteQueue` ordering invariant:** once any tail is pending, every subsequent `write` must enqueue rather than write directly, or response bytes would interleave and corrupt the frame stream.

## Edge Cases & Failure Modes

- **Frame size cap (64MB):** a declared length over `MAX_FRAME_SIZE` throws `FrameSizeError`, clears the parser buffer (stops processing attacker bytes), sends an error, and ends the socket. Legal large partial frames are *not* dropped mid-assembly — only over-cap declarations are.
- **Slowloris mitigation:** a peer that starts a frame (buffers partial bytes) but stalls is closed after `idleTimeoutMs` (default 60s, `tcp.ts:41`). The timer is armed **only while `hasPartialFrame` is true** and reset on each data event, so healthy idle-but-complete connections are never disturbed. Implemented as a manual `setTimeout` because Bun (1.3.x) has no socket `idleTimeout` and `socket.timeout()` resets on every byte (a 1-byte-per-window trickle would defeat it). `0` disables.
- **Write-side memory DoS:** if a client stops reading while the server keeps producing responses, `SocketWriteQueue.bytesQueued` grows; once it exceeds `maxWriteQueueBytes` (default 64MB, `tcp.ts:49`), `isOverBudget` trips and the connection is `terminate()`d after clearing the queue (`tcp.ts:236`, also enforced in `broadcast` at `tcp.ts:354`). `0` disables.
- **Malformed input:** non-msgpack or non-object bodies → `Invalid command format`; bodies lacking `cmd` → `Invalid command`. Neither closes the connection — only a returned framed error.
- **Error sanitization:** handler errors whose message contains `SQLITE` or `database` are rewritten to `Internal server error` before being framed back (`tcp.ts:273-275`, mirrored in `handler.ts:99-100`), so internal storage details never leak to clients.
- **Job release on disconnect with retry:** `releaseClientJobsWithRetry` (`tcp.ts:58`) retries up to 3× with exponential backoff (100/200/400ms). If all retries fail, it falls back to `forceReleaseClientJobs`, which unconditionally clears client tracking (prevents a `connections`/ownership Map leak) and resets heartbeats so the stall detector recovers any orphaned `active` jobs on its next tick — chosen over leaking the jobs.
- **Idempotency** is a command-level concern (`jobId`/`uniqueKey`/`dedup`), not a transport one; the transport delivers exactly the frames it parsed. `reqId` is purely for client-side response correlation and is echoed back verbatim, including on errors.
- **TLS startup invariant:** `loadTlsOptions` runs *before* `Bun.listen` binds the port (`tcp.ts:330`), so a bad cert/key path fails fast at startup instead of leaving a half-started listener.
- **`stop()`** stops the listener and `socket.end()`s every connection (clearing each stall timer first); `broadcast()` frames the message once and fans it out through each connection's `SocketWriteQueue`.

## Configuration

| Env var / option | Default | Effect |
| --- | --- | --- |
| `TcpServerConfig.port` / `TCP_PORT` | `6789` | Listen port. |
| `TcpServerConfig.hostname` / `HOST` | `0.0.0.0` | Bind address. |
| `TCP_SOCKET_PATH` | — | Unix domain socket path (overrides host/port; wired by the entrypoint, see [Configuration & Entrypoint](./configuration.md)). |
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
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md) — the rate limiter consulted on every `data` event.
- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md) — semantics of the core commands and lock tokens.
- [HTTP / REST / SSE / WebSocket API](./http-api.md) — the alternative transport.
- [Stats, Metrics & Monitoring](./stats-and-monitoring.md) — `Hello`, `Ping`, `Stats`, `Metrics`, `Prometheus`.
