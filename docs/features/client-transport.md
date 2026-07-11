# Client Transport (TCP pool, reconnect, batching)

> **Category:** Client SDK · **Source:** `src/client/tcpPool.ts`, `src/client/tcp/client.ts`, `src/client/tcp/connection.ts`, `src/client/tcp/reconnect.ts`, `src/client/tcp/health.ts`, `src/client/tcp/types.ts`, `src/client/tcp/shared.ts`, `src/client/tcpClient.ts`, `src/client/queue/addBatcher.ts`

## Purpose

The client transport layer is the wire-level plumbing that the [Queue](./client-queue-sdk.md) and [Worker](./client-worker-sdk.md) SDKs use to talk to a remote bunqueue server over TCP. It owns the socket lifecycle: a load-aware connection pool (`TcpConnectionPool`), per-connection request/response multiplexing with msgpack framing and reqId-based pipelining (`TcpClient`), automatic reconnection with exponential backoff (`ReconnectManager`), liveness detection via ping and half-open-socket recovery (`HealthTracker`), optional TLS, and transparent write coalescing of `add()` calls (`AddBatcher`). It exists so the higher-level SDKs can issue command objects and await responses without managing sockets, retries, or batching themselves.

## Responsibilities & Scope

Owns:
- TCP connection establishment (plaintext or TLS), `connectTimeout` enforcement, and OS-level TCP keepalive setup (`connection.ts:155`).
- Frame parsing / framing of msgpack-encoded command and response objects (delegated to `FrameParser` from the [TCP protocol](./tcp-protocol.md) module).
- Request/response correlation via per-command `reqId`, pipelining up to `maxInFlight` commands, and per-command timeouts.
- Reconnection scheduling with exponential backoff + jitter and a configurable attempt cap.
- Connection health: periodic ping, ping-failure counting, and half-open detection (sustained command timeouts force a reconnect, #94).
- Pooling: round-robin load-aware client selection, reference-counted shared pools keyed by `host:port:poolSize:token:tls`.
- Auto-batching of `Queue.add()` calls into a single `addBulk`/`PUSHB` round-trip.

Does NOT own:
- The wire format / framing algorithm itself — see [TCP Wire Protocol & Framing](./tcp-protocol.md) (`FrameParser`, `pack`/`unpack`).
- Command semantics (what `PUSH`/`PULL`/`ACK` do server-side) — see [TCP Server Command Handlers](./tcp-server-handlers.md) and [Job Lifecycle](./job-lifecycle.md).
- Embedded (in-process) mode — when `embedded: true`, the SDK bypasses this layer entirely and the pool/batcher are `null` (`queue.ts:71-76`).
- Higher-level retry/DLQ/stall policy — see [Client SDK: Queue](./client-queue-sdk.md).
- Worker re-registration logic (the transport only emits the `connected`/reconnect signal; the Worker reacts).

## Dependencies

Internal:
- `FrameParser`, `FrameSizeError` from `src/infrastructure/server/protocol` — see [TCP Wire Protocol & Framing](./tcp-protocol.md). Used for `FrameParser.frame(...)` on write and incremental `addData(...)` on read.
- Consumed by [Client SDK: Queue](./client-queue-sdk.md) (`queue.ts` wires `TcpConnectionPool` + `AddBatcher`), [Client SDK: Worker](./client-worker-sdk.md) (`worker.ts` creates a pool sized `min(concurrency, 8)` and subscribes via `onReconnect`), and [Store-and-Forward](./store-and-forward.md).

External / runtime:
- `Bun.connect` (TCP socket), `node:fs` `readFileSync` (TLS CA file, read into bytes), `Bun.hash` (pool key hashing).
- `msgpackr` — `pack` / `unpack` for the binary protocol.
- Node `events.EventEmitter` (base class for `TcpClient` and `ReconnectManager`).

## Public Interface

### Classes

`TcpConnectionPool` (`tcpPool.ts:20`) — pool of `TcpClient`s:
```typescript
constructor(options: PoolOptions = {})           // PoolOptions extends Partial<ConnectionOptions> + { poolSize?: number }
async connect(): Promise<void>                    // connect all clients in parallel
async send(command: Record<string, unknown>): Promise<Record<string, unknown>>
async sendParallel(commands: Array<Record<string, unknown>>): Promise<Array<Record<string, unknown>>>
onReconnect(cb: () => void): void                 // fires on every (re)connect of any pooled client
isConnected(): boolean
getConnectedCount(): number
getPoolSize(): number
addRef(): void                                    // shared-pool refcount
release(): void                                   // decrement; close() at zero
close(): void
isClosed(): boolean
getHealth(): { healthy; connectedCount; totalCount; clients: ConnectionHealth[]; avgLatencyMs; totalCommands; totalErrors }
```

Module functions (`tcpPool.ts`):
- `getSharedPool(options?: PoolOptions): TcpConnectionPool` — get-or-create a refcounted pool keyed by `getPoolKey` (`tcpPool.ts:242`).
- `releaseSharedPool(pool): void`, `closeAllSharedPools(): void`.

`TcpClient extends EventEmitter` (`tcp/client.ts:61`) — single connection:
```typescript
constructor(options: Partial<ConnectionOptions> = {})
async connect(): Promise<void>
async send(command: Record<string, unknown>): Promise<Record<string, unknown>>
async ping(): Promise<boolean>
close(): void
isConnected(): boolean
getState(): 'connected' | 'connecting' | 'disconnected' | 'closed'
getHealth(): ConnectionHealth
getInFlightCount(): number
```

`ReconnectManager extends EventEmitter` (`tcp/reconnect.ts:19`): `setClosed`, `isClosed`, `reset`, `cancelReconnect`, `canReconnect`, `scheduleReconnect(connectFn)`.

`HealthTracker` (`tcp/health.ts:25`): `recordSuccess`, `recordError`, `recordConnected`, `recordPingSuccess`, `recordPingFailure(): boolean`, `recordCommandTimeout(): boolean`, `getHealth(state)`, `startPing(fn)`, `stopPing`.

`CommandQueue` (`tcp/connection.ts:246`): `enqueue`, `dequeue`, `remove(id)`, `addInFlight`, `removeByReqId`, `canSendMore(maxInFlight)`, `hasPending`, `getInFlightCount`, `rejectAll(error)`.

`AddBatcher<T>` (`queue/addBatcher.ts:44`):
```typescript
constructor(config: AddBatcherConfig, flushCb: FlushCallback<T>)
enqueue(name, data, opts?): Promise<Job<T>>
async flush(): Promise<void>
async waitForInFlight(): Promise<void>
stop(): void                                      // rejects remaining pending with "AddBatcher stopped"
hasPending(): boolean
```

`ClientClosedError extends Error` (`tcp/client.ts:22`) — sentinel for the synthetic rejection issued by `close()`/`rejectAll()`.

Helper: `createConnection(target, connectTimeout, events): Promise<ConnectionResult>` and `buildClientTls(tls)` (`tcp/connection.ts:68,43`).

`src/client/tcpClient.ts` is a deprecated re-export shim (`@deprecated Import from './tcp' instead`) that re-exports `TcpClient`, `getSharedTcpClient`, `closeSharedTcpClient`, `DEFAULT_CONNECTION`, and the `ConnectionOptions`/`ConnectionHealth` types. Shared single-client variants live in `tcp/shared.ts` (`getSharedTcpClient`, `closeSharedTcpClient`).

### TCP commands issued by this layer

- `Auth` — sent via `sendDirect` during `doConnect` when a `token` is configured (`client.ts:207`).
- `Ping` — sent by the health timer and by the Worker heartbeat path.
- All other commands (`PUSH`, `PUSHB`, `PULL`, `ACK`, `FAIL`, queries, control, …) pass through opaquely as `Record<string, unknown>`; the transport adds a `reqId` field and reads `response.reqId`.

### Events emitted (`TcpClient`)

`connected`, `disconnected`, `reconnecting` (`{ attempt, delay }`), `maxReconnectAttemptsReached`, `error` (`Error`), `warning` (`{ type, reqId? }`; types `unknown_response`, `malformed_frame`), `health` (`{ type, latency?, reason? }`; types `ping_success`, `ping_failed`, `unhealthy`). `ReconnectManager` emits `reconnecting` and `maxReconnectAttemptsReached`.

## Data Models

Key shapes defined by this layer:

`ConnectionOptions` (`tcp/types.ts:19`) with `DEFAULT_CONNECTION` (`tcp/types.ts:83`): `host='localhost'`, `port=6789`, `token=''`, `tls=false`, `maxReconnectAttempts=Infinity`, `reconnectDelay=100`, `maxReconnectDelay=30000`, `connectTimeout=5000`, `commandTimeout=30000`, `autoReconnect=true`, `pingInterval=30000`, `maxPingFailures=3`, `maxCommandTimeouts=3`, `pipelining=true`, `maxInFlight=100`.

`ClientTlsOptions` (`tcp/types.ts:11`): `{ rejectUnauthorized?: boolean; caFile?: string }`. `tls` accepts `boolean | ClientTlsOptions`.

`PoolOptions` (`tcpPool.ts:8`) = `Partial<ConnectionOptions>` + `poolSize?` (default 4, floored to `>= 1`).

`PendingCommand` (`tcp/types.ts:102`): `{ id, reqId, command, resolve, reject, timeout, promise? }`.

`ConnectionHealth` (`tcp/types.ts:59`): `{ healthy, state, lastSuccessAt, lastErrorAt, avgLatencyMs, consecutivePingFailures, consecutiveCommandTimeouts, totalCommands, totalErrors, uptimeMs }`.

`AutoBatchOptions` (`client/types.ts:433`): `{ enabled?: boolean; maxSize?=50; maxDelayMs?=5 }`. `AddBatcherConfig` (`addBatcher.ts:23`) adds `maxPending?` (default 10000).

## Business Logic / Control Flow

### Send path (pipelined)
1. `TcpConnectionPool.send` rejects if closed, else picks a client via `getNextClient` (`tcpPool.ts:81`): scans from `currentIndex` for the first `isConnected()` client (advancing `currentIndex`); if all are down it falls back to plain round-robin so the chosen client triggers its own reconnect.
2. `TcpClient.send` (`client.ts:450`) assigns a monotonic `id` and a wrapped `reqId` (`generateReqId` masks to 31 bits, `client.ts:357`), enqueues the `PendingCommand`, then: if disconnected and not connecting it kicks off `connect().catch(()=>{})`; if connected it calls `processQueue`.
3. `processQueue` (`client.ts:418`) drains the queue while `hasPending()` and `canSendMore(maxInFlight)` (backpressure at `inFlightByReqId.size < maxInFlight`, `connection.ts:277`), (re)arms the per-command timeout, moves the command to in-flight, and writes `FrameParser.frame(pack(next.command))`.
4. On inbound data, `connection.ts` `data` handler feeds bytes to `frameParser.addData(data)` and dispatches each complete frame to `handleData` (`client.ts:218`): `unpack` the frame, match `response.reqId` against in-flight, `clearTimeout`, resolve, and re-run `processQueue` to send more.

### Connect path
`doConnect` (`client.ts:171`) calls `createConnection` (msgpack over `Bun.connect`, TLS via `buildClientTls`), enables TCP keepalive (`setKeepAlive(true, 15000)`, best-effort, `connection.ts:155`), authenticates via `sendDirect({cmd:'Auth'})` when a token is set, then marks connected and `recordConnected()`. `connect` (`client.ts:130`) resets the reconnect counter, emits `connected`, starts the ping timer, and flushes the queue. Concurrent `connect()` calls dedupe through `waitForConnection` (`client.ts:156`).

### TLS server-certificate verification (#109)

`Bun.connect` does **not** reject an unauthorized peer on the client side — it completes the socket regardless of `ca`/`rejectUnauthorized`. It does, however, compute the peer's authorization result and pass it to the `handshake(socket, success, authorizationError)` callback. `createConnection` enforces it there: `tlsRequiresVerification` (`connection.ts`) treats verification as the default for any TLS connection and only an explicit `rejectUnauthorized: false` opts out (encryption-only). On a required-verification connection a non-null `authorizationError` (wrong/absent CA, self-signed, hostname mismatch) closes the socket and rejects with `TLS verification failed: <reason>`; otherwise the connection resolves.

Two ordering facts drive the implementation: (1) `buildClientTls` reads `caFile` into **bytes** (`readFileSync`) rather than passing a `Bun.file` handle, so Bun computes `authorizationError` against the pinned CA; (2) once a `handshake` handler is registered, Bun fires `open` **before** the TLS handshake completes (without one, `open` fires only after). So for every TLS connection the resolve is gated on `handshake`, not `open` — resolving in `open` would let the pool write its first command onto a socket whose handshake is still in flight and lose the bytes. Plaintext has no handshake event and still resolves in `open`.

### Reconnect path
On close/error/forced teardown, `scheduleReconnect` (`reconnect.ts:79`) increments `reconnectAttempts`, emits `maxReconnectAttemptsReached` (and stops) once it exceeds `maxReconnectAttempts`, else computes `baseDelay = min(reconnectDelay * 2^(attempt-1), maxReconnectDelay)` plus `Math.random()*0.3*baseDelay` jitter, emits `reconnecting`, and arms a single timer (guards against double-scheduling via `reconnectTimer`/`closed`).

### Half-open detection (#94)
A dead peer with no FIN/RST leaves writes succeeding while no response returns. Two recovery signals: (a) ping failures — `handlePingFailure` (`client.ts:302`) forces reconnect once `recordPingFailure()` hits `maxPingFailures`; (b) command timeouts — `handleCommandTimeout` (`client.ts:319`) forces reconnect once `recordCommandTimeout()` hits `maxCommandTimeouts` (default 3, 0 disables). Any successful command or ping resets the consecutive-timeout counter (`health.ts:46,72`), so it only fires on a sustained run. `forceReconnect` (`client.ts:326`) tears down the socket (swallowing `end()` errors), `rejectAll`s in-flight commands immediately (preventing stale timeouts from re-triggering a reconnect storm), and reschedules.

### Auto-batch path
`Queue.add` routes through `AddBatcher.enqueue` unless `opts.durable` is set or the batcher is disabled (`queue.ts:215`). `enqueue` (`addBatcher.ts:61`) pushes the entry, then: flush immediately if `pending.length >= maxSize`; **also** flush immediately if no flush is in-flight (`!this.flushing`) — this gives sequential `await`ed adds zero added latency; otherwise arm a `maxDelayMs` timer so concurrent adds coalesce. `doFlush` (`addBatcher.ts:108`) loops `flushOnce` until the buffer drains, so items arriving during a flush are batched into the next round-trip. `flushOnce` splices the whole buffer, calls `flushCb` (which invokes `addBulk` → `PUSHB`), and fan-out-resolves/rejects each caller by index.

## Concurrency & Locking

No mutexes — single-threaded JS event loop. Concurrency is managed by:
- **Pipelining window**: `maxInFlight` (default 100) bounds simultaneously outstanding commands per `TcpClient`; `processQueue` stops dequeuing when the window is full and resumes as responses arrive.
- **reqId correlation**: responses are matched by `reqId` (`inFlightByReqId` map), so out-of-order responses across pipelined commands are handled correctly; a legacy single-command fallback exists in `handleData` (`client.ts:236`).
- **Connect dedupe**: the `connecting` flag + `waitForConnection` ensure overlapping `connect()` calls share one attempt.
- **Reconnect single-flight**: `scheduleReconnect` no-ops if a timer is already armed or the manager is closed.
- **Batcher flush serialization**: the `flushing` flag plus `inFlightFlushes` set ensure only one flush loop runs at a time; `waitForInFlight()` lets `close()` await outstanding flushes (`queue.ts:608-611`).
- **Shared-pool refcounting**: `addRef`/`release` (`tcpPool.ts:153`) — the pool closes only when the count hits zero; `getSharedPool` removes a closed/errored pool from the map before recreating.

## Edge Cases & Failure Modes

- **Malformed frame**: `handleData` catch (`client.ts:247`) treats the framed stream as unrecoverable — emits `warning {malformed_frame}`, `rejectAll`s every pending/in-flight command (so they don't hang until per-command timeout), and `forceReconnect`s for a clean stream.
- **Frame too large**: `FrameSizeError` from `addData` surfaces as an `error` event (`connection.ts:131`) and the read returns without dispatching.
- **Command timeout taxonomy** (`client.ts:461`): a still-queued command (never written) is rejected but does NOT count toward dead-link detection; an in-flight command that got no response rejects AND calls `handleCommandTimeout`.
- **Connection lost / close**: `handleClose` (`client.ts:262`) rejects all in-flight with `Connection lost` and reconnects only if it was previously connected and `canReconnect()`.
- **Intentional close idempotency / unhandled-rejection safety**: `close` (`client.ts:511`) sets closed, stops ping, cancels reconnect, installs a process-level `unhandledRejection` filter that swallows only `ClientClosedError` (`installClientClosedFilter`, `client.ts:45`), and `rejectAll`s with `ClientClosedError`. `rejectAll` attaches a silent `.catch` to each `cmd.promise` before rejecting (`connection.ts:342`) so fire-and-forget callers (heartbeats, polling loops) don't surface unhandled rejections.
- **Max reconnect attempts reached**: `TcpClient` constructor rejects all queued commands with `Max reconnection attempts reached` (`client.ts:125`).
- **TLS connect failure**: `Bun.connect` may reject (handshake refused) instead of firing `connectError`; the `.catch` in `createConnection` (`connection.ts:221`) routes it to the same rejection so callers never hang past `connectTimeout`.
- **Socket error listener**: `TcpConnectionPool` attaches a no-op `error` listener to each client (`tcpPool.ts:70`) so an EventEmitter `error` (e.g. TLS handshake garbage) never crashes the process.
- **Durable bypass**: `durable` jobs skip the batcher and go out as individual `PUSH` (`queue.ts:215`); `Store-and-Forward` disables auto-batch entirely (`forwarder.ts: autoBatch:{enabled:false}`).
- **Batcher overflow**: at `maxPending` (10000) `enqueue` drops and rejects the oldest 10% with `Add buffer overflow - oldest entries dropped` (`addBatcher.ts:69`).
- **Batcher stop**: `stop()` rejects all remaining pending with `AddBatcher stopped`; `enqueue` after stop rejects immediately.
- **Memory bound**: `HealthTracker` keeps only the last 10 latencies (`MAX_LATENCY_HISTORY`, `health.ts:36`) for the rolling average.
- **reqId wrap**: counter masks to `0x7fffffff` (`client.ts:359`); collisions across a 2^31 window of simultaneously in-flight commands are not defended against (impractical given `maxInFlight`).
- **Pool degraded mode**: when every client is disconnected, `getNextClient` still returns one (round-robin) so the send attempt drives reconnection rather than failing fast.

## Configuration

All knobs come through `ConnectionOptions` / `PoolOptions` (programmatic; no env vars are read directly in this layer). Defaults from `DEFAULT_CONNECTION` (`tcp/types.ts:83`):

| Option | Default | Effect |
| --- | --- | --- |
| `host` / `port` | `localhost` / `6789` | server target |
| `token` | `''` | sends `Auth` on connect when set |
| `tls` | `false` | `true` = system CAs; object = `{caFile, rejectUnauthorized}` |
| `poolSize` | 4 (Queue); `min(concurrency, 8)` (Worker) | clients per pool, floored to ≥1 |
| `maxReconnectAttempts` | `Infinity` | cap before `maxReconnectAttemptsReached` |
| `reconnectDelay` / `maxReconnectDelay` | 100 / 30000 ms | backoff base / ceiling |
| `connectTimeout` | 5000 ms | per-connect deadline |
| `commandTimeout` | 30000 ms | per-command deadline |
| `autoReconnect` | `true` | enable reconnection |
| `pingInterval` | 30000 ms (0 = off) | health ping cadence |
| `maxPingFailures` | 3 | consecutive ping fails → reconnect |
| `maxCommandTimeouts` | 3 (0 = off) | consecutive timeouts → reconnect (#94) |
| `pipelining` | `true` | forwarded by the pool, but not read by the send path (see caveat below) |
| `maxInFlight` | 100 | pipelining window per `TcpClient` (forwarded by the pool) |
| `autoBatch.maxSize` / `maxDelayMs` / `enabled` | 50 / 5 ms / true (TCP) | `add()` coalescing |
| `autoBatch.maxPending` | 10000 | batcher overflow bound |

Caveat: the `pipelining` flag is declared, defaulted, and forwarded, but never read by `TcpClient` (the reqId send path is unconditional, so pipelining is effectively always on). `TcpConnectionPool` forwards both `pipelining` and `maxInFlight` into every `TcpClient` it constructs (`tcpPool.ts:51`), so a `maxInFlight` override takes effect on pooled connections too.

Shared pools are reused only when `poolSize === 4 && !token` for a Queue (`queue.ts:82`); otherwise a dedicated pool is created. Pool sharing key (`getPoolKey`, `tcpPool.ts:230`) includes host, port, poolSize, a 16-bit token hash, the JSON-stringified TLS config, and `pipelining`/`maxInFlight`, so plaintext and TLS pools to the same target never alias and Queues with different pipelining windows never share a pool.

## Related Docs

- [Client SDK: Queue](./client-queue-sdk.md) — wires the pool + `AddBatcher` and routes `add()`.
- [Client SDK: Worker (& sandboxed)](./client-worker-sdk.md) — pool sizing, `onReconnect` re-registration.
- [TCP Wire Protocol & Framing](./tcp-protocol.md) — `FrameParser`, msgpack framing.
- [TCP Server Command Handlers](./tcp-server-handlers.md) — server side of these commands.
- [Security: TLS, Auth, CORS](./security-tls-auth.md) — `Auth` command, TLS server config.
- [Store-and-Forward & BullMQ Compatibility](./store-and-forward.md) — edge→central forwarding over this transport.
- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md) — command semantics.
