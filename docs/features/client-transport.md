# Client Transport (TCP pool, reconnect, batching)

> **Category:** Client SDK · **Source:** `src/client/tcpPool.ts`, `src/client/tcp/`, `src/client/tcp/runtime/`, `src/client/tcp/types/`, `src/client/tcpClient.ts`, `src/client/queue/addBatcher.ts`, `src/client/queue-events/tcpSubscription.ts`

## Purpose

The client transport layer is the wire-level plumbing that the [Queue](./client-queue-sdk.md) and [Worker](./client-worker-sdk.md) SDKs use to talk to a remote bunqueue server over TCP. It owns the socket lifecycle: a load-aware connection pool (`TcpConnectionPool`), per-connection request/response multiplexing with msgpack framing and reqId-based pipelining (`TcpClient`), ordered short-write buffering, automatic reconnection with exponential backoff (`ReconnectManager`), liveness detection via ping and half-open-socket recovery (`HealthTracker`), optional TLS, and transparent write coalescing of `add()` calls (`AddBatcher`). It exists so the higher-level SDKs can issue command objects and await responses without managing sockets, retries, or batching themselves.

## Responsibilities & Scope

Owns:
- TCP connection establishment (plaintext or TLS), `connectTimeout` enforcement, and OS-level TCP keepalive setup (`tcp/transport.ts:43-189`).
- Frame parsing / framing of msgpack-encoded command and response objects (delegated to `FrameParser` from the [TCP protocol](./tcp-protocol.md) module).
- Per-physical-socket outbound ordering: partial `Bun.Socket.write()` results are
  retained, later frames queue behind the missing tail, and `drain` resumes the
  write without replaying bytes across reconnects.
- Request/response correlation via per-command `reqId`, pipelining up to `maxInFlight` commands, and per-command timeouts.
- Dispatch of unsolicited `{ type: 'event', event: JobEvent }` frames before
  request correlation, so a subscription event cannot satisfy or reorder an
  in-flight command.
- Reconnection scheduling with exponential backoff + jitter and a configurable attempt cap.
- Connection health: periodic ping, ping-failure counting, and half-open detection (sustained command timeouts force a reconnect, #94).
- Pooling: round-robin load-aware client selection, reference-counted shared pools keyed by `host:port:poolSize:token:tls`.
- Auto-batching of `Queue.add()` calls into a single `addBulk`/`PUSHB` round-trip.
- Dedicated authenticated, reconnecting queue-event subscriptions used by
  `QueueEvents` and TCP Worker `stalled` notifications.

Does NOT own:
- The wire format / framing algorithm itself — see [TCP Wire Protocol & Framing](./tcp-protocol.md) (`FrameParser`, `pack`/`unpack`).
- Command semantics (what `PUSH`/`PULL`/`ACK` do server-side) — see [TCP Server Command Handlers](./tcp-server-handlers.md) and [Job Lifecycle](./job-lifecycle.md).
- Embedded (in-process) mode — when `embedded: true`, the SDK bypasses this layer entirely and the pool/batcher are `null` (`queue/runtime/state.ts:31-38`).
- Higher-level retry/DLQ/stall policy — see [Client SDK: Queue](./client-queue-sdk.md).
- Worker re-registration logic (the transport only emits the `connected`/reconnect signal; the Worker reacts).

## Dependencies

Internal:
- `FrameParser`, `FrameSizeError` from `src/infrastructure/server/protocol` — see [TCP Wire Protocol & Framing](./tcp-protocol.md). Used for `FrameParser.frame(...)` on write and incremental `addData(...)` on read.
- `SocketWriteQueue` from `src/infrastructure/server/socketWriteQueue.ts` — the
  same ordered short-write primitive used by the server response path.
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
- `getSharedPool(options?: PoolOptions): TcpConnectionPool` — get-or-create a refcounted pool keyed by `getPoolKey` (`tcpPool.ts:229-269`).
- `releaseSharedPool(pool): void`, `closeAllSharedPools(): void`.

`TcpClient extends EventEmitter` (`tcp/client.ts:7`, through its runtime base classes) — single connection:
```typescript
constructor(options: Partial<ConnectionOptions> = {})
async connect(): Promise<void>
async send(command: Record<string, unknown>): Promise<Record<string, unknown>>
async hello(): Promise<HelloResponse>
async ping(): Promise<boolean>
close(): void
isConnected(): boolean
getState(): 'connected' | 'connecting' | 'disconnected' | 'closed'
getHealth(): ConnectionHealth
getInFlightCount(): number
```

`ReconnectManager extends EventEmitter` (`tcp/reconnect.ts:19`): `setClosed`, `isClosed`, `reset`, `cancelReconnect`, `canReconnect`, `scheduleReconnect(connectFn)`.

`HealthTracker` (`tcp/health.ts:25`): `recordSuccess`, `recordError`, `recordConnected`, `recordPingSuccess`, `recordPingFailure(): boolean`, `recordCommandTimeout(): boolean`, `getHealth(state)`, `startPing(fn)`, `stopPing`.

`CommandQueue` (`tcp/commandQueue.ts:4`): `enqueue`, `dequeue`, `remove(id)`, `addInFlight`, `removeByReqId`, `canSendMore(maxInFlight)`, `hasPending`, `getInFlightCount`, `rejectAll(error)`.

`AddBatcher<T>` (`queue/addBatcher.ts:44`):
```typescript
constructor(config: AddBatcherConfig, flushCb: FlushCallback<T>)
enqueue(name, data, opts?): Promise<Job<T>>
async flush(): Promise<void>
async waitForInFlight(): Promise<void>
stop(): void                                      // rejects remaining pending with "AddBatcher stopped"
hasPending(): boolean
```

`ClientClosedError extends Error` (`tcp/errors.ts:2`) — sentinel for the synthetic rejection issued by `close()`/`rejectAll()`.

Helpers: `createConnection(target, connectTimeout, events): Promise<ConnectionResult>` and `buildClientTls(tls)` (`tcp/transport.ts:43-189`, `tcp/transport.ts:26-35`).

`src/client/tcpClient.ts` is a deprecated re-export shim (`@deprecated Import from './tcp' instead`) that re-exports `TcpClient`, `getSharedTcpClient`, `closeSharedTcpClient`, `DEFAULT_CONNECTION`, and the `ConnectionOptions`/`ConnectionHealth` types. Shared single-client variants live in `tcp/shared.ts` (`getSharedTcpClient`, `closeSharedTcpClient`).

### TCP commands issued by this layer

- `Hello` — explicit protocol-version and capability negotiation via `hello()` (`tcp/runtime/commands.ts:10-21`).
- `Auth` — sent via `sendDirect` during `doConnect` when a `token` is configured (`tcp/runtime/connectivity.ts:52-90`).
- `Ping` — sent by the health timer and by the Worker heartbeat path.
- `SubscribeEvents` / `UnsubscribeEvents` — select or clear the one queue event
  stream associated with a dedicated connection.
- All other commands (`PUSH`, `PUSHB`, `PULL`, `ACK`, `FAIL`, queries, control, …) pass through opaquely as `Record<string, unknown>`; the transport adds a `reqId` field and reads `response.reqId`.

### Events emitted (`TcpClient`)

`connected`, `disconnected`, `reconnecting` (`{ attempt, delay }`), `maxReconnectAttemptsReached`, `error` (`Error`), `warning` (`{ type, reqId? }`; types `unknown_response`, `malformed_frame`), `health` (`{ type, latency?, reason? }`; types `ping_success`, `ping_failed`, `unhealthy`), and the internal typed `queueEvent` (`JobEvent`) consumed by `TcpEventSubscription`. `ReconnectManager` emits `reconnecting` and `maxReconnectAttemptsReached`.

## Data Models

Key shapes defined by this layer:

`ConnectionOptions` (`tcp/types/connection.ts:3-19`) with `DEFAULT_CONNECTION` (`tcp/types/connection.ts:34-50`): `host='localhost'`, `port=6789`, `token=''`, `tls=false`, `maxReconnectAttempts=Infinity`, `reconnectDelay=100`, `maxReconnectDelay=30000`, `connectTimeout=5000`, `commandTimeout=30000`, `autoReconnect=true`, `pingInterval=30000`, `maxPingFailures=3`, `maxCommandTimeouts=3`, `pipelining=true`, `maxInFlight=100`.

`ClientTlsOptions` (`tcp/types/tls.ts:1-4`): `{ rejectUnauthorized?: boolean; caFile?: string }`. `tls` accepts `boolean | ClientTlsOptions`.

`PoolOptions` (`tcpPool.ts:8`) = `Partial<ConnectionOptions>` + `poolSize?` (default 4, floored to `>= 1`).

`PendingCommand` (`tcp/types/command.ts:1-9`): `{ id, reqId, command, resolve, reject, timeout, promise? }`.

`ConnectionHealth` (`tcp/types/connection.ts:21-32`): `{ healthy, state, lastSuccessAt, lastErrorAt, avgLatencyMs, consecutivePingFailures, consecutiveCommandTimeouts, totalCommands, totalErrors, uptimeMs }`.

`AutoBatchOptions` (`client/types/connection.ts:20-24`): `{ enabled?: boolean; maxSize?=50; maxDelayMs?=5 }`. `AddBatcherConfig` (`queue/addBatcher.ts:23-30`) adds `maxPending?` (default 10000).

## Business Logic / Control Flow

### Send path (pipelined)
1. `TcpConnectionPool.send` rejects if closed, else picks a client via `getNextClient` (`tcpPool.ts:82-107`): scans from `currentIndex` for the first `isConnected()` client (advancing `currentIndex`); if all are down it falls back to plain round-robin so the chosen client triggers its own reconnect.
2. `TcpClient.send` (`tcp/runtime/commands.ts:86-137`) assigns a monotonic `id` and a wrapped `reqId` (`generateReqId` masks to 31 bits, `tcp/runtime/health.ts:134-137`), enqueues the `PendingCommand`, then: if disconnected and not connecting it kicks off `connect().catch(()=>{})`; if connected it calls `processQueue`.
3. `processQueue` drains the command queue while `hasPending()` and
   `canSendMore(maxInFlight)`, (re)arms the timeout, moves the command to
   in-flight, and frames it. `createConnection` sends the bytes through one
   `SocketWriteQueue` owned by that physical socket. A short write retains the
   exact tail; later frames wait behind it until Bun invokes `drain`.
4. On inbound data, the socket feeds bytes to `frameParser.addData(data)` and
   dispatches each complete frame to `handleData`. A validated `type:'event'`
   envelope emits `queueEvent` and returns immediately. Every other frame
   follows request correlation: match `response.reqId` against in-flight,
   clear its timeout, resolve, and re-run `processQueue` to send more.

### Dedicated event subscription

`TcpEventSubscription` owns one `TcpClient` and one queue key. It resolves the
same token and connection defaults as the queue/worker transport, connects and
authenticates first, sends `SubscribeEvents`, and reports ready only after the
acknowledgement. A disconnect invalidates the current generation; the client's
normal reconnect emits `connected`, which starts exactly one fresh subscription
attempt. `close()` invalidates pending work and closes the socket, so no event
can be delivered after teardown.

### Connect path
`doConnect` (`tcp/runtime/connectivity.ts:52-90`) calls `createConnection` (msgpack over `Bun.connect`, TLS via `buildClientTls`), enables TCP keepalive (`setKeepAlive(true, 15000)`, best-effort, `tcp/transport.ts:97-118`), authenticates via `sendDirect({cmd:'Auth'})` when a token is set, then marks connected and `recordConnected()`. `connect` (`tcp/runtime/connectivity.ts:11-33`) resets the reconnect counter, emits `connected`, starts the ping timer, and flushes the queue. Concurrent `connect()` calls dedupe through `waitForConnection` (`tcp/runtime/connectivity.ts:37-50`).

### TLS server-certificate verification (#109)

`Bun.connect` does **not** reject an unauthorized peer on the client side — it completes the socket regardless of `ca`/`rejectUnauthorized`. It does, however, compute the peer's authorization result and pass it to the `handshake(socket, success, authorizationError)` callback. `createConnection` enforces it there: `tlsRequiresVerification` (`tcp/transport.ts:37-41`) treats verification as the default for any TLS connection and only an explicit `rejectUnauthorized: false` opts out (encryption-only). On a required-verification connection a non-null `authorizationError` (wrong/absent CA, self-signed, hostname mismatch) closes the socket and rejects with `TLS verification failed: <reason>`; otherwise the connection resolves (`tcp/transport.ts:119-136`).

Two ordering facts drive the implementation: (1) `buildClientTls` reads `caFile` into **bytes** (`readFileSync`) rather than passing a `Bun.file` handle, so Bun computes `authorizationError` against the pinned CA; (2) once a `handshake` handler is registered, Bun fires `open` **before** the TLS handshake completes (without one, `open` fires only after). So for every TLS connection the resolve is gated on `handshake`, not `open` — resolving in `open` would let the pool write its first command onto a socket whose handshake is still in flight and lose the bytes. Plaintext has no handshake event and still resolves in `open`.

### Reconnect path
On close/error/forced teardown, `scheduleReconnect` (`reconnect.ts:79`) increments `reconnectAttempts`, emits `maxReconnectAttemptsReached` (and stops) once it exceeds `maxReconnectAttempts`, else computes `baseDelay = min(reconnectDelay * 2^(attempt-1), maxReconnectDelay)` plus `Math.random()*0.3*baseDelay` jitter, emits `reconnecting`, and arms a single timer (guards against double-scheduling via `reconnectTimer`/`closed`).

### Half-open detection (#94)
A dead peer with no FIN/RST leaves writes succeeding while no response returns. Two recovery signals: (a) ping failures — `handlePingFailure` (`tcp/runtime/health.ts:96-103`) forces reconnect once `recordPingFailure()` hits `maxPingFailures`; (b) command timeouts — `handleCommandTimeout` (`tcp/runtime/health.ts:105-110`) forces reconnect once `recordCommandTimeout()` hits `maxCommandTimeouts` (default 3, 0 disables). Any successful command or ping resets the consecutive-timeout counter (`tcp/health.ts:40-48`, `tcp/health.ts:68-74`), so it only fires on a sustained run. `forceReconnect` (`tcp/runtime/health.ts:112-126`) tears down the socket (swallowing `end()` errors), `rejectAll`s in-flight commands immediately (preventing stale timeouts from re-triggering a reconnect storm), and reschedules.

### Auto-batch path
`Queue.add` routes through `AddBatcher.enqueue` unless `opts.durable` is set or the batcher is disabled (`queue/runtime/queries.ts:10-15`). `enqueue` (`queue/addBatcher.ts:61-91`) pushes the entry, then: flush immediately if `pending.length >= maxSize`; **also** flush immediately if no flush is in-flight (`!this.flushing`) — this gives sequential `await`ed adds zero added latency; otherwise arm a `maxDelayMs` timer so concurrent adds coalesce. `doFlush` (`queue/addBatcher.ts:108-119`) loops `flushOnce` until the buffer drains, so items arriving during a flush are batched into the next round-trip. `flushOnce` (`queue/addBatcher.ts:127-150`) splices the whole buffer, calls `flushCb` (which invokes `addBulk` → `PUSHB`), and fan-out-resolves/rejects each caller by index.

## Concurrency & Locking

No mutexes — single-threaded JS event loop. Concurrency is managed by:
- **Pipelining window**: `maxInFlight` (default 100) bounds simultaneously outstanding commands per `TcpClient`; `processQueue` stops dequeuing when the window is full and resumes as responses arrive.
- **Byte-order gate**: the connection-local write queue serializes partially
  written frame tails ahead of every later frame. It is cleared on explicit
  close or disconnect and is never inherited by a replacement socket.
- **reqId correlation**: responses are matched by `reqId` (`inFlightByReqId` map), so out-of-order responses across pipelined commands are handled correctly; a legacy single-command fallback exists in `handleData` (`tcp/runtime/health.ts:27-46`).
- **Connect dedupe**: the `connecting` flag + `waitForConnection` ensure overlapping `connect()` calls share one attempt.
- **Reconnect single-flight**: `scheduleReconnect` no-ops if a timer is already armed or the manager is closed.
- **Batcher flush serialization**: the `flushing` flag plus `inFlightFlushes` set ensure only one flush loop runs at a time; `disconnect()` awaits `flush()` and `waitForInFlight()` before stopping the batcher (`queue/runtime/connection.ts:5-12`).
- **Shared-pool refcounting**: `addRef`/`release` (`tcpPool.ts:154-165`) — the pool closes only when the count hits zero; `getSharedPool` removes a closed pool from the map before recreating. Each Queue records whether it has released its constructor-owned reference, so repeated `close()` calls and `disconnect()` followed by `close()` cannot decrement the same ownership twice or close a peer Queue's pool.

## Edge Cases & Failure Modes

- **Malformed frame**: `handleData` catch (`tcp/runtime/health.ts:19-54`) treats the framed stream as unrecoverable — emits `warning {malformed_frame}`, `rejectAll`s every pending/in-flight command (so they don't hang until per-command timeout), and `forceReconnect`s for a clean stream.
- **Malformed event envelope**: an unsolicited frame must contain a finite
  timestamp and string `eventType`, `queue`, and `jobId`. An invalid envelope is
  treated as a malformed stream and reconnects; it never falls through to the
  legacy current-command response slot.
- **Frame too large**: `FrameSizeError` from `addData` surfaces as an `error` event (`tcp/transport.ts:80-95`) and the read returns without dispatching.
- **Command timeout taxonomy** (`tcp/runtime/commands.ts:93-107`): a still-queued command (never written) is rejected but does NOT count toward dead-link detection; an in-flight command that got no response rejects AND calls `handleCommandTimeout`.
- **Connection lost / close**: `handleClose` (`tcp/runtime/health.ts:57-70`) rejects all in-flight with `Connection lost` and reconnects only if it was previously connected and `canReconnect()`.
- **Outbound short write**: Bun TCP writes are unbuffered and may accept fewer
  bytes than supplied. The accepted prefix is not resent; the remaining tail
  is queued and flushed on `drain`. A closed/throwing socket or a client queue
  above 64 MiB is terminated so pending commands reject instead of waiting for
  a corrupted response stream.
- **Intentional close idempotency / unhandled-rejection safety**: `close` (`tcp/runtime/lifecycle.ts:6-17`) sets closed, stops ping, cancels reconnect, installs the one-time synthetic-close rejection handler (`tcp/errors.ts:11-18`), and `rejectAll`s with `ClientClosedError`. `rejectAll` attaches a silent `.catch` to each `cmd.promise` before rejecting (`tcp/commandQueue.ts:71-91`) so fire-and-forget callers (heartbeats, polling loops) don't surface unhandled rejections.
- **Max reconnect attempts reached**: `TcpClientState` rejects all queued commands with `Max reconnection attempts reached` when the reconnect manager emits its terminal event (`tcp/runtime/state.ts:70-74`).
- **TLS connect failure**: `Bun.connect` may reject (handshake refused) instead of firing `connectError`; the `.catch` in `createConnection` (`tcp/transport.ts:169-181`) routes it to the same rejection so callers never hang past `connectTimeout`.
- **Socket error listener**: `TcpConnectionPool` attaches a no-op `error` listener to each client (`tcpPool.ts:68-73`) so an EventEmitter `error` (e.g. TLS handshake garbage) never crashes the process.
- **Durable bypass**: `durable` jobs skip the batcher and go out as individual `PUSH` (`queue/runtime/queries.ts:10-15`); `Store-and-Forward` disables auto-batch entirely (`forwarder.ts: autoBatch:{enabled:false}`).
- **Batcher overflow**: at `maxPending` (10000) `enqueue` drops and rejects the oldest 10% with `Add buffer overflow - oldest entries dropped` (`queue/addBatcher.ts:68-74`).
- **Batcher stop**: `stop()` rejects all remaining pending with `AddBatcher stopped`; `enqueue` after stop rejects immediately.
- **Memory bound**: `HealthTracker` keeps only the last 10 latencies (`MAX_LATENCY_HISTORY`, `tcp/health.ts:36`) for the rolling average.
- **reqId wrap**: counter masks to `0x7fffffff` (`tcp/runtime/health.ts:134-137`); collisions across a 2^31 window of simultaneously in-flight commands are not defended against (impractical given `maxInFlight`).
- **Pool degraded mode**: when every client is disconnected, `getNextClient` still returns one (round-robin) so the send attempt drives reconnection rather than failing fast.

## Configuration

All knobs come through `ConnectionOptions` / `PoolOptions` (programmatic; no env vars are read directly in this layer). Defaults from `DEFAULT_CONNECTION` (`tcp/types/connection.ts:34-50`):

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

Caveat: the `pipelining` flag is declared, defaulted, and forwarded, but never read by `TcpClient` (the reqId send path is unconditional, so pipelining is effectively always on). `TcpConnectionPool` forwards both `pipelining` and `maxInFlight` into every `TcpClient` it constructs (`tcpPool.ts:49-67`), so a `maxInFlight` override takes effect on pooled connections too.

Shared pools are reused only when `poolSize === 4 && !token` for a Queue (`queue/runtime/state.ts:39-70`); otherwise a dedicated pool is created. Pool sharing key (`getPoolKey`, `tcpPool.ts:229-246`) includes host, port, poolSize, a 16-bit token hash, the JSON-stringified TLS config, and `pipelining`/`maxInFlight`, so plaintext and TLS pools to the same target never alias and Queues with different pipelining windows never share a pool.

## Related Docs

- [Client SDK: Queue](./client-queue-sdk.md) — wires the pool + `AddBatcher` and routes `add()`.
- [Client SDK: Worker (& sandboxed)](./client-worker-sdk.md) — pool sizing, `onReconnect` re-registration.
- [TCP Wire Protocol & Framing](./tcp-protocol.md) — `FrameParser`, msgpack framing.
- [TCP Server Command Handlers](./tcp-server-handlers.md) — server side of these commands.
- [Security: TLS, Auth, CORS](./security-tls-auth.md) — `Auth` command, TLS server config.
- [Store-and-Forward & BullMQ Compatibility](./store-and-forward.md) — edge→central forwarding over this transport.
- [Job Lifecycle (push / pull / ack / fail)](./job-lifecycle.md) — command semantics.
