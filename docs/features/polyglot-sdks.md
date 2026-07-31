# Polyglot SDK quality contract

The six official network clients under `sdk/` share one production contract.
Language APIs remain idiomatic, while transport, delivery, safety, and
observability behavior must agree with `docs/protocol.md`.

## Official clients

| SDK | Producer | Worker | Flow | Verified TLS | Structured telemetry |
| --- | --- | --- | --- | --- | --- |
| TypeScript | yes | concurrent | yes | yes | callback + lifecycle events |
| Python | yes | concurrent | yes | yes | callback |
| PHP | yes | sequential | yes | yes | callback |
| Go | yes | concurrent | yes | yes | callback |
| Rust | yes | bounded threads | yes | yes | callback |
| Elixir | yes | `Task.async_stream` | yes | yes | callback |

WebAssembly is not treated as a seventh runtime yet. Browser WASM has no
portable raw-TCP primitive, while WASI socket and TLS support depends on the
host. A future WASM client must either target an authenticated HTTP/WebSocket
bridge or a capability-enabled WASI host and pass the same conformance and
telemetry contract before it is listed as official.

## Required invariants

Every SDK must:

1. frame standard MessagePack maps with a 4-byte big-endian body length and
   reject serialization failures and bodies above 64 MiB before allocating or
   writing the frame, without retaining a timer, pending entry, or capacity
   slot;
2. send `Auth` before any other command on each connection generation;
3. correlate replies with `reqId`, tear down ambiguous timed-out streams, and
   reconnect lazily with bounded backoff;
4. recursively make integers JavaScript-safe, keep wire maps string-keyed
   (reject or explicitly stringify non-string associative keys while
   preserving lists), and normalize ext type 0 to the language's null value;
5. preserve every advertised job, scheduler, rate-limit, and flow option
   instead of silently dropping fields;
6. clamp batch and long-poll values to protocol limits;
7. keep active job leases alive, bound pulls by available concurrency, and
   surface ACK/FAIL errors;
8. plan every flow completely before I/O and submit it with one atomic `PUSHF`,
   so validation or transport failure creates no partial graph;
9. expose typed connection, timeout, command, authentication, protocol, and
   unrecoverable-processing errors.

## Atomic flow contract

Each SDK owns a pure planner for trees, chains, and fan-in graphs. The planner
allocates every ID before transport, rejects empty/duplicate IDs, cycles,
shared nodes, reserved metadata keys, unsupported option combinations, excessive
depth/size, and produces reciprocal `parentId`/`childrenIds`/`dependsOn` edges.
Only a valid plan is encoded as one `PUSHF` command. Returned flow nodes are
rebuilt from the broker's authoritative snapshots, with an exact cardinality
and ID-set check; clients never synthesize a successful graph after a partial
or malformed response.

`UpdateParent` remains a server compatibility command for already-published SDK
versions. It is not part of current official `FlowProducer` creation.

## Telemetry contract

Telemetry is opt-in and dependency-free. Each SDK emits the idiomatic
equivalent of connection/reconnection, authentication, command latency and
outcome, timeout, transport error, and close events. Worker retry events are
included where a retry loop exists.

Callbacks are isolated from queue correctness: a callback exception or panic
must not fail a command, poison a connection mutex, or leak a worker slot.
Events may contain endpoint, generation, command name, request id, duration,
outcome, and sanitized error text. They must never contain authentication
tokens, job payloads, job results, private keys, or CA contents.

## Test layers

Native regression suites cover protocol encoding, option mapping, failure
classification, timeout/reconnect behavior, authentication, TLS verification,
worker concurrency/lease behavior, atomic flow rejection, and telemetry
isolation.
The shared conformance suite then validates 17 public protocol behaviors
against a fresh broker through each SDK's real driver.

Each native suite also exercises realistic broker-backed business flows. The
common invoice-reconciliation scenario bulk-enqueues distinct payloads, drains
them with the SDK's real worker model, reads every persisted result by job id,
and verifies a deterministic checksum. Together with the retry/DLQ, burst,
heartbeat, graceful-shutdown, and flow tests, this detects job loss, duplicate
accounting, result cross-talk, and lease regressions beyond command-level
conformance. Every language now has deterministic generated-payload corpora,
concurrent custom-id idempotency and single-lease races across independent
live connections, malformed-input recovery, and a bounded producer spike. Go
adds the native race detector plus a first-class fuzz target. Rust and Elixir
use hard broker termination to prove durable jobs remain visible through
restart; the TypeScript, Python, PHP, and Go suites retain their equivalent
restart coverage.

Each SDK additionally runs native property-based tests over its pure flow
planner. TypeScript uses fast-check; Python, PHP, Go, Rust, and Elixir use their
ecosystem equivalents so generated values and shrinking integrate with the
native runner. Shared invariants cover conservation, unique IDs, reciprocal
edges, acyclicity, ordering semantics, reserved metadata, option preservation,
determinism under a supplied ID stream, and no transport call for invalid
input. Seeds and minimized counterexamples are printed by the native tools.

Mutation testing is a distinct scheduled/manual campaign after the bounded
suite is green. Each SDK has a pinned mutation engine scoped to the planner and
its properties. Surviving mutations are either killed with a stronger invariant
or documented as equivalent; mutation is not placed inside the ordinary
test-driven edit loop or the offline release sandbox. The workflow may make a
ratchet stricter than the tool configuration, but it may not override it with a
lower value; in particular, PHP's Infection gate enforces the checked-in 99%
MSI and covered-MSI floor.

The manual TypeScript SDK publisher accepts only the current `origin/main`
commit. Selecting a feature branch or a stale main commit in the Actions UI
fails before dependencies are installed, packaged artifacts are created, or
registry credentials are used.

Each SDK also owns an opt-in sustained profile that reuses one connection while
repeatedly adding, querying, and resetting configurable batches. Weekly CI runs
these profiles for 15 minutes, runs the compatibility matrix, and checks live
dependency advisories. The normal sandbox remains bounded, reproducible, and
offline.

`bun run test:sandbox:sdk` is the authoritative gate. It builds six pinned
toolchain images and runs format/static checks, package-manifest builds, native
tests, and conformance in parallel, without network access or host mounts. It
emits complete logs, container resource samples, per-suite JSON, and aggregate
`summary.json`/`summary.md` under `artifacts/test-sandbox-sdk/<timestamp>/`.

Any change below `sdk/` must pass this gate in addition to the core
`bun run test:sandbox` gate.

The GitHub release graph calls `.github/workflows/sdk.yml` as a reusable
workflow. Its six language jobs converge on `sdk-gate`; the root `quality-gate`
requires that result together with every core/docs suite, and all binary,
container, GitHub-release, and npm publication paths are downstream. The SDK
workflow has no separate push trigger, avoiding a race where publishing could
finish before an independent SDK failure arrived.

SQLite disk-full, WAL/power-loss, and schema migration tests remain broker
responsibilities. An SDK cannot inject a filesystem failure into a remote
database; it instead proves typed connection failure, lazy reconnect, durable
job visibility, and producer idempotency. The delivery contract is
at-least-once, not exactly-once processing.
