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

## Core feature parity audit

The source-level audit on **2026-08-01** compared the public Bun client with
the six TCP SDKs. Its conclusion is unambiguous: **no external SDK currently
has the complete core feature surface**. “Full” below means that the typed SDK
exposes the core behavior with the same selectors and queue scoping; “partial”
means that a subset exists or an exposed option has different semantics. A
dash means there is no typed helper, even if an application could issue a raw
protocol command itself.

| Core surface | TypeScript | Python | PHP | Go | Rust | Elixir |
| --- | --- | --- | --- | --- | --- | --- |
| Producer, bulk add, job options | Full | Full | Full | Full | Full | Partial¹ |
| Worker delivery, leases, heartbeat, ACK/FAIL | Full | Full | Full² | Full | Full | Full |
| State queries and exhaustive pagination | Partial³ | Partial³ | Partial³ | Partial³ | Partial³ | Partial³ |
| Non-serialization `Job` operations | 13/32 | 13/32 | 4/32 | 4/32 | 3/32 | 2/32 |
| Dedup owner lookup and key release | Partial⁴ | Partial⁴ | — | — | — | — |
| Dependency pagination/counts and waiting-children transition | Partial | Partial | Partial | Partial | — | — |
| Rate/concurrency mutation | Full | Full | Partial⁵ | Partial⁵ | Partial⁵ | Full |
| Rate/concurrency readback and max/TTL status | — | — | — | — | — | — |
| Rich DLQ entries, statistics, filtered retry | — | — | — | — | — | — |
| Bulk retry with state/count/timestamp selectors | Partial⁶ | Partial⁶ | — | — | — | — |
| Atomic flow tree and chain creation | Full | Full | Full | Full | Full | Full |
| Flow bulk, fan-in, and tree readback | Full | Full | Read only | Read only | — | — |
| Queue-scoped worker discovery | Partial⁷ | Partial⁷ | Partial⁷ | Partial⁷ | — | — |
| Scheduler CRUD and queue-scoped list | Full | Full | Partial⁸ | Partial⁸ | Partial | Partial⁸ |
| Stats, metrics, and webhooks | Full | Full | Partial⁹ | Partial⁹ | — | — |
| Queue groups and store-and-forward | — | — | — | — | — | — |
| Simple all-in-one mode | Full | Full | — | — | — | — |
| Workflow/saga engine | — | — | — | — | — | — |
| Authentication, verified TLS, telemetry | Full | Full | Full | Full | Full | Full |
| Embedded SQLite, queue events, sandboxed workers | Bun-only | Bun-only | Bun-only | Bun-only | Bun-only | Bun-only |

Audit notes:

1. Elixir maps `deduplication` to the nested wire object but does not derive
   the owning `uniqueKey` from its `id`; explicit `uniqueKey` still works.
2. PHP intentionally processes sequentially; this is a worker model choice,
   not a delivery-correctness gap.
3. All SDKs can request finite offset/limit pages. None mirrors the Bun
   client's exhaustive `end=-1` contract: TypeScript substitutes a 1,000-row
   cap, Python converts it to a zero limit, and the other clients expose only a
   finite limit. Applications must paginate explicitly.
4. TypeScript and Python expose an owner lookup but route it through
   `GetJobByCustomId`. Custom IDs and deduplication keys are separate indexes,
   so this can return the wrong answer; neither SDK exposes key release.
5. PHP, Go, and Rust preserve rate duration/TTL but expose no global
   concurrency mutation helper.
6. Failed-job count works. Completed retry drops `count`, neither client
   exposes the terminal `timestamp` cutoff, and TypeScript discards the applied
   count from its return type.
7. These SDKs decode `ListWorkers` but return the server-wide registry rather
   than filtering it to `queue.name`; their count helpers are global too.
8. PHP, Go, and Elixir return every server scheduler from `CronList`, not just
   the current queue. Rust has create/get/remove but no list helper and lacks
   some scheduler flags.
9. PHP and Go expose stats and webhooks but not metrics; Rust and Elixir expose
    none of the three typed surfaces.

The full 32-method Bun `Job` denominator excludes `toJSON` and `asJSON`.
TypeScript/Python cover progress, logging, state, remove/retry, child values,
data/priority/delay mutation, promote, lock extension, delayed transition, and
discard. PHP/Go cover progress, logging, state, and lock extension; Rust covers
progress, logging, and lock extension; Elixir covers progress and logging.

As a secondary transport diagnostic, the broker command union contains 89
literal commands. The production source trees reference 74 in TypeScript, 74
in Python, 56 in PHP, 56 in Go, 44 in Rust, and 51 in Elixir. These are **not
parity percentages**: the union includes dashboard, maintenance, and alternate
primitive commands, while one higher-level feature can compose several
commands. The method/semantics matrix above is the authoritative audit.

The TypeScript and Python duration gap found by this audit is closed by a
real-broker regression that reads the applied window through `GetQueueLimits`.
The remaining gaps require separate per-SDK TDD changes and the mandatory
`bun run test:sandbox:sdk` gate.

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
   unrecoverable-processing errors;
10. treat the broker's terminal transition response as authoritative: an exact
    `already-finalized` lease generation must not emit a contradictory local
    terminal event or increment a terminal counter, and ACK batches must use
    `ignoredIndices` rather than infer positions from duplicate-capable IDs.

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
The shared conformance suite then validates 18 public protocol behaviors
against a fresh broker through each SDK's real driver. Every official SDK must
pass the same checks twice: once with the unchanged SQLite backend and once with
PostgreSQL 18.6. Storage remains a broker concern. Driver processes retain their
language toolchain environment, while a case-insensitive policy removes
bunqueue, PostgreSQL/libpq, AWS/S3, storage/TLS, and delimiter-named credential
variables; endpoints and optional tokens arrive only through the driver
protocol. Collision tests keep non-secret toolchain names available. This
reduces accidental disclosure but does not turn repository-owned driver code
into an untrusted-code sandbox. The PostgreSQL harness assigns every broker an
isolated namespace, confirms exit with bounded `SIGTERM`/`SIGKILL` handling,
and only then deletes its rows. Startup failures follow the same ownership
order. The gate waits for every started SDK suite before aggregate cleanup.
Docker teardown checks exit status, retains failed resources for retry, never
claims a container name before successful creation, and aggregates startup plus
cleanup errors. Its thin CLI runner, server/driver
harness, independent wire verifier, shared check support, and two check groups
are separate TypeScript modules so process orchestration cannot silently become
part of the verification oracle.

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
suite is green. Every SDK except TypeScript has a pinned mutation engine scoped
to the planner and its properties. Surviving mutations are either killed with a stronger invariant
or documented as equivalent; mutation is not placed inside the ordinary
test-driven edit loop or the offline release sandbox. The workflow may make a
ratchet stricter than the tool configuration, but it may not override it with a
lower value; in particular, PHP's Infection gate enforces the checked-in 99%
MSI and covered-MSI floor.

A mutation job must provision every toolchain its SDK suite spawns, not only
the mutation engine. The Go campaign gathers baseline coverage by running
`go test`, whose `TestMain` starts a real broker with `bun src/main.ts`, so the
job installs the pinned Bun version alongside Go; without it the campaign dies
at `server start failed: exec: "bun": executable file not found in $PATH`
before a single mutant is generated.

The weekly advisory job audits each SDK's own dependency graph, including the
mutation toolchain, which is where transitive advisories usually surface. That
is why the TypeScript SDK no longer has one. StrykerJS pulled the only advisory
findings this repository ever had to answer for — `qs@6.15.1` through
`typed-rest-client` (GHSA-q8mj-m7cp-5q26), then `fast-uri@3.1.4` through `ajv`
(GHSA-7p8r-x3mc-p8w7) — and neither package was ever reachable from the
published client. Pinning overrides for a development-only mutation engine
traded recurring audit noise for no user-visible safety, so the engine was
removed instead. The TypeScript planners keep their generated-property coverage
via fast-check in `bun run test:property`, and the other five SDKs still mutate
the same planner and snapshot-validator surface.

The manual TypeScript SDK publisher accepts only the current `origin/main`
commit. Selecting a feature branch or a stale main commit in the Actions UI
fails before dependencies are installed, packaged artifacts are created, or
registry credentials are used.

Each SDK also owns an opt-in sustained profile that reuses one connection while
repeatedly adding, querying, and resetting configurable batches. Weekly CI runs
these profiles for 15 minutes, runs the compatibility matrix, and checks live
dependency advisories. The normal sandbox remains bounded and reproducible.

`bun run test:sandbox:sdk` is the authoritative gate. It builds six pinned
toolchain images and runs format/static checks, package-manifest builds, native
tests, and conformance in parallel. Suite containers and one disposable
PostgreSQL 18.6 container share a dedicated Docker-internal network with no
external route; there are no host mounts, credentials, home directories, or
Docker sockets. Each suite runs conformance first with SQLite and then with its
isolated PostgreSQL namespace. The gate emits complete logs, container resource
samples, per-suite JSON, and aggregate
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
