# Go SDK invariants

These are the contracts that Go implementation changes must preserve. Protocol
shapes come from [docs/protocol.md](../../docs/protocol.md); file ownership and
commands are summarized in [CLAUDE.md](CLAUDE.md).

## Connection and frame state

- Each TCP frame is a four-byte big-endian length plus one MessagePack map. The
  payload alone is capped at 64 MiB.
- `Connection` serializes calls with its mutex. Every call receives a fresh
  `reqId`, and one absolute read/write deadline covers the complete round trip,
  including skipped responses with unrelated IDs.
- `Auth` is the first frame after every connect. Failed auth cannot advance the
  connection generation or leave a reusable socket.
- Timeout, truncation, invalid MessagePack, and oversized frames close the
  socket. A later call reconnects and authenticates instead of continuing on a
  framing-ambiguous stream.
- TLS verification is on by default. Telemetry is payload-free, runs outside the
  connection mutex, and recovers callback panics.

## Go values and wire options

- `encodeFrame` applies reflection-recursive `jsSafe` and MessagePack compact
  integers. Typed maps, slices, pointers, structs, and interfaces all receive
  the same int64/uint64 conversion, non-string-key rejection, and cycle guard.
- `time.Time` becomes JavaScript-safe Unix milliseconds. Incoming msgpackr
  extension type 0 is normalized recursively to `nil`.
- `PUSH` and `PUSHB` encode job names in top-level `name` and preserve `data`
  unchanged, including a user-owned `data.name`, scalar, slice, or nil.
  `Job.Data()` therefore returns `any`; legacy envelopes alone lose their
  string `name` marker during decoding.
- Scheduler identity uses `name`, spawned jobs use `jobName`, and scheduler
  `data` remains user-owned.
- `optionsToWire` rejects unknown keys and performs exact mappings such as
  `attempts` to `maxAttempts`, custom IDs, deduplication, and debounce.
  Meaningful zero and false values cannot be compacted away.

## Queue identity and idempotency

- `Add` and `AddBulk` map `JobOptions["jobId"]` to wire `customId`. The broker,
  not a client-side check-then-write sequence, owns custom-ID idempotency.
- Bulk request/response ordering and ID correlation cannot lose or duplicate a
  member. Retrying a custom ID must identify the existing logical job.
- Invalid payload graphs, unsafe numbers, and option keys detected locally fail
  before a frame is written. Outside `FlowProducer`, queue-name validation is
  authoritative at the broker.
- Unique-key, deduplication, delay, priority, and LIFO values are forwarded
  without client-side scheduling reinterpretation.

## Worker concurrency and leases

- Pull, command/ACK, and heartbeat traffic use independent connections. A long
  poll cannot block completion or renewal.
- The bounded goroutine pool never exceeds configured concurrency. Shared held
  leases and lifecycle state remain synchronized, including during `Close`.
- A worker registers before pulling and after each connection-generation
  change. Registration is marked complete only after broker success.
- Every active job is paired with its delivery token. ACK/FAIL and batched
  heartbeat calls use the same ID/token pair exactly once; completed jobs leave
  the held set before future renewal.
- Positive finite heartbeat intervals create one ticker. Zero, negative, NaN,
  infinity, or `DisableHeartbeat` create none.
- Processor panics become failures with the real stack and do not kill the
  worker. “completed” is emitted only after successful ACK; unrecoverable
  errors skip retries.
- Successful transport does not imply that a terminal transition applied.
  `{applied:false, reason:"already-finalized"}` is an authoritative no-op:
  release the held lease without incrementing counters or emitting
  `completed`/`failed`. Malformed terminal evidence emits `error`.

## FlowProducer and atomic PUSHF

- `flowPlanner` performs no network I/O. It validates 256-byte name/queue
  bounds, queue syntax `^[a-zA-Z0-9_\-.:]+$`, depth/job limits, reserved data,
  option ownership, and unique colon-free IDs before commit.
- Custom `jobId` is carried as `customId`; otherwise `randomFlowID` uses
  cryptographic randomness and produces a canonical UUIDv4.
- The planner alone owns `parentId`, `dependsOn`, and `childrenIds`. Repeat,
  deduplication, and debounce options are not composable with an atomic flow and
  are rejected before transport.
- Child and parent links are reciprocal in the single request. Every child
  names its parent; every parent lists the same children as dependencies and
  children. The plan is emitted child-before-parent.
- `AddChain` links each step only to its immediate predecessor. Go enforces flat
  chains statically: `ChainStep` has no `Children` field, so nested chain input
  does not compile.
- A non-empty plan performs exactly one `PUSHF`; there are no observable partial
  pushes, parent backpatch calls, cancellation loops, or rollback races.
- A timeout after `PUSHF` is an ambiguous outcome because commit may already be
  durable. Retried production graphs use the same explicit
  `JobOptions["jobId"]` on every node. A retry either commits after a
  non-commit or receives `already exists`; the SDK surfaces that collision for
  reconciliation and never fabricates successful snapshots.
- `validateFlowSnapshots` accepts only the exact planned count with unique,
  known IDs and the expected queue for each ID. Snapshot order may differ;
  identity and queue may not.

## Query, control, and admin surfaces

- Unwrap each response according to its handler: logs/workers/webhooks and
  child values live under `data`, while pause and progress values are top-level.
- Only APIs declared nullable convert a broker not-found error to `nil`.
  Authentication, transport, and response-shape errors remain visible.
- Buffered SQLite queries require observable-state polling in tests. Fixed
  sleeps are not a substitute for the expected state.
- Scheduler limit removal, explicit pointer booleans, webhook field names,
  rate-limit duration/TTL, DLQ operations, and worker stats match the server
  command definitions.

## Generated, mutation, and broker tests

- `go test -run 'FlowPlanner|FlowProducerRejectsOwnedTopology|FlowCommit|RandomFlowID' -count=1 -v ./...`
  runs Rapid 1.3.0 shrinking properties and focused flow regressions.
- Rapid prints a replay seed and may write a failfile under
  `testdata/rapid/<test>/`. Reproduce with
  `go test -run '<test>' -rapid.seed=<seed> -count=1`; preserve the minimized
  counterexample as a deterministic regression once the bug is confirmed.
- Gremlins 0.6.0 is pinned by the install command in [README.md](README.md).
  `gremlins unleash --config .gremlins.yaml` mutates only the planner, secure ID
  generator, and pure snapshot validator. Its machine-readable report is
  `build/gremlins.json`.
- `go test -v ./... -count=1`, focused `-race` checks, the native fuzz target,
  cross-language conformance, and `bun run test:sandbox:sdk` cover the real
  broker path. On failure retain the Rapid seed/failfile, exact command, server
  log, and sandbox report rather than rerunning without evidence.
