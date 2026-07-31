# Elixir SDK invariants

These are the behavioral boundaries for the OTP client. The normative wire
contract is [`../../docs/protocol.md`](../../docs/protocol.md), and public use
is shown in [`README.md`](README.md).

## Transport, framing, and authentication

- One `Bunqueue.Connection` GenServer exclusively owns one socket. Callers
  exchange commands through `GenServer.call`; they never send, receive, or
  close socket bytes directly.
- TCP and SSL remain `:binary`, passive, raw streams. A frame is a big-endian
  32-bit length plus MessagePack, capped at 64 MiB before send and before body
  receive/allocation.
- Each command has a unique request ID. A mismatched response ID is a protocol
  error and the connection state drops the socket.
- Timeout, decode, transport, and protocol failures disconnect the stream.
  Reconnect is lazy on the next call and increments the generation.
- If a token is configured, `Auth` is the first exchange of every generation.
  Authentication errors remain distinct from command and connection errors.
- TLS uses peer and hostname verification by default, with OTP system CAs or a
  caller-supplied CA file. `verify: false` is explicit and never inferred.

## Serialization and options

- `Bunqueue.Wire.js_safe/1` recursively converts integers outside signed int32
  to floats, stringifies map keys, and runs before every MessagePack encode.
  IDs beyond exact float64 range are strings.
- Incoming extension type 0 becomes `nil`; other extensions and non-map
  top-level responses are protocol errors.
- `Bunqueue.Options` is an allowlist. Unknown job, scheduler, and scheduler-job
  keys raise `ArgumentError`; accepted keys must never be silently discarded.
- `jobId` becomes `customId` only inside bulk/flow job input. Scheduler `limit`
  and `tz` become `maxLimit` and `timezone`.
- Job payloads always contain a string `"name"` key. Scalar data is retained
  under `"payload"`.

## Queue and idempotency

- A `Queue` is a name plus a connection; it does not cache broker state.
  Owned connections close with the queue, borrowed connections do not.
- Custom IDs identify one logical job at the broker. Retried `PUSHB` and
  `PUSHF` requests rely on broker-side idempotency/atomicity, never a local
  lookup-before-write race.
- Only a not-found `CommandError` becomes `{:ok, nil}` for job/custom-ID and
  scheduler lookup. All other typed errors propagate unchanged.
- Negative query offsets and limits clamp to zero. Wait timeout clamps to
  `0..600_000` ms and the connection call gets five seconds of transport
  headroom.
- Queue control and administration return only after the broker response; no
  local pause, count, DLQ, rate, concurrency, or cron state is authoritative.

## Worker, leases, and OTP isolation

- Worker construction creates separate command and heartbeat Connection
  GenServers plus one `WorkerLifecycle` GenServer. These linked OTP processes
  belong to the caller's supervision tree; socket ownership must not leak into
  handler tasks.
- `batch_size` is clamped to `1..1000`, poll timeout to `0..30_000`, and
  `pull_count/1` is at most concurrency.
- PULLB jobs and tokens must have equal lengths. Each zipped pair is processed
  in `Task.async_stream` with bounded concurrency and infinite task timeout so
  the worker does not abandon a live lease handler.
- A positive heartbeat interval starts one linked heartbeat process per active
  job on the dedicated heartbeat connection. Its stop message includes the
  owning handler PID, preventing one handler's mailbox traffic from stopping
  another job's heartbeat.
- ACK/FAIL uses the exact lease token. A handler is counted successful or
  failed only after that command succeeds; ACK/FAIL errors are surfaced.
- Exceptions, throws, and exits are isolated into failure payloads with bounded
  stack traces. `UnrecoverableError` is the only client signal that skips
  retries.
- `WorkerLifecycle` is the stop barrier: once stopping begins no new run enters;
  the owner waits for every active run and ACK/FAIL, followers wait for the same
  completion, then unregister/close happens once.

## FlowProducer

- `FlowPlanner` builds an immutable complete plan before `Connection.call/2`.
  Parent IDs are allocated before descendants and every ID is known before
  commit.
- Generated IDs are 16 cryptographically random bytes encoded as lowercase
  hex. Explicit `jobId` values are also sent as `input.customId`. IDs are
  unique, non-empty, colon-free, valid UTF-8, and at most 1,024 characters.
- Tree topology is reciprocal: child `parentId`, `data.__parentId`, and
  `data.__parentQueue` match parent `childrenIds`, `dependsOn`, and
  `data.__childrenIds`.
- A chain step depends only on the immediately preceding ID and records it in
  `data.__flowParentId`; the first marker is `nil`. `children: []` is accepted
  for flat-shape compatibility, while non-empty or non-list children are
  rejected.
- User data cannot contain `"name"` or any `"__"*` key after atom/string-key
  normalization. `parentId`, `dependsOn`, and `childrenIds` options are
  rejected by presence, including empty lists.
- Repeat, unique-key/deduplication, and debounce options are rejected before
  socket use. Descendants stop after depth 100 from a depth-zero root, and a
  plan stops at 10,000 jobs.
- A non-empty tree or chain produces one `PUSHF`, with no per-node push,
  `UpdateParent`, or rollback. Empty chains perform no I/O.
- A timeout after `PUSHF` is ambiguous because the broker may already have
  committed. A retryable production graph reuses a stable explicit `jobId` on
  every node. A retry either commits after a non-commit or receives
  `already exists`; the SDK surfaces that collision for reconciliation and
  never fabricates successful snapshots.
- `FlowSnapshots.index!/2` requires an exact requested-ID/requested-queue
  bijection: correct count, maps only, no duplicate, missing, unexpected, or
  cross-queue snapshot. Nodes are built from those snapshots.

## Query and administration

- Response fields are read from their documented envelope. Missing optional
  fields use the public default; malformed transport envelopes are not
  converted to successful state.
- WaitJob distinguishes completion, failed state, and typed timeout. Job log
  range fields map to `"start"` and `"end"`.
- DLQ counts, clean IDs, queue counts, progress, pause state, scheduler
  snapshots, rate limits, and concurrency limits come from broker replies.
- `Job` retains the authoritative raw snapshot, lease token when present, and
  the owning Connection GenServer for progress/log follow-up commands.

## Verification, seeds, and replay

- Shrinkable tree, custom-ID, and chain properties live in
  `test/flow_planner_property_test.exs`. Snapshot bijection cases live in
  `test/flow_snapshots_test.exs`; real atomic order is in
  `test/flow_e2e_test.exs`.
- Run:

  ```bash
  mix test test/flow_planner_property_test.exs test/flow_snapshots_test.exs
  mix test test/flow_e2e_test.exs
  ```

- ExUnit prints the seed for every campaign. Replay with
  `mix test <path> --seed <seed>`. Preserve StreamData's smallest shrunk input
  in a deterministic regression before changing the implementation.
- StreamData is pinned to 1.4.0. `mix mutants` runs two Muex 0.8.1 campaigns:
  the planner against its property tests and the snapshot validator against
  its exact-bijection tests.

  ```bash
  mix mutants
  ```

  Record total, killed, survived, equivalent, invalid, and timeout counts for
  both reports. A survivor requires a stronger assertion or explicit,
  justified equivalence.
- Every E2E test uses `Bunqueue.TestBroker`: a linked owner process, dynamic
  ports, an isolated temporary SQLite path, and cleanup through `stop/1`.
