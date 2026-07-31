# Rust SDK invariants

This file records properties that must remain true across refactors. The
normative wire contract is [`../../docs/protocol.md`](../../docs/protocol.md);
the public entry points are listed in [`README.md`](README.md).

## Transport, framing, and authentication

- `Connection` owns one lazily opened socket behind a shared `Arc<Mutex<_>>`.
  A complete request/write/read exchange is serialized while that mutex is
  held; bytes from concurrent callers must never interleave.
- Every frame is `u32` big-endian length followed by MessagePack. Both
  directions are capped at 64 MiB before allocating or writing the body.
- Every normal command gets a unique `rust-N` `reqId`. A present response
  `reqId` must match; mismatch tears down the stream.
- A timeout, truncated frame, decode error, or I/O failure makes the current
  stream unusable. The next command may reconnect; it must not continue reading
  the old stream.
- When a token is configured, `Auth` is the first exchange on every connection
  generation. Authentication failures are typed separately.
- rustls verifies the certificate and requested host against system roots plus
  the optional custom CA. There is no implicit insecure TLS mode.

## Serialization and options

- Outgoing map keys are UTF-8 strings. Outgoing MessagePack extensions are
  rejected. Incoming extension type 0 is normalized recursively to `Nil`.
- Integers outside signed int32 are recursively converted to `f64` for
  JavaScript interoperability. Numeric identifiers beyond `2^53` must be
  strings.
- `JobOptions` is the complete accepted public option set. A field must be
  mapped to its exact protocol spelling or rejected; it must never disappear
  silently.
- `job_id` maps to `jobId` for `PUSH` and to `customId` inside bulk/flow input.
  Scheduler templates use only the documented scheduler-safe subset.
- Job data always reaches the broker as a map containing `name`. Scalar input
  is retained under `payload`.

## Queue and idempotency

- The broker is authoritative for scheduling and state transitions. Queue
  methods issue one command and must not emulate state locally.
- A custom ID denotes one logical job. `add_bulk` and atomic flows preserve it
  as `customId`; concurrent retries must resolve through broker idempotency,
  never a client-side check-then-push race.
- `get_job` and `get_job_by_custom_id` translate only an actual not-found
  command error to `Ok(None)`. Transport, auth, protocol, and other command
  failures remain errors.
- Pagination clamps negative offset/limit to zero and preserves the server's
  ordering. Control/admin commands return only after their server response.
- Queue and connection owners close their resources explicitly; borrowed
  connections are not closed by `Queue::with_connection`.

## Worker, leases, and Rust ownership

- `batch_size` is clamped to `1..=1000`, poll timeout to `0..=30_000`, and a
  pull is bounded by concurrency. A lease must not wait outside the processor
  pool without heartbeat ownership.
- Each pulled job and lease token travel together into an owned worker thread.
  ACK/FAIL uses that exact token; success is counted only after ACK/FAIL
  succeeds.
- A positive heartbeat interval creates a dedicated connection and thread for
  that active job. The heartbeat thread is signalled, joined, and its
  connection closed before processing returns.
- Every processor `JoinHandle` is joined, including after an earlier thread
  fails or panics. `join_all` records the first error but never abandons later
  handles.
- Processor closures are `Send + Sync + 'static`; jobs, tokens, and snapshots
  moved into threads are owned values. Do not replace this with borrowed data
  whose lifetime can end while a thread is live.
- Registration is repeated after a connection-generation change. Stop prevents
  new pulls; close unregisters best-effort and closes the connection.

## FlowProducer

- `flow_plan.rs` is pure except for secure ID generation. It allocates every
  parent ID before visiting children and resolves the whole tree/chain before
  network I/O.
- Generated IDs contain 128 random bits encoded as 32 lowercase hex characters.
  Explicit IDs are also sent as `input.customId`. IDs are non-empty, unique
  within the plan, at most 1,024 characters, and contain no colon.
- Tree edges are reciprocal: a child has `parentId`, `data.__parentId`, and
  `data.__parentQueue`; its parent has identical `childrenIds`, `dependsOn`,
  and `data.__childrenIds`.
- In a chain, step `N` depends only on step `N-1`, and
  `data.__flowParentId` is `Nil` for the first step or that prior ID.
- User data cannot provide `name` or any `__*` marker. Callers cannot provide
  `parent_id`, `depends_on`, or `children_ids`. Repeat,
  deduplication/unique-key, and debounce are rejected before I/O.
- Tree descendants are bounded to depth 100 from a depth-zero root, and
  commits to 10,000 jobs.
- A non-empty plan emits exactly one `PUSHF`; there is no compensating
  rollback. The response must contain exactly one map snapshot for every
  requested ID, with the matching queue and no duplicate/unexpected ID.
- A transport timeout after `PUSHF` cannot distinguish rejection from an
  already-durable commit. Retryable production graphs reuse a stable explicit
  `job_id` for every node. A retry either commits after a non-commit or receives
  `already exists`; the SDK surfaces that collision for reconciliation and
  never fabricates successful snapshots.
- `FlowNode` is constructed only from validated broker snapshots. Request
  payloads are not synthesized into a successful response.

## Query and administration

- Response keys are read from the documented envelope (`job`, `jobs`, `data`,
  `counts`, `cron`, and so on); a missing optional value is distinct from an
  invalid transport response.
- `wait_for_job` clamps to `0..=600_000` ms, adds transport headroom, returns a
  completed result, reports failed state, or returns a typed timeout.
- DLQ retry/purge counts, clean IDs, pause state, and rate-limit fields reflect
  the broker response. Scheduler `limit` and timezone map to `maxLimit` and
  `timezone`.
- Job objects retain their connection and authoritative raw map so follow-up
  progress, log, lock-extension, query, and admin calls preserve identity.

## Verification, seeds, and replay

- Pure tree/chain properties live in `src/flow_plan_tests.rs`; malformed
  snapshot cases exercise `flow_commit.rs`. `tests/flow_atomic.rs` proves tree
  and chain behavior against a fresh real broker.
- Run the fixed 256-case shrinkable campaign with:

  ```bash
  cargo test --locked flow_plan_tests -- --nocapture
  ```

- On failure, preserve the smallest counterexample printed by proptest and any
  generated `proptest-regressions/` entry. Replay the reported RNG seed with
  `PROPTEST_RNG_SEED=<seed>` and the exact test filter; increase verbosity with
  `PROPTEST_VERBOSE=1`.
- Mutation uses cargo-mutants 26.0.0 and
  [`.cargo/mutants.toml`](.cargo/mutants.toml), scoped to `flow_plan.rs` and
  the pure snapshot/node validation in `flow_commit.rs`. Return-value
  replacements for the impure `commit_flow` transport wrapper are excluded;
  its one-command behavior is exercised against a real broker:

  ```bash
  cargo install cargo-mutants --version 26.0.0 --locked
  cargo mutants
  ```

  Preserve `mutants.out/outcomes.json` and every survivor diff. A survivor is a
  missing assertion or a documented equivalent mutant, never a reason to
  weaken a property.
- Real-broker flow replay:

  ```bash
  cargo test --locked --test flow_atomic -- --nocapture
  cargo test --locked --test flow_rollback -- --nocapture
  ```

  Each test reserves dynamic ports and removes its temporary SQLite directory
  through the `Server` drop guard.
