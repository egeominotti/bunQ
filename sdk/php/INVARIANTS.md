# PHP SDK invariants

This file records correctness properties that must remain true across refactors.
The wire source of truth is the repository [protocol specification](../../docs/protocol.md);
the implementation map is in [CLAUDE.md](CLAUDE.md).

## Transport, frames, and authentication

- A frame is a four-byte unsigned big-endian payload length followed by one
  MessagePack map. The 64 MiB limit applies to the payload, not the prefix.
- Every normal command has a unique `reqId`. A response with another request ID
  may be skipped, but it cannot reset the command's single absolute deadline.
- `Auth` is the first command on each new socket. A failed authentication never
  turns the connection into a usable session.
- A timeout or malformed/oversized frame tears down the socket because its frame
  boundary is no longer trustworthy. The next command reconnects and
  reauthenticates lazily.
- TLS verifies the peer and hostname unless the caller explicitly opts out.
  Telemetry never includes tokens or command payloads, and callback failures do
  not affect protocol state.

## Serialization and options

- `Protocol::jsSafe()` traverses every outgoing value. Integers outside int32
  are encoded as float64, recursive depth is bounded, and non-string map keys
  are rejected before MessagePack encoding.
- Incoming msgpackr extension type 0 is normalized recursively to `null`.
- The job name lives inside `data.name`; public job data accessors remove that
  protocol field.
- `Options::toWire()` preserves every supported option and rejects unknown
  keys. `attempts` maps to `maxAttempts`; `jobId` maps to `customId` at enqueue
  boundaries; deduplication and debounce expand to their protocol fields.
- Compacting removes only absent/null fields. Values such as `false`, `0`, and
  empty lists that carry protocol meaning must survive.

## Queue and idempotency

- `add()` and `addBulk()` preserve an explicit custom ID through `customId`;
  retries with the same custom ID rely on broker idempotency and must not create
  a second logical job.
- Bulk responses must map back to all requested jobs without silently dropping
  IDs or options.
- Flow queue names are validated locally; general queue handles still rely on
  broker validation. Any option/payload defect detected locally must fail
  before opening a connection.
- Deduplication/unique-key behavior is broker-owned. The client may map the
  option shape but must not emulate or weaken its atomicity.

## Worker, leases, and completion

- The PHP worker is sequential. It registers before the first pull and again
  after the connection generation changes; a failed registration is retried.
- Pull tokens identify the active lease. `ACK`, `FAIL`, progress, and heartbeat
  operations must use the matching job ID/token and transition the lease once.
- Batch heartbeats renew every still-held lease between jobs. A single handler
  that can exceed the lock TTL must call `Job::extendLock()` itself.
- Heartbeat intervals that are zero, negative, NaN, or infinite are disabled;
  they must never create a busy loop.
- A completion event is emitted only after the broker accepts `ACK`. Processor
  failures are sent through `FAIL`; unrecoverable failures skip retries and
  retain a bounded, throw-site-first stack.
- Cleanup removes processed jobs from the worker's held-token set so a later
  heartbeat cannot renew a completed lease.

## FlowProducer and atomic PUSHF

- Planning is pure and complete before broker I/O. It validates names, protocol
  queue syntax/length, the 100-level depth and 10,000-job limits, children and
  option shapes, unique IDs, and reserved data keys.
- Tree `children`, chain `steps`, and returned snapshot collections are protocol
  lists (`array_is_list`); empty lists are valid, associative arrays are not.
- Explicit `jobId` values are non-empty, at most 1,024 bytes, contain no `:`,
  and become `customId`. Otherwise the planner allocates a cryptographically
  random lowercase-hex ID that is portable across runtimes.
- User data cannot set `name` or any `__*` marker. User options cannot set
  `parentId`, `dependsOn`, or `childrenIds`; those links belong exclusively to
  the planner. Repeat, deduplication, and debounce are rejected in flows.
- Every tree edge is reciprocal before commit: a child carries its parent ID
  and parent queue, while the parent carries the same child in both
  `dependsOn` and `childrenIds`. Jobs are emitted child-before-parent.
- A chain step depends only on its immediate predecessor. `children: []` is a
  valid empty shape; non-empty or non-array `children` are rejected before I/O.
- A non-empty plan issues exactly one `PUSHF`. There are no intermediate
  `PUSH`, `UpdateParent`, `Cancel`, or best-effort rollback commands.
- A transport timeout after `PUSHF` is ambiguous: the broker may already have
  committed. A retryable production graph therefore gives every node the same
  stable explicit `jobId` on every attempt. A retry either commits after a
  non-commit or receives `already exists`; the SDK surfaces that collision for
  reconciliation and never turns it into synthetic successful snapshots.
- Authoritative snapshots must have the exact requested cardinality and a
  one-to-one match on both ID and queue. Missing, malformed, duplicate, unknown,
  or cross-queue snapshots fail the operation.

## Query and administration

- Response envelopes are command-specific: for example logs, workers,
  child-values, and webhooks are under `data`; pause and progress fields are
  top-level. Do not generalize one unwrap rule to every command.
- APIs documented as nullable translate only broker “not found” responses to
  `null`; transport, authentication, and malformed-response failures propagate.
- SQLite-backed listing can lag buffered writes. Tests observe state with
  `waitUntil` instead of replacing a state assertion with a timing guess.
- Scheduler execution-limit removal, webhook ID naming, rate-limit duration/TTL,
  and explicit boolean values remain aligned with the server handlers.

## Property, mutation, and end-to-end gates

- `composer test:property` runs Eris 1.1.0 with PHPUnit 10.5. It checks flow
  topology, order, wire preservation, generated IDs, one-command atomicity,
  snapshot agreement, and zero-I/O invalidity with shrinking.
- Reproduce a property failure with the printed seed:
  `ERIS_SEED=<seed> vendor/bin/phpunit --filter '<test method>'`. Record the seed
  and minimized values in a deterministic regression before changing code.
- PHP 8.4 + PCOV runs Infection 0.34.1 through `composer mutation`. The
  [configuration](infection.json5) mutates only `src/Flow` (the pure planner,
  ID generation, and snapshot validator) and enforces the recorded MSI ratchet.
  Reports are `build/infection.log`, `.html`, `.json`, and
  `infection-summary.json`.
- `php tests/run-e2e.php` runs the property suite first, then real-broker tests.
  The conformance driver and repository `bun run test:sandbox:sdk` remain final
  gates. Preserve the complete seed, replay command, server log, and retained
  sandbox artifacts for every failure.
