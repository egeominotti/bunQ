# Python SDK invariants

This file defines the contracts that must remain true for the synchronous
Python 3.9+ TCP SDK. The Bun server owns scheduling and persistence; Python
must preserve the wire contract and lifecycle semantics.

## Transport, framing, and authentication

- A frame is one unsigned 32-bit big-endian length followed by one standard
  MessagePack map. Oversized or unserializable commands fail before a pending
  future is retained or bytes are written.
- The reader thread accepts fragmented and coalesced frames and settles the
  future for the matching string `reqId` exactly once.
- Pending futures and timers are removed on success, command error, timeout,
  disconnect, and close. A timeout from an older socket generation cannot tear
  down a newer connection.
- Calls from multiple producer/worker threads remain safe: connection-state,
  writes, and the pending map use their dedicated locks without reversing lock
  order.
- `Auth` is the first application command when a token is configured.
  Authentication failures remain typed; tokens and payloads never enter
  telemetry. `Hello` negotiates `PROTOCOL_VERSION`.
- TLS verifies peers by default. Custom CA and explicit verification opt-out
  preserve identical framing and response handling.

## Serialization and option mapping

- `msgpack` is the only runtime dependency. Nested command maps use string keys
  and contain no cycles or unsupported values.
- Python integers outside int32 are normalized to float64 only when exactly
  representable up to 2^53. This prevents the Bun decoder from producing
  arithmetic-breaking `BigInt` values.
- `_compact` removes `None`, never meaningful `False` or `0`. Explicit empty
  collections are preserved when the public contract distinguishes them.
- The job name travels in `data["name"]`.
- `job_options` is the canonical snake_case-to-wire mapping:
  `attempts -> maxAttempts`, `job_id -> jobId`, and corresponding camelCase
  names for retention, lease, dependency, and failure-policy fields.

## Queue and idempotency

- `Queue.add(..., job_id=...)` uses a broker custom ID. Concurrent retries from
  independent connections resolve to one logical job.
- `add_bulk` preserves request/result cardinality and ordering; malformed or
  incomplete broker responses are errors, never partial success.
- Queue identity is immutable for an instance, and commands cannot inherit
  options or data from another queue.
- An uncertain transport retry must remain safe when a custom ID is used.
- Context-manager and explicit close paths close owned resources once without
  invalidating a caller-owned connection.

## Worker leases, heartbeat, ACK, and FAIL

- Each delivery has one lock token. ACK, FAIL, heartbeat, and lock extension
  use that token and cannot be applied to a different delivery.
- Heartbeats remain active until processing and any ACK batch settle, then stop
  on every terminal path.
- A worker concurrency slot is released exactly once after success, processor
  failure, transport failure, cancellation, or callback failure.
- `completed` events and processed counters occur only after successful ACK.
  ACK/ACKB failure emits `error` and cannot report false completion.
- Processor exceptions send FAIL with a capped traceback.
  `UnrecoverableError` bypasses retries; ordinary exceptions retain retry
  semantics.
- Graceful close stops polling, maintains necessary heartbeats, flushes ACK
  batches, and joins worker threads without abandoning active jobs.

## FlowProducer atomic graph

- `flow_plan.py` and `flow_plan_legacy.py` are pure. They perform no socket I/O
  and allocate every ID before `flow.py` calls the transport.
- `opts["job_id"]` is both the planned ID and `input["customId"]`;
  `input["jobId"]` is removed. Generated IDs use `uuid4().hex`, are non-empty,
  unique in the batch, at most 1024 characters, and contain no `:`.
- Each creation method sends at most one `PUSHF`. Broker rejection leaves zero
  jobs; sequential `PUSH`/`UpdateParent` and compensating cancellation are
  forbidden.
- A transport timeout after sending `PUSHF` is an ambiguous outcome, not proof
  that no graph exists. A retryable production flow assigns a stable
  `opts["job_id"]` to every node. Retrying the same graph either commits it
  (when the first request did not) or returns the broker's `already exists`
  collision; the SDK must surface that collision for reconciliation and must
  not synthesize successful snapshots. Regenerated IDs cannot provide this
  guarantee.
- Graph references are batch-local and reciprocal. Parents list children in
  ordered `childrenIds` and `dependsOn`; children point back with `parentId`
  and matching `__parentId`/`__parentQueue`; parent data carries the same
  ordered `__childrenIds`. Dependencies are acyclic.
- User data cannot own `name` or any `__*` marker. User `parent_id`,
  `depends_on`, and `children_ids` are rejected.
- `repeat`, `deduplication`, `unique_key`, and `debounce` are unsupported
  inside atomic flows. Flat chain/fan-in steps reject non-empty nested children
  and every non-list `children` value, including `None`; `children=[]` is
  accepted as semantically empty.
- Queue defaults merge below per-job options. Explicit `tags=[]`, boolean
  false, numeric zero, scheduling/retention, and failure-policy fields must not
  disappear during mapping. When present, `opts`, `queues_options`, and every
  per-queue defaults value must be dictionaries even when falsy; only omission
  or `None` means “no options”. `queues_options.*.job_id` is rejected before
  ID allocation because a queue default cannot define per-job identity.
- `PUSHF` success must contain exactly one dictionary snapshot per requested
  ID, with the expected queue and no duplicate or foreign IDs. `FlowNode`
  instances are built from those snapshots, not placeholder dictionaries.

## Query and administration

- Response placement mirrors the server handler. Logs, workers, child values,
  and webhook data are unwrapped from `data`; state, counts, pull tokens, and
  push IDs use their protocol-defined top-level fields.
- Only a real “not found” `CommandError` maps to `None`. Auth, timeout,
  connection, serialization, and unrelated command errors propagate.
- Query filters, ranges, and pagination are forwarded without changing broker
  order. Integer normalization must not alter exact safe identifiers.
- Destructive admin commands return the broker count/result and remain scoped
  to the selected queue unless explicitly global.

## Executable evidence

Pure flow tests require no broker. Hypothesis shrinking is deterministic in
the checked-in campaign; an explicit seed makes CI runs and failures portable:

```bash
python -m pytest \
  tests/test_flow_plan_property.py \
  tests/test_flow_plan_validation.py \
  tests/test_flow_plan_limits.py \
  tests/test_flow_plan_contract.py \
  tests/test_flow_plan_wire_contract.py \
  tests/test_flow_commit.py \
  --hypothesis-seed=20260730
```

Replay a failing seed with the same `--hypothesis-seed`. For a printed
Hypothesis reproduction blob, temporarily apply its
`@reproduce_failure(<version>, <blob>)` decorator to the failing property,
preserve the minimized example as a deterministic regression, then remove the
temporary decorator.

Mutation is a final gate and requires Python 3.10+ because mutmut 3 does;
runtime support remains Python 3.9. `pyproject.toml` limits mutation to the two
pure planner modules plus the pure snapshot validator in `flow_commit.py` and
selects only no-broker planner/commit tests:

```bash
python -m pip install -e '.[test,mutation]'
mutmut run
mutmut results
```

Public behavior requires a fresh real broker:

```bash
python tests/test_integration.py
python tests/run_e2e.py
```

The harness uses dynamic ports and a temporary SQLite directory. Flow E2E must
cover trees, chains, fan-in, custom IDs, reciprocal snapshots, reads, and
“invalid batch creates zero jobs”.
