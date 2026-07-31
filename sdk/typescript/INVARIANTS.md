# TypeScript SDK invariants

This file defines the contracts that must remain true for
`bunqueue-client`. The SDK is a TCP client shared by Node.js, Bun, Deno, and
Cloudflare Workers with `nodejs_compat`; it is not an alternate queue engine.

## Transport, framing, and authentication

- Every frame is one unsigned 32-bit big-endian payload length followed by one
  standard MessagePack map. The 64 MiB frame limit is enforced before a
  pending request, timer, or backpressure slot is retained.
- `FrameParser` must accept partial headers, partial payloads, and multiple
  frames per socket read without copying previously consumed bytes
  quadratically.
- Every command has one unique string `reqId`. A response settles only its
  matching pending call, exactly once; timeout, serialization failure,
  disconnect, and close remove its timer and pending entry.
- Pipelining may change response arrival order, never response ownership.
  Backpressure bounds in-flight calls without dropping or duplicating them.
- Reconnect is lazy. A timeout from an old connection generation must not tear
  down a newer socket.
- When configured, `Auth` is the first application command on a connection.
  Authentication failures remain typed and tokens never enter logs or
  telemetry. `Hello` negotiates `PROTOCOL_VERSION`.
- TLS verification is enabled by default. Custom CA files and an explicit
  verification opt-out preserve the same command framing.

## Serialization and option mapping

- `msgpackr` is used in standard MessagePack mode. Record extensions are
  forbidden because other SDKs cannot decode them.
- Wire values must be portable: reject `BigInt`, non-finite numbers, cycles,
  accessors, functions, symbols, non-string map keys, and custom object types
  before writing. Plain objects, arrays, dates, and binary values remain valid.
- `undefined` fields are omitted; meaningful `false`, `0`, empty strings, and
  explicit empty arrays are not removed accidentally.
- The public job name always travels in `data.name`. User data cannot silently
  replace protocol-owned fields.
- `wireJobOptions` is the single public-to-wire mapping:
  `attempts -> maxAttempts`, `jobId -> jobId` for ordinary `PUSH`, snake-free
  camelCase wire names for every other option. Adding an option requires a
  mapping assertion and a real-server test.

## Queue and idempotency

- `Queue.add` returns the broker-assigned snapshot identity. A provided
  `jobId` is a custom idempotency key: concurrent retries from independent
  connections resolve to one logical job, not multiple queue entries.
- `addBulk` preserves request/result cardinality and ordering. A malformed
  response cannot be accepted as a successful partial batch.
- A queue name is immutable for a `Queue` instance. Commands cannot leak state
  or options from another queue.
- Producer retries may repeat a command after an uncertain transport outcome;
  custom-ID behavior must remain safe under that ambiguity.
- `close()` rejects or settles pending work predictably and closes only
  connections owned by the instance.

## Worker leases, heartbeat, ACK, and FAIL

- One job delivery has one lease token and at most one active processor in the
  client. ACK, FAIL, heartbeat, and lock-extension commands use that exact
  token.
- Heartbeats continue while user work or an ACK batch is unsettled and stop on
  every terminal path. A heartbeat failure is observable; it cannot fabricate
  completion.
- A concurrency slot is acquired and released exactly once, including
  processor throws, serialization errors, connection errors, cancellation,
  and raising event listeners.
- `completed` and processed counters advance only after ACK succeeds. ACK or
  ACKB transport failure emits `error`, not a false completion.
- Processor failure sends FAIL with the configured stack limit.
  `UnrecoverableError` sets `unrecoverable: true`; retryable failures do not.
- Graceful close stops pulling, keeps required heartbeats alive, flushes
  pending ACK batches, and waits for in-flight processors.

## FlowProducer atomic graph

- Tree, bulk-tree, chain, and fan-in planners are pure: no connection is used
  until the complete graph has validated and every ID has been allocated.
- `opts.jobId` is the planned job ID and becomes `input.customId`; `input.jobId`
  is never sent in `PUSHF`. Generated IDs use `randomUUID()` and are non-empty,
  unique within the batch, at most 1024 characters, and contain no `:`.
- One public creation call sends at most one `PUSHF`. Success creates the whole
  graph; rejection creates no jobs. Client-side `PUSH` + `UpdateParent` and
  best-effort rollback are forbidden.
- A transport timeout after sending `PUSHF` is an ambiguous outcome, not proof
  that no graph exists. A retryable production flow assigns a stable
  `opts.jobId` to every node. Retrying the same graph either commits it (when
  the first request did not) or returns the broker's `already exists`
  collision; the SDK must surface that collision for reconciliation and must
  not synthesize successful snapshots. Regenerated IDs cannot provide this
  guarantee.
- Every reference is closed within the batch. For each tree/fan-in edge:
  the parent lists the child in both `childrenIds` and `dependsOn`, the child
  has `parentId`, and data markers contain the same parent ID/queue and ordered
  child IDs. Dependency graphs are acyclic.
- Planner-owned data keys are immutable. User data named `name` or beginning
  with `__` is rejected. User `parentId`, `dependsOn`, and `childrenIds` are
  rejected instead of merged.
- `repeat`, `deduplication`, and `debounce` are rejected because they can
  change graph identity or cardinality. Flat chain/fan-in steps reject
  non-empty nested children and malformed non-array children; `children: []`
  is semantically empty.
- Queue defaults merge below per-job options. Supported scheduling, retention,
  failure-policy, logging, and size options must survive planning unchanged.
  `queuesOptions.*.jobId` is rejected before ID allocation because a queue
  default cannot define per-job identity.
- A successful response contains exactly one snapshot for every requested ID,
  with the expected queue and no duplicate or foreign ID. Public `FlowNode`
  objects are built from those snapshots, never synthetic placeholder jobs.

## Query and administration

- Response placement follows the broker handler, not intuition. Wrapped values
  such as logs, workers, children values, and webhooks are read from `data`;
  state, counts, pulled jobs/tokens, and push IDs remain top-level where the
  protocol defines them.
- Only a broker “not found” command error maps to `null`. Timeout, auth,
  connection, serialization, and other server errors remain observable.
- Filter, state, range, and pagination arguments are forwarded without local
  reordering. Returned arrays retain broker order.
- Destructive admin operations return the broker count/result and stay scoped
  to the instance queue unless the API is explicitly global.

## Executable evidence

Pure flow campaigns require no broker and use deterministic shrinking:

```bash
BUNQUEUE_FLOW_PBT_SEED=20260730 bun run test:property
# Replay a printed fast-check counterexample:
BUNQUEUE_FLOW_PBT_SEED=<signed-seed> \
  BUNQUEUE_FLOW_PBT_PATH='<printed-path>' bun run test:property
```

Mutation is a final gate, not an edit-loop command. `stryker.config.mjs`
mutates only `src/flow-plan.ts`, `src/flow-plan-legacy.ts`, and the pure
snapshot validator in `src/flow-commit.ts`; its command runner executes only
no-broker planner/commit tests:

```bash
bun run test:mutation
```

Public behavior requires real-server evidence and cross-runtime evidence:

```bash
bun run build && bun run check
bun tests/integration.ts
bun tests/e2e.ts
node --experimental-strip-types tests/e2e.ts
deno run -A tests/e2e.ts
bun run test:workers
```

The E2E harness must use a fresh broker, dynamic ports, and a temporary SQLite
directory. `/flows` in the Workers suite must exercise generated IDs so
`node:crypto.randomUUID` portability cannot regress unnoticed.
