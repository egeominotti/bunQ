# Core Public API End-to-End Matrix

> **Category:** Verification · **Source:** `test/core-e2e/`,
> `.github/workflows/ci.yml`, `package.json`

## Purpose

This suite proves that every callable instance method on the Bun core client
objects is connected to a real implementation. It is fail-closed: exported
runtime classes are discovered automatically from the TypeScript entrypoints at
test time, with the structural `Job` object included explicitly. Adding a class
or method without an end-to-end scenario makes the CI job fail. Standalone
module functions and error constructors are outside this instance-method
matrix.

The suite complements the guide-section matrix and focused regressions. The
guide matrix proves documented examples and feature narratives; this matrix
proves the complete callable surface, including compatibility helpers and
maintenance methods that are easy to omit from representative tests.

## Audited surface

The current source compiler discovers **320 methods**:

| Surface             | Methods |
| ------------------- | ------: |
| `Queue`             |     128 |
| `Bunqueue`          |      47 |
| `Job`               |      34 |
| `Worker`            |      22 |
| `TcpConnectionPool` |      13 |
| `Engine`            |      17 |
| `Workflow`          |      14 |
| `QueueGroup`        |      12 |
| `FlowProducer`      |      11 |
| `WorkflowEmitter`   |       8 |
| `QueueEvents`       |       6 |
| `SandboxedWorker`   |       6 |
| `Forwarder`         |       2 |
| **Total**           | **320** |

`support/public-surface.ts` uses the TypeScript checker rather than a maintained
class or method list. It scans the client and workflow entrypoints, resolves
their exported classes plus the `Job` interface, keeps public callable
declarations owned by `src/client/`, removes inherited Node `EventEmitter`
methods, and emits stable `Class.method` keys. The final assertion requires
exact equality between the applicable set and successfully exercised methods.
The runner registers **604 applicable method-mode checks**. The
13 `TcpConnectionPool` methods are TCP-only. Another 23 legacy synchronous
snapshot methods have authoritative async companions and are embedded-only by
signature because they cannot await a TCP round trip. Those 37 non-dual cells
are explicit `N/A`, not fabricated sentinel passes. The remaining 284 methods
run in both modes. Including inventory and hygiene assertions, the focused
suite has 608 tests.

## Runtime contract

Every dual-mode contract runs twice and creates fresh state each time:

```text
embedded -> QueueManager -> unique temporary SQLite database
TCP      -> Queue/Worker client -> dynamic-port broker -> unique temporary SQLite database
```

`TcpConnectionPool` has no embedded implementation, so its 13 methods execute
once against a fresh dynamic-port TCP broker and are marked not applicable in
the embedded column.

There are no mocks, spies, or stubbed broker responses. A source scan in the
suite rejects test-double calls under `test/core-e2e/{contracts,support,fixtures}`.
The tracker records a method only after its operation and state assertions have
completed. It has no expected-rejection or sentinel-success category:
capability boundaries are explicit `N/A` cells, and every `PASS` is a successful
real operation.

Each successful operation records its mode, contract, operation kind, duration,
and exact source location. The suite writes the complete 320-row matrix to:

- `artifacts/core-e2e/public-api-matrix.md` for human review;
- `artifacts/core-e2e/public-api-matrix.json` for automation.

Every applicable cell must contain evidence, while unsupported cells must be
explicitly `N/A` and empty. GitHub Actions uploads both files as the
`core-public-api-e2e-matrix` artifact even when the job fails, so a missing or
interrupted method remains inspectable.

The TCP harness always sets `embedded: false` explicitly. This matters because
the repository test preload sets `BUNQUEUE_EMBEDDED=1`; relying on implicit mode
selection would silently exercise the local manager instead of the broker.

Documented runtime boundaries retain their exact scope:

- `QueueEvents`, `Worker.stalled`, `Queue.waitJobUntilFinished`, and
  `FlowProducer.getParentResult(s)` execute authoritatively through embedded
  and TCP transports. Event contracts assert real broker payloads, queue
  filtering, authentication, unsubscribe, reconnect/resubscribe, and cleanup.
- synchronous `QueueGroup` bulk operations target the embedded manager;
  `*Async` methods are the authoritative TCP operations and are asserted against
  the remote broker.
- synchronous Queue/Bunqueue query and DLQ snapshots are embedded-only cells;
  their async counterparts prove the authoritative result in both runtimes.
- fire-and-forget mutations execute in both modes, while their contracts observe
  the eventual state through authoritative queries. Async mutation companions
  are used whenever acknowledgement or a returned count is part of the API.

## Contract layout

`test/core-e2e/contracts/` separates scenarios by responsibility:

- Queue query, control, limits/worker discovery, schedulers, DLQ, dependencies,
  forwarding, and queue events;
- Worker lifecycle, manual processing, locks, cancellation, limiter state, and
  sandboxed child-process execution;
- all live `Job` state, mutation, transition, dependency, serialization, and
  completion methods;
- FlowProducer graphs, QueueGroup namespaces, and the all-in-one `Bunqueue`
  facade;
- shared QueueEvents, worker-stall, wait-result, and Flow-result contracts that
  run unchanged through embedded and fresh-broker TCP wrappers;
- every public `TcpConnectionPool` connection, command, health, lifecycle and
  reference-management method against a real broker;
- Workflow DSL nodes, execution/recovery/signals/compensation/archival, and the
  typed workflow emitter.

Each contract owns its queues, workers, server, pools, database, and cleanup.
The two modes execute sequentially because the embedded manager is a deliberate
process-wide singleton.

The Worker cancellation scenario uses processor-entry readiness in addition to
broker state. A batch pull can mark both leases `active` before the second
processor reaches `startJob()`; waiting for both processor entries proves
`cancelAllJobs()` against executing jobs without broadening its semantics to
buffered leases.

## Commands and CI

Run the focused gate with:

```bash
bun run test:core-e2e
```

GitHub Actions runs it as `test-core-e2e` on a fresh VM. The root
`quality-gate` requires that job alongside unit, TCP-script, embedded-script,
WebSocket/SSE, docs, typecheck, lint, and all official SDK jobs. The file also
matches Bun's regular test discovery, so `bun run test:sandbox` executes it
again inside the isolated unit container. The dedicated CI job uploads the
human-readable and machine-readable matrices for 30 days.

## Maintenance rule

When a public method is added, the compiler-discovered expected set grows
immediately. Add a real scenario to the appropriate contract; do not add an
allowlist entry or mark coverage before the operation succeeds. Confirmed
engine bugs need a focused `test/repro-*.test.ts` regression in addition to the
matrix scenario so the original failure remains obvious and cheap to replay.

## Related Docs

- [Public API Completeness](./public-api-completeness.md)
- [Documented Feature Verification](./documented-feature-verification.md)
- [Test Isolation](../testing.md)
- [Model-Based Queue Verification](./model-based-testing.md)
- [Client SDK: Queue](./client-queue-sdk.md)
- [Client SDK: Worker](./client-worker-sdk.md)
- [Workflow Engine](./workflow-engine.md)
- [architecture](../architecture.md)
