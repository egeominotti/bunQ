# Workflow Engine (saga orchestration)

> **Category:** Orchestration · **Source:** `src/client/workflow/workflow.ts`, `src/client/workflow/engine.ts`, `src/client/workflow/executor.ts`, `src/client/workflow/runner.ts`, `src/client/workflow/loops.ts`, `src/client/workflow/compensator.ts`, `src/client/workflow/recovery.ts`, `src/client/workflow/store.ts`, `src/client/workflow/emitter.ts`, `src/client/workflow/types.ts`, `src/client/workflow/index.ts`

## Purpose

A lightweight multi-step orchestration layer built on top of a bunqueue `Queue`/`Worker` pair. A `Workflow` is a pure DSL builder that produces an ordered array of typed nodes (steps, branches, parallel groups, sub-workflows, signal waits, loops, maps). The `Engine` drives those nodes node-by-node by enqueuing one job per node onto an internal queue; each job advances the execution exactly one node forward. State is persisted in a dedicated SQLite table so executions survive crashes, and saga-style compensation rolls back completed steps in reverse order on failure. It exists to give bunqueue users orchestration primitives (retries, parallelism, human-in-the-loop signals, nested workflows) without pulling in an external workflow runtime.

## Responsibilities & Scope

Owns:

- The workflow **DSL** (`Workflow` builder) and its node model (`src/client/workflow/workflow.ts`).
- **Execution scheduling**: turning nodes into `__wf:steps` jobs and walking `currentNodeIndex` forward (`executor.ts`).
- **Step execution semantics**: retry with exponential backoff, per-step timeout, input/output schema validation (`runner.ts`).
- **Saga compensation** in reverse order (`compensator.ts`).
- **Loops & transforms**: `doUntil`/`doWhile`/`forEach`/`map` (`loops.ts`).
- **Crash recovery** of orphaned executions (`recovery.ts`).
- **Persistence** of execution rows + archival/cleanup (`store.ts`, its own SQLite DB).
- **Typed observability events** (`emitter.ts`, 11 event types).

Does NOT own:

- Job storage, sharding, priority, DLQ, ack/fail — delegated to the underlying [Core Queue Engine](./core-queue-engine.md), [Job Lifecycle](./job-lifecycle.md) and [Persistence](./persistence.md) via the embedded `Queue`/`Worker`.
- Pulling/leasing/heartbeats of step jobs — handled by the [Client SDK: Worker](./client-worker-sdk.md).
- Parent/child job graphs (a different feature) — see [FlowProducer & Job Dependencies](./flow-producer.md).
- The schema validation library itself: any object with a `.parse()` method works (Zod/ArkType/Valibot), duck-typed via `SchemaLike`.

## Dependencies

Internal:

- `../queue/queue` (`Queue`) and `../worker/worker` (`Worker`) — the engine instantiates one of each on the `__wf:steps` queue ([Client SDK: Queue](./client-queue-sdk.md), [Client SDK: Worker](./client-worker-sdk.md)).
- `../types` (`ConnectionOptions`) for TCP connection config.
- `../../require-bun` — Bun-only runtime guard imported first in `index.ts:21`.

External / runtime:

- `bun:sqlite` (`Database`) — the `WorkflowStore` opens its own DB (`store.ts:67`), WAL mode, separate from the queue's persistence DB.
- `msgpackr` (`Packr`/`Unpackr`, `structuredClone: true`) — serializes `input`/`steps`/`signals` blobs (`store.ts:9-19`).
- Bun/JS `setTimeout` for backoff, step timeouts, and signal-timeout timers.

## Public Interface

Exported from `bunqueue/workflow` (`index.ts`): classes `Workflow`, `Engine`, `WorkflowEmitter`, plus the types below.

### `Workflow<TInput, TSteps>` builder (`workflow.ts`)

Each method mutates `this.nodes` and returns a re-typed `this` so step results accumulate into `TSteps`:

- `step(name, handler, options?)` — `options`: `retry` (default `3`), `timeout` (default `30_000` ms), `compensate`, `inputSchema`, `outputSchema` (`workflow.ts:40-58`).
- `branch(condition)` then `path(name, builder)` — `path()` throws `"path() must follow a branch() call"` if not preceded by a branch (`workflow.ts:61-82`).
- `parallel(builder)` — requires ≥1 step or throws (`workflow.ts:85-98`).
- `subWorkflow(name, inputMapper)` — result stored under key `sub:<name>` (`workflow.ts:101-111`).
- `waitFor(event, { timeout? })` (`workflow.ts:114-117`).
- `doUntil(condition, builder, { maxIterations? })` — default `maxIterations` `100` (`workflow.ts:120-140`).
- `doWhile(condition, builder, { maxIterations? })` — default `100` (`workflow.ts:143-163`).
- `forEach(items, name, handler, options?)` — default `maxIterations` `1000` (`workflow.ts:166-190`).
- `map(name, transform)` — synchronous/async transform stored under `name` (`workflow.ts:193-202`).
- `getStepNames()` — flat list used for duplicate detection (`workflow.ts:205-222`).

### `Engine` facade (`engine.ts`)

```ts
constructor(opts: EngineOptions = {})
register(workflow: Workflow): this
start(workflowName: string, input?: unknown): Promise<RunHandle>
getExecution(id: string): Execution | null
listExecutions(workflowName?, state?): Execution[]
signal(executionId: string, event: string, payload?: unknown): Promise<void>
recover(): Promise<RecoverResult>
on(type, listener) / onAny(listener) / off(type, listener) / offAny(listener): this
subscribe(executionId: string, callback): () => void   // per-execution unsubscribe fn
cleanup(maxAgeMs: number, states?): number
archive(maxAgeMs: number, states?): number
getArchivedCount(): number
close(force = false): Promise<void>
```

`EngineOptions`: `embedded?`, `dataPath?`, `connection?`, `queueName?` (default `__wf:steps`), `concurrency?` (default `5`), `onEvent?` (`types.ts:236-246`, `engine.ts:23,33,60`).

### Internal queue/job names

- Queue name: `__wf:steps` (`DEFAULT_QUEUE_NAME`, `engine.ts:23`).
- Every node is enqueued as job name `wf:step` with `StepJobData` payload (`executor.ts:287`).

### Events emitted (`WorkflowEventType`, `types.ts:188-199`) — 11 total

`step:started`, `step:completed`, `step:failed`, `step:retry`, `workflow:started`, `workflow:completed`, `workflow:failed`, `workflow:compensating`, `workflow:waiting`, `signal:received`, `signal:timeout`. See [Webhooks, Events & Job Logs](./webhooks-and-events.md) for the queue-level event system (these are independent, in-process listeners).

## Data Models

See [data-model](../data-model.md) for full definitions. Key shapes (`types.ts`):

- `Execution` (`types.ts:173-185`): `{ id, workflowName, state, input, steps: Record<string, StepRecord>, currentNodeIndex, resolvedSteps?, signals, createdAt, updatedAt }`.
- `ExecutionState` (`types.ts:153`): `'running' | 'waiting' | 'completed' | 'failed' | 'compensating'`.
- `StepRecord` (`types.ts:158-170`): `{ status, result?, error?, startedAt?, completedAt?, attempts?, loopItem?, loopIndex? }` — `loopItem`/`loopIndex` persist a `forEach` iteration's `__item`/`__index` so compensation can reconstruct per-iteration context.
- `StepContext` (`types.ts:8-20`): `{ input, steps, signals, executionId }` passed to every handler; `steps` contains only **completed** step results (built in `buildContext`, `runner.ts:205-216`).
- `WorkflowNode` discriminated union (`types.ts:140-150`): `step | branch | waitFor | parallel | subWorkflow | doUntil | doWhile | forEach | map`.
- `StepJobData` (`types.ts:255-259`): `{ executionId, workflowName, nodeIndex }` — the on-the-wire job payload.

SQLite table `workflow_executions` (id PK, workflow_name, state, input/steps/resolved_steps/signals BLOBs, current_node_index, created_at, updated_at) plus `workflow_executions_archive` (same + `archived_at`), indexed on `workflow_name` and `state` (`store.ts:21-51`).

## Business Logic / Control Flow

**Start** (`executor.ts:49-70`): `register()` rejects duplicate step names across the flattened node graph (`executor.ts:41-46`). `start()` validates the workflow exists and has ≥1 node, mints `id = wf_<ts>_<rand>`, saves an `Execution` at `currentNodeIndex: 0` state `running`, emits `workflow:started`, then `enqueue()`s the first `wf:step` job.

**Per-node loop** (`processStep`, `executor.ts:72-100`): the worker handler calls `processStep(data)`. It loads the execution; if it is absent or not in `running`/`waiting` it returns `null` (execution-level idempotency guard — re-delivered jobs for terminal executions are no-ops). A `waiting` execution is flipped back to `running` so the `waitFor` node re-checks its timeout. It dispatches the node via `executeNode` (`executor.ts:124-139`), then each handler calls `advance(idx+1)`.

**advance** (`executor.ts:269-279`): bumps `currentNodeIndex`, persists, and if past the last node sets `completed` + emits `workflow:completed`; otherwise `enqueue()`s the next job. This is what serializes a single execution into a chain of one-node-at-a-time jobs.

**Step** (`runStep` → `executeStepWithRetry`, `runner.ts:38-126`): loops `attempt` `1..def.retry`. Per attempt it writes a `running` `StepRecord`, validates `inputSchema.parse(ctx.input)` if present, runs `runWithTimeout(handler, def.timeout)` (`runner.ts:17-35`), validates `outputSchema.parse(result)`, then writes a `completed` record and emits `step:completed`. On error it emits `step:retry` and sleeps `backoffDelay(attempt)` = `min(500 * 2^(attempt-1), 30_000)` + up to 50% jitter (`runner.ts:9-14`). After the last attempt it writes a `failed` record, emits `step:failed`, and throws.

**Branch** (`runBranch`, `executor.ts:147-161`): evaluates `condition(ctx)` → path name; runs that path's steps sequentially (missing path = skip), then advances.

**Parallel** (`executeParallelSteps`, `runner.ts:129-146`): runs all steps via `Promise.allSettled`; if any rejected, throws an `AggregateError` of the collected errors.

**Sub-workflow** (`executeSubWorkflow`, `runner.ts:149-175`): `start()`s the named workflow and **polls** `getFn(id)` every `100ms` up to `300_000ms`; on `completed` it collects completed step results into a map stored under `sub:<name>`; on `failed` or timeout it throws.

**Loops/map** (`loops.ts`): `doUntil` runs the body then checks `condition(ctx, iteration)`; `doWhile` checks first; both throw if `maxIterations` exceeded. `forEach` extracts `items`, throws if `items.length > maxIterations`, and for each item runs an indexed step named `<step>:<i>` whose context is enriched with `steps.__item`/`steps.__index`, persisting `loopItem`/`loopIndex` afterward (`loops.ts:77-100`). `map` runs `transform(ctx)` and stores the result as a completed step (`loops.ts:104-121`).

**waitFor** (`runWaitFor`, `executor.ts:196-236`): if the signal is already present, advance. Otherwise, if a `timeout` is set, it tracks `__waitFor:<event>` start time; if elapsed ≥ timeout it emits `signal:timeout`, marks the wait record failed, sets state `failed`, runs compensation, emits `workflow:failed`, and throws `WaitForSignalError`; else it arms a `setTimeout` via `scheduleTimeoutCheck` for the remaining time. Either way it sets state `waiting`, emits `workflow:waiting`, and throws `WaitForSignalError`. The sentinel is caught in `processStep` (`executor.ts:92`) and turned into `return null`, so the step job acks cleanly without triggering compensation.

**signal** (`executor.ts:102-128`): loads the execution (throws if not found), clears any pending timeout timer, records `signals[event] = payload` (idempotent), and emits `signal:received` — **always**, so a signal that lands *before* the run parks is still consumed at the `waitFor` via its `signals[event] !== undefined` gate. The resume is then **gated on state**: only a genuinely-parked run (`state === 'waiting'`) is flipped to `running`, persisted, and re-enqueued at `currentNodeIndex` so `processStep` re-runs the `waitFor` node and advances. For any other state (still running an earlier step, already resumed, completed, or failed) it just persists the recorded signal and returns **without** enqueuing. The `state === 'waiting'` check and the flip to `running` are synchronous (no `await` between them), and `store.update` persists `running` before the first `await this.enqueue`; a second concurrent/duplicate `signal()` therefore reads `running` back from the store and returns early. Duplicate/concurrent signals collapse to a **single resume**, so every post-`waitFor` step runs exactly once.

**Compensation** (`runCompensation`, `compensator.ts:19-56`): collects completed steps whose names do **not** start with `__`, reverses them, sets state `compensating` (+ emits `workflow:compensating`), and calls each step's `compensate(ctx)`. For `forEach` records it restores `__item`/`__index` from `loopItem`/`loopIndex`. Compensation handler errors are swallowed so the chain continues. Final state is set to `failed`. Triggered from the generic `processStep` catch (`executor.ts:96`), the `waitFor` timeout path, and recovery.

**Recovery** (`recoverExecutions`, `recovery.ts:26-49`): scans `listRecoverable()` (states `running`/`waiting`/`compensating`, `store.ts:96-98`). `running` → re-enqueue at `currentNodeIndex`. `waiting` → if not actually a `waitFor` node re-enqueue; else if signal already arrived resume, else re-arm the timeout for the remaining time (or re-enqueue immediately if already elapsed). `compensating` → re-run `runCompensation` from scratch (handlers must be idempotent). Returns `RecoverResult { running, waiting, compensating, total }`.

## Concurrency & Locking

There is **no executor-level lock per execution**. Concurrency control is delegated entirely to the underlying `Worker` (`concurrency` default `5`, `engine.ts:60`) and the queue's leasing — see [Concurrency & Locking](./concurrency-and-locking.md) and [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md).

Within a single execution, nodes are serialized because each `advance()` enqueues exactly one successor job, and a single in-flight job mutates the in-memory `Execution` object then persists it via `WorkflowStore.update` (`store.ts:122-133`, a single `UPDATE` statement). A `parallel` node is the only intra-node concurrency, fanning out via `Promise.allSettled` inside one job.

Signal-timeout timers live in an in-memory `Map<execId, timer>` on the executor (`executor.ts:26`). `scheduleTimeoutCheck` (`executor.ts:290-299`) `set()`s the timer without `clearTimeout`-ing a previously stored one, so re-entering a `waitFor` node can leave an orphaned timer that still fires.

## Edge Cases & Failure Modes

- **Step re-execution is NOT idempotent.** `executeStepWithRetry` always re-runs the handler for the node at `currentNodeIndex`; there is no "already completed" short-circuit at the step level. Re-delivery, `recover()` re-enqueue of a `running` execution, or two jobs landing on the same node can run a side-effecting step more than once. The execution-level guard (`processStep` returns `null` for non-`running`/`waiting` executions, `executor.ts:74`) only prevents re-runs of *terminal* executions.
- **Duplicate/concurrent `signal()` no longer double-executes.** `signal()` gates its resume on `state === 'waiting'` and does the state check + flip to `running` synchronously, persisting `running` before its first `await` (`executor.ts:121-127`). A second signal for the same parked run — sequential *or* concurrent — reads `running` back from the store and returns after only recording its payload, so the post-`waitFor` steps run exactly once (covered by `test/repro-workflow-signal-double.test.ts`). A signal that arrives *before* the run parks is likewise just recorded and consumed later at the `waitFor`.
- **Residual timeout-timer vs. `signal()` race (narrow, pre-existing).** `signal()` clears the pending timeout timer, so a signal that wins the race cancels the timeout. But if the timeout timer has *already fired* and enqueued a job at the `waitFor` node and a `signal()` then also enqueues, two jobs for the same node can both pass the `running`/`waiting` guard under `concurrency > 1` and advance — there is still no per-node dedup in `processStep`/`runWaitFor`. Treat post-`waitFor` steps as needing idempotent side effects. (Tracked in the project audit backlog.)
- **In-memory store when `dataPath` is omitted.** `WorkflowStore` opens `dataPath ?? ':memory:'` (`store.ts:67`). In TCP/`connection` mode (no `dataPath`) execution state is in-memory only — it is lost on restart and `recover()` finds nothing. Persistence requires passing `dataPath` (typically with `embedded: true`).
- **Retry vs. compensation.** A step failing all `retry` attempts throws out of `processStep`, which sets `failed`, persists, emits `workflow:failed`, runs compensation, then re-throws — failing the underlying `wf:step` job. If the queue retries that job, `processStep` short-circuits because the execution is now `failed`, so compensation does not double-run via that path.
- **Crash mid-compensation** leaves state `compensating`; `recover()` re-runs the whole compensation set, so compensate handlers must be idempotent.
- **Parallel partial failure** rejects the whole node with `AggregateError`; already-completed sibling steps remain `completed` and become candidates for compensation.
- **Loop guards.** `doUntil`/`doWhile` throw on `maxIterations` (default `100`); `forEach` throws if `items.length` exceeds `maxIterations` (default `1000`) *before* running any iteration.
- **Sub-workflow** is polled, not event-driven: a sub-workflow that runs longer than `300_000ms` makes the parent step throw a timeout (`runner.ts:157,174`).
- **Compensation never rolls back internal bookkeeping steps** (names prefixed `__`, e.g. `__waitFor:*`), and swallows handler errors (`compensator.ts:26,49`).
- **List queries are capped** at 100 rows (`getExecution`/`listExecutions`, `store.ts:86-95`); `archive` moves at most 1000 rows per call (`store.ts:173`). Use repeated `archive()`/`cleanup()` calls for large backlogs.
- **Listener safety:** emitter dispatch wraps each listener in try/catch so a throwing observer cannot break event delivery (`emitter.ts:112-130`).
- **Duplicate step names** across the whole node graph are rejected at `register()` (`executor.ts:41-46`).

## Configuration

The engine is configured via `EngineOptions` (no dedicated env vars):

| Option | Default | Effect |
| ------ | ------- | ------ |
| `embedded` | `undefined` | Run the internal `Queue`/`Worker` in-process vs. against a TCP server. |
| `dataPath` | `undefined` | SQLite path for both the queue and the `WorkflowStore`; **omit → `:memory:`** for the store (no persistence). |
| `connection` | `undefined` | `ConnectionOptions` for TCP mode (host/port/tls). |
| `queueName` | `__wf:steps` | Internal queue name for step jobs. |
| `concurrency` | `5` | Worker concurrency for `wf:step` jobs. |
| `onEvent` | `undefined` | Catch-all listener registered via `emitter.onAny`. |

The internal `Queue`/`Worker` inherit standard bunqueue connection/persistence behavior; see [Configuration & Entrypoint](./configuration.md) for the global env vars that affect the underlying server (e.g. `BUNQUEUE_DATA_PATH`, `TCP_PORT`).

Step-level knobs are set per `.step()`: `retry` (default `3`), `timeout` (default `30_000`ms), `inputSchema`/`outputSchema`, `compensate`. Loop bounds: `doUntil`/`doWhile` `maxIterations` `100`, `forEach` `maxIterations` `1000`.

## Related Docs

- [Client SDK: Queue](./client-queue-sdk.md) and [Client SDK: Worker](./client-worker-sdk.md) — the primitives the engine wraps.
- [FlowProducer & Job Dependencies](./flow-producer.md) — the other orchestration feature (parent/child job graphs).
- [Simple Mode (Bunqueue all-in-one)](./simple-mode.md) — Queue+Worker convenience wrapper.
- [Persistence](./persistence.md) and [Job Lifecycle](./job-lifecycle.md) — how the underlying step jobs are stored and processed.
- [Concurrency & Locking](./concurrency-and-locking.md), [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md) — worker concurrency model.
- [Webhooks, Events & Job Logs](./webhooks-and-events.md) — queue-level events (distinct from workflow events).
- [architecture](../architecture.md), [data-model](../data-model.md).
