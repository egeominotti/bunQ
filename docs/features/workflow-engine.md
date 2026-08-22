# Workflow Engine (saga orchestration)

> **Category:** Orchestration · **Source:** `src/client/workflow/`. The facade and
> graph live in `engine.ts`/`workflow.ts`; lifecycle dispatch is split across
> `executor*.ts`, `executionFence.ts`, `runner*.ts`, `loops.ts`,
> `forEachRunner.ts`, `mapRunner.ts`,
> `subWorkflowRunner.ts`, `waitFor.ts` and `workflowDecisions.ts`; durability is
> split across `store*.ts`, `definitionGuard.ts` and `workflowDefinition.ts`;
> rollback is split across `compensator.ts`, `compensation*.ts`,
> `compensationClaim.ts`, `unwindPlan.ts` and `rollbackControl.ts`.

## Purpose

A lightweight multi-step orchestration layer built on top of a bunqueue `Queue`/`Worker` pair. A `Workflow` is a pure DSL builder that produces an ordered array of typed nodes (steps, branches, parallel groups, sub-workflows, signal waits, loops, maps). The `Engine` drives those nodes node-by-node by enqueuing one job per node onto an internal queue; each job advances the execution exactly one node forward. State is persisted in a dedicated SQLite table so executions survive crashes, and saga-style compensation rolls back completed steps in reverse order on failure. It exists to give bunqueue users orchestration primitives (retries, parallelism, human-in-the-loop signals, nested workflows) without pulling in an external workflow runtime.

## Responsibilities & Scope

Owns:

- The workflow **DSL** (`Workflow` builder) and its node model (`src/client/workflow/workflow.ts`).
- **Execution scheduling**: turning nodes into `__wf:steps` jobs and walking `currentNodeIndex` forward (`executor.ts`).
- **Step execution semantics**: retry with exponential backoff, per-step timeout, input/output schema validation (`runner.ts`).
- **Saga compensation** in reverse order (`compensator.ts`).
- **Loops & transforms**: `doUntil`/`doWhile` (`loops.ts`), `forEach`
  (`forEachRunner.ts`) and durable `map` lifecycle handling (`mapRunner.ts`).
- **Crash recovery** of orphaned executions (`recovery.ts`).
- **Persistence** of execution rows + archival/cleanup (`store.ts`, its own SQLite DB).
- **Typed observability events** (`emitter.ts`, 15 event types, including four `compensation:*`).
- **Idempotency identity** per step and direction (`identity.ts`).
- **Operator control of a parked unwind** (`rollbackControl.ts`).

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

- `bun:sqlite` (`Database`) — the `WorkflowStore` opens its own connection in WAL
  mode (`store.ts`). In Embedded mode the Engine passes the same `dataPath` to
  the store and Queue/Worker, so two connections share one file. In TCP mode
  `dataPath` is client-local workflow state and the broker owns a different
  persistence file.
- `msgpackr` (`Packr`/`Unpackr`, `structuredClone: true`) — serializes
  `input`/`steps`/`signals` blobs (`storeCodec.ts`).
- Bun/JS `setTimeout` for backoff, step timeouts, and signal-timeout timers.

## Public Interface

Exported from `bunqueue/workflow` (`index.ts`): classes `Workflow`, `Engine`, `WorkflowEmitter`, plus the types below.

### `Workflow<TInput, TSteps>` builder (`workflow.ts`)

Each method mutates `this.nodes` and returns a re-typed `this` so step results accumulate into `TSteps`:

- `step(name, handler, options?)` — `options`: `retry` (default `3`), `timeout` (default `30_000` ms), `compensate`, `inputSchema`, `outputSchema` (`workflow.ts:40-58`).
- `branch(condition)` then `path(name, builder)` — `path()` throws `"path() must follow a branch() call"` if not preceded by a branch (`workflow.ts:61-82`).
- `parallel(builder)` — requires ≥1 step or throws (`workflow.ts:85-98`).
- `subWorkflow(name, inputMapper, { timeout?, pollInterval? })` — result stored
  under key `sub:<name>`; defaults are five minutes and 100 ms.
- `waitFor(event, { timeout? })` (`workflow.ts:114-117`).
- `doUntil(condition, builder, { maxIterations? })` — default `maxIterations` `100` (`workflow.ts:120-140`).
- `doWhile(condition, builder, { maxIterations? })` — default `100` (`workflow.ts:143-163`).
- `forEach(items, name, handler, options?)` — default `maxIterations` `1000`.
- `pivot()` — marks the point of no return. Once passed, `committedAt` is set and NOTHING is eligible for rollback, including steps before it.

`path()`, `parallel()`, `doUntil()` and `doWhile()` reject any non-step node
their builder produced (`onlySteps`). A branch path name can be declared once;
redeclaring it is an error rather than a silent `Map.set()` replacement.
Registration rejects duplicate names, the reserved `__*` and `sub:*`
namespaces, loop `name:index` collisions, and duplicate `waitFor` events.
`retry` and iteration limits must be positive safe integers. Timeouts must be
finite and non-negative; child timeout/poll intervals must be greater than zero.
- `map(name, transform)` — synchronous/async transform stored under `name` (`workflow.ts:193-202`).
- `getStepNames()` — flat list used for duplicate detection (`workflow.ts:205-222`).

### `Engine` facade (`engine.ts`)

```ts
constructor(opts: EngineOptions = {})
register(workflow: Workflow): this
start(workflowName: string, input?: unknown): Promise<RunHandle>
getExecution(id: string): Execution | null
listExecutions(workflowName?, state?, { limit?, offset? }): Execution[]
signal(executionId: string, event: string, payload?: unknown): Promise<void>
recover(): Promise<RecoverResult>
resumeCompensation(executionId: string): Promise<void>
abandonCompensation(executionId: string): Promise<void>
on(type, listener) / onAny(listener) / off(type, listener) / offAny(listener): this
subscribe(executionId: string, callback): () => void   // per-execution unsubscribe fn
cleanup(maxAgeMs: number, states?): number
archive(maxAgeMs: number, states?): number
getArchivedCount(): number
close(force = false): Promise<void>
```

Execution pages default to 100 rows, accept 1–1000 rows, and order by
`created_at DESC, id DESC`. Offset pagination is deterministic for a stable
dataset; it is not a snapshot/cursor guarantee while rows are being inserted.

`EngineOptions`: `embedded?`, `dataPath?`, `connection?`, `queueName?` (default `__wf:steps`), `concurrency?` (default `5`), `onEvent?`.

### Internal queue/job names

- Queue name: `__wf:steps` (`DEFAULT_QUEUE_NAME`, `engine.ts:23`).
- Every top-level node is enqueued as job name `wf:step` with `StepJobData`
  payload. Inline branch, parallel and loop steps run within that node job.

### Events emitted (`WorkflowEventType`) — 15 total

`step:started`, `step:completed`, `step:failed`, `step:retry`,
`workflow:started`, `workflow:completed`, `workflow:failed`,
`workflow:compensating`, `workflow:waiting`, `signal:received`,
`signal:timeout`, and `compensation:started|completed|failed|skipped`. See
[Webhooks, Events & Job Logs](./webhooks-and-events.md) for the independent
queue-level event system.

### Operator control of a parked unwind (`rollbackControl.ts`)

- `Engine.resumeCompensation(id)` — retry the handler that parked a `compensation-stuck` run, then finish the unwind.
- `Engine.abandonCompensation(id)` — record every outstanding eligible step as `compensation-skipped` and make the run terminal.

### Step context additions

`StepContext` carries a cooperative `AbortSignal` plus `idempotencyKey` on
every ordinary step attempt. Compensate handlers additionally receive
`forwardIdempotencyKey` — the key the forward execution used, so a rollback
can reconcile with the provider when the forward outcome is in doubt.

## Data Models

See [data-model](../data-model.md) for full definitions. Key shapes (`types.ts`):

- `Execution`: `{ id, workflowName, state, input, steps, currentNodeIndex,
  resolvedSteps?, decisions?, definitionHash?, rollbackStatus?, failureReason?,
  committedAt?, signals, parentExecutionId?, createdAt, updatedAt }`.
  `decisions` journals branch, loop, item-extraction and child-input choices so
  re-entry does not re-evaluate non-deterministic control flow.
  `definitionHash` binds the row to the sealed structural graph and explicit
  workflow `revision`.
- `ExecutionState`: `running` | `waiting` | `completed` | `failed` | `compensating` | `compensation-stuck`. The last is deliberately **non-terminal**: a definitive compensation failure parks the run for an operator.
- `StepRecord`: `{ status, result?, error?, startedAt?, completedAt?, attempts?, loopItem?, loopIndex?, compensation?, idempotencyKey?, occurrence?, childExecutionId? }`.
  - `loopItem`/`loopIndex` persist a loop iteration's `__item`/`__index` so compensation reconstructs per-iteration context. Written in a `finally`, so the iteration that *throws* also carries them — otherwise its compensate handler gets an undefined `__item` and silently releases nothing.
  - `doUntil`/`doWhile` write the per-iteration mirror `name:N` in a `finally` for the same reason. `unwindSet` drops the bare `name` mirror whenever a `name:0` sibling exists, because that mirror is a copy of the LAST iteration and compensating it too would undo that iteration twice. Written only on success, the iteration that *threw* existed under the bare name alone and the two rules combined to lose it entirely: a loop that charged every turn and failed on turn 2 refunded turns 0 and 1, left turn 2 standing, and still reported `rollbackStatus: 'completed'`. That record was unreachable even by `abandonCompensation`, which walks the same set.
  - `compensation` is `{ status: 'compensated' | 'compensation-failed' | 'compensation-skipped', at, error? }`. Exactly one per eligible step, discharged when the run terminates.
  - `idempotencyKey` is `run:step#occurrence:direction` (`identity.ts`), persisted with the START record so a rollback can reconcile even when the body never got to write an output.
  - `childExecutionId` on a `sub:<name>` record is the handle used to run the child's own unwind. The record is written `running` as soon as the child is claimed and settled `completed` or `failed` when the child ends. Settling on failure is load-bearing: `unwindSet` drops anything that is neither `completed` nor `failed` before the `sub:` branch can admit it, so a record left `running` took the child out of the parent's unwind and the parent reported `rollbackStatus: 'completed'` over a child parked with resources still held.
  - `compensatable` records whether the step ran with a `compensate` handler. Nothing else can tell "never owed a reversal" from "owed one and the handler has since been renamed away", and both `unwindSet` and `owesOutcome` read it for the vanished-definition case. They must agree: while `owesOutcome` answered from the definition alone it walked past exactly the record `unwindSet` had kept, and `abandonCompensation` ended a terminal run with a step carrying no outcome.
- `StepContext` (`stepTypes.ts`): `{ input, steps, signals, executionId,
  signal?, idempotencyKey?, forwardIdempotencyKey? }`; `steps` contains only
  **completed** step results.
- `WorkflowNode` discriminated union (`stepTypes.ts`): `step | branch |
  waitFor | parallel | subWorkflow | doUntil | doWhile | forEach | map |
  pivot`.
- `StepJobData` (`executionTypes.ts:109-114`):
  `{ executionId, workflowName, nodeIndex }` — the on-the-wire job payload.

SQLite table `workflow_executions` (id PK, workflow_name, state,
input/steps/resolved_steps/signals/meta BLOBs, current_node_index, created_at,
updated_at) plus `workflow_executions_archive` (same + `archived_at`).
`meta` contains rollback state, failure reason, pivot, parent, decision journal
and definition hash. Composite indexes cover every filtered list order and
recoverable-state ordering.

## Business Logic / Control Flow

**Registration/start** (`executor.ts`, `executorLifecycle.ts`): registration
validates and seals the definition. Its SHA-256 structural identity includes
node options plus the explicit `revision`; a different definition cannot be
registered while a live execution is bound to the old one. `start()` mints an
opaque `wf_` plus 16 CSPRNG bytes, persists the execution and its definition
hash, then publishes the first job. If publication fails, the new row is
removed so the caller is not handed an unreachable execution.

**Per-node loop** (`processStep`, `executor.ts:72-100`): the worker handler calls `processStep(data)`. It loads the execution; if it is absent or not in `running`/`waiting` it returns `null` (execution-level idempotency guard — re-delivered jobs for terminal executions are no-ops). A `waiting` execution is flipped back to `running` so the `waitFor` node re-checks its timeout. It dispatches the node via `executeNode` (`executor.ts:137-152`), then each handler calls `advance(idx+1)`.

**advance** (`executor.ts:282-292`): bumps `currentNodeIndex`, persists, and if past the last node sets `completed` + emits `workflow:completed`; otherwise `enqueue()`s the next job. This is what serializes a single execution into a chain of one-node-at-a-time jobs.

**Step** (`executeStepWithRetry`, `runner.ts`): attempt count is cumulative
across re-entry, and a completed occurrence is not dispatched again. The
running record, stable idempotency key and attempt count are persisted before
the handler. Input parsing/coercion is cached once for that retry episode;
output parsing applies per handler result. Timeout uses chunked timers and
aborts `ctx.signal`, so handlers can cancel cooperative downstream I/O. Failure
writes preserve the handler error even if the diagnostic store write also
fails.

**Branch** (`executorNodes.ts`, `workflowDecisions.ts`): the selected path is
journaled before its effects run. A non-string or undeclared path fails
explicitly; it is never treated as an empty branch.

**Parallel** (`executeParallelSteps`, `runner.ts:129-146`): runs all steps via `Promise.allSettled`; if any rejected, throws an `AggregateError` of the collected errors.

**Sub-workflow** (`executorNodes.ts`, `subWorkflowRunner.ts`): mapped input is
journaled. The parent claims the child ID before polling and adopts an existing
child on re-entry; a durable running child whose first queue publication was
lost is republished. Poll interval and timeout come from the node definition.
The deadline is based on the child's original `createdAt`, so restart does not
reset it. Completion returns the child's completed result map; failure,
`compensation-stuck`, or deadline expiry settles the parent record as failed.

**Loops/map** (`loops.ts`, `mapRunner.ts`): loop conditions and `forEach`
item snapshots are journaled; completed indexed occurrences are memoised.
`map` persists `running` before transformation, then a terminal completed or
failed record and matching event. A persisted completed map is not transformed
again after recovery.

**waitFor** (`waitFor.ts`): if the signal is already present, advance.
Otherwise it transactionally parks the execution unless a signal won the race.
For a timed gate it persists `__waitFor:<event>` with the original start time
and arms only the remaining delay. At expiry it re-reads signals before
failing, records the timeout reason, compensates, emits the timeout/failure
events and throws `WaitForSignalError`. The sentinel is caught by
`processStep`, so normal parking acks the node job without treating the pause
as a workflow failure.

**signal** (`executorLifecycle.ts` → `SignalCoordinator.record`): delivery,
first-writer-wins payload acceptance and the single resume claim occur inside
one immediate SQLite transaction. A second delivery for the same event is
rejected and cannot replace the accepted payload. Presence is tested by own key,
not value, so an explicit `undefined` payload still opens the gate. If publishing
the claimed resume fails, the execution is restored to `waiting` while retaining
the durable signal, allowing recovery to republish it.

**Compensation** (`runCompensation`, `compensator.ts`): builds the unwind set, reverses it (insertion order = start order, so this is reverse *start* order — deterministic where completion order is not), sets state `compensating`, and settles each step with exactly one outcome.

Eligibility: user steps only (no `__` bookkeeping), status `completed` **or `failed`** — the failing step is the one most likely to need undoing, because a charge that reached the provider and then lost its response is recorded failed while the money has moved. Nothing is eligible once `committedAt` is set: past the pivot the saga is committed and recovery is forward-only.

- **Never twice after a persisted outcome.** A step already carrying a
  `compensation` outcome is skipped. A handler interrupted after its external
  undo but before that outcome write can replay, so compensations remain
  idempotent at the provider boundary.
- **Park, do not plough on.** A handler that throws settles as `compensation-failed` and the loop **breaks**. The remaining steps are left *without* an outcome on purpose — the run is parked (`compensation-stuck`, `rollbackStatus: 'stuck'`), not finished, and pre-marking them skipped would make a later resume believe they were handled.
- **Bounded like the forward path, and bounded even when the forward path is not.** The bound comes from `decideUnwindAction` (`unwindPlan.ts`) as `def.timeout > 0 ? def.timeout : DEFAULT_COMPENSATE_TIMEOUT_MS`, so the 30 s default from `workflow.ts:121` applies to a reversal too, and a timeout settles as `compensation-failed` (`error: 'Step timed out after Nms'`) exactly like a throw. `timeout: 0` disables the bound on the forward path only: an unbounded reversal would hold the process-global compensation claim forever, locking operator control and, after force-close, preventing a replacement Engine from recovering the run. A hung HTTP call to a provider that is down — precisely when rollbacks run — must instead settle as a failed reversal and park the run in `compensation-stuck`, where an operator can resume or abandon it.
- **Nested sagas.** A `sub:<name>` record is compensated by running the CHILD's `runCompensation` (`unwindChild`). A child that parks makes the parent's sub-step throw, so the parent parks too rather than reporting a clean rollback over a half-undone child. Resuming an ancestor may retry a child that is still `compensation-stuck`; it may not reopen a child explicitly abandoned to the terminal `failed` + `rollbackStatus: 'stuck'` pair.
- **Operator exits** (`rollbackControl.ts`): `resumeCompensation` asks for the retry with a FLAG (`retryFailed`) rather than clearing the failed outcome, so the operator's record survives until a real outcome replaces it. Clearing was worse than it looked: it persisted the wipe before running anything, so a resume that met a failing store left a durable row with the diagnostic gone and the run marked `compensating`, re-driven at every startup, and guarding that needed a deep snapshot, a restore path and a second write that could mask the original error. `abandonCompensation` records every outstanding eligible step as `compensation-skipped` and makes the run terminal — this is where "exactly one outcome" is discharged. That terminal decision is monotonic: a later parent retry parks on the abandoned child instead of dispatching its reversal again.

Triggered from the generic `processStep` catch, the `waitFor` timeout path, and recovery.

**Recovery** (`recovery.ts`): scans `running`, `waiting`, `compensating`, and
failed rows whose unwind never began. Running work is re-enqueued; waiting
timeouts retain their original start time; a delivered signal resumes
immediately; failed-before-unwind rows enter compensation. Compensation is
idempotent at the persisted outcome level. A recovery call on the same live
Engine skips an unwind already owned by that Engine and reports no recovered
work; it never waits for user compensation code. If a force-closed Engine leaves
its JavaScript compensation handler alive briefly, a replacement Engine in the
same process waits on that exact claim owner's completion latch, reloads the
durable row through its own store, and retries only when compensation is still
owed. This prevents both a self-deadlock and a lost wake-up without treating the
process-local claim as a distributed lock. Definitions are rebound only when a
legacy row has no hash; a hashed row with a different registered graph fails
closed.

A child with a live parent is excluded from top-level recovery because its
lifecycle belongs to that parent. Re-entry resumes the claimed child rather
than provisioning another one. If the parent died between child persistence
and parent claim persistence, the parent adopts the child by
`parentExecutionId` and workflow name. A child whose parent row is gone becomes
top-level recoverable so it cannot remain orphaned forever.

## Concurrency & Locking

There is no cross-process workflow lock. Inside one executor,
`nodesInFlight` holds a claim for `<executionId>:<nodeIndex>` while the node is
running. Together with state and cursor admission checks, this rejects
overlapping duplicate deliveries without using a queue deduplication ID that
could suppress a legitimate later re-enqueue.

Compensation uses a process-global `Map<executionId, claim>` because a forced
Engine close can overlap a replacement Engine using the same durable store.
Claim acquisition is atomic in the JavaScript process. The owner releases an
identity-checked, non-rejecting completion latch in `finally`; a recovery path
that loses the claim compares the owning `WorkflowStore` identity. The same
Engine returns without waiting or counting work; a replacement Engine waits for
the latch, reloads the row, and either stops on the persisted outcome or resumes
the still-owed unwind. This is local coordination only: compensations must
remain idempotent at the external provider boundary for crash and multi-process
safety.

Within a single execution, nodes are serialized because each `advance()` enqueues exactly one successor job, and a single in-flight job mutates the in-memory `Execution` object then persists it via `WorkflowStore.update` (`store.ts:122-133`, a single `UPDATE` statement). A `parallel` node is the only intra-node concurrency, fanning out via `Promise.allSettled` inside one job.

Signal-timeout timers live in an in-memory `Map<execId, timer>`. Scheduling a
replacement clears the previous handle; `Engine.close()` clears the map before
closing the worker and queue.

## Edge Cases & Failure Modes

- **Interrupted work is at-least-once.** A persisted completed step or map is
  skipped on re-entry, and cumulative attempt counts cannot exceed `retry`.
  Work left `running` still has an unknown external outcome and may run again.
  `ctx.idempotencyKey` stays stable so the provider can absorb that replay.
- **Loop iterations are memoised.** `runIteration` (`loops.ts`) skips an iteration whose `name:iteration` record is already `completed`, restoring the base-name record so the condition and downstream steps see it. Only `completed` is skipped — an iteration left `running` by a crash is genuinely unfinished. Without this a loop replayed from zero on every re-entry, and the restarted in-memory counter overwrote earlier per-iteration records with later content, corrupting the transcript it was meant to preserve.
- **Duplicate/concurrent `signal()` cannot double-execute.** Signal insertion
  and the `waiting -> running` resume claim share one immediate SQLite
  transaction. The first payload for an event wins; a duplicate event is
  rejected without overwriting it. Different early events may be recorded, but
  only one caller can claim a parked node's resume. A signal accepted before
  the run parks is consumed when that gate is reached.
- **Timeout-timer vs. `signal()` race — now closed by a per-node claim.** `signal()` clears the pending timeout timer, so a signal that wins the race cancels the timeout. If the timer had *already fired* and enqueued a job at the `waitFor` node while `signal()` also enqueued, both jobs used to pass the `running`/`waiting` guard under `concurrency > 1` and advance the run twice. `processStep` now holds `nodesInFlight`, a claim keyed `<executionId>:<nodeIndex>` for the duration of the node, so the second job returns immediately. Two guards, different jobs: the cursor check (`data.nodeIndex !== exec.currentNodeIndex`) drops a job for a node the run has already left; the claim drops a concurrent second job for the node it is on. A deterministic `jobId` was tried instead and removed — dedup at the queue can swallow a *legitimate* re-enqueue and wedge the run, whereas a dropped duplicate cannot.
- **One `Engine` per process — enforced for conflicting paths.**
  `src/client/manager.ts` memoises a process-global `QueueManager`. A later
  engine with a different explicit `dataPath` now throws synchronously instead
  of sharing the first engine's queue while opening a separate workflow store.
  `Engine.close()` does not reset that process-wide manager; close all embedded
  clients and call `shutdownManager()` before switching paths. Engines using
  the same or omitted path still lack independent coordination, so treat one
  engine per process as a hard requirement.

- **In-memory store when `dataPath` is omitted.** `WorkflowStore` opens `dataPath ?? ':memory:'` (`store.ts:67`). In TCP/`connection` mode (no `dataPath`) execution state is in-memory only — it is lost on restart and `recover()` finds nothing. Persistence requires passing `dataPath` (typically with `embedded: true`).
- **Retry vs. compensation.** A step failing all `retry` attempts throws out of `processStep`, which sets `failed`, persists, emits `workflow:failed`, runs compensation, then re-throws — failing the underlying `wf:step` job. If the queue retries that job, `processStep` short-circuits because the execution is now `failed`, so compensation does not double-run via that path.
- **Forced-close generation fence.** `WorkflowExecutor.close(true)` aborts the
  shared `WorkflowExecutionFence` before Worker teardown. Admission and every
  asynchronous continuation check that fence before mutating the execution,
  writing SQLite, publishing a node, arming a timeout, emitting an event, or
  starting new user code. It covers retry backoff, maps, loops and journaled
  decisions, sub-workflow start/adoption/polling, `waitFor`, lifecycle calls,
  and recovery. The close sentinel passes through parallel `AggregateError`
  and error-preservation catches without being recorded as an application
  failure. A child that completes after its old parent closes therefore cannot
  advance that parent, and a late map error cannot start rollback. Work already
  inside a forward handler cannot be cancelled; its external effect remains
  at-least-once and must use the stable idempotency key. A compensation handler
  that already owns the process-local claim may checkpoint its outcome and
  release that claim, but the fence prevents the old Engine from starting the
  next reversal. Graceful `close()` does not abort the fence and drains the
  active control flow normally.
- **Crash or force-close mid-compensation** leaves state `compensating`;
  `recover()` re-enters the unwind, but per-step outcomes are checkpointed, so
  only steps without a settled outcome are attempted again. When an old Engine's
  JavaScript handler is still alive in the same process, its replacement waits
  for the exact claim to settle before re-reading and deciding whether a retry is
  owed; recovery on the same live Engine skips its own in-flight unwind.
- **Parallel partial failure** rejects the whole node with `AggregateError`; already-completed sibling steps remain `completed` and become candidates for compensation.
- **Loop guards.** `doUntil`/`doWhile` throw on `maxIterations` (default `100`), which fails the run and triggers the unwind; `forEach` throws if `items` is not an array, and then if `items.length` exceeds `maxIterations` (default `1000`) — both *before* running any iteration. `Array.isArray` is the predicate deliberately: the old `.length` duck-test accepted a number (zero iterations, run still `completed`) and a string (one iteration per character). Proxied and cross-realm arrays still pass, since `Array.isArray` tests the internal slot; typed arrays and other array-likes no longer do.
- **Loop iterations are the unwind set, not the mirror.** Every iteration is recorded under `name:iteration` *and* mirrored under the bare `name` so the loop condition and downstream steps can read the last result. On rollback the **indexed records are compensated** — `findStepDef` resolves `turn:0` in a second, iteration-only pass — and `unwindSet` excludes the bare mirror so the final iteration is not compensated twice.
- **Sub-workflow timeout is configurable, not cancellation.** On expiry the
  parent fails, but a still-running child is not forcibly cancelled. Rolling a
  live child back would race its forward executor, so the parent may park in
  `compensation-stuck` until the child settles and an operator resumes or
  abandons the unwind.
- **Compensation never rolls back internal bookkeeping steps** (names prefixed `__`, e.g. `__waitFor:*`).
- **Sub-builders accept steps only.** `path()`, `parallel()`, `doUntil()` and `doWhile()` throw at build time if the builder produced any non-step node (`onlySteps`, `workflow.ts`). These bodies execute inline inside one job, so a `waitFor` there has no node index to park at and a nested `branch` has no dispatcher. They used to be filtered out silently, which turned an approval gate written inside a path into a no-op the run sailed straight through.
- **Loop index namespace is reserved.** `register()` rejects a step whose name matches `<loopStep>:<digits>` (`assertNoIndexCollision`). Both silent outcomes are corruption: the loop overwrites the user's step, or — with memoisation — mistakes it for its own completed work and skips the iteration.
- **The `signals` column is owned by `storeSignals.ts` alone.** `store.update()` never writes it. A worker holds one in-memory `Execution` for a whole node, so rewriting `signals` from that stale snapshot destroyed payloads delivered mid-step and parked runs forever. `recordSignal`/`parkForSignal` read-modify-write it inside a transaction, and the resume is claimed with a conditional state UPDATE so duplicate signals collapse to exactly one resume.
- **`waitFor` timer handles are chunked** at `2**31-1` ms while the original
  persisted deadline remains unchanged; without chunking, larger timer values
  wrap and fire immediately.
- **List pages are bounded.** The default is 100 and the maximum is 1000;
  callers retrieve later deterministic pages with `offset`. Offset pages do not
  promise snapshot stability under concurrent insertion. `archive` moves at
  most 1000 rows per call.
- **The retention cutoff is inclusive** (`updated_at <= Date.now() - maxAgeMs`). A strict `<` made `cleanup(0)`/`archive(0)` skip every row stamped in the current millisecond, which is where a run that has just reached a terminal state sits, so the documented "flush everything terminal now" call returned 0 (`test/repro-workflow-archive-boundary.test.ts`).
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

Step-level knobs are set per `.step()`: `retry` (default `3`), `timeout`
(default `30_000`ms, `0` disables the forward bound),
`inputSchema`/`outputSchema`, `compensate`. Loop bounds:
`doUntil`/`doWhile` `maxIterations` `100`, `forEach` `maxIterations` `1000`.
`subWorkflow` defaults to `timeout: 300_000` and `pollInterval: 100`.

## Why this is Bun-only, and what porting it would take

The engine is not exposed over the wire protocol and is not implemented in any of the six clients (verified: zero files matching `workflow` or `compensate` under `sdk/*`). That is a consequence of the DSL, not an oversight:

```ts
BranchCondition = (ctx) => string              // arbitrary closure
LoopCondition   = (ctx, iteration) => boolean  // arbitrary closure
```

`branch()` and `doUntil()` take **functions, not data**, so a server can never evaluate them. A Temporal-style split (server orchestrates, clients supply handlers) would require a serialisable DSL, which is a breaking change to the public builder API.

What *is* portable: the step job payload is only `{ executionId, workflowName, nodeIndex }` — no code crosses a boundary — and handlers resolve from an in-process registry. The single blocker is that `WorkflowStore` opens a local `bun:sqlite` file, so execution state is unreachable from another process or language.

So the shape a port must take is: **the server owns execution state**, and each SDK reimplements the DSL and node walk natively, where handlers and conditions are ordinary functions in that language.

The server surface needed mirrors `WorkflowStore`'s ten methods (`save`, `get`, `update`, `recordSignal`, `parkForSignal`, `list`, `listRecoverable`, `cleanup`, `archive`, `getArchivedCount`). Two of those **must stay atomic server-side**: `recordSignal` and `parkForSignal` are a read-modify-write on one row from concurrent connections. An SDK that rebuilds either as a get plus an update reintroduces the lost-update bug where a signal delivered while the run is parking is silently dropped and the execution waits forever (`storeSignals.ts` exists precisely to make that one transaction).

Note that adding those commands to `src/domain/types/command.ts` is not a free first step: `docs/protocol.md` designates that file as the normative source for client authors, and `tsconfig.build.json` emits all of `src/**` into the published package. Command types without handlers would ship to npm as a public promise the server answers with `Unknown command`. Land the types, the handlers, `protocol.md` and the conformance driver together.

## Agent framework integrations

The engine has **no dependency on any agent framework**. Integration is always the same seam: one agent turn becomes one `.step()`, whose `compensate` handler reads the effects the step *returned* (never a closure variable, which is empty after a restart while the effect still stands).

Four frameworks are covered by executable examples. All are devDependencies only.

| Framework | Package | Verified by |
|---|---|---|
| Vercel AI SDK | `ai` | `test/workflow-ai-sdk-agent.test.ts` (mock model), `scripts/ai-sdk/saga-live-e2e.ts` (live) |
| Claude Agent SDK | `@anthropic-ai/claude-agent-sdk` | `scripts/agent-sdks/claude-agent-live.ts` (live; needs network + Claude Code harness), plus the journaling seam in `test/workflow-agent-sdks.test.ts` |
| OpenAI Agents SDK | `@openai/agents` | `test/workflow-agent-sdks.test.ts`, real `Agent`/`tool`/`run` with a scripted `Model` |
| Mastra | `@mastra/core` | `test/workflow-agent-sdks.test.ts`, real agent with `MockLanguageModelV3` |
| LangGraph | `@langchain/langgraph` | `test/workflow-agent-sdks.test.ts`, real compiled graph, no model needed |

Two constraints worth recording, both found by running the code:

- **Mastra hands the validated input straight to `execute`**, not wrapped in `{ context }`. Destructuring `{ context }` yields `undefined` and the tool silently does nothing.
- **OpenAI Agents' `setDefaultModelProvider` is process-global**, and it is asked for a model on *every* turn. Returning a fresh scripted model from it resets the script cursor and the agent loops until `MaxTurnsExceeded`; the registration also leaks across tests. Pass the model to `new Agent({ model })` instead.

User-facing page: `docs/src/content/docs/guide/workflow/agent-sdks.md`.

## The two decisions that are pure functions

Every rollback and duplicate-execution defect this engine shipped lived in decision
logic, not in I/O, and while the two were tangled the only way to test a decision was to
stand up an Engine, a database and a real race, then infer it from side effects. Both
are now values.

**`unwindPlan.decideUnwindAction(wf, name, record, halted, retryFailed?)`** returns what
to do with one eligible record: `skip`, `stop`, `halt-vanished`, `halt-failed`,
`unwind-child`, or `compensate` with its bound. The ORDER of its checks is load-bearing
and each one earned its place by breaking:

1. a record whose step no longer resolves but which already carries an outcome halts,
   checked BEFORE the settled test, which would otherwise skip it and let the pass
   report a clean rollback over a renamed step;
2. a settled record is never re-run, except a `compensation-failed` one when an operator
   explicitly asked for the retry;
3. an unresolved `compensation-failed` HALTS rather than being skipped, or a second pass
   after a crash walks past a refund that never went through;
4. once halted, the rest are left without an outcome so a resume can still reach them.

**`admission.decideAdmission(exec, nodeIndex, inFlight)`** returns whether a node job may
run, with the rejection reason distinguished: `missing`, `not-live`, `stale-cursor`,
`already-in-flight`. Delivery is at-least-once, so the same job arrives twice routinely,
and a missing guard let a duplicate re-run the node and every node after it. `recovery.ts`
consults the same function before re-enqueuing, so `RecoverResult` counts work that
actually happened.

Both are covered by generated inputs in `test/workflow-properties.test.ts`.

## Determinism: the injected clock

Every timestamp, retry-jitter draw, entropy read, `setTimeout` and
`clearTimeout` in this module
goes through `clock()` (`clock.ts`). The default is the real clock and is installed by
default, so nothing changes for a caller who does not ask otherwise.

The reason is evidence, not taste. Each crash-window defect this engine shipped took a
property-based campaign roughly one run in eleven to surface, and the seed that
produced it did NOT replay it: the seed drove the command sequence while the
interleaving came from real timers. A failing seed you cannot replay is a bug report
you cannot act on. With `simulatedClock(seed)` installed, retry backoff, signal
timeouts, execution IDs and every persisted timestamp become functions of that seed.
The simulator keeps ID entropy on a separate deterministic stream so minting an
ID does not perturb the retry-jitter sequence. The real clock obtains 16 bytes
from Web Crypto for every execution ID.

Scope, stated so a green run is not over-read: SQLite, the embedded queue's worker loop
and the OS scheduler are still real, so a whole `Engine` is not deterministic. What is
deterministic is the engine's own contribution, which is where its bugs have lived.

Measured on a live engine, a run with two retries at the default backoff:

| clock | attempts | wall time | simulated time |
|---|---:|---:|---:|
| real | 3 | 1794 ms | n/a |
| simulated | 3 | 66 ms | 18000 ms |

`test/workflow-dst.test.ts` covers the clock itself and that live case.
`test/workflow-properties.test.ts` covers the pure core with generated inputs; it found
the inherited-member gate defect on its 202nd case.

## Documentation example verification

User-facing examples are executable specifications, with external providers
replaced only at their boundary:

| Documentation surface | Executable evidence |
| --- | --- |
| Quick start, steps, rollback, durability, approval and Vercel AI SDK | `test/workflow-docs-examples.test.ts` |
| Pagination, map lifecycle, schema parse count, builder bounds, definition sealing and child deadlines | `test/repro-workflow-api-hardening.test.ts`, `test/repro-workflow-durability-gaps.test.ts`, `test/repro-workflow-production-safety.test.ts` |
| OpenAI Agents SDK, Claude session seam, Mastra and LangGraph | `test/workflow-agent-sdks.test.ts` using the installed packages |
| Live Claude/Vercel provider calls and real process kill | opt-in scripts under `scripts/agent-sdks/` and `scripts/ai-sdk/`; offline equivalents own the isolated CI gate |
| Historical workflow blog | reuses the same builder forms and scenarios above; no separate pseudo-API |

Short fragments that only inspect an already-tested execution (for example
`exec.rollbackStatus`) are assertions in those suites. Live scripts require
credentials and network and are never presented as part of the isolated,
credential-free sandbox result.

## Performance evidence

`bench/workflow-engine.ts` measures linear, inline-parallel, compensation, and
wait/signal workflows through this public Engine facade. Every sample receives
a fresh process and SQLite state; TCP samples additionally receive a fresh
broker, broker database, queue, and dynamic ports. The runner validates every
persisted terminal execution plus exact lifecycle, step, signal, and
compensation event counts.

On the 2026-07-30 native Ryzen 9 campaign, the tuned single-engine linear
workload measured 2,700 workflows/s Embedded (`concurrency:128`) and 3,187
workflows/s TCP (`concurrency:64`), with 21 measured 1,000-execution processes
per mode. The TCP topology can be faster for no-op nodes because the broker and
Workflow Store use separate processes and SQLite files; this is not evidence
that network transport is cheaper.

`bench/workflow-engine/scale.ts` starts 1/4/8/12 independent engines behind a
common barrier. With 5,000 executions per instance and the TCP protocol safety
limit explicitly raised to one million requests per window, median x12
throughput was 25,873 workflows/s Embedded and 17,496 TCP. TCP was already at
17,407 at x8, so the shared host was saturated.

The default TCP safety cap is operationally visible: 3,000 linear workflows
completed in 749 ms, while 3,500 crossed the default 10,000 requests/client/
60-second window and completed in about 60.16 seconds with `Rate limit
exceeded` on ACK batches. Raising only `RATE_LIMIT_MAX_REQUESTS` restored a
3,855 workflows/s median. Default and tuned results are deliberately reported
separately.

See [Benchmarking and Performance Evidence](./benchmarks.md) for runner controls
and [Native Engineering Benchmark — 2026-07-30](../benchmarks/native-engineering-2026-07-30.md)
for distributions, latency, scale-out resources, causal diagnostics, and
integrity totals.

## Related Docs

- [Client SDK: Queue](./client-queue-sdk.md) and [Client SDK: Worker](./client-worker-sdk.md) — the primitives the engine wraps.
- [FlowProducer & Job Dependencies](./flow-producer.md) — the other orchestration feature (parent/child job graphs).
- [Simple Mode (Bunqueue all-in-one)](./simple-mode.md) — Queue+Worker convenience wrapper.
- [Persistence](./persistence.md) and [Job Lifecycle](./job-lifecycle.md) — how the underlying step jobs are stored and processed.
- [Concurrency & Locking](./concurrency-and-locking.md), [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md) — worker concurrency model.
- [Webhooks, Events & Job Logs](./webhooks-and-events.md) — queue-level events (distinct from workflow events).
- [Benchmarking and Performance Evidence](./benchmarks.md) — benchmark contract and maintained runner catalogue.
- [architecture](../architecture.md), [data-model](../data-model.md).
