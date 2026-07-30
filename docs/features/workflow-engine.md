# Workflow Engine (saga orchestration)

> **Category:** Orchestration · **Source:** `src/client/workflow/workflow.ts`, `src/client/workflow/engine.ts`, `src/client/workflow/executor.ts`, `src/client/workflow/runner.ts`, `src/client/workflow/loops.ts`, `src/client/workflow/compensator.ts`, `src/client/workflow/recovery.ts`, `src/client/workflow/store.ts`, `src/client/workflow/emitter.ts`, `src/client/workflow/types.ts`, `src/client/workflow/waitFor.ts`, `src/client/workflow/identity.ts`, `src/client/workflow/rollbackControl.ts`, `src/client/workflow/storeSignals.ts`, `src/client/workflow/storeCodec.ts`, `src/client/workflow/clock.ts`, `src/client/workflow/unwindPlan.ts`, `src/client/workflow/admission.ts`, `src/client/workflow/index.ts`

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
- `forEach(items, name, handler, options?)` — default `maxIterations` `1000`.
- `pivot()` — marks the point of no return. Once passed, `committedAt` is set and NOTHING is eligible for rollback, including steps before it.

`path()`, `parallel()`, `doUntil()` and `doWhile()` reject any non-step node their builder produced (`onlySteps`), and `register()` rejects a step name colliding with a loop's `name:index` namespace (`assertNoIndexCollision`), and rejects two `waitFor` nodes on the same event (`assertNoDuplicateWaitFor`): a delivered signal is never consumed, so one `signal()` would open every gate naming that event.
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
- Every node is enqueued as job name `wf:step` with `StepJobData` payload (`executor.ts:300`).

### Events emitted (`WorkflowEventType`, `types.ts:188-199`) — 11 total

`step:started`, `step:completed`, `step:failed`, `step:retry`, `workflow:started`, `workflow:completed`, `workflow:failed`, `workflow:compensating`, `workflow:waiting`, `signal:received`, `signal:timeout`. See [Webhooks, Events & Job Logs](./webhooks-and-events.md) for the queue-level event system (these are independent, in-process listeners).

### Operator control of a parked unwind (`rollbackControl.ts`)

- `Engine.resumeCompensation(id)` — retry the handler that parked a `compensation-stuck` run, then finish the unwind.
- `Engine.abandonCompensation(id)` — record every outstanding eligible step as `compensation-skipped` and make the run terminal.

### Step context additions

`StepContext` carries `idempotencyKey` on every step, and compensate handlers additionally receive `forwardIdempotencyKey` — the key the forward execution used, so a rollback can reconcile with the provider when the forward outcome is in doubt.

## Data Models

See [data-model](../data-model.md) for full definitions. Key shapes (`types.ts`):

- `Execution`: `{ id, workflowName, state, input, steps, currentNodeIndex, resolvedSteps?, rollbackStatus?, failureReason?, committedAt?, signals, createdAt, updatedAt }`. `rollbackStatus` (absent (`undefined`) | `completed` | `not-applicable` | `stuck`) is a separate axis from `failureReason`: the rollback is what the engine did *after* the failure, not why it failed, and collapsing them makes it impossible to alert on the right thing. `committedAt` is the node index at which `.pivot()` committed.
- `ExecutionState`: `running` | `waiting` | `completed` | `failed` | `compensating` | `compensation-stuck`. The last is deliberately **non-terminal**: a definitive compensation failure parks the run for an operator.
- `StepRecord`: `{ status, result?, error?, startedAt?, completedAt?, attempts?, loopItem?, loopIndex?, compensation?, idempotencyKey?, occurrence?, childExecutionId? }`.
  - `loopItem`/`loopIndex` persist a loop iteration's `__item`/`__index` so compensation reconstructs per-iteration context. Written in a `finally`, so the iteration that *throws* also carries them — otherwise its compensate handler gets an undefined `__item` and silently releases nothing.
  - `doUntil`/`doWhile` write the per-iteration mirror `name:N` in a `finally` for the same reason. `unwindSet` drops the bare `name` mirror whenever a `name:0` sibling exists, because that mirror is a copy of the LAST iteration and compensating it too would undo that iteration twice. Written only on success, the iteration that *threw* existed under the bare name alone and the two rules combined to lose it entirely: a loop that charged every turn and failed on turn 2 refunded turns 0 and 1, left turn 2 standing, and still reported `rollbackStatus: 'completed'`. That record was unreachable even by `abandonCompensation`, which walks the same set.
  - `compensation` is `{ status: 'compensated' | 'compensation-failed' | 'compensation-skipped', at, error? }`. Exactly one per eligible step, discharged when the run terminates.
  - `idempotencyKey` is `run:step#occurrence:direction` (`identity.ts`), persisted with the START record so a rollback can reconcile even when the body never got to write an output.
  - `childExecutionId` on a `sub:<name>` record is the handle used to run the child's own unwind. The record is written `running` as soon as the child is claimed and settled `completed` or `failed` when the child ends. Settling on failure is load-bearing: `unwindSet` drops anything that is neither `completed` nor `failed` before the `sub:` branch can admit it, so a record left `running` took the child out of the parent's unwind and the parent reported `rollbackStatus: 'completed'` over a child parked with resources still held.
  - `compensatable` records whether the step ran with a `compensate` handler. Nothing else can tell "never owed a reversal" from "owed one and the handler has since been renamed away", and both `unwindSet` and `owesOutcome` read it for the vanished-definition case. They must agree: while `owesOutcome` answered from the definition alone it walked past exactly the record `unwindSet` had kept, and `abandonCompensation` ended a terminal run with a step carrying no outcome.
- `StepContext` (`types.ts:8-20`): `{ input, steps, signals, executionId }` passed to every handler; `steps` contains only **completed** step results (built in `buildContext`, `runner.ts:205-216`).
- `WorkflowNode` discriminated union (`types.ts:140-150`): `step | branch | waitFor | parallel | subWorkflow | doUntil | doWhile | forEach | map`.
- `StepJobData` (`types.ts:255-259`): `{ executionId, workflowName, nodeIndex }` — the on-the-wire job payload.

SQLite table `workflow_executions` (id PK, workflow_name, state, input/steps/resolved_steps/signals BLOBs, current_node_index, created_at, updated_at) plus `workflow_executions_archive` (same + `archived_at`), indexed on `workflow_name` and `state` (`store.ts:21-51`).

## Business Logic / Control Flow

**Start** (`executor.ts:49-70`): `register()` rejects duplicate step names across the flattened node graph (`executor.ts:41-46`). `start()` validates the workflow exists and has ≥1 node, mints `id = wf_<ts>_<rand>`, saves an `Execution` at `currentNodeIndex: 0` state `running`, emits `workflow:started`, then `enqueue()`s the first `wf:step` job.

**Per-node loop** (`processStep`, `executor.ts:72-100`): the worker handler calls `processStep(data)`. It loads the execution; if it is absent or not in `running`/`waiting` it returns `null` (execution-level idempotency guard — re-delivered jobs for terminal executions are no-ops). A `waiting` execution is flipped back to `running` so the `waitFor` node re-checks its timeout. It dispatches the node via `executeNode` (`executor.ts:137-152`), then each handler calls `advance(idx+1)`.

**advance** (`executor.ts:282-292`): bumps `currentNodeIndex`, persists, and if past the last node sets `completed` + emits `workflow:completed`; otherwise `enqueue()`s the next job. This is what serializes a single execution into a chain of one-node-at-a-time jobs.

**Step** (`runStep` → `executeStepWithRetry`, `runner.ts:38-126`): loops `attempt` `1..def.retry`. Per attempt it writes a `running` `StepRecord`, validates AND COERCES with `inputSchema.parse(ctx.input)` if present, using the returned value when it is not `undefined`, runs `runWithTimeout(handler, def.timeout)` (`runner.ts:17-35`), validates and coerces with `outputSchema.parse(result)`, using the returned value when it is not `undefined`, then writes a `completed` record and emits `step:completed`. On error it emits `step:retry` and sleeps `backoffDelay(attempt)` = `min(500 * 2^(attempt-1), 30_000)` + up to 50% jitter (`runner.ts:9-14`). After the last attempt it writes a `failed` record, emits `step:failed`, and throws.

**Branch** (`runBranch`, `executor.ts:160-174`): evaluates `condition(ctx)` → path name; runs that path's steps sequentially (missing path = skip), then advances.

**Parallel** (`executeParallelSteps`, `runner.ts:129-146`): runs all steps via `Promise.allSettled`; if any rejected, throws an `AggregateError` of the collected errors.

**Sub-workflow** (`executeSubWorkflow`, `runner.ts:149-175`): `start()`s the named workflow and **polls** `getFn(id)` every `100ms` up to `300_000ms`; on `completed` it collects completed step results into a map stored under `sub:<name>`; on `failed` or timeout it throws.

**Loops/map** (`loops.ts`): `doUntil` runs the body then checks `condition(ctx, iteration)`; `doWhile` checks first; both throw if `maxIterations` exceeded. `forEach` extracts `items`, **throws unless `Array.isArray(items)`**, then throws if `items.length > maxIterations`, and for each item runs an indexed step named `<step>:<i>` whose context is enriched with `steps.__item`/`steps.__index`, persisting `loopItem`/`loopIndex` in a `finally` (so the iteration that throws still carries its item into compensation). `map` runs `transform(ctx)` and stores the result as a completed step.

**waitFor** (`runWaitFor`, `executor.ts:209-249`): if the signal is already present, advance. Otherwise, if a `timeout` is set, it tracks `__waitFor:<event>` start time; if elapsed ≥ timeout it emits `signal:timeout`, marks the wait record failed, sets state `failed`, runs compensation, emits `workflow:failed`, and throws `WaitForSignalError`; else it arms a `setTimeout` via `scheduleTimeoutCheck` for the remaining time. Either way it sets state `waiting`, emits `workflow:waiting`, and throws `WaitForSignalError`. The sentinel is caught in `processStep` (`executor.ts:92`) and turned into `return null`, so the step job acks cleanly without triggering compensation.

**signal** (`executor.signal` → `SignalCoordinator.record`): clears any pending timeout timer, then records `signals[event] = payload` and claims the resume **inside one transaction**, and emits `signal:received` — **always**, so a signal that lands *before* the run parks is still consumed at the `waitFor`. Presence is tested by **key** (`hasSignal`, i.e. `event in signals`), never by value: `payload` is optional, the codec runs with `structuredClone: true` and round-trips `undefined` faithfully, so a value test reports "no signal" for `signal(id, event)` with no payload — the run then resumed, re-entered the `waitFor`, was told nothing had arrived, and re-parked (`test/repro-workflow-signal-no-payload.test.ts`). The same key test guards the in-memory pre-check and the timeout re-read in `waitFor.ts` and the crash-recovery resume in `recovery.ts`. The resume itself is a conditional `UPDATE ... SET state = 'running' WHERE id = ? AND state = 'waiting'`: only a genuinely-parked run is claimed and re-enqueued at `currentNodeIndex`. For any other state (still running an earlier step, already resumed, completed, or failed) the payload is recorded and nothing is enqueued. Because the claim is a single conditional UPDATE rather than an in-memory check, duplicate/concurrent signals collapse to a **single resume** at the database, so every post-`waitFor` step runs exactly once.

**Compensation** (`runCompensation`, `compensator.ts`): builds the unwind set, reverses it (insertion order = start order, so this is reverse *start* order — deterministic where completion order is not), sets state `compensating`, and settles each step with exactly one outcome.

Eligibility: user steps only (no `__` bookkeeping), status `completed` **or `failed`** — the failing step is the one most likely to need undoing, because a charge that reached the provider and then lost its response is recorded failed while the money has moved. Nothing is eligible once `committedAt` is set: past the pivot the saga is committed and recovery is forward-only.

- **Never twice.** A step already carrying a `compensation` outcome is skipped, so an unwind interrupted by a crash resumes where it stopped.
- **Park, do not plough on.** A handler that throws settles as `compensation-failed` and the loop **breaks**. The remaining steps are left *without* an outcome on purpose — the run is parked (`compensation-stuck`, `rollbackStatus: 'stuck'`), not finished, and pre-marking them skipped would make a later resume believe they were handled.
- **Bounded like the forward path, and bounded even when the forward path is not.** The bound comes from `decideUnwindAction` (`unwindPlan.ts`) as `def.timeout > 0 ? def.timeout : DEFAULT_COMPENSATE_TIMEOUT_MS`, so the 30 s default from `workflow.ts:121` applies to a reversal too, and a timeout settles as `compensation-failed` (`error: 'Step timed out after Nms'`) exactly like a throw. `timeout: 0` disables the bound on the forward path only: an unbounded reversal would hold the process-global `inFlight` claim forever, locking the run out of `recover()`, `resumeCompensation` and `abandonCompensation` and leaving it `compensating` rather than parked. An unbounded handler that wedges — a hung HTTP call to a provider that is down, which is precisely when rollbacks run — would leave the run in `compensating`, not `compensation-stuck`: no parked run for an operator to resume or abandon, and every later `recover()` finding the claim still held and returning without having done anything.
- **Nested sagas.** A `sub:<name>` record is compensated by running the CHILD's `runCompensation` (`unwindChild`). A child that parks makes the parent's sub-step throw, so the parent parks too rather than reporting a clean rollback over a half-undone child.
- **Operator exits** (`rollbackControl.ts`): `resumeCompensation` asks for the retry with a FLAG (`retryFailed`) rather than clearing the failed outcome, so the operator's record survives until a real outcome replaces it. Clearing was worse than it looked: it persisted the wipe before running anything, so a resume that met a failing store left a durable row with the diagnostic gone and the run marked `compensating`, re-driven at every startup, and guarding that needed a deep snapshot, a restore path and a second write that could mask the original error. `abandonCompensation` `abandonCompensation` records every outstanding eligible step as `compensation-skipped` and makes the run terminal — this is where "exactly one outcome" is discharged.

Triggered from the generic `processStep` catch, the `waitFor` timeout path, and recovery.

**Recovery** (`recoverExecutions`, `recovery.ts:26-49`): scans `listRecoverable()` (states `running`/`waiting`/`compensating`, `store.ts:96-98`). `running` → re-enqueue at `currentNodeIndex`. `waiting` → if not actually a `waitFor` node re-enqueue; else if signal already arrived resume, else re-arm the timeout for the remaining time (or re-enqueue immediately if already elapsed). `compensating` → re-run `runCompensation` from scratch (handlers must be idempotent). Returns `RecoverResult { running, waiting, compensating, total }`.

A `subWorkflow` CHILD is excluded from `listRecoverable()` (`store.ts`): it carries `parentExecutionId` in its meta blob and its lifecycle belongs to the parent, which unwinds it through `unwindChild`. Driving it independently re-ran its steps, and the fresh records carried no `compensation`, so the never-twice guard did not fire on the parent's later unwind and the child's reversal was dispatched a second time (`test/repro-model-child-recovered-alone.test.ts`). A child whose parent row no longer exists IS returned, so an orphan is not stranded non-terminal forever.

That exception is scoped to a missing parent row, and it does NOT make every child reachable. Two consequences follow, both expected:

- **Abandoned children stay non-terminal.** `executeSubWorkflow` starts a fresh child on every entry to the node: it never resumes the `childExecutionId` recorded in a previous `sub:` record. So each retry of the sub-workflow node's job, and each `recover()` that re-drives a parent parked on that node, leaves the previous child row behind in `running`. Its parent row still exists, so the filter excludes it, and `cleanup()`/`archive()` default to `['completed','failed']`, so nothing removes it either. Those rows accumulate and are inert: the parent's `sub:` record is only written once a child reaches `completed`, so an abandoned child is never referenced by `unwindChild` and never has its compensation dispatched. Operators debugging child executions that sit in `running` forever are seeing this, not a wedged engine. `cleanup(maxAgeMs, ['running'])` does reap them, but it selects on age and state only — it cannot tell an abandoned child from a live parent or a genuinely long-running execution, and will delete those too. Use an age well beyond the longest legitimate run.
- **The exception cannot resurrect the double-unwind.** A child is only unwound through the `sub:` record that names it, and that record exists only for a child that reached `completed` — a state `listRecoverable()` never returns. During the unwind itself the child does turn `compensating` and so does become selectable if the parent row is deleted concurrently (`cleanup(0)` racing a parent that is already persisted `failed` while its compensation runs); in-process that second unwind loses the `inFlight` claim in `compensator.ts` and does nothing. Across processes it reduces to the pre-existing limitation that the claim is process-global.

## Concurrency & Locking

There is **no executor-level lock per execution**. Concurrency control is delegated entirely to the underlying `Worker` (`concurrency` default `5`, `engine.ts:60`) and the queue's leasing — see [Concurrency & Locking](./concurrency-and-locking.md) and [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md).

Within a single execution, nodes are serialized because each `advance()` enqueues exactly one successor job, and a single in-flight job mutates the in-memory `Execution` object then persists it via `WorkflowStore.update` (`store.ts:122-133`, a single `UPDATE` statement). A `parallel` node is the only intra-node concurrency, fanning out via `Promise.allSettled` inside one job.

Signal-timeout timers live in an in-memory `Map<execId, timer>` on the executor (`executor.ts:26`). `scheduleTimeoutCheck` (`executor.ts:303-312`) `set()`s the timer without `clearTimeout`-ing a previously stored one, so re-entering a `waitFor` node can leave an orphaned timer that still fires.

## Edge Cases & Failure Modes

- **Step re-execution is at-least-once.** `executeStepWithRetry` re-runs the handler for the node at `currentNodeIndex`; there is no completed-short-circuit at the *node* level. Re-delivery, `recover()` re-enqueue of a `running` execution, or two jobs on the same node can run a side-effecting step more than once. `ctx.idempotencyKey` is stable across every one of those paths so a provider can absorb the repeat. Loop *iterations* ARE memoised (see below); branch paths and parallel groups are not, and re-run whole.
- **Loop iterations are memoised.** `runIteration` (`loops.ts`) skips an iteration whose `name:iteration` record is already `completed`, restoring the base-name record so the condition and downstream steps see it. Only `completed` is skipped — an iteration left `running` by a crash is genuinely unfinished. Without this a loop replayed from zero on every re-entry, and the restarted in-memory counter overwrote earlier per-iteration records with later content, corrupting the transcript it was meant to preserve.
- **Duplicate/concurrent `signal()` no longer double-executes.** `signal()` gates its resume on `state === 'waiting'` and does the state check + flip to `running` synchronously, persisting `running` before its first `await` (`executor.ts:121-127`). A second signal for the same parked run — sequential *or* concurrent — reads `running` back from the store and returns after only recording its payload, so the post-`waitFor` steps run exactly once (covered by `test/repro-workflow-signal-double.test.ts`). A signal that arrives *before* the run parks is likewise just recorded and consumed later at the `waitFor`.
- **Timeout-timer vs. `signal()` race — now closed by a per-node claim.** `signal()` clears the pending timeout timer, so a signal that wins the race cancels the timeout. If the timer had *already fired* and enqueued a job at the `waitFor` node while `signal()` also enqueued, both jobs used to pass the `running`/`waiting` guard under `concurrency > 1` and advance the run twice. `processStep` now holds `nodesInFlight`, a claim keyed `<executionId>:<nodeIndex>` for the duration of the node, so the second job returns immediately. Two guards, different jobs: the cursor check (`data.nodeIndex !== exec.currentNodeIndex`) drops a job for a node the run has already left; the claim drops a concurrent second job for the node it is on. A deterministic `jobId` was tried instead and removed — dedup at the queue can swallow a *legitimate* re-enqueue and wedge the run, whereas a dropped duplicate cannot.
- **One `Engine` per process, per data path — a real constraint, not a documented guarantee.** `src/client/manager.ts` memoises a process-global `QueueManager` with `instance ??= new QueueManager({ dataPath })`, so the FIRST caller's data path wins and every later `Engine` silently shares it. Two engines with different `dataPath` values in one process therefore collide on one internal `__wf:steps` queue: engine B's step jobs are pulled by engine A's worker, whose `store.get()` against a different SQLite file returns `null`, and `processStep` returns `null` — the job is ACKed as success and the run sits in `running` forever. This predates the saga work and is not enforced in code. Until `getSharedManager` keys by `dataPath`, treat one engine per process as a hard requirement.

- **In-memory store when `dataPath` is omitted.** `WorkflowStore` opens `dataPath ?? ':memory:'` (`store.ts:67`). In TCP/`connection` mode (no `dataPath`) execution state is in-memory only — it is lost on restart and `recover()` finds nothing. Persistence requires passing `dataPath` (typically with `embedded: true`).
- **Retry vs. compensation.** A step failing all `retry` attempts throws out of `processStep`, which sets `failed`, persists, emits `workflow:failed`, runs compensation, then re-throws — failing the underlying `wf:step` job. If the queue retries that job, `processStep` short-circuits because the execution is now `failed`, so compensation does not double-run via that path.
- **Crash mid-compensation** leaves state `compensating`; `recover()` re-enters the unwind, but per-step outcomes are checkpointed, so only the steps that had not settled are attempted again.
- **Parallel partial failure** rejects the whole node with `AggregateError`; already-completed sibling steps remain `completed` and become candidates for compensation.
- **Loop guards.** `doUntil`/`doWhile` throw on `maxIterations` (default `100`), which fails the run and triggers the unwind; `forEach` throws if `items` is not an array, and then if `items.length` exceeds `maxIterations` (default `1000`) — both *before* running any iteration. `Array.isArray` is the predicate deliberately: the old `.length` duck-test accepted a number (zero iterations, run still `completed`) and a string (one iteration per character). Proxied and cross-realm arrays still pass, since `Array.isArray` tests the internal slot; typed arrays and other array-likes no longer do.
- **Loop iterations are the unwind set, not the mirror.** Every iteration is recorded under `name:iteration` *and* mirrored under the bare `name` so the loop condition and downstream steps can read the last result. On rollback the **indexed records are compensated** — `findStepDef` resolves `turn:0` in a second, iteration-only pass — and `unwindSet` excludes the bare mirror so the final iteration is not compensated twice.
- **Sub-workflow** is polled, not event-driven: a sub-workflow that runs longer than `300_000ms` makes the parent step throw a timeout (`runner.ts:157,174`). The parent then settles its `sub:` record `failed` while the child is *still running*, so `unwindChild` refuses any child whose state is `running` or `waiting` and throws instead. Rolling a live child back would put two writers on one row: the child's own `advance()` overwrites the compensation from its stale snapshot, compensate handlers interleave with forward steps, and the child can still reach `completed` with its reversals already executed. The parent parks for an operator rather than claiming a rollback whose subject is still changing the world.
- **Compensation never rolls back internal bookkeeping steps** (names prefixed `__`, e.g. `__waitFor:*`).
- **Sub-builders accept steps only.** `path()`, `parallel()`, `doUntil()` and `doWhile()` throw at build time if the builder produced any non-step node (`onlySteps`, `workflow.ts`). These bodies execute inline inside one job, so a `waitFor` there has no node index to park at and a nested `branch` has no dispatcher. They used to be filtered out silently, which turned an approval gate written inside a path into a no-op the run sailed straight through.
- **Loop index namespace is reserved.** `register()` rejects a step whose name matches `<loopStep>:<digits>` (`assertNoIndexCollision`). Both silent outcomes are corruption: the loop overwrites the user's step, or — with memoisation — mistakes it for its own completed work and skips the iteration.
- **The `signals` column is owned by `storeSignals.ts` alone.** `store.update()` never writes it. A worker holds one in-memory `Execution` for a whole node, so rewriting `signals` from that stale snapshot destroyed payloads delivered mid-step and parked runs forever. `recordSignal`/`parkForSignal` read-modify-write it inside a transaction, and the resume is claimed with a conditional state UPDATE so duplicate signals collapse to exactly one resume.
- **`waitFor` timeouts are clamped** to `2**31-1` ms and re-armed in chunks; larger values wrapped to 1ms and fired immediately.
- **List queries are capped** at 100 rows (`listExecutions`, `store.ts:86-95`; `getExecution` is an uncapped by-id lookup); `archive` moves at most 1000 rows per call (`store.ts:173`). Use repeated `archive()`/`cleanup()` calls for large backlogs.
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

Step-level knobs are set per `.step()`: `retry` (default `3`), `timeout` (default `30_000`ms), `inputSchema`/`outputSchema`, `compensate`. Loop bounds: `doUntil`/`doWhile` `maxIterations` `100`, `forEach` `maxIterations` `1000`.

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

Every `Date.now()`, `Math.random()`, `setTimeout` and `clearTimeout` in this module
goes through `clock()` (`clock.ts`). The default is the real clock and is installed by
default, so nothing changes for a caller who does not ask otherwise.

The reason is evidence, not taste. Each crash-window defect this engine shipped took a
property-based campaign roughly one run in eleven to surface, and the seed that
produced it did NOT replay it: the seed drove the command sequence while the
interleaving came from real timers. A failing seed you cannot replay is a bug report
you cannot act on. With `simulatedClock(seed)` installed, retry backoff, signal
timeouts, execution ids and every persisted timestamp become functions of that seed.

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
