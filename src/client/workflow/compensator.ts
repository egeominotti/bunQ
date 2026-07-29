/**
 * Saga rollback — runs compensate handlers in reverse start order.
 *
 * Four properties this file is responsible for, each of which used to be violated:
 *
 *  - EXACTLY ONE OUTCOME per eligible step. Success is recorded as loudly as
 *    failure, and a step the unwind never reached is recorded as skipped rather
 *    than left blank. "Never zero, never two" is not checkable otherwise.
 *  - NEVER TWICE. A step that already carries an outcome is not re-run, so an
 *    unwind interrupted by a crash resumes where it stopped instead of replaying
 *    reversals from the top.
 *  - HALT, NOT PLOUGH ON. A compensation that fails definitively stops the chain:
 *    continuing past it would undo things whose dependencies are still standing.
 *    The remaining eligible steps are marked skipped so the gap is visible.
 *  - PIVOT. Past `.pivot()` the saga is committed; those steps are never eligible.
 *
 * `rollbackStatus` on the execution is a separate axis from the failure reason. The
 * rollback is what the engine did *after* the failure, not why it failed, and an
 * operator needs to alert on "the refund never went through" independently of "the
 * payment failed".
 */

import type { CompensationStatus, Execution, StepRecord } from './types';
import type { Workflow } from './workflow';
import type { WorkflowStore } from './store';
import type { WorkflowEmitter } from './emitter';
import { findStepDef, buildContext, runWithTimeout } from './runner';
import { idempotencyKey, loopBaseName, describeError } from './identity';
import { clock } from './clock';
import { decideUnwindAction, owesOutcome } from './unwindPlan';
import { isLive } from './admission';

/** Sentinel error thrown when execution must pause for a signal */
export class WaitForSignalError extends Error {
  constructor(readonly event: string) {
    super(`Waiting for signal: ${event}`);
  }
}

/**
 * Executions with an unwind in flight in THIS process.
 *
 * The "never compensate twice" guard inside the loop reads `record.compensation`
 * from the caller's own snapshot, which was loaded before any concurrent unwind
 * settled anything. Two overlapping unwinds of the same execution therefore both see
 * an unsettled record and both run the handler: a refund issued twice, stock released
 * twice. `recover()` reaches this legitimately (it lists `compensating` runs) and can
 * be called on a live engine that is already driving one.
 *
 * Scope is the process, which is what the set can actually enforce. It is NOT a
 * distributed lock: two processes on one database can still overlap, and so can two
 * Engine instances that somehow reach the same execution. An earlier version of this
 * comment justified the scope by citing a single-instance guarantee in
 * `docs/features/workflow-engine.md`; that statement is not in that file and the
 * citation was wrong.
 */
const inFlight = new Set<string>();

/**
 * Deliberately NOT exported, and deliberately never bulk-cleared.
 *
 * `Engine.close()` used to call a `releaseUnwindClaim()` that emptied this set. The
 * set is process-global, so closing ANY engine dropped the claim protecting an unwind
 * still running under a DIFFERENT one, and the next caller re-dispatched compensate
 * handlers that were mid-flight: the duplicate refund this claim exists to stop.
 *
 * Nothing needs to clear it. `runCompensation` releases in a `finally`, so a claim
 * outlives its unwind by nothing, bounded by the step's own `timeout` even if the
 * handler wedges. If the process dies the module state dies with it.
 */

/**
 * Outcome of asking for an unwind. `claim-lost` means another unwind of the same
 * execution is already in flight, so THIS call did nothing and its caller must not
 * treat the rollback as done.
 */
export type UnwindOutcome = 'ran' | 'claim-lost';

/** Run compensation handlers in reverse start order for every eligible step. */
// 6 params, one over the limit. The registry is needed to unwind a nested saga through the
// child's own workflow, and `opts` carries the operator retry; folding them into one bag
// would hide which of the two a call site is actually asking for, across four call sites
// that each pass a different combination.
// biome-ignore lint/complexity/useMaxParams: see above
export async function runCompensation(
  exec: Execution,
  wf: Workflow,
  store: WorkflowStore,
  emitter: WorkflowEmitter | null,
  workflows?: Map<string, Workflow>,
  opts: { retryFailed?: boolean } = {}
): Promise<UnwindOutcome> {
  const eligible = unwindSet(exec, wf);

  if (eligible.length === 0) {
    exec.rollbackStatus = 'not-applicable';
    // The run must also become terminal here. On the `runNode` path the caller has
    // already set `failed`, which hid this; `recoverExecutions` has no such caller, so
    // a persisted `compensating` run whose steps no longer resolve (a deploy renamed a
    // step) stayed `compensating` forever, was returned by `listRecoverable()` again,
    // and was re-driven and re-counted at every single startup.
    if (exec.state !== 'failed' && exec.state !== 'completed') exec.state = 'failed';
    // Unguarded: a throw here replaces the caller's node error, which is the masking this
    // module fixed elsewhere, but nothing was eligible so there is no rollback to lose, and
    // a `failureReason` from the original failure is already on disk. (On the `runNode`
    // path its caller wrote it; the `recovery.ts` and `rollbackControl.ts` entries have no
    // such caller, and reach here on a row that already carries one.)
    //
    // Unlike the other unguarded writes, this one does NOT self-heal: on the `runNode` path
    // disk ends `failed`, which `listRecoverable()` does not cover, so `rollbackStatus`
    // stays absent instead of `'not-applicable'` for good. That residue is the honest
    // reading anyway, since absent means no unwind was attempted, and guarding the write
    // would swallow a store failure with nothing left to report it.
    store.update(exec);
    return 'ran';
  }

  // Claim before the first await. Losing the claim means another unwind of this same
  // execution is already running, and the caller MUST be told: a parent rolling back
  // a sub-workflow cannot tell "the child finished" from "someone else is driving the
  // child" if both look like a plain return, and it would settle the child's record
  // as `compensated` while that other unwind was still free to fail.
  if (inFlight.has(exec.id)) return 'claim-lost';
  inFlight.add(exec.id);
  try {
    await unwind({ exec, wf, store, emitter, eligible, workflows, ...opts });
  } finally {
    inFlight.delete(exec.id);
  }
  return 'ran';
}

/** Everything one unwind pass needs. Grouped so the signature stays readable. */
interface UnwindPass {
  exec: Execution;
  wf: Workflow;
  store: WorkflowStore;
  emitter: WorkflowEmitter | null;
  eligible: [string, StepRecord][];
  workflows?: Map<string, Workflow>;
  retryFailed?: boolean;
}

async function unwind(pass: UnwindPass): Promise<void> {
  const { exec, wf, store, emitter, eligible, workflows, retryFailed } = pass;
  const baseCtx = buildContext(exec);
  let haltedAt: string | null = null;
  /** A store write that failed, kept so the caller still learns why the pass stopped. */
  let writeFailure: unknown;

  exec.state = 'compensating';
  try {
    store.update(exec);
  } catch (err) {
    // The write that happens on EVERY unwind, and it was the last one left unguarded.
    //
    // Unguarded, the throw escaped with the run left `compensating` in memory and, since
    // the caller on the `runNode` path has already persisted `failed`, `failed` on disk
    // with zero reversals run. `listRecoverable()` covers `running|waiting|compensating`,
    // so recovery never revisited it, and both operator exits require
    // `compensation-stuck`, so `resumeCompensation` and `abandonCompensation` both threw.
    // No reversals, no signal, and no way back in
    // (`test/repro-workflow-unwind-write-failure.test.ts`).
    //
    // Nothing has been undone at this point, so parking is the honest state as well as
    // the actionable one: the unwind is owed and has not happened.
    writeFailure = err;
    haltedAt = '(the compensating transition)';
  }
  if (writeFailure === undefined) {
    emitter?.emitWorkflow('workflow:compensating', exec.id, exec.workflowName, 'compensating');
  }

  for (const [name, record] of eligible) {
    // A pass that could not record its own state transition decides nothing.
    //
    // Setting `haltedAt` above is not enough on its own: `decideUnwindAction` checks the
    // vanished-step case FIRST, ahead of the halted check, deliberately, so a renamed step
    // still had `compensation-failed` written and a `compensation:failed` emitted in a pass
    // where no handler ran and the store had already refused everything. The in-memory
    // outcomes then disagreed with a disk that had received nothing, and the event pointed
    // an operator at the wrong cause
    // (`test/repro-workflow-unwind-write-failure.test.ts`).
    if (writeFailure !== undefined) break;

    // The DECISION lives in `unwindPlan.ts` as a pure function; this loop only carries
    // it out. Every rollback defect this engine shipped was in these few lines of
    // reasoning rather than in the I/O around them, and while the two were tangled the
    // only way to test a decision was to stand up a database and infer it from side
    // effects.
    const action = decideUnwindAction(wf, name, record, haltedAt !== null, retryFailed);
    if (action.kind === 'stop') break;
    if (action.kind === 'skip') continue;
    if (action.kind === 'halt-failed') {
      // Its outcome is already recorded and still stands: nothing to write, everything
      // to stop for.
      haltedAt = name;
      continue;
    }
    if (action.kind === 'halt-vanished') {
      haltedAt = name;
      settle(record, 'compensation-failed', action.error);
      emitter?.emitStep('compensation:failed', exec.id, exec.workflowName, name, {
        error: 'step no longer declared',
      });
      continue;
    }

    emitter?.emitStep('compensation:started', exec.id, exec.workflowName, name);
    try {
      if (action.kind === 'unwind-child') {
        await unwindChild(record, store, emitter, workflows, { retryFailed });
      } else {
        const def = findStepDef(wf, name);
        await runWithTimeout(
          def?.compensate?.(compensationContext(exec, baseCtx, name, record)),
          action.timeoutMs
        );
      }
      settle(record, 'compensated');
      emitter?.emitStep('compensation:completed', exec.id, exec.workflowName, name);
    } catch (err) {
      const message = describeError(err);
      settle(record, 'compensation-failed', message);
      emitter?.emitStep('compensation:failed', exec.id, exec.workflowName, name, {
        error: message,
      });
      haltedAt = name;
    }
    try {
      store.update(exec);
    } catch (err) {
      // The outcome just recorded did not reach disk.
      //
      // This write used to sit outside any catch, so a `SQLITE_BUSY` escaped the whole
      // pass and left the run in `compensating`: `resumeCompensation` and
      // `abandonCompensation` both require `compensation-stuck`, so the operator had no
      // exit at all, while `listRecoverable()` DOES return `compensating` and the next
      // `recover()` re-drove the pass and ran the unpersisted reversal a second time
      // (`test/repro-workflow-unwind-write-failure.test.ts`).
      //
      // Stop here rather than carrying on. Every further reversal would have the same
      // problem persisting its outcome, and an unrecorded reversal is one that runs
      // again: "never twice" lost one handler at a time. The record itself is left
      // exactly as it is — overwriting a reversal that provably succeeded with a write
      // error would destroy the outcome this module works hardest to protect.
      writeFailure = err;
      haltedAt = name;
      break;
    }
  }

  if (haltedAt !== null) {
    exec.state = 'compensation-stuck';
    exec.rollbackStatus = 'stuck';
  } else {
    exec.state = 'failed';
    exec.rollbackStatus = 'completed';
  }
  try {
    store.update(exec);
  } catch (err) {
    // If the store is what broke, parking cannot be persisted either. The in-memory run
    // is still left parked so a caller holding it sees an actionable state, and the
    // ORIGINAL write error is the one that propagates: it is the cause, and this second
    // failure is its consequence.
    if (writeFailure === undefined) throw err;
  }
  if (writeFailure !== undefined) throw writeFailure;
}

/**
 * Give up on a parked unwind: every eligible step still without an outcome is
 * recorded as skipped, and the run becomes terminal. This is where "exactly one
 * outcome per eligible step" is finally discharged.
 */
export function abandonCompensation(
  exec: Execution,
  wf: Workflow,
  store: WorkflowStore,
  emitter: WorkflowEmitter | null
): void {
  for (const [name, record] of unwindSet(exec, wf)) {
    if (record.compensation) continue;
    if (!owesOutcome(wf, name, record)) continue;
    const reason = 'unwind abandoned by operator';
    settle(record, 'compensation-skipped', reason);
    emitter?.emitStep('compensation:skipped', exec.id, exec.workflowName, name, { error: reason });
  }
  exec.state = 'failed';
  exec.rollbackStatus = 'stuck';
  store.update(exec);
}

/**
 * Steps eligible for rollback, in the order they must be undone.
 *
 * `exec.steps` is written when a step STARTS, so reversing insertion order gives
 * reverse start order — deterministic even when parallel steps finish out of
 * sequence, which completion order is not.
 *
 * A `failed` step is included on purpose: it is the one most likely to need undoing,
 * because a charge that reached the provider and then lost its response is recorded
 * failed while the money has already moved.
 */
function unwindSet(exec: Execution, wf: Workflow): [string, StepRecord][] {
  // The saga committed at the pivot. Backward recovery is off for the WHOLE run,
  // including the steps before it — unwinding them now would contradict work the
  // outside world has already been told about.
  if (exec.committedAt !== undefined) return [];

  return Object.entries(exec.steps)
    .filter(([name, s]) => {
      if (name.startsWith('__')) return false; // engine bookkeeping, not a user step
      if (s.status !== 'completed' && s.status !== 'failed') return false;
      // `doUntil`/`doWhile` write BOTH a per-iteration record (`turn:0`, `turn:1`, ...)
      // and a bare `turn` mirroring the last iteration, so downstream steps and the
      // loop condition can read it by name. Compensating both would undo the final
      // iteration twice. The indexed records are the real history; the mirror is not.
      //
      // Gated on the name actually being a loop body. Testing only for a `${name}:0`
      // sibling dropped a plain step called `foo` from the unwind whenever an
      // unrelated step called `foo:0` existed: `foo` finished with NO outcome while
      // the run still reported `rollbackStatus: 'completed'`, which is the one
      // reading an operator alerting on rollback failure must be able to trust.
      if (isLoopBody(wf, name) && exec.steps[`${name}:0`] !== undefined) return false;
      // A nested workflow is eligible through its child handle rather than a step
      // definition: rolling it back means running the child's own unwind.
      if (name.startsWith('sub:')) return s.childExecutionId !== undefined;
      if (findStepDef(wf, name) !== null) return true;
      // The definition is gone. Two kinds of record are still owed a reversal, and
      // dropping either is how a parked run whose step a deploy renamed reported
      // `rollbackStatus: 'completed'` over work nobody undid: nothing was left to halt
      // on, so the unwind reached the end and called itself clean.
      //
      // One already carries an outcome, so it was part of the set when the earlier
      // attempt ran. The other carries none but ran WITH a handler, which is the
      // likelier case, because the unwind simply had not reached it yet
      // (`test/repro-workflow-nested-and-settled.test.ts`).
      return s.compensation !== undefined || s.compensatable === true;
    })
    .reverse();
}

/**
 * Roll back a nested workflow by running the CHILD's own unwind.
 *
 * A child that succeeded before its parent failed used to be left untouched: every
 * resource it created stayed live with nothing pointing at it. Its compensation is
 * not a handler the parent can call — it is the child's whole rollback.
 *
 * If the child parks (`compensation-stuck`), the parent inherits it: this throws, so
 * the parent's own unwind halts and parks too. A half-rolled-back child is not
 * something the parent can paper over.
 */
async function unwindChild(
  record: StepRecord,
  store: WorkflowStore,
  emitter: WorkflowEmitter | null,
  workflows?: Map<string, Workflow>,
  /**
   * Forwarded from the parent's own pass.
   *
   * Without it `resumeCompensation(parentId)` on a nested saga was a silent no-op that
   * resolved successfully: the retry stopped at the parent, the child halted on its own
   * `compensation-failed`, `unwindChild` threw, and the parent re-parked. The operator
   * got a clean return for an action that did nothing, which is the same anti-pattern
   * the signal API was just fixed for, and the guide names this exact call as the way
   * out of a parent that inherited its child's park.
   */
  opts: { retryFailed?: boolean } = {}
): Promise<void> {
  const childId = record.childExecutionId;
  if (!childId) throw new Error('sub-workflow record carries no child execution id');
  if (!workflows) throw new Error('workflow registry unavailable to unwind a sub-workflow');

  const child = store.get(childId);
  if (!child) throw new Error(`child execution "${childId}" not found`);
  const childWf = workflows.get(child.workflowName);
  if (!childWf) throw new Error(`child workflow "${child.workflowName}" is not registered`);

  // A child that has NOT stopped is never rolled back. Running its reversals while it is
  // still stepping forward puts two writers on one row: the child's own `advance()`
  // overwrites the compensation from its stale snapshot, compensate handlers interleave
  // with forward steps, and the child can go on to reach `completed` with its reversals
  // already executed.
  //
  // The parent reaches this legitimately. `executeSubWorkflow` gives up after a hardcoded
  // 300 second ceiling, which the guide documents as a supported case, and the parent
  // then settles its `sub:` record `failed` while the child is very much alive. Refusing
  // here rather than at the settle keeps the record truthful: the parent's node DID fail.
  // The parent still does not claim a clean rollback, it parks for an operator, which is
  // the correct outcome for "the child may or may not still be changing the world".
  if (isLive(child.state)) {
    throw new Error(
      `child "${child.workflowName}" (${childId}) is still ${child.state}; it cannot be rolled back until it stops`
    );
  }

  const outcome = await runCompensation(child, childWf, store, emitter, workflows, opts);

  // Another driver holds this child's unwind. It may still fail, so recording the
  // parent's `sub:` record as compensated here would claim a rollback whose result is
  // not yet known, and nothing would ever correct it.
  if (outcome === 'claim-lost') {
    throw new Error(
      `child "${child.workflowName}" (${childId}) is being rolled back by another driver; outcome unknown`
    );
  }

  if (child.rollbackStatus === 'stuck') {
    throw new Error(`child "${child.workflowName}" (${childId}) parked mid-rollback`);
  }
  // A child past its own .pivot() is committed: nothing of it was undone. Reporting
  // the parent's `sub:` record as 'compensated' would claim a rollback that provably
  // did not happen, and an operator reading the parent would see a clean unwind over
  // a child that still holds every effect it created.
  if (child.rollbackStatus === 'not-applicable' && child.committedAt !== undefined) {
    throw new CommittedChildError(child.workflowName, childId);
  }
}

/**
 * A sub-workflow that passed its own pivot cannot be rolled back. Distinct from a
 * handler failure so the parent's record carries the reason rather than a generic
 * error, and so the parent parks (operator decision) instead of silently claiming
 * success.
 */
export class CommittedChildError extends Error {
  constructor(workflowName: string, executionId: string) {
    super(
      `child "${workflowName}" (${executionId}) is committed past its pivot; nothing was rolled back`
    );
    this.name = 'CommittedChildError';
  }
}

/** Is `name` the body of a doUntil/doWhile loop, i.e. a step that writes a mirror record? */
function isLoopBody(wf: Workflow, name: string): boolean {
  return wf.nodes.some(
    (node) =>
      (node.type === 'doUntil' || node.type === 'doWhile') &&
      node.def.steps.some((s) => s.name === name)
  );
}

function settle(record: StepRecord, status: CompensationStatus, error?: string): void {
  record.compensation = { status, at: clock().now(), ...(error ? { error } : {}) };
}

/**
 * Context handed to a compensate handler.
 *
 * Beyond the run's own data it carries the identity needed to reconcile: this
 * rollback's key, and the key the forward step used. When the forward outcome is in
 * doubt the handler can ask the provider what actually happened by key rather than
 * depending on an output that may never have been persisted.
 */
function compensationContext(
  exec: Execution,
  baseCtx: ReturnType<typeof buildContext>,
  name: string,
  record: StepRecord
) {
  const occurrence = record.occurrence ?? 0;
  // Rebind the step's bare name to THIS record's own result.
  //
  // `buildContext` exposes results under bare names, and a loop's bare name mirrors
  // its LAST iteration. Without this, every iteration's compensate handler reads
  // `ctx.steps.charge` and sees the final charge: three charges produced three
  // refunds of the third one. `base` is the name without the `:index` suffix, so a
  // non-loop step rebinds to itself, which is a no-op.
  // Only a numeric suffix is an iteration. Stripping at the last colon rebound
  // `payment:charge` onto `payment`, feeding that handler a sibling step's result.
  const base = loopBaseName(name);
  const ctx = {
    ...baseCtx,
    steps: { ...baseCtx.steps, [base]: record.result, [name]: record.result },
    idempotencyKey: idempotencyKey(exec.id, name, occurrence, 'compensate'),
    forwardIdempotencyKey: record.idempotencyKey,
  };
  // For forEach iterations, restore that iteration's __item/__index so the handler
  // knows exactly which item it is rolling back.
  if (record.loopIndex === undefined) return ctx;
  return {
    ...ctx,
    steps: { ...ctx.steps, __item: record.loopItem, __index: record.loopIndex },
  };
}
