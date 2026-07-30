/**
 * Loop execution logic for the Workflow Engine.
 */

import type {
  StepDefinition,
  StepContext,
  Execution,
  LoopDefinition,
  ForEachDefinition,
} from './types';
import type { WorkflowEmitter } from './emitter';
import { executeStepWithRetry, buildContext } from './runner';
import { forEachItemsDecisionKey, loopDecisionKey, resolveDecision } from './workflowDecisions';

export { executeMap } from './mapRunner';

/**
 * Run one iteration of a loop body step, unless it already ran.
 *
 * Two properties depend on this, and both were broken:
 *
 * TRANSCRIPT. `exec.steps[name]` is overwritten every iteration, so on its own a
 * loop keeps a sliding window of exactly one turn — by turn 3 the transcript of
 * turn 1 is gone and an agent is asked to reason without its own history
 * (test/workflow-ai-sdk-agent.test.ts measured prompt sizes 1, 3, 3 where they must
 * be 1, 3, 5). Each iteration is therefore also kept under `name:iteration`.
 *
 * RESUME. A loop is a single job, so re-entering the node used to replay every
 * iteration from zero: an agent killed at turn 7 re-paid for turns 1-6, and the
 * restarted in-memory counter overwrote `turn:0` and `turn:1` with turn 2's content,
 * corrupting the very transcript above. Memoising against the persisted per-iteration
 * record fixes both at once — the counter realigns naturally, because iteration 0
 * finds `turn:0` already complete and skips it.
 *
 * Only `completed` is skipped. An iteration left `running` by a crash is genuinely
 * unfinished and must run again.
 *
 * The base name keeps holding the LAST iteration, which is the documented contract
 * for downstream steps.
 *
 * The indexed copies ARE the unwind set: `findStepDef` resolves `turn:0` through a
 * second, iteration-only pass, and `unwindSet` excludes the bare `turn` mirror so the
 * last iteration is not compensated twice. An earlier version of this comment said
 * the opposite, which was true only while a loop compensated its last iteration
 * alone.
 */
async function runIteration(
  step: StepDefinition,
  exec: Execution,
  iteration: number,
  emitter: WorkflowEmitter | null,
  updateFn: (exec: Execution) => void
): Promise<void> {
  const indexed = `${step.name}:${iteration}`;
  const already = exec.steps[indexed];
  if (already?.status === 'completed') {
    // Restore the base name so the loop condition and downstream steps see this
    // iteration's result without the body running a second time.
    exec.steps[step.name] = { ...already };
    updateFn(exec);
    return;
  }

  let thrown: unknown;
  let threw = false;
  try {
    await executeStepWithRetry(step, buildContext(exec), exec, { emitter, updateFn }, iteration);
  } catch (error) {
    // Caught rather than left to a `finally`, because the mirror write below must not be
    // able to REPLACE this error. An exception thrown from a `finally` supersedes the one
    // in flight, so a store that refused the mirror write turned "provider timeout after
    // the charge settled" into "SQLITE_BUSY": that message is what `failureReason`
    // records, so the operator lost the only account of what actually went wrong
    // (`test/repro-workflow-loop-failed-iteration.test.ts`).
    thrown = error;
    threw = true;
  }

  {
    // Writing the mirror on BOTH paths is load-bearing, for the same reason
    // `executeForEach` writes its own in a `finally`.
    //
    // The indexed copies ARE the unwind set, and `unwindSet` drops the bare mirror
    // whenever a `${name}:0` sibling exists, because that mirror is the LAST iteration
    // and compensating it as well would undo that iteration twice. Written only on
    // success, the iteration that THREW existed under the bare name alone, so the two
    // rules combined to lose it: a `doUntil` that charged on every turn and failed on
    // turn 2 refunded turns 0 and 1, left turn 2's charge standing, and still reported
    // `rollbackStatus: 'completed'`. That record was unreachable even by
    // `abandonCompensation`, which walks the same set
    // (`test/repro-workflow-loop-failed-iteration.test.ts`).
    //
    // A failed iteration is the one MOST likely to need undoing: a charge that reached
    // the provider and then lost its response is recorded failed while the money has
    // already moved.
    const record = exec.steps[step.name];
    if (record) {
      // The in-memory copy is what the unwind reads, so it happens regardless. Only the
      // PERSIST can fail, and when the step itself failed, that step's error is the cause
      // and a write that fails while reporting it is downstream. With no step error to
      // protect, a write that cannot be persisted is the failure and must surface: an
      // iteration whose record never reached disk is one whose reversal runs again.
      exec.steps[indexed] = { ...record };
      try {
        updateFn(exec);
      } catch (writeError) {
        if (!threw) throw writeError;
      }
    }
  }

  if (threw) throw thrown;
}

/** Execute a doUntil loop: run steps, then check condition. Repeat until condition returns true. */
export async function executeDoUntil(
  def: LoopDefinition,
  exec: Execution,
  emitter: WorkflowEmitter | null,
  updateFn: (exec: Execution) => void
): Promise<void> {
  let iteration = 0;

  let shouldStop = false;
  while (!shouldStop) {
    if (iteration >= def.maxIterations) {
      throw new Error(`doUntil exceeded maxIterations (${def.maxIterations})`);
    }
    for (const step of def.steps) {
      await runIteration(step, exec, iteration, emitter, updateFn);
    }
    iteration++;
    const ctx = buildContext(exec);
    shouldStop = await resolveDecision(
      exec,
      loopDecisionKey('doUntil', def.steps[0].name, iteration),
      () => def.condition(ctx, iteration),
      updateFn
    );
    if (typeof shouldStop !== 'boolean') {
      throw new Error('doUntil condition must return a boolean');
    }
  }
}

/** Execute a doWhile loop: check condition first, then run steps. Repeat while condition is true. */
export async function executeDoWhile(
  def: LoopDefinition,
  exec: Execution,
  emitter: WorkflowEmitter | null,
  updateFn: (exec: Execution) => void
): Promise<void> {
  for (let iteration = 0; ; iteration++) {
    const ctx = buildContext(exec);
    const shouldContinue = await resolveDecision(
      exec,
      loopDecisionKey('doWhile', def.steps[0].name, iteration),
      () => def.condition(ctx, iteration),
      updateFn
    );
    if (typeof shouldContinue !== 'boolean') {
      throw new Error('doWhile condition must return a boolean');
    }
    if (!shouldContinue) break;

    if (iteration >= def.maxIterations) {
      throw new Error(`doWhile exceeded maxIterations (${def.maxIterations})`);
    }
    for (const step of def.steps) {
      await runIteration(step, exec, iteration, emitter, updateFn);
    }
  }
}

/** Execute a forEach loop: iterate over items, executing the step for each */
export async function executeForEach(
  def: ForEachDefinition,
  exec: Execution,
  emitter: WorkflowEmitter | null,
  updateFn: (exec: Execution) => void
): Promise<void> {
  const ctx = buildContext(exec);
  const items = await resolveDecision(
    exec,
    forEachItemsDecisionKey(def.step.name),
    () => structuredClone(def.items(ctx)),
    updateFn
  );

  // Anything with a `length` used to be accepted, and JavaScript is generous about
  // what has one. A number iterated ZERO times and the run reported `completed`, so a
  // batch that processed nothing was indistinguishable from a batch with nothing to
  // do. A string iterated its CHARACTERS, so an id list that arrived as `'u1,u2'`
  // silently processed five "items" nobody passed. `null`/`undefined` were caught only
  // by accident, because reading `.length` throws. The likely production shapes were
  // exactly the ones that passed (`test/repro-workflow-foreach-non-array.test.ts`).
  if (!Array.isArray(items)) {
    throw new Error(
      `forEach items must be an array, got ${items === null ? 'null' : typeof items}`
    );
  }

  if (items.length > def.maxIterations) {
    throw new Error(`forEach items (${items.length}) exceeds maxIterations (${def.maxIterations})`);
  }

  for (let i = 0; i < items.length; i++) {
    const item = structuredClone(items[i]);
    const indexedName = `${def.step.name}:${i}`;
    const indexedStep: StepDefinition = {
      ...def.step,
      name: indexedName,
      handler: (stepCtx: StepContext) => {
        const enrichedCtx: StepContext = {
          ...stepCtx,
          steps: { ...stepCtx.steps, __item: item, __index: i },
        };
        return def.step.handler(enrichedCtx);
      },
    };
    // Memoised the same way as doUntil/doWhile: an item already provisioned before a
    // crash must not be provisioned again when the node is re-entered. Restore the
    // declared bare name too: it is the public result of the final iteration, while
    // the indexed record remains the durable execution/compensation identity.
    const completed = exec.steps[indexedName];
    if (completed?.status === 'completed') {
      exec.steps[def.step.name] = { ...completed };
      updateFn(exec);
      continue;
    }

    const stepCtx = buildContext(exec);
    let thrown: unknown;
    let threw = false;
    try {
      await executeStepWithRetry(indexedStep, stepCtx, exec, { emitter, updateFn }, i);
    } catch (error) {
      // Same reason `runIteration` catches instead of using a `finally`: a write that
      // fails below must not REPLACE the step's own error, which is what `failureReason`
      // records and the only account of what went wrong.
      thrown = error;
      threw = true;
    }

    {
      // Persist this iteration's __item/__index alongside the step record so saga
      // compensation can restore the correct per-iteration context later.
      //
      // Recording on BOTH paths is load-bearing: the iteration that THROWS is itself
      // eligible for compensation, and without its item recorded its compensate handler
      // is handed an undefined `__item` and silently releases nothing — the reservation
      // it had already taken at the warehouse leaks
      // (test/workflow-saga-extreme.test.ts).
      const record = exec.steps[indexedName];
      if (record) {
        record.loopItem = item;
        record.loopIndex = i;
        // Keep the documented aggregate view in sync on success and failure. The
        // indexed record is still the authoritative unit of work; compensator.ts
        // excludes this mirror whenever an indexed sibling exists.
        exec.steps[def.step.name] = { ...record };
        try {
          updateFn(exec);
        } catch (writeError) {
          if (!threw) throw writeError;
        }
      }
    }

    if (threw) throw thrown;
  }
}
