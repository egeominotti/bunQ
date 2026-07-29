/**
 * WorkflowRunner - Step execution with retry, parallel execution, sub-workflow dispatch
 */

import type { StepDefinition, StepContext, Execution } from './types';
import { idempotencyKey, isIterationOf, describeError } from './identity';
import type { Workflow } from './workflow';
import type { WorkflowEmitter } from './emitter';
import { clock } from './clock';

/** Exponential backoff with jitter */
function backoffDelay(attempt: number, baseMs = 500, maxMs = 30_000): number {
  const delay = Math.min(baseMs * 2 ** (attempt - 1), maxMs);
  const jitter = delay * 0.5 * clock().random();
  return delay + jitter;
}

/** Run a promise with a timeout */
export function runWithTimeout<T>(promise: Promise<T> | T, timeoutMs: number): Promise<T> {
  if (!(promise instanceof Promise)) return Promise.resolve(promise);
  if (timeoutMs <= 0) return promise;
  return new Promise<T>((resolve, reject) => {
    const timer = clock().setTimeout(() => {
      reject(new Error(`Step timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    promise.then(
      (v) => {
        clock().clearTimeout(timer);
        resolve(v);
      },
      (e: unknown) => {
        clock().clearTimeout(timer);
        // `describeError`, not `String`: this wrapper runs BEFORE the compensator's own
        // catch, so converting here with `String` is what destroyed a structured throw
        // into `[object Object]` no matter how carefully the catch handled it.
        reject(e instanceof Error ? e : new Error(describeError(e)));
      }
    );
  });
}

/** Engine hooks a step needs; they always travel together. */
export interface StepHooks {
  emitter: WorkflowEmitter | null;
  updateFn: (exec: Execution) => void;
}

/** Execute a step with retry logic and exponential backoff */
export async function executeStepWithRetry(
  def: StepDefinition,
  ctx: StepContext,
  exec: Execution,
  hooks: StepHooks,
  occurrence = 0
): Promise<void> {
  const { emitter, updateFn } = hooks;
  const maxAttempts = def.retry;
  let lastError: Error | undefined;
  // Derived once, outside the retry loop, and persisted with the START record: a
  // rollback for a step whose outcome is unknown needs this key to reconcile, and by
  // then the body may never have reached the point of writing anything.
  const forwardKey = idempotencyKey(exec.id, def.name, occurrence, 'forward');
  const stepCtx: StepContext = { ...ctx, idempotencyKey: forwardKey };

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const prev = exec.steps[def.name] as { startedAt?: number } | undefined;
    exec.steps[def.name] = {
      status: 'running',
      startedAt: prev?.startedAt ?? clock().now(),
      attempts: attempt,
      idempotencyKey: forwardKey,
      occurrence,
    };
    updateFn(exec);
    emitter?.emitStep('step:started', exec.id, exec.workflowName, def.name, {
      attempt,
      maxAttempts,
    });

    try {
      let input = stepCtx.input;
      if (def.inputSchema) {
        try {
          // The RETURN VALUE matters. `parse()` is the coercing entry point of every
          // schema library the docs point at: `.default()` fills gaps, `.transform()`
          // rewrites, `z.coerce.date()` builds a Date from a string. Calling it purely
          // for its throw validated the shape and silently dropped every coercion, so
          // a step declaring `.default('EUR')` ran with no currency at all
          // (`test/repro-workflow-gate-and-schema.test.ts`). A validator that returns
          // nothing is still supported: `undefined` means "I only assert", so the
          // original value is kept rather than blanked.
          const parsed = def.inputSchema.parse(stepCtx.input);
          if (parsed !== undefined) input = parsed;
        } catch (e) {
          throw new Error(`Input validation failed for "${def.name}": ${describeError(e)}`, {
            cause: e,
          });
        }
      }
      const handlerCtx = input === stepCtx.input ? stepCtx : { ...stepCtx, input };
      let result = await runWithTimeout(def.handler(handlerCtx), def.timeout);
      if (def.outputSchema) {
        try {
          const parsed = def.outputSchema.parse(result);
          if (parsed !== undefined) result = parsed;
        } catch (e) {
          throw new Error(`Output validation failed for "${def.name}": ${describeError(e)}`, {
            cause: e,
          });
        }
      }
      exec.steps[def.name] = {
        status: 'completed',
        compensatable: def.compensate !== undefined,
        result,
        startedAt: exec.steps[def.name].startedAt,
        completedAt: clock().now(),
        attempts: attempt,
        idempotencyKey: forwardKey,
        occurrence,
      };
      updateFn(exec);
      emitter?.emitStep('step:completed', exec.id, exec.workflowName, def.name, {
        result,
        attempt,
        maxAttempts,
      });
      return;
    } catch (err) {
      lastError = err instanceof Error ? err : new Error(describeError(err));
      if (attempt < maxAttempts) {
        emitter?.emitStep('step:retry', exec.id, exec.workflowName, def.name, {
          error: lastError.message,
          attempt,
          maxAttempts,
        });
        await new Promise<void>((r) => clock().setTimeout(() => r(), backoffDelay(attempt)));
        continue;
      }
    }
  }

  const finalError = lastError ?? new Error('Step failed');
  exec.steps[def.name] = {
    status: 'failed',
    compensatable: def.compensate !== undefined,
    error: String(finalError),
    startedAt: exec.steps[def.name].startedAt,
    completedAt: clock().now(),
    attempts: maxAttempts,
    idempotencyKey: forwardKey,
    occurrence,
  };
  // The step's own error is the cause; a write that fails while RECORDING it is
  // downstream, and letting it propagate instead replaced "warehouse rejected the
  // reservation" with "SQLITE_BUSY". `executor.ts` writes whatever surfaces into
  // `failureReason`, so the operator got the name of the database's problem and no trace
  // of the real one (`test/repro-workflow-step-error-masking.test.ts`).
  //
  // The same masking was fixed in `loops.ts` one frame above this, at the call sites.
  // That was not a fix of the behaviour: this writer is what every step goes through,
  // loop or not, so the property only holds once it is enforced here.
  //
  // On the success path above there is no step error to protect, so that write stays
  // unguarded and a store that cannot persist a completed step still fails loudly: a
  // record that never reached disk is one whose work runs again.
  try {
    updateFn(exec);
  } catch {
    // Deliberately swallowed: `finalError` is thrown below and carries the real cause.
  }
  emitter?.emitStep('step:failed', exec.id, exec.workflowName, def.name, {
    error: String(finalError),
    attempt: maxAttempts,
    maxAttempts,
  });
  throw finalError;
}

/** Execute multiple steps in parallel via Promise.allSettled */
export async function executeParallelSteps(
  steps: StepDefinition[],
  ctx: StepContext,
  exec: Execution,
  emitter: WorkflowEmitter | null,
  updateFn: (exec: Execution) => void
): Promise<void> {
  const results = await Promise.allSettled(
    steps.map((def) => executeStepWithRetry(def, ctx, exec, { emitter, updateFn }))
  );
  const failed = results.filter((r): r is PromiseRejectedResult => r.status === 'rejected');
  if (failed.length > 0) {
    const errors = failed.map((r) =>
      r.reason instanceof Error ? r.reason : new Error(describeError(r.reason))
    );
    throw new AggregateError(errors, errors[0].message);
  }
}

/** Execute a sub-workflow by starting it and polling for completion */
// 6 params, one over the limit. `existingChildId` is what makes re-entering this node resume
// the child a restart already started, instead of abandoning it and provisioning a second
// one, so it belongs in the signature where a caller cannot forget it rather than in an
// options bag where omitting it looks deliberate.
// biome-ignore lint/complexity/useMaxParams: see above
export async function executeSubWorkflow(
  workflowName: string,
  input: unknown,
  startFn: (name: string, input: unknown) => Promise<{ id: string }>,
  getFn: (id: string) => Execution | null,
  pollIntervalMs = 100,
  /**
   * A child this node already started, from an earlier entry into the same node.
   *
   * Without it the node started a BRAND NEW child every time it was re-entered, and
   * re-entry is routine: a restart followed by `recover()` re-enqueues the parent's
   * current node. Measured across one restart, the child ran twice and both rows were
   * left `running` forever, since a child is excluded from recovery while its parent
   * exists and `cleanup`/`archive` only reap terminal states. Duplicated work, not just
   * a leaked row: a child that provisions a resource provisioned it twice
   * (`test/repro-workflow-orphan-child.test.ts`).
   */
  existingChildId?: string
): Promise<{ results: Record<string, unknown>; executionId: string }> {
  // Resume the existing child when it is still there. A row that has been cleaned away
  // cannot be resumed, so that case starts fresh.
  const existing = existingChildId ? getFn(existingChildId) : null;
  const handle = existing ? { id: existing.id } : await startFn(workflowName, input);
  const maxWait = 300_000;
  const start = clock().now();

  while (clock().now() - start < maxWait) {
    const subExec = getFn(handle.id);
    if (subExec?.state === 'completed') {
      const results: Record<string, unknown> = {};
      for (const [name, record] of Object.entries(subExec.steps)) {
        if (record.status === 'completed') results[name] = record.result;
      }
      return { results, executionId: handle.id };
    }
    if (subExec?.state === 'failed') {
      throw new Error(`Sub-workflow "${workflowName}" (${handle.id}) failed`);
    }
    // A child that parks mid-rollback is terminal FOR THIS POLL: nothing it does next
    // happens without an operator. Waiting for it was measured at the full 300 s, after
    // which the parent reported a timeout, which is the wrong diagnostic for precisely
    // the scenario this module exists to handle, and it held a worker slot for five
    // minutes to say it. The parent parks too, with the real reason.
    if (subExec?.state === 'compensation-stuck') {
      throw new Error(
        `Sub-workflow "${workflowName}" (${handle.id}) is parked mid-rollback ` +
          `(compensation-stuck); resolve it with resumeCompensation or abandonCompensation`
      );
    }
    await new Promise<void>((r) => clock().setTimeout(() => r(), pollIntervalMs));
  }
  throw new Error(`Sub-workflow "${workflowName}" (${handle.id}) timed out`);
}

/** Find a step definition by name across all node types */
export function findStepDef(wf: Workflow, name: string): StepDefinition | null {
  for (const node of wf.nodes) {
    if (node.type === 'step' && node.def.name === name) return node.def;
    if (node.type === 'branch') {
      for (const steps of node.def.paths.values()) {
        const found = steps.find((s) => s.name === name);
        if (found) return found;
      }
    }
    if (node.type === 'parallel') {
      const found = node.def.steps.find((s) => s.name === name);
      if (found) return found;
    }
    if (node.type === 'doUntil' || node.type === 'doWhile') {
      const found = node.def.steps.find((s) => s.name === name);
      if (found) return found;
    }
    if (node.type === 'forEach') {
      if (node.def.step.name === name) return node.def.step;
    }
  }
  // Only now consider iteration records. Two passes, not one: a loop body named
  // `charge` and a separate step named `charge:extra` both "start with charge:", so a
  // single prefix pass resolved `charge:extra` to the LOOP's definition. Its own
  // rollback never ran and the loop's ran twice. Exact names win outright, and an
  // iteration suffix must be numeric — the only suffix this engine generates.
  for (const node of wf.nodes) {
    if (node.type === 'doUntil' || node.type === 'doWhile') {
      const found = node.def.steps.find((s) => isIterationOf(name, s.name));
      if (found) return found;
    }
    if (node.type === 'forEach' && isIterationOf(name, node.def.step.name)) {
      return node.def.step;
    }
  }
  return null;
}

/** Build a StepContext from the current execution state */
export function buildContext(exec: Execution): StepContext {
  const stepResults: Record<string, unknown> = {};
  for (const [name, record] of Object.entries(exec.steps)) {
    if (record.status === 'completed') stepResults[name] = record.result;
  }
  return {
    input: exec.input,
    steps: stepResults,
    signals: exec.signals,
    executionId: exec.id,
  };
}
