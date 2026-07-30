/**
 * WorkflowRunner - Step execution with retry, parallel execution, sub-workflow dispatch
 */

import type { StepDefinition, StepContext, Execution } from './types';
import { idempotencyKey, isIterationOf, describeError } from './identity';
import type { Workflow } from './workflow';
import type { WorkflowEmitter } from './emitter';
import { clock } from './clock';
import { retryBackoffDelay, runWithTimeout } from './runnerTiming';

export { runWithTimeout } from './runnerTiming';
export { executeSubWorkflow } from './subWorkflowRunner';

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
  const current = exec.steps[def.name];
  const previous = current && (current.occurrence ?? 0) === occurrence ? current : undefined;
  const attemptsUsed = previous?.attempts ?? 0;
  if (previous?.status === 'completed') return;
  if (previous?.status === 'failed' && attemptsUsed >= maxAttempts) {
    throw new Error(previous.error ?? `Step "${def.name}" exhausted its retry budget`);
  }
  let lastError: Error | undefined;
  // Derived once, outside the retry loop, and persisted with the START record: a
  // rollback for a step whose outcome is unknown needs this key to reconcile, and by
  // then the body may never have reached the point of writing anything.
  const forwardKey =
    previous?.idempotencyKey ?? idempotencyKey(exec.id, def.name, occurrence, 'forward');
  const stepCtx: StepContext = { ...ctx, idempotencyKey: forwardKey };
  let validatedInput = stepCtx.input;
  let inputValidationError: Error | undefined;
  let inputParsed = false;
  let finalAttempt = attemptsUsed;

  for (let attempt = attemptsUsed + 1; attempt <= maxAttempts; attempt++) {
    finalAttempt = attempt;
    exec.steps[def.name] = {
      status: 'running',
      startedAt: previous?.startedAt ?? clock().now(),
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
      if (!inputParsed) {
        inputParsed = true;
        try {
          // Input is immutable for one step occurrence. Parse once so coercion is
          // stable and handler retries do not repeat user schema side effects.
          const parsed = def.inputSchema?.parse(stepCtx.input);
          if (parsed !== undefined) validatedInput = parsed;
        } catch (error) {
          inputValidationError = new Error(
            `Input validation failed for "${def.name}": ${describeError(error)}`,
            { cause: error }
          );
        }
      }
      if (inputValidationError) throw inputValidationError;
      const controller = new AbortController();
      const handlerCtx: StepContext = {
        ...stepCtx,
        input: validatedInput,
        signal: controller.signal,
      };
      let result = await runWithTimeout(def.handler(handlerCtx), def.timeout, controller);
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
        await new Promise<void>((r) => clock().setTimeout(() => r(), retryBackoffDelay(attempt)));
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
    attempts: finalAttempt,
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
    attempt: finalAttempt,
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
