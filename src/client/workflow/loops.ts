/**
 * Loop & Map execution logic for the Workflow Engine
 * Handles doUntil, doWhile, forEach, and map node types.
 */

import type {
  StepDefinition,
  StepContext,
  Execution,
  LoopDefinition,
  ForEachDefinition,
  MapDefinition,
} from './types';
import type { WorkflowEmitter } from './emitter';
import { executeStepWithRetry, buildContext } from './runner';

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
      const ctx = buildContext(exec);
      await executeStepWithRetry(step, ctx, exec, emitter, updateFn);
    }
    iteration++;
    const ctx = buildContext(exec);
    shouldStop = await def.condition(ctx, iteration);
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
    const shouldContinue = await def.condition(ctx, iteration);
    if (!shouldContinue) break;

    if (iteration >= def.maxIterations) {
      throw new Error(`doWhile exceeded maxIterations (${def.maxIterations})`);
    }
    for (const step of def.steps) {
      const stepCtx = buildContext(exec);
      await executeStepWithRetry(step, stepCtx, exec, emitter, updateFn);
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
  const items = def.items(ctx);

  if (items.length > def.maxIterations) {
    throw new Error(`forEach items (${items.length}) exceeds maxIterations (${def.maxIterations})`);
  }

  for (let i = 0; i < items.length; i++) {
    const item = items[i];
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
    const stepCtx = buildContext(exec);
    await executeStepWithRetry(indexedStep, stepCtx, exec, emitter, updateFn);
    // Persist this iteration's __item/__index alongside the step record so
    // saga compensation can restore the correct per-iteration context later.
    // executeStepWithRetry has just written this record (it throws otherwise).
    const record = exec.steps[indexedName];
    record.loopItem = item;
    record.loopIndex = i;
    updateFn(exec);
  }
}

/** Execute a map node: transform step results into a new value */
export async function executeMap(
  def: MapDefinition,
  exec: Execution,
  emitter: WorkflowEmitter | null,
  updateFn: (exec: Execution) => void
): Promise<void> {
  const ctx = buildContext(exec);
  emitter?.emitStep('step:started', exec.id, exec.workflowName, def.name);
  const result = await def.transform(ctx);
  exec.steps[def.name] = {
    status: 'completed',
    result,
    startedAt: Date.now(),
    completedAt: Date.now(),
  };
  updateFn(exec);
  emitter?.emitStep('step:completed', exec.id, exec.workflowName, def.name, { result });
}
