/** Durable forEach execution, split from conditional loop runners. */

import type { Execution, ForEachDefinition, StepContext, StepDefinition } from './types';
import type { WorkflowEmitter } from './emitter';
import { assertWorkflowActive, isWorkflowExecutionClosed } from './executionFence';
import { buildContext, executeStepWithRetry } from './runner';
import { forEachItemsDecisionKey, resolveDecision } from './workflowDecisions';

export async function executeForEach(
  def: ForEachDefinition,
  exec: Execution,
  emitter: WorkflowEmitter | null,
  updateFn: (exec: Execution) => void,
  assertActive: () => void = assertWorkflowActive
): Promise<void> {
  assertActive();
  const items = await resolveDecision(
    exec,
    forEachItemsDecisionKey(def.step.name),
    () => structuredClone(def.items(buildContext(exec))),
    updateFn,
    assertActive
  );
  assertActive();

  // Array-likes used to report success while doing the wrong amount of work: a number
  // ran zero items and a string ran one item per character.
  if (!Array.isArray(items)) {
    throw new Error(
      `forEach items must be an array, got ${items === null ? 'null' : typeof items}`
    );
  }
  if (items.length > def.maxIterations) {
    throw new Error(`forEach items (${items.length}) exceeds maxIterations (${def.maxIterations})`);
  }

  for (let i = 0; i < items.length; i++) {
    assertActive();
    const item = structuredClone(items[i]);
    const indexedName = `${def.step.name}:${i}`;
    const indexedStep: StepDefinition = {
      ...def.step,
      name: indexedName,
      handler: (stepCtx: StepContext) =>
        def.step.handler({
          ...stepCtx,
          steps: { ...stepCtx.steps, __item: item, __index: i },
        }),
    };

    // A completed item is memoised; the bare name remains the aggregate view of the
    // last iteration for loop conditions and downstream steps.
    const completed = exec.steps[indexedName];
    if (completed?.status === 'completed') {
      exec.steps[def.step.name] = { ...completed };
      updateFn(exec);
      continue;
    }

    let thrown: unknown;
    let threw = false;
    try {
      await executeStepWithRetry(
        indexedStep,
        buildContext(exec),
        exec,
        { emitter, updateFn, assertActive },
        i
      );
      assertActive();
    } catch (error) {
      if (isWorkflowExecutionClosed(error)) throw error;
      assertActive();
      thrown = error;
      threw = true;
    }

    // Persist the item identity on both success and failure. A failed provider call
    // can still need compensation, and its reversal needs the original item/index.
    const record = exec.steps[indexedName];
    if (record) {
      record.loopItem = item;
      record.loopIndex = i;
      exec.steps[def.step.name] = { ...record };
      try {
        updateFn(exec);
      } catch (writeError) {
        if (isWorkflowExecutionClosed(writeError)) throw writeError;
        if (!threw) throw writeError;
      }
    }

    if (threw) throw thrown;
  }
}
