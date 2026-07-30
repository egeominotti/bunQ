import { clock } from './clock';
import type { WorkflowEmitter } from './emitter';
import { describeError } from './identity';
import { buildContext } from './runner';
import type { Execution, MapDefinition } from './types';

/** Execute a map node with the same durable lifecycle signals as a regular step. */
export async function executeMap(
  def: MapDefinition,
  exec: Execution,
  emitter: WorkflowEmitter | null,
  updateFn: (exec: Execution) => void
): Promise<void> {
  if (exec.steps[def.name]?.status === 'completed') return;

  const startedAt = clock().now();
  exec.steps[def.name] = { status: 'running', startedAt };
  updateFn(exec);
  emitter?.emitStep('step:started', exec.id, exec.workflowName, def.name);

  let result: unknown;
  try {
    result = await def.transform(buildContext(exec));
  } catch (error) {
    const failure = error instanceof Error ? error : new Error(describeError(error));
    exec.steps[def.name] = {
      status: 'failed',
      error: describeError(failure),
      startedAt,
      completedAt: clock().now(),
    };
    try {
      updateFn(exec);
    } catch {
      // Preserve the transform failure; generic workflow failure handling retries the write.
    }
    emitter?.emitStep('step:failed', exec.id, exec.workflowName, def.name, {
      error: describeError(failure),
    });
    throw failure;
  }

  // A completed transform whose write fails must stay completed in memory. Reclassifying
  // that infrastructure failure as a transform failure would discard a valid result.
  exec.steps[def.name] = {
    status: 'completed',
    result,
    startedAt,
    completedAt: clock().now(),
  };
  updateFn(exec);
  emitter?.emitStep('step:completed', exec.id, exec.workflowName, def.name, { result });
}
