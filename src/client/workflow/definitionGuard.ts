/** Runtime guard binding persisted executions to one sealed workflow graph. */

import type { Execution } from './types';
import type { Workflow } from './workflow';

export class WorkflowDefinitionMismatchError extends Error {
  constructor(executionId: string, workflowName: string) {
    super(
      `Workflow definition mismatch for active execution "${executionId}" of "${workflowName}"`
    );
    this.name = 'WorkflowDefinitionMismatchError';
  }
}

/**
 * Legacy rows have no hash; bind them once to the currently registered graph. New rows
 * always arrive pre-bound by startExecution.
 */
export function bindExecutionDefinition(
  exec: Execution,
  workflow: Workflow,
  update: (exec: Execution) => void
): void {
  const registeredHash = workflow.seal();
  if (exec.definitionHash === undefined) {
    exec.definitionHash = registeredHash;
    update(exec);
    return;
  }
  if (exec.definitionHash !== registeredHash) {
    throw new WorkflowDefinitionMismatchError(exec.id, exec.workflowName);
  }
}
