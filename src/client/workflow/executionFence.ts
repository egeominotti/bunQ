/** Lifecycle fence shared by every asynchronous workflow execution path. */

/** Internal control-flow signal used to abandon work owned by a closed Engine. */
export class WorkflowExecutionClosedError extends Error {
  constructor() {
    super('Workflow execution belongs to a force-closed Engine');
    this.name = 'WorkflowExecutionClosedError';
  }
}

export function isWorkflowExecutionClosed(error: unknown): boolean {
  return (
    error instanceof WorkflowExecutionClosedError ||
    (error instanceof AggregateError && error.errors.some(isWorkflowExecutionClosed))
  );
}

/** A graceful close leaves this active; only close(true) aborts in-flight control flow. */
export class WorkflowExecutionFence {
  private readonly controller = new AbortController();

  readonly assertActive = (): void => {
    if (this.controller.signal.aborted) throw new WorkflowExecutionClosedError();
  };

  isActive(): boolean {
    return !this.controller.signal.aborted;
  }

  close(force: boolean): void {
    if (force) this.controller.abort();
  }
}

/** Default for isolated runner tests that do not own an Engine lifecycle. */
export const assertWorkflowActive = (): void => undefined;
