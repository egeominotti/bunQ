/**
 * Operator actions on a parked unwind.
 *
 * A `compensation-stuck` run is waiting for a human decision, which is why the state
 * is not terminal: the engine has done everything it safely can, and the remaining
 * choice, was the reversal fixed or do we accept a partial rollback, is not one it can
 * make. Both exits are explicit, and both leave the execution record saying exactly
 * what happened.
 */

import type { Execution } from './types';
import type { Workflow } from './workflow';
import type { WorkflowStore } from './store';
import type { WorkflowEmitter } from './emitter';
import { abandonCompensation, runCompensation, type UnwindOutcome } from './compensator';

export interface RollbackDeps {
  store: WorkflowStore;
  emitter: WorkflowEmitter | null;
  workflows: Map<string, Workflow>;
}

/**
 * Retry the compensation that parked the run, then carry on with the rest.
 *
 * The retry is asked for with a FLAG rather than by clearing the failed outcome first.
 * Clearing worked, and it cost more than it bought: it wiped the operator's
 * `compensation-failed` record in memory and persisted that wipe BEFORE running
 * anything, so a resume that then met a failing store left a run whose durable row had
 * lost the very diagnostic the operator was acting on, sitting in `compensating` and
 * therefore handed back by `listRecoverable()` at every subsequent startup. Guarding
 * that needed a deep snapshot, a restore path, and a second write which could itself
 * throw and mask the original error.
 *
 * None of that machinery is needed if nothing is destroyed in the first place. The
 * record stays until a real outcome replaces it, and a store that fails simply leaves
 * the parked run exactly as the operator found it.
 */
export async function resumeCompensation(deps: RollbackDeps, executionId: string): Promise<void> {
  const { exec, wf } = parked(deps, executionId);

  const outcome: UnwindOutcome = await runCompensation(
    exec,
    wf,
    deps.store,
    deps.emitter,
    deps.workflows,
    { retryFailed: true }
  );

  if (outcome !== 'ran') {
    // Nothing was mutated: `runCompensation` returns this before the unwind begins.
    throw new Error(`execution "${executionId}" is already being rolled back by another driver`);
  }
}

/** Accept a partial rollback: the outstanding steps are recorded as skipped. */
export function abandonParkedCompensation(deps: RollbackDeps, executionId: string): void {
  const { exec, wf } = parked(deps, executionId);
  abandonCompensation(exec, wf, deps.store, deps.emitter);
}

function parked(deps: RollbackDeps, executionId: string): { exec: Execution; wf: Workflow } {
  const exec = deps.store.get(executionId);
  if (!exec) throw new Error(`Execution "${executionId}" not found`);
  if (exec.state !== 'compensation-stuck') {
    throw new Error(
      `Execution "${executionId}" is "${exec.state}", not a parked unwind ("compensation-stuck")`
    );
  }
  const wf = deps.workflows.get(exec.workflowName);
  if (!wf) throw new Error(`Workflow "${exec.workflowName}" not registered`);
  return { exec, wf };
}
