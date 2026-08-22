/**
 * Public saga-compensation entry points.
 *
 * A pass is checkpointed per step, halts on an uncertain reversal, and is protected
 * from overlapping in-process drivers. The execution mechanics live in
 * compensationPass.ts; record selection lives in compensationSupport.ts.
 */

import { unwind } from './compensationPass';
import { AbandonedCompensationError } from './compensationChild';
import { acquireCompensationClaim } from './compensationClaim';
import type { LostCompensationClaim } from './compensationClaim';
import { settle, unwindSet } from './compensationSupport';
import type { WorkflowEmitter } from './emitter';
import { assertWorkflowActive } from './executionFence';
import type { WorkflowStore } from './store';
import type { Execution } from './types';
import { owesOutcome } from './unwindPlan';
import type { Workflow } from './workflow';

export { AbandonedCompensationError, CommittedChildError } from './compensationChild';

/** Sentinel thrown when an execution deliberately parks for a signal. */
export class WaitForSignalError extends Error {
  constructor(readonly event: string) {
    super(`Waiting for signal: ${event}`);
  }
}

/**
 * This is not a distributed lock. It prevents two local recovery/operator paths from
 * reading the same unsettled snapshot and issuing the same reversal twice. A losing
 * recovery path receives the current owner's completion latch and can retry safely.
 */
export type UnwindOutcome = 'ran' | LostCompensationClaim;

/** Run compensation handlers in reverse start order for every eligible step. */
// oxlint-disable-next-line max-params -- registry and operator retry are independent inputs
export async function runCompensation(
  exec: Execution,
  wf: Workflow,
  store: WorkflowStore,
  emitter: WorkflowEmitter | null,
  workflows?: Map<string, Workflow>,
  opts: { retryFailed?: boolean; assertActive?: () => void } = {}
): Promise<UnwindOutcome> {
  const assertActive = opts.assertActive ?? assertWorkflowActive;
  assertActive();
  // `failed` + `stuck` is the durable terminal form written by an explicit
  // abandon. No caller, including an ancestor retry, may reopen that decision.
  if (exec.state === 'failed' && exec.rollbackStatus === 'stuck') {
    throw new AbandonedCompensationError(exec.workflowName, exec.id);
  }

  const eligible = unwindSet(exec, wf);

  if (eligible.length === 0) {
    assertActive();
    exec.rollbackStatus = 'not-applicable';
    if (exec.state !== 'failed' && exec.state !== 'completed') exec.state = 'failed';
    store.update(exec);
    return 'ran';
  }

  assertActive();
  const claim = acquireCompensationClaim(exec.id, store);
  if (claim.kind === 'claim-lost') return claim;
  try {
    await unwind(
      { exec, wf, store, emitter, eligible, workflows, ...opts, assertActive },
      ({ exec, workflow, store, emitter, workflows, retryFailed, assertActive }) =>
        runCompensation(exec, workflow, store, emitter, workflows, { retryFailed, assertActive })
    );
  } finally {
    claim.release();
  }
  return 'ran';
}

/**
 * Give up on a parked unwind and record an explicit skipped outcome for every
 * remaining eligible step.
 */
export function abandonCompensation(
  exec: Execution,
  wf: Workflow,
  store: WorkflowStore,
  emitter: WorkflowEmitter | null,
  assertActive: () => void = assertWorkflowActive
): void {
  for (const [name, record] of unwindSet(exec, wf)) {
    assertActive();
    if (record.compensation) continue;
    if (!owesOutcome(wf, name, record)) continue;
    const reason = 'unwind abandoned by operator';
    settle(record, 'compensation-skipped', reason);
    emitter?.emitStep('compensation:skipped', exec.id, exec.workflowName, name, { error: reason });
    assertActive();
  }
  assertActive();
  exec.state = 'failed';
  exec.rollbackStatus = 'stuck';
  store.update(exec);
}
