/**
 * Recovery logic for orphaned workflow executions after crash/restart
 *
 * Handles three states:
 * - 'running': re-enqueue the step at currentNodeIndex
 * - 'waiting': re-arm timeout timer or resume if signal already arrived
 * - 'compensating': re-run compensation from the beginning (must be idempotent)
 */

import type { Queue } from '../queue/queue';
import type { Workflow } from './workflow';
import type { WorkflowStore } from './store';
import type { WorkflowEmitter } from './emitter';
import type { Execution, StepJobData, RecoverResult, WorkflowNode } from './types';
import { runCompensation } from './compensator';
import { hasSignal } from './storeSignals';
import { clock, type TimerHandle } from './clock';
import { decideAdmission } from './admission';

export interface RecoverDeps {
  store: WorkflowStore;
  queue: Queue;
  workflows: Map<string, Workflow>;
  emitter: WorkflowEmitter | null;
  timeoutTimers: Map<string, TimerHandle>;
  scheduleTimeoutCheck: (id: string, wfName: string, nodeIdx: number, ms: number) => void;
  /**
   * Nodes this engine is executing right now. Recovery consults it for the same reason
   * the `compensating` branch consults the unwind claim: re-enqueueing a node that is
   * already in flight produces a job the admission check will reject, and counting it
   * as a recovery reports work that did not happen. A step running without a bound is
   * the ordinary way to reach this.
   */
  nodesInFlight: ReadonlySet<string>;
}

export async function recoverExecutions(deps: RecoverDeps): Promise<RecoverResult> {
  const { store, workflows } = deps;
  const executions = store.listRecoverable();
  const result: RecoverResult = { running: 0, waiting: 0, compensating: 0, total: 0 };

  for (const snapshot of executions) {
    const wf = workflows.get(snapshot.workflowName);
    if (!wf) continue;

    // Re-read before driving it. The list above is a single snapshot taken before the
    // loop, and each iteration awaits: driving a parent runs its sub-workflow's unwind
    // and settles the CHILD's records in the store, but this loop still holds the
    // child's pre-unwind copy. Using that copy, every `if (record.compensation)`
    // guard reads an unsettled snapshot and dispatches the same handlers a second
    // time, issuing a duplicate refund and leaving no trace: both rows end `failed`
    // with `rollbackStatus: 'completed'`. The in-flight `inFlight` claim does not
    // help here, because the two unwinds are sequential, not concurrent.
    const exec = store.get(snapshot.id);
    if (!exec) continue;

    if (exec.state === 'running') {
      const admission = decideAdmission(exec, exec.currentNodeIndex, deps.nodesInFlight);
      // Only `already-in-flight` is reachable here: the state is `running` by the
      // branch and the index is the execution's own cursor.
      if (admission.kind === 'run') {
        await enqueueExecution(exec, deps.queue);
        result.running++;
      }
    } else if (exec.state === 'waiting') {
      await recoverWaiting(exec, wf, deps);
      result.waiting++;
    } else if (exec.state === 'compensating') {
      // A lost claim means another driver owns this unwind, so nothing happened here
      // and counting it as a recovery would overstate what recover() did.
      const outcome = await runCompensation(exec, wf, store, deps.emitter, deps.workflows);
      if (outcome === 'ran') result.compensating++;
    }
  }

  result.total = result.running + result.waiting + result.compensating;
  return result;
}

async function enqueueExecution(exec: Execution, queue: Queue): Promise<void> {
  const jobData: StepJobData = {
    executionId: exec.id,
    workflowName: exec.workflowName,
    nodeIndex: exec.currentNodeIndex,
  };
  // No deterministic jobId, for the reason documented in WorkflowExecutor.enqueue:
  // the duplicate this path can create is neutralised by the cursor guard in
  // processStep, and dedup here risks swallowing a legitimate re-enqueue after a
  // restart, which wedges the run for good.
  await queue.add('wf:step', jobData);
}

async function recoverWaiting(exec: Execution, wf: Workflow, deps: RecoverDeps): Promise<void> {
  const node = wf.nodes[exec.currentNodeIndex] as WorkflowNode | undefined;
  if (node?.type !== 'waitFor') {
    await enqueueExecution(exec, deps.queue);
    return;
  }

  // Check if signal already arrived while we were down. Key presence, not value:
  // a payload-less signal records the key with an `undefined` value (see hasSignal).
  if (hasSignal(exec.signals, node.event)) {
    exec.state = 'running';
    deps.store.update(exec);
    await enqueueExecution(exec, deps.queue);
    return;
  }

  // Re-arm timeout if configured
  if (node.timeout !== undefined) {
    if (deps.timeoutTimers.has(exec.id)) return;

    const waitKey = `__waitFor:${node.event}`;
    const waitRecord = exec.steps[waitKey] as { startedAt?: number } | undefined;
    const waitingSince = waitRecord?.startedAt ?? exec.updatedAt;
    const elapsed = clock().now() - waitingSince;
    const remaining = node.timeout - elapsed;

    if (remaining <= 0) {
      exec.state = 'running';
      deps.store.update(exec);
      await enqueueExecution(exec, deps.queue);
    } else {
      deps.scheduleTimeoutCheck(exec.id, exec.workflowName, exec.currentNodeIndex, remaining);
    }
  }
}
