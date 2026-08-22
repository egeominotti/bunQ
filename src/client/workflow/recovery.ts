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
import type { Execution, RecoverResult, WorkflowNode } from './types';
import { runCompensation } from './compensator';
import { hasSignal } from './storeSignals';
import { clock, type TimerHandle } from './clock';
import { decideAdmission } from './admission';
import { bindExecutionDefinition } from './definitionGuard';
import { enqueueWorkflowStep } from './executorQueue';

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
  assertActive: () => void;
}

export async function recoverExecutions(deps: RecoverDeps): Promise<RecoverResult> {
  const { store, workflows } = deps;
  deps.assertActive();
  const executions = store.listRecoverable();
  const result: RecoverResult = { running: 0, waiting: 0, compensating: 0, total: 0 };

  for (const snapshot of executions) {
    deps.assertActive();
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
    bindExecutionDefinition(exec, wf, (value) => store.update(value));

    if (exec.state === 'running') {
      const admission = decideAdmission(exec, exec.currentNodeIndex, deps.nodesInFlight);
      // Only `already-in-flight` is reachable here: the state is `running` by the
      // branch and the index is the execution's own cursor.
      if (admission.kind === 'run') {
        await enqueueWorkflowStep(deps.queue, exec, deps.assertActive);
        deps.assertActive();
        result.running++;
      }
    } else if (exec.state === 'waiting') {
      await recoverWaiting(exec, wf, deps);
      deps.assertActive();
      result.waiting++;
    } else if (
      exec.state === 'compensating' ||
      (exec.state === 'failed' && exec.rollbackStatus === undefined)
    ) {
      // `runNode` persists the failure before it starts the unwind. A hard kill in
      // between leaves a terminal-looking row with rollback still owed. Such rows are
      // recoverable until runCompensation writes an explicit rollbackStatus.
      // A lost claim means another driver owns this unwind, so nothing happened here
      // and counting it as a recovery would overstate what recover() did.
      if (await recoverCompensation(exec.id, wf, deps)) {
        deps.assertActive();
        result.compensating++;
      }
    }
  }

  result.total = result.running + result.waiting + result.compensating;
  return result;
}

async function recoverCompensation(
  executionId: string,
  wf: Workflow,
  deps: RecoverDeps
): Promise<boolean> {
  while (true) {
    deps.assertActive();
    const exec = deps.store.get(executionId);
    if (
      !exec ||
      (exec.state !== 'compensating' &&
        !(exec.state === 'failed' && exec.rollbackStatus === undefined))
    ) {
      return false;
    }
    bindExecutionDefinition(exec, wf, (value) => deps.store.update(value));
    const outcome = await runCompensation(exec, wf, deps.store, deps.emitter, deps.workflows, {
      assertActive: deps.assertActive,
    });
    deps.assertActive();
    if (outcome === 'ran') return true;

    // A live unwind owned by this executor's store is not orphaned. Waiting here
    // would deadlock callers that deliberately hold the handler open until recover()
    // returns, and it would make a health-time recovery call wait for user code.
    if (outcome.sameOwner) return false;

    // A force-closed Engine can leave its JavaScript compensation handler alive
    // briefly after its store is closed. A replacement Engine has a different store
    // identity, so it waits for that owner, then re-reads the durable row: it may have
    // finished, or this engine must resume the owed unwind.
    await outcome.settled;
    deps.assertActive();
  }
}

async function recoverWaiting(exec: Execution, wf: Workflow, deps: RecoverDeps): Promise<void> {
  const node = wf.nodes[exec.currentNodeIndex] as WorkflowNode | undefined;
  if (node?.type !== 'waitFor') {
    await enqueueWorkflowStep(deps.queue, exec, deps.assertActive);
    deps.assertActive();
    return;
  }

  // The signal may have been persisted before the crash, or accepted after this
  // engine was recreated but before recover() reached the row. Key presence, not
  // value: a payload-less signal records the key with an `undefined` value.
  if (hasSignal(exec.signals, node.event)) {
    deps.assertActive();
    exec.state = 'running';
    deps.store.update(exec);
    await enqueueWorkflowStep(deps.queue, exec, deps.assertActive);
    deps.assertActive();
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
      deps.assertActive();
      exec.state = 'running';
      deps.store.update(exec);
      await enqueueWorkflowStep(deps.queue, exec, deps.assertActive);
      deps.assertActive();
    } else {
      deps.assertActive();
      deps.scheduleTimeoutCheck(exec.id, exec.workflowName, exec.currentNodeIndex, remaining);
    }
  }
}
