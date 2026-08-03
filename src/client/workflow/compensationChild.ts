/** Nested-saga compensation. */

import { isLive } from './admission';
import type { LostCompensationClaim } from './compensationClaim';
import type { WorkflowEmitter } from './emitter';
import type { WorkflowStore } from './store';
import type { Execution, StepRecord } from './types';
import type { Workflow } from './workflow';

export interface ChildUnwindRequest {
  exec: Execution;
  workflow: Workflow;
  store: WorkflowStore;
  emitter: WorkflowEmitter | null;
  workflows?: Map<string, Workflow>;
  retryFailed?: boolean;
}

export type ChildUnwind = (request: ChildUnwindRequest) => Promise<'ran' | LostCompensationClaim>;

export interface UnwindChildRequest {
  record: StepRecord;
  store: WorkflowStore;
  emitter: WorkflowEmitter | null;
  workflows?: Map<string, Workflow>;
  runChild: ChildUnwind;
  retryFailed?: boolean;
}

/** Run the child workflow's own unwind and propagate every uncertain outcome. */
export async function unwindChild(request: UnwindChildRequest): Promise<void> {
  const { record, store, emitter, workflows, runChild, retryFailed } = request;
  const childId = record.childExecutionId;
  if (!childId) throw new Error('sub-workflow record carries no child execution id');
  if (!workflows) throw new Error('workflow registry unavailable to unwind a sub-workflow');

  const child = store.get(childId);
  if (!child) throw new Error(`child execution "${childId}" not found`);
  const childWf = workflows.get(child.workflowName);
  if (!childWf) throw new Error(`child workflow "${child.workflowName}" is not registered`);

  if (child.state === 'failed' && child.rollbackStatus === 'stuck') {
    throw new AbandonedCompensationError(child.workflowName, childId);
  }

  // Forward progress and rollback must never write the same child concurrently.
  if (isLive(child.state)) {
    throw new Error(
      `child "${child.workflowName}" (${childId}) is still ${child.state}; ` +
        'it cannot be rolled back until it stops'
    );
  }

  const outcome = await runChild({
    exec: child,
    workflow: childWf,
    store,
    emitter,
    workflows,
    retryFailed,
  });
  if (outcome !== 'ran') {
    throw new Error(
      `child "${child.workflowName}" (${childId}) is being rolled back by another driver; ` +
        'outcome unknown'
    );
  }

  // The recursive unwind may have crossed an async boundary or been driven by
  // another engine. Interpret the authoritative row, not the snapshot passed in.
  const settledChild = store.get(childId);
  if (!settledChild) throw new Error(`child execution "${childId}" disappeared during rollback`);
  if (settledChild.state === 'failed' && settledChild.rollbackStatus === 'stuck') {
    throw new AbandonedCompensationError(settledChild.workflowName, childId);
  }
  if (settledChild.rollbackStatus === 'stuck') {
    throw new Error(`child "${settledChild.workflowName}" (${childId}) parked mid-rollback`);
  }
  if (settledChild.rollbackStatus === 'not-applicable' && settledChild.committedAt !== undefined) {
    throw new CommittedChildError(settledChild.workflowName, childId);
  }
}

export class AbandonedCompensationError extends Error {
  constructor(workflowName: string, executionId: string) {
    super(
      `child "${workflowName}" (${executionId}) has an explicitly abandoned rollback; ` +
        'it is terminal and cannot be resumed through an ancestor'
    );
    this.name = 'AbandonedCompensationError';
  }
}

export class CommittedChildError extends Error {
  constructor(workflowName: string, executionId: string) {
    super(
      `child "${workflowName}" (${executionId}) is committed past its pivot; ` +
        'nothing was rolled back'
    );
    this.name = 'CommittedChildError';
  }
}
