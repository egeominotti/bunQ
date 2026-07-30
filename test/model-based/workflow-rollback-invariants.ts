/**
 * Rollback completeness, dispatch and state-axis invariants.
 */

import type { Execution, StepRecord } from '../../src/client/workflow';
import type { InvariantFail } from './workflow-invariants';
import {
  compensationBehavior,
  executionSpec,
  isIterationMirror,
  pivotAt,
} from './workflow-spec-analysis';
import type { Ledger, LedgerCall, WorkflowSpec } from './workflow-spec';

export function checkCompensationDispatches(
  parentSpec: WorkflowSpec,
  ledger: Ledger,
  executions: Execution[],
  fail: InvariantFail
): void {
  const groups = new Map<string, LedgerCall[]>();
  for (const call of ledger.compensations) {
    const key = `${call.executionId}/${call.name}#${call.occurrence}`;
    const group = groups.get(key) ?? [];
    group.push(call);
    groups.set(key, group);
  }
  for (const [identity, calls] of groups) {
    const first = calls[0];
    if (!first) continue;
    const exec = executions.find((candidate) => candidate.id === first.executionId);
    const spec = exec ? executionSpec(parentSpec, exec) : null;
    if (!exec || !spec) {
      fail('I7 compensation sanity', `handler dispatched for unknown ${identity}`);
    }
    const behavior = compensationBehavior(spec, first.name);
    if (behavior === 'none') {
      fail('I7 compensation sanity', `${identity} dispatched without a declared handler`);
    }
    const successes = calls.filter((call) => call.outcome === 'completed').length;
    if (successes > 1) {
      fail('I7 compensation exactly-once', `${identity} completed ${successes} times`);
    }
    const resumes = ledger.operatorActions.filter(
      (action) =>
        action.kind === 'resume' &&
        actionAffectsExecution(action.executionId, first.executionId, executions)
    ).length;
    const maxCalls = behavior === 'ok' ? 1 : behavior === 'fail-once' ? 2 : 1 + resumes;
    if (calls.length > maxCalls) {
      fail(
        'I7 compensation dispatch bound',
        `${identity} dispatched ${calls.length} times; behavior ${behavior} allows ${maxCalls}`
      );
    }
  }
}

function actionAffectsExecution(
  targetId: string,
  executionId: string,
  executions: Execution[]
): boolean {
  let current = executions.find((exec) => exec.id === executionId);
  while (current) {
    if (current.id === targetId) return true;
    const parentId = current.parentExecutionId;
    current = parentId ? executions.find((exec) => exec.id === parentId) : undefined;
  }
  return false;
}

export function checkRollbackCoherence(
  spec: WorkflowSpec,
  ledger: Ledger,
  exec: Execution,
  fail: InvariantFail
): void {
  const eligible = eligibleRecords(spec, exec);
  const outcomes = Object.values(exec.steps).flatMap((record) =>
    record.compensation ? [record.compensation.status] : []
  );

  if (exec.committedAt !== undefined) checkPivot(spec, ledger, exec, fail);

  if (exec.state === 'running' || exec.state === 'waiting' || exec.state === 'completed') {
    if (exec.rollbackStatus !== undefined) {
      fail(
        'I10 state/rollback coherence',
        `${exec.id} is ${exec.state} with rollbackStatus ${exec.rollbackStatus}`
      );
    }
    if (outcomes.length > 0) {
      fail('I10 state/rollback coherence', `${exec.id} is ${exec.state} with rollback outcomes`);
    }
  }

  if (exec.state === 'compensation-stuck' && exec.rollbackStatus !== 'stuck') {
    fail(
      'I10 state/rollback coherence',
      `${exec.id} is compensation-stuck with rollbackStatus ${String(exec.rollbackStatus)}`
    );
  }
  if (exec.state === 'failed' && exec.rollbackStatus === undefined) {
    fail('I10 state/rollback coherence', `${exec.id} is terminal failed with rollback still owed`);
  }
  if (
    (exec.state === 'failed' ||
      exec.state === 'compensating' ||
      exec.state === 'compensation-stuck') &&
    !exec.failureReason &&
    !exec.parentExecutionId
  ) {
    fail('I10 state/rollback coherence', `${exec.id} has failure state without failureReason`);
  }

  if (exec.rollbackStatus === 'completed') {
    for (const [name, record] of eligible) {
      if (record.compensation?.status !== 'compensated') {
        fail(
          'I11 rollback completeness',
          `${exec.id}/${name} has ${String(record.compensation?.status)} under a clean rollback`
        );
      }
      if (!name.startsWith('sub:')) requireSuccessfulDispatch(ledger, exec.id, name, fail);
    }
  }

  if (exec.rollbackStatus === 'not-applicable' && eligible.length > 0) {
    fail(
      'I11 rollback completeness',
      `${exec.id} reports not-applicable with eligible records: ${eligible.map(([n]) => n).join(',')}`
    );
  }
  if (exec.rollbackStatus === 'stuck') {
    if (
      !outcomes.some(
        (status) => status === 'compensation-failed' || status === 'compensation-skipped'
      )
    ) {
      fail(
        'I10 state/rollback coherence',
        `${exec.id} reports stuck without failed/skipped outcome`
      );
    }
    if (exec.state === 'failed') {
      for (const [name, record] of eligible) {
        if (!record.compensation) {
          fail(
            'I11 rollback completeness',
            `${exec.id}/${name} has no outcome after the operator terminated the unwind`
          );
        }
      }
    }
  }
  if (exec.rollbackStatus === 'completed' && outcomes.some((status) => status !== 'compensated')) {
    fail('I11 rollback completeness', `${exec.id} clean rollback contains a non-success outcome`);
  }
}

function eligibleRecords(spec: WorkflowSpec, exec: Execution): [string, StepRecord][] {
  if (exec.committedAt !== undefined) return [];
  return Object.entries(exec.steps).filter(([name, record]) => {
    if (name.startsWith('__')) return false;
    if (record.status !== 'completed' && record.status !== 'failed') return false;
    if (isIterationMirror(spec, name, exec.steps)) return false;
    if (name.startsWith('sub:')) return record.childExecutionId !== undefined;
    return record.compensatable === true;
  });
}

function requireSuccessfulDispatch(
  ledger: Ledger,
  executionId: string,
  name: string,
  fail: InvariantFail
): void {
  const succeeded = ledger.compensations.some(
    (call) => call.executionId === executionId && call.name === name && call.outcome === 'completed'
  );
  if (!succeeded) {
    fail('I11 rollback completeness', `${executionId}/${name} has no successful handler dispatch`);
  }
}

function checkPivot(
  spec: WorkflowSpec,
  ledger: Ledger,
  exec: Execution,
  fail: InvariantFail
): void {
  const index = exec.committedAt;
  if (index === undefined || !pivotAt(spec, index) || exec.currentNodeIndex < index) {
    fail('I12 pivot coherence', `${exec.id} carries invalid committedAt ${String(index)}`);
  }
  if (ledger.compensations.some((call) => call.executionId === exec.id)) {
    fail('I12 pivot coherence', `${exec.id} dispatched compensation after crossing its pivot`);
  }
  if (Object.values(exec.steps).some((record) => record.compensation !== undefined)) {
    fail('I12 pivot coherence', `${exec.id} persisted a compensation outcome after its pivot`);
  }
  if (exec.state === 'failed' && exec.rollbackStatus !== 'not-applicable') {
    fail(
      'I12 pivot coherence',
      `${exec.id} failed past pivot with rollbackStatus ${String(exec.rollbackStatus)}`
    );
  }
}
