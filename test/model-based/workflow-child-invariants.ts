/**
 * Parent/child ownership invariants for generated sub-workflows.
 */

import type { Execution, StepRecord } from '../../src/client/workflow';
import type { InvariantFail } from './workflow-invariants';
import { childName, expectedChildNames } from './workflow-spec-analysis';
import type { WorkflowSpec } from './workflow-spec';

export function checkChildConsistency(
  spec: WorkflowSpec,
  parent: Execution,
  executions: Execution[],
  fail: InvariantFail
): void {
  const children = executions.filter((exec) => exec.parentExecutionId === parent.id);
  const expected = expectedChildNames(spec);
  const references = parentReferences(parent);

  for (const reference of references) {
    const child = children.find((candidate) => candidate.id === reference.childId);
    if (!child) {
      fail(
        'I8 child ownership',
        `${parent.id}/${reference.recordName} points to missing child ${reference.childId}`
      );
    }
    if (child.parentExecutionId !== parent.id) {
      fail('I8 child ownership', `${child.id} points to parent ${child.parentExecutionId}`);
    }
    if (reference.recordName !== `sub:${child.workflowName}`) {
      fail(
        'I8 child ownership',
        `${reference.recordName} points to workflow ${child.workflowName}`
      );
    }
    checkSettledReference(parent, child, reference.record, fail);
  }

  for (const child of children) {
    if (!expected.has(child.workflowName)) {
      fail('I8 child ownership', `${parent.id} owns unexpected workflow ${child.workflowName}`);
    }
    const refs = references.filter((reference) => reference.childId === child.id);
    if (refs.length === 0 && !inClaimWindow(spec, parent, child)) {
      fail('I8 child ownership', `${child.id} is not claimed by any parent step record`);
    }
    if (refs.length > 1) {
      fail('I8 child ownership', `${child.id} is claimed by ${refs.length} parent records`);
    }
  }

  for (const workflowName of expected) {
    const sameNode = children.filter((child) => child.workflowName === workflowName);
    if (sameNode.length > 1) {
      fail(
        'I8 child ownership',
        `${parent.id} started ${sameNode.length} children for sub:${workflowName}`
      );
    }
  }
}

interface ParentReference {
  recordName: string;
  childId: string;
  record: StepRecord;
}

function parentReferences(parent: Execution): ParentReference[] {
  return Object.entries(parent.steps).flatMap(([recordName, record]) =>
    recordName.startsWith('sub:') && record.childExecutionId
      ? [{ recordName, childId: record.childExecutionId, record }]
      : []
  );
}

function inClaimWindow(spec: WorkflowSpec, parent: Execution, child: Execution): boolean {
  if (parent.state !== 'running') return false;
  const node = spec.nodes[parent.currentNodeIndex];
  return node?.kind === 'subWorkflow' && child.workflowName === childName(node.step.name);
}

function checkSettledReference(
  parent: Execution,
  child: Execution,
  record: StepRecord,
  fail: InvariantFail
): void {
  if ((parent.state === 'completed' || parent.state === 'failed') && record.status === 'running') {
    fail('I8 child ownership', `${parent.id} is terminal with running child record ${child.id}`);
  }
  if (record.compensation?.status === 'compensated') {
    if (
      child.state !== 'failed' ||
      (child.rollbackStatus !== 'completed' && child.rollbackStatus !== 'not-applicable')
    ) {
      fail(
        'I8 child ownership',
        `${child.id} does not reflect parent record's successful compensation`
      );
    }
  }
  if (
    parent.state === 'completed' &&
    record.status === 'completed' &&
    child.state !== 'completed'
  ) {
    fail('I8 child ownership', `${parent.id} completed over non-completed child ${child.id}`);
  }
}
