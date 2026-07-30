/**
 * Cross-cutting invariants checked after every generated operator action.
 */

import type { Execution } from '../../src/client/workflow';
import { checkChildConsistency } from './workflow-child-invariants';
import {
  checkCompensationDispatches,
  checkRollbackCoherence,
} from './workflow-rollback-invariants';
import {
  declaredNames,
  executionSpec,
  retryBudget,
  selectedBranchMissing,
  stepsGatedBy,
} from './workflow-spec-analysis';
import type { Ledger, WorkflowSpec } from './workflow-spec';

export interface InvariantState {
  spec: WorkflowSpec;
  ledger: Ledger;
  /** Signals accepted by engine.signal(), in delivery order. */
  delivered: Map<string, unknown>;
  maxNodeIndex: Map<string, number>;
  terminalStates: Map<string, string>;
}

export type InvariantFail = (id: string, detail: string) => never;
export class InvariantViolation extends Error {}

const TERMINAL = new Set(['completed', 'failed']);

export function checkInvariants(
  state: InvariantState,
  parent: Execution | null,
  executions: Execution[] = parent ? [parent] : []
): void {
  if (!parent) return;
  const fail: InvariantFail = (id, detail) => {
    throw new InvariantViolation(
      `${id} violated: ${detail}\nspec=${JSON.stringify(state.spec)}\n` +
        `ledger=${JSON.stringify(state.ledger)}\nexecutions=${JSON.stringify(
          executions.map(executionSummary)
        )}`
    );
  };

  checkSignalFidelity(state, parent, fail);
  checkGateOrdering(state, fail);
  checkCompensationDispatches(state.spec, state.ledger, executions, fail);

  for (const exec of executions) {
    const spec = executionSpec(state.spec, exec);
    if (!spec) {
      fail(
        'I8 child ownership',
        `related execution "${exec.id}" has unknown workflow "${exec.workflowName}"`
      );
    }
    checkProgress(state, exec, fail);
    checkTerminalState(state, exec, fail);
    checkNameDomain(spec, exec, fail);
    checkRetryBudget(spec, state.ledger, exec, fail);
    checkMapLifecycle(spec, state.ledger, exec, fail);
    checkBranchTotality(spec, exec, fail);
    checkRollbackCoherence(spec, state.ledger, exec, fail);
  }
  checkChildConsistency(state.spec, parent, executions, fail);
}

function checkSignalFidelity(state: InvariantState, exec: Execution, fail: InvariantFail): void {
  for (const [event, payload] of state.delivered) {
    if (!Object.hasOwn(exec.signals, event)) {
      fail('I1 signal fidelity', `accepted signal "${event}" is absent from exec.signals`);
    }
    if (JSON.stringify(exec.signals[event]) !== JSON.stringify(payload)) {
      fail(
        'I1 signal fidelity',
        `signal "${event}" changed: expected ${JSON.stringify(payload)}, got ${JSON.stringify(exec.signals[event])}`
      );
    }
  }
}

function checkGateOrdering(state: InvariantState, fail: InvariantFail): void {
  const gated = stepsGatedBy(state.spec);
  for (const call of state.ledger.steps) {
    const event = gated.get(call.name);
    if (event !== undefined && !state.delivered.has(event)) {
      fail(
        'I4 waitFor gating',
        `step "${call.name}" ran before gating signal "${event}" was accepted`
      );
    }
  }
}

function checkProgress(state: InvariantState, exec: Execution, fail: InvariantFail): void {
  const previous = state.maxNodeIndex.get(exec.id) ?? 0;
  if (exec.currentNodeIndex < previous) {
    fail('I2 monotonic progress', `${exec.id} cursor went ${previous} -> ${exec.currentNodeIndex}`);
  }
  state.maxNodeIndex.set(exec.id, Math.max(previous, exec.currentNodeIndex));
}

function checkTerminalState(state: InvariantState, exec: Execution, fail: InvariantFail): void {
  const previous = state.terminalStates.get(exec.id);
  if (previous === 'failed' && exec.state !== 'failed') {
    fail('I3 no resurrection', `${exec.id} left failed for ${exec.state}`);
  }
  if (previous === 'completed' && !exec.parentExecutionId && exec.state !== 'completed') {
    fail('I3 no resurrection', `${exec.id} left completed for ${exec.state}`);
  }
  if (
    previous === 'completed' &&
    exec.parentExecutionId &&
    !['completed', 'compensating', 'compensation-stuck', 'failed'].includes(exec.state)
  ) {
    fail('I3 no resurrection', `completed child ${exec.id} moved forward to ${exec.state}`);
  }
  if (TERMINAL.has(exec.state)) state.terminalStates.set(exec.id, exec.state);
}

function checkNameDomain(spec: WorkflowSpec, exec: Execution, fail: InvariantFail): void {
  const allowed = declaredNames(spec);
  for (const name of Object.keys(exec.steps)) {
    if (!allowed.has(name)) {
      fail('I6 name domain', `${exec.id} contains undeclared durable key "${name}"`);
    }
  }
}

function checkRetryBudget(
  spec: WorkflowSpec,
  ledger: Ledger,
  exec: Execution,
  fail: InvariantFail
): void {
  const groups = new Map<string, typeof ledger.steps>();
  for (const call of ledger.steps) {
    if (call.executionId !== exec.id) continue;
    const key = `${call.name}#${call.occurrence}`;
    const group = groups.get(key) ?? [];
    group.push(call);
    groups.set(key, group);
  }
  for (const [identity, calls] of groups) {
    const first = calls[0];
    if (!first) continue;
    const budget = retryBudget(spec, first.name);
    if (budget === undefined) {
      fail('I5 retry bound', `${exec.id} dispatched undeclared handler "${first.name}"`);
    }
    if (calls.length > budget) {
      fail(
        'I5 retry bound',
        `${exec.id}/${identity} dispatched ${calls.length} times, cumulative budget ${budget}`
      );
    }
    const keys = new Set(calls.map((call) => call.idempotencyKey));
    if (keys.size > 1) {
      fail('I5 retry identity', `${exec.id}/${identity} changed idempotency key across retries`);
    }
  }
  for (const [name, record] of Object.entries(exec.steps)) {
    const budget = retryBudget(spec, name);
    if (budget !== undefined && (record.attempts ?? 0) > budget) {
      fail(
        'I5 retry bound',
        `${exec.id}/${name} persisted ${record.attempts} attempts, budget ${budget}`
      );
    }
  }
}

function checkMapLifecycle(
  spec: WorkflowSpec,
  ledger: Ledger,
  exec: Execution,
  fail: InvariantFail
): void {
  for (const node of spec.nodes) {
    if (node.kind !== 'map') continue;
    const calls = ledger.maps.filter(
      (call) => call.executionId === exec.id && call.name === node.name
    );
    if (calls.length > 1) {
      fail(
        'I11 map exclusive delivery',
        `${exec.id}/${node.name} transformed ${calls.length} times`
      );
    }
    const call = calls[0];
    if (!call) continue;
    const record = exec.steps[node.name];
    if (!record) {
      fail('I11 map lifecycle', `${exec.id}/${node.name} dispatched with no durable record`);
    }
    if (record.status !== call.outcome) {
      fail(
        'I11 map lifecycle',
        `${exec.id}/${node.name} outcome ${call.outcome}, record ${record.status}`
      );
    }
    if (
      record.startedAt === undefined ||
      record.completedAt === undefined ||
      record.completedAt < record.startedAt
    ) {
      fail('I11 map lifecycle', `${exec.id}/${node.name} has incoherent timestamps`);
    }
    if (call.outcome === 'failed' && !record.error?.includes('scripted map failure')) {
      fail('I11 map lifecycle', `${exec.id}/${node.name} lost its transform failure`);
    }
  }
}

function checkBranchTotality(spec: WorkflowSpec, exec: Execution, fail: InvariantFail): void {
  for (let index = 0; index < spec.nodes.length; index++) {
    if (!selectedBranchMissing(spec, index)) continue;
    if (exec.currentNodeIndex > index || exec.state === 'completed') {
      fail(
        'I9 branch totality',
        `${exec.id} advanced past branch ${index}, whose selected path is not declared`
      );
    }
  }
}

function executionSummary(exec: Execution): object {
  return {
    id: exec.id,
    workflowName: exec.workflowName,
    parentExecutionId: exec.parentExecutionId,
    state: exec.state,
    currentNodeIndex: exec.currentNodeIndex,
    rollbackStatus: exec.rollbackStatus,
    failureReason: exec.failureReason,
    committedAt: exec.committedAt,
    signals: exec.signals,
    steps: exec.steps,
  };
}
