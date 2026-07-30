/**
 * Pure metadata queries over generated workflow specs.
 */

import type { Execution, StepRecord } from '../../src/client/workflow';
import type { CompensationBehavior, NodeSpec, StepSpec, WorkflowSpec } from './workflow-spec';

/** Registered workflow name of the generated one-step child. */
export function childName(stepName: string): string {
  return `child_${stepName}`;
}

export function subRecordName(stepName: string): string {
  return `sub:${childName(stepName)}`;
}

/** The spec represented by a parent or one of its generated child executions. */
export function executionSpec(parent: WorkflowSpec, exec: Execution): WorkflowSpec | null {
  if (exec.workflowName === parent.name) return parent;
  const node = parent.nodes.find(
    (candidate) =>
      candidate.kind === 'subWorkflow' && childName(candidate.step.name) === exec.workflowName
  );
  if (node?.kind !== 'subWorkflow') return null;
  return { name: exec.workflowName, nodes: [{ kind: 'step', step: node.step }] };
}

/** All durable record names the spec may legitimately produce. */
export function declaredNames(spec: WorkflowSpec): Set<string> {
  const names = new Set<string>();
  for (const node of spec.nodes) {
    if (node.kind === 'step') names.add(node.step.name);
    else if (node.kind === 'parallel') for (const step of node.steps) names.add(step.name);
    else if (node.kind === 'branch') {
      for (const path of node.paths) for (const step of path.steps) names.add(step.name);
    } else if (node.kind === 'forEach') {
      // The indexed records are execution identities; the bare key mirrors the last
      // result for the public `ctx.steps[name]` contract.
      names.add(node.step.name);
      for (let i = 0; i < node.count; i++) names.add(`${node.step.name}:${i}`);
    } else if (node.kind === 'map') names.add(node.name);
    else if (node.kind === 'subWorkflow') names.add(subRecordName(node.step.name));
    else if (node.kind === 'waitFor') names.add(`__waitFor:${node.event}`);
  }
  return names;
}

/** Forward-handler names and the signal that must precede them. */
export function stepsGatedBy(spec: WorkflowSpec): Map<string, string> {
  const gated = new Map<string, string>();
  let pending: string | null = null;
  for (const node of spec.nodes) {
    if (node.kind === 'waitFor') {
      pending = node.event;
      continue;
    }
    if (pending === null) continue;
    for (const name of nodeStepNames(node)) gated.set(name, pending);
  }
  return gated;
}

export function retryBudget(spec: WorkflowSpec, recordName: string): number | undefined {
  return stepForRecord(spec, recordName)?.retry;
}

export function compensationBehavior(spec: WorkflowSpec, recordName: string): CompensationBehavior {
  return stepForRecord(spec, recordName)?.compensation ?? 'none';
}

export function selectedBranchMissing(spec: WorkflowSpec, index: number): boolean {
  const node = spec.nodes[index];
  return (
    node?.kind === 'branch' &&
    !node.paths.some((path) => path.name === node.pick && path.steps.length > 0)
  );
}

export function pivotAt(spec: WorkflowSpec, index: number): boolean {
  return spec.nodes[index]?.kind === 'pivot';
}

/** True for the aggregate mirror of an indexed forEach history. */
export function isIterationMirror(
  spec: WorkflowSpec,
  name: string,
  steps: Record<string, StepRecord>
): boolean {
  const node = spec.nodes.find(
    (candidate) => candidate.kind === 'forEach' && candidate.step.name === name
  );
  return node?.kind === 'forEach' && steps[`${name}:0`] !== undefined;
}

export function expectedChildNames(spec: WorkflowSpec): Set<string> {
  return new Set(
    spec.nodes
      .filter((node) => node.kind === 'subWorkflow')
      .map((node) => childName(node.step.name))
  );
}

function stepForRecord(spec: WorkflowSpec, recordName: string): StepSpec | undefined {
  for (const node of spec.nodes) {
    if (node.kind === 'step' && node.step.name === recordName) return node.step;
    if (node.kind === 'parallel') {
      const found = node.steps.find((step) => step.name === recordName);
      if (found) return found;
    }
    if (node.kind === 'branch') {
      for (const path of node.paths) {
        const found = path.steps.find((step) => step.name === recordName);
        if (found) return found;
      }
    }
    if (node.kind === 'forEach') {
      if (node.step.name === recordName || indexedFrom(node.step.name, recordName))
        return node.step;
    }
    if (node.kind === 'subWorkflow' && node.step.name === recordName) return node.step;
  }
  return undefined;
}

function indexedFrom(base: string, name: string): boolean {
  if (!name.startsWith(`${base}:`)) return false;
  return /^\d+$/.test(name.slice(base.length + 1));
}

function nodeStepNames(node: NodeSpec): string[] {
  if (node.kind === 'step') return [node.step.name];
  if (node.kind === 'parallel') return node.steps.map((step) => step.name);
  if (node.kind === 'branch') {
    return node.paths.flatMap((path) => path.steps.map((step) => step.name));
  }
  if (node.kind === 'forEach') {
    return Array.from({ length: node.count }, (_, index) => `${node.step.name}:${index}`);
  }
  if (node.kind === 'map') return [node.name];
  if (node.kind === 'subWorkflow') return [node.step.name];
  return [];
}
