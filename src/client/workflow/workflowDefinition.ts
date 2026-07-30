/**
 * Stable workflow-definition identity and sealing.
 *
 * Executions persist the hash produced here. It describes the durable graph, while
 * `revision` lets callers explicitly mark handler/condition changes that cannot be
 * inferred safely from closures.
 */

import { createHash } from 'node:crypto';
import type { StepDefinition, WorkflowNode } from './types';

interface WorkflowDefinitionSource {
  readonly name: string;
  readonly revision: string;
  readonly nodes: WorkflowNode[];
}

function stepShape(def: StepDefinition): Record<string, unknown> {
  return {
    name: def.name,
    retry: def.retry,
    timeout: def.timeout,
    compensates: def.compensate !== undefined,
    validatesInput: def.inputSchema !== undefined,
    validatesOutput: def.outputSchema !== undefined,
  };
}

function nodeShape(node: WorkflowNode): Record<string, unknown> {
  if (node.type === 'step') return { type: node.type, step: stepShape(node.def) };
  if (node.type === 'branch') {
    return {
      type: node.type,
      paths: [...node.def.paths.entries()]
        .sort(([left], [right]) => left.localeCompare(right))
        .map(([name, steps]) => [name, steps.map(stepShape)]),
    };
  }
  if (node.type === 'parallel') {
    return { type: node.type, steps: node.def.steps.map(stepShape) };
  }
  if (node.type === 'subWorkflow') {
    return {
      type: node.type,
      name: node.name,
      timeout: node.timeout,
      pollInterval: node.pollInterval,
    };
  }
  if (node.type === 'waitFor') {
    return { type: node.type, event: node.event, timeout: node.timeout ?? null };
  }
  if (node.type === 'doUntil' || node.type === 'doWhile') {
    return {
      type: node.type,
      maxIterations: node.def.maxIterations,
      steps: node.def.steps.map(stepShape),
    };
  }
  if (node.type === 'forEach') {
    return {
      type: node.type,
      maxIterations: node.def.maxIterations,
      step: stepShape(node.def.step),
    };
  }
  if (node.type === 'map') return { type: node.type, name: node.def.name };
  return { type: 'pivot' };
}

export function workflowDefinitionHash(workflow: WorkflowDefinitionSource): string {
  const definition = {
    name: workflow.name,
    revision: workflow.revision,
    nodes: workflow.nodes.map(nodeShape),
  };
  return createHash('sha256').update(JSON.stringify(definition)).digest('hex');
}

function sealMap(map: Map<string, StepDefinition[]>): void {
  const immutable = () => {
    throw new Error('A registered workflow definition is sealed');
  };
  Object.defineProperties(map, {
    set: { value: immutable },
    delete: { value: immutable },
    clear: { value: immutable },
  });
  Object.freeze(map);
}

function sealStep(def: StepDefinition): void {
  Object.freeze(def);
}

function sealNode(node: WorkflowNode): void {
  if (node.type === 'step') sealStep(node.def);
  else if (node.type === 'branch') {
    for (const steps of node.def.paths.values()) {
      for (const step of steps) sealStep(step);
      Object.freeze(steps);
    }
    sealMap(node.def.paths);
    Object.freeze(node.def);
  } else if (node.type === 'parallel') {
    for (const step of node.def.steps) sealStep(step);
    Object.freeze(node.def.steps);
    Object.freeze(node.def);
  } else if (node.type === 'doUntil' || node.type === 'doWhile') {
    for (const step of node.def.steps) sealStep(step);
    Object.freeze(node.def.steps);
    Object.freeze(node.def);
  } else if (node.type === 'forEach') {
    sealStep(node.def.step);
    Object.freeze(node.def);
  } else if (node.type === 'map') {
    Object.freeze(node.def);
  }
  Object.freeze(node);
}

export function sealWorkflowDefinition(workflow: WorkflowDefinitionSource): string {
  const hash = workflowDefinitionHash(workflow);
  for (const node of workflow.nodes) sealNode(node);
  Object.freeze(workflow.nodes);
  return hash;
}
