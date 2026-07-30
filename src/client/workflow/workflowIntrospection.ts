import type { WorkflowNode } from './types';

/** Names that produce indexed records such as `name:0`. */
export function indexedStepNames(nodes: readonly WorkflowNode[]): string[] {
  const names: string[] = [];
  for (const node of nodes) {
    if (node.type === 'doUntil' || node.type === 'doWhile') {
      for (const step of node.def.steps) names.push(step.name);
    } else if (node.type === 'forEach') {
      names.push(node.def.step.name);
    }
  }
  return names;
}

/** Every record name declared by a workflow graph. */
export function stepNames(nodes: readonly WorkflowNode[]): string[] {
  const names: string[] = [];
  for (const node of nodes) {
    if (node.type === 'step') names.push(node.def.name);
    else if (node.type === 'branch') {
      for (const steps of node.def.paths.values()) {
        for (const step of steps) names.push(step.name);
      }
    } else if (node.type === 'parallel') {
      for (const step of node.def.steps) names.push(step.name);
    } else if (node.type === 'subWorkflow') {
      names.push(`sub:${node.name}`);
    } else if (node.type === 'doUntil' || node.type === 'doWhile') {
      for (const step of node.def.steps) names.push(step.name);
    } else if (node.type === 'forEach') {
      names.push(node.def.step.name);
    } else if (node.type === 'map') {
      names.push(node.def.name);
    }
  }
  return names;
}
