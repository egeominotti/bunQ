import type { AtomicFlowBatchInput, AtomicFlowJobInput } from '../../domain/types/flow';

interface Topology {
  readonly dependencies: string[];
  readonly dependencySet: Set<string>;
  readonly children: string[];
  readonly childSet: Set<string>;
}

function assertMatchingIds(actual: unknown, expected: string[], name: string): void {
  if (
    !Array.isArray(actual) ||
    actual.length !== expected.length ||
    actual.some((id, index) => id !== expected[index])
  ) {
    throw new Error(`${name} does not match the canonical flow topology`);
  }
}

function assertAcyclic(topology: ReadonlyMap<string, Topology>): void {
  const remaining = new Map([...topology].map(([id, links]) => [id, links.dependencies.length]));
  const dependents = new Map<string, string[]>();
  for (const [id, links] of topology) {
    for (const dependency of links.dependencies) {
      const list = dependents.get(dependency) ?? [];
      list.push(id);
      dependents.set(dependency, list);
    }
  }
  const ready = [...remaining].filter(([, count]) => count === 0).map(([id]) => id);
  let visited = 0;
  while (ready.length > 0) {
    const id = ready.pop();
    if (id === undefined) throw new Error('flow topology traversal failed');
    visited++;
    for (const dependent of dependents.get(id) ?? []) {
      const count = remaining.get(dependent);
      if (count === undefined) throw new Error(`flow dependency not found: ${dependent}`);
      const next = count - 1;
      remaining.set(dependent, next);
      if (next === 0) ready.push(dependent);
    }
  }
  if (visited !== topology.size) throw new Error('flow contains a dependency cycle');
}

function buildTopology(job: AtomicFlowJobInput): Topology {
  const dependencies = (job.input.dependsOn ?? []).map(String);
  const children = (job.input.childrenIds ?? []).map(String);
  const dependencySet = new Set(dependencies);
  const childSet = new Set(children);
  if (dependencySet.size !== dependencies.length) {
    throw new Error(`duplicate dependency in flow job: ${String(job.id)}`);
  }
  if (childSet.size !== children.length) {
    throw new Error(`duplicate child in flow job: ${String(job.id)}`);
  }
  return { dependencies, dependencySet, children, childSet };
}

/** Validate references, symmetric parent edges, metadata, and acyclicity in O(V+E). */
export function validateFlowTopology(
  batch: AtomicFlowBatchInput,
  dataById: ReadonlyMap<string, Record<string, unknown>>
): void {
  const jobsById = new Map(batch.jobs.map((job) => [String(job.id), job]));
  const topology = new Map(batch.jobs.map((job) => [String(job.id), buildTopology(job)]));

  for (const job of batch.jobs) {
    const id = String(job.id);
    const links = topology.get(id);
    if (!links) throw new Error(`flow topology missing job: ${id}`);
    for (const dependency of links.dependencies) {
      if (!jobsById.has(dependency)) throw new Error(`flow dependency not found: ${dependency}`);
      if (dependency === id) throw new Error(`flow job cannot depend on itself: ${id}`);
    }
    for (const child of links.children) {
      if (!links.dependencySet.has(child)) {
        throw new Error(`flow child is missing from dependencies: ${child}`);
      }
      const childJob = jobsById.get(child);
      if (!childJob) throw new Error(`flow child not found: ${child}`);
      if (String(childJob.input.parentId) !== id) {
        throw new Error(`flow child ${child} does not point back to parent ${id}`);
      }
    }

    const parentId = job.input.parentId ? String(job.input.parentId) : null;
    if (parentId) {
      const parent = jobsById.get(parentId);
      if (!parent) throw new Error(`flow parent not found: ${parentId}`);
      const parentLinks = topology.get(parentId);
      if (!parentLinks?.childSet.has(id)) {
        throw new Error(`flow parent ${parentId} does not own child ${id}`);
      }
      const data = dataById.get(id);
      if (!data) throw new Error(`flow metadata missing for child ${id}`);
      if (data.__parentId !== parentId || data.__parentQueue !== parent.queue) {
        throw new Error(`flow child ${id} metadata does not match parent ${parentId}`);
      }
    }
    if (links.children.length > 0) {
      const data = dataById.get(id);
      if (!data) throw new Error(`flow metadata missing for parent ${id}`);
      assertMatchingIds(data.__childrenIds, links.children, `flow parent ${id} children`);
    }
  }
  assertAcyclic(topology);
}
