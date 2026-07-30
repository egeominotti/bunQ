import { afterAll, describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import { FlowProducer, Queue, shutdownManager } from '../../src/client';
import { getSharedManager } from '../../src/client/manager';
import type { FlowJob, JobNode } from '../../src/client/flowTypes';
import type { JobId } from '../../src/domain/types/job';

interface FlowSpec {
  label: number;
  payload: unknown;
  priority: number;
  queue: 'a' | 'b' | 'c';
  children: FlowSpec[];
}

interface ExpectedNode {
  id: string;
  parentId: string | null;
  queueName: string;
  spec: FlowSpec;
  children: ExpectedNode[];
}

const nodeFields = {
  label: fc.integer(),
  payload: fc.jsonValue(),
  priority: fc.integer({ min: 0, max: 20 }),
  queue: fc.constantFrom<'a' | 'b' | 'c'>('a', 'b', 'c'),
};
const leaf = fc.record({ ...nodeFields, children: fc.constant<FlowSpec[]>([]) });
const branch = fc.record({
  ...nodeFields,
  children: fc.array(leaf, { maxLength: 3 }),
});
const root = fc.record({
  ...nodeFields,
  children: fc.array(fc.oneof(leaf, branch), { maxLength: 3 }),
});

function materialize(
  spec: FlowSpec,
  prefix: string,
  path: string,
  parentId: string | null
): { flow: FlowJob; expected: ExpectedNode } {
  const id = `${prefix}-job-${path}`;
  const children = spec.children.map((child, index) =>
    materialize(child, prefix, `${path}-${index}`, id)
  );
  return {
    flow: {
      name: `node-${spec.label}`,
      queueName: `${prefix}-queue-${spec.queue}`,
      data: { label: spec.label, payload: spec.payload, path },
      opts: { jobId: id, priority: spec.priority },
      children: children.map((child) => child.flow),
    },
    expected: {
      id,
      parentId,
      queueName: `${prefix}-queue-${spec.queue}`,
      spec,
      children: children.map((child) => child.expected),
    },
  };
}

function flatten(nodes: ExpectedNode[]): ExpectedNode[] {
  const result: ExpectedNode[] = [];
  const visit = (node: ExpectedNode): void => {
    result.push(node);
    for (const child of node.children) visit(child);
  };
  for (const node of nodes) visit(node);
  return result;
}

function assertTree(
  actual: JobNode | null,
  expected: ExpectedNode,
  depth: number,
  maxChildren: number
): void {
  expect(actual).not.toBeNull();
  expect(actual!.job.id).toBe(expected.id);
  expect(actual!.job.queueName).toBe(expected.queueName);
  expect(actual!.job.data).toEqual({
    label: expected.spec.label,
    payload: expected.spec.payload,
    path: expected.id.slice(expected.id.indexOf('-job-') + 5),
  });
  const children =
    depth <= 0
      ? []
      : expected.children.slice(0, maxChildren === Infinity ? undefined : maxChildren);
  if (children.length === 0) {
    expect(actual!.children).toBeUndefined();
    return;
  }
  expect(actual!.children).toHaveLength(children.length);
  children.forEach((child, index) => {
    assertTree(actual!.children![index], child, depth - 1, maxChildren);
  });
}

async function assertStoredGraph(nodes: ExpectedNode[]): Promise<void> {
  const manager = getSharedManager();
  for (const expected of flatten(nodes)) {
    const job = await manager.getJob(expected.id as JobId);
    expect(job).not.toBeNull();
    expect(job!.queue).toBe(expected.queueName);
    expect(job!.parentId ? String(job!.parentId) : null).toBe(expected.parentId);
    expect(job!.childrenIds.map(String)).toEqual(expected.children.map((child) => child.id));
    expect(job!.dependsOn.map(String)).toEqual(expected.children.map((child) => child.id));
    const expectedState =
      expected.children.length > 0
        ? 'waiting-children'
        : expected.spec.priority > 0
          ? 'prioritized'
          : 'waiting';
    expect(await manager.getJobState(job!.id)).toBe(expectedState);
  }
}

async function queuedCount(queueNames: Set<string>): Promise<number> {
  let total = 0;
  for (const queueName of queueNames) {
    const queue = new Queue(queueName, { embedded: true });
    const counts = await queue.getJobCountsAsync();
    total += counts.waiting + counts.prioritized + counts.delayed + counts['waiting-children'];
    await queue.close();
  }
  return total;
}

describe('FlowProducer generated invariant model', () => {
  afterAll(shutdownManager);

  test('generated cross-queue trees remain atomic, symmetric, and conserved', async () => {
    const seed = optionalInteger(Bun.env.BUNQUEUE_FLOW_MODEL_SEED);
    const numRuns = optionalInteger(Bun.env.BUNQUEUE_FLOW_MODEL_RUNS) ?? 60;
    let run = 0;

    await fc.assert(
      fc.asyncProperty(
        fc.array(root, { minLength: 1, maxLength: 3 }),
        fc.integer({ min: 0, max: 3 }),
        fc.integer({ min: 0, max: 3 }),
        async (specs, depth, maxChildren) => {
          const prefix = `fp-model-${seed ?? 'random'}-${run++}`;
          const planned = specs.map((spec, index) =>
            materialize(spec, prefix, String(index), null)
          );
          const expected = planned.map((entry) => entry.expected);
          const allExpected = flatten(expected);
          const queueNames = new Set(allExpected.map((node) => node.queueName));
          const producer = new FlowProducer({ embedded: true });
          try {
            const result = await producer.addBulk(planned.map((entry) => entry.flow));
            expect(flattenResultIds(result)).toEqual(new Set(allExpected.map((node) => node.id)));
            await assertStoredGraph(expected);
            expect(await queuedCount(queueNames)).toBe(allExpected.length);

            const fetched = await producer.getFlow({
              id: expected[0].id,
              queueName: expected[0].queueName,
              depth,
              maxChildren,
            });
            assertTree(fetched, expected[0], depth, maxChildren);

            const rollbackId = `${prefix}-rollback`;
            await expect(
              producer.addBulk([
                {
                  name: 'must-not-publish',
                  queueName: expected[0].queueName,
                  data: {},
                  opts: { jobId: rollbackId },
                },
                {
                  name: 'collision',
                  queueName: expected[0].queueName,
                  data: {},
                  opts: { jobId: expected[0].id },
                },
              ])
            ).rejects.toThrow('already exists');
            expect(await getSharedManager().getJob(rollbackId as JobId)).toBeNull();
            expect(await queuedCount(queueNames)).toBe(allExpected.length);
          } finally {
            await producer.close();
            shutdownManager();
          }
        }
      ),
      { endOnFailure: true, numRuns, seed, verbose: 2 }
    );
  }, 60_000);
});

function flattenResultIds(nodes: JobNode[]): Set<string> {
  const ids = new Set<string>();
  const visit = (node: JobNode): void => {
    ids.add(node.job.id);
    for (const child of node.children ?? []) visit(child);
  };
  for (const node of nodes) visit(node);
  return ids;
}

function optionalInteger(value: string | undefined): number | undefined {
  if (value === undefined || value === '') return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) throw new Error(`expected a signed safe integer: ${value}`);
  return parsed;
}
