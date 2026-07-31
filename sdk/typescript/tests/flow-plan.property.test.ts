import { describe, expect, test } from 'bun:test';
import fc from 'fast-check';

import { type AtomicFlowJobInput, type FlowIdFactory, planFlows } from '../src/flow-plan.js';
import { planBulkThen, planChain } from '../src/flow-plan-legacy.js';
import type { FlowJob, FlowStep } from '../src/flow-types.js';
import type { JobOptions } from '../src/types.js';

const pbtSeed = Number.parseInt(process.env.BUNQUEUE_FLOW_PBT_SEED ?? '20260730', 10);
if (!Number.isInteger(pbtSeed)) throw new Error('BUNQUEUE_FLOW_PBT_SEED must be an integer');

function campaign(numRuns: number): { numRuns: number; seed: number; path?: string } {
  const path = process.env.BUNQUEUE_FLOW_PBT_PATH;
  return path ? { numRuns, seed: pbtSeed, path } : { numRuns, seed: pbtSeed };
}

const token = fc
  .array(fc.constantFrom(...'abcdefghijklmnopqrstuvwxyz0123456789-_'), {
    minLength: 1,
    maxLength: 12,
  })
  .map((parts) => parts.join(''));

interface GeneratedData {
  value: number;
  label: string;
}

function tree(depth = 0): fc.Arbitrary<FlowJob<GeneratedData>> {
  return fc.record({
    name: token,
    queueName: token,
    data: fc.record({ value: fc.integer(), label: token }),
    children:
      depth >= 3 ? fc.constant([]) : fc.array(tree(depth + 1), { minLength: 0, maxLength: 3 }),
  });
}

const step = fc.record({
  name: token,
  queueName: token,
  data: fc.record({ value: fc.integer(), label: token }),
}) as fc.Arbitrary<FlowStep<GeneratedData>>;

function sequentialIds(prefix = 'generated'): FlowIdFactory {
  let index = 0;
  return () => `${prefix}-${index++}`;
}

function childrenOf(job: AtomicFlowJobInput): string[] {
  return (job.input.childrenIds as string[] | undefined) ?? [];
}

function dependenciesOf(job: AtomicFlowJobInput): string[] {
  return (job.input.dependsOn as string[] | undefined) ?? [];
}

function assertTreePlan(flows: FlowJob<GeneratedData>[]): void {
  const plan = planFlows(flows, undefined, sequentialIds());
  const byId = new Map(plan.jobs.map((job) => [job.id, job]));
  const positions = new Map(plan.jobs.map((job, index) => [job.id, index]));
  expect(byId.size).toBe(plan.jobs.length);

  for (const job of plan.jobs) {
    expect(job.id).not.toContain(':');
    const data = job.input.data as Record<string, unknown>;
    for (const childId of childrenOf(job)) {
      const child = byId.get(childId);
      expect(child).toBeDefined();
      expect(dependenciesOf(job)).toContain(childId);
      expect(child?.input.parentId).toBe(job.id);
      expect((child?.input.data as Record<string, unknown>).__parentId).toBe(job.id);
      expect((child?.input.data as Record<string, unknown>).__parentQueue).toBe(job.queue);
      expect(positions.get(childId)).toBeLessThan(positions.get(job.id) ?? -1);
    }
    expect(data.__childrenIds).toEqual(childrenOf(job).length > 0 ? childrenOf(job) : undefined);
    if (childrenOf(job).length === 0) {
      expect(job.input).not.toHaveProperty('childrenIds');
      expect(job.input).not.toHaveProperty('dependsOn');
    }
    const parentId = job.input.parentId as string | undefined;
    if (parentId) expect(childrenOf(byId.get(parentId)!)).toContain(job.id);
  }

  const compare = (source: FlowJob<GeneratedData>, plannedId: string): void => {
    const planned = byId.get(plannedId)!;
    expect((planned.input.data as Record<string, unknown>).name).toBe(source.name);
    expect(planned.queue).toBe(source.queueName);
    const sourceChildren = source.children ?? [];
    expect(childrenOf(planned)).toHaveLength(sourceChildren.length);
    sourceChildren.forEach((child, index) => {
      compare(child, childrenOf(planned)[index]);
    });
  };
  flows.forEach((flow, index) => {
    compare(flow, plan.roots[index].id);
  });
  const compareResultShape = (
    source: FlowJob<GeneratedData>,
    planned: (typeof plan.roots)[number]
  ): void => {
    const sourceChildren = source.children ?? [];
    if (sourceChildren.length === 0) {
      expect(planned.children).toBeUndefined();
      return;
    }
    expect(planned.children).toHaveLength(sourceChildren.length);
    sourceChildren.forEach((child, index) => {
      compareResultShape(child, planned.children![index]);
    });
  };
  flows.forEach((flow, index) => {
    compareResultShape(flow, plan.roots[index]);
  });
}

describe('atomic flow tree planner properties', () => {
  test('preserves shape, uniqueness, closure and reciprocal links', () => {
    fc.assert(
      fc.property(fc.array(tree(), { minLength: 0, maxLength: 4 }), assertTreePlan),
      campaign(250)
    );
  });

  test('maps supported options and jobId to customId without leaking jobId', () => {
    const options = fc.record({
      priority: fc.integer({ min: -100, max: 100 }),
      delay: fc.integer({ min: 0, max: 10_000 }),
      attempts: fc.integer({ min: 1, max: 20 }),
      ttl: fc.integer({ min: 0, max: 10_000 }),
      timeout: fc.integer({ min: 0, max: 10_000 }),
      lifo: fc.boolean(),
      durable: fc.boolean(),
      removeOnComplete: fc.boolean(),
      removeOnFail: fc.boolean(),
      tags: fc.array(token, { maxLength: 4 }),
      groupId: token,
      jobId: token.map((value) => `custom-${value}`),
    });
    fc.assert(
      fc.property(options, (opts) => {
        const plan = planFlows(
          [{ name: 'job', queueName: 'queue', data: { value: 1, label: 'x' }, opts }],
          undefined,
          sequentialIds()
        );
        const input = plan.jobs[0].input;
        expect(plan.jobs[0].id).toBe(opts.jobId);
        expect(input.customId).toBe(opts.jobId);
        expect(input).not.toHaveProperty('jobId');
        expect(input.maxAttempts).toBe(opts.attempts);
        for (const key of [
          'priority',
          'delay',
          'ttl',
          'timeout',
          'lifo',
          'durable',
          'removeOnComplete',
          'removeOnFail',
          'tags',
          'groupId',
        ] as const) {
          expect(input[key]).toEqual(opts[key]);
        }
      }),
      campaign(200)
    );
  });

  test('queue defaults merge below per-job options', () => {
    const plan = planFlows(
      [{ name: 'job', queueName: 'queue', opts: { priority: 9 } }],
      { queuesOptions: { queue: { priority: 1, attempts: 4 } } },
      sequentialIds()
    );
    expect(plan.jobs[0].input.priority).toBe(9);
    expect(plan.jobs[0].input.maxAttempts).toBe(4);
  });

  test('rejects reserved data and unsupported atomic options', () => {
    fc.assert(
      fc.property(token, (suffix) => {
        expect(() =>
          planFlows([{ name: 'job', queueName: 'queue', data: { [`__${suffix}`]: true } }])
        ).toThrow(/reserved/);
      }),
      campaign(100)
    );
    for (const opts of [
      { repeat: {} },
      { deduplication: { id: 'same' } },
      { debounce: { id: 'same' } },
    ] satisfies JobOptions[]) {
      expect(() => planFlows([{ name: 'job', queueName: 'queue', opts }])).toThrow(/not supported/);
    }
  });
});

describe('legacy atomic flow planner properties', () => {
  test('flat planners reject nested children instead of silently dropping them', () => {
    const nested = {
      name: 'outer',
      queueName: 'queue',
      children: [{ name: 'inner', queueName: 'queue' }],
    } as unknown as FlowStep;
    expect(() => planChain([nested], sequentialIds())).toThrow(/nested children/);
    expect(() =>
      planBulkThen([nested], { name: 'final', queueName: 'queue' }, sequentialIds())
    ).toThrow(/nested children/);
  });

  test('chains are closed, unique and ordered', () => {
    fc.assert(
      fc.property(fc.array(step, { maxLength: 30 }), (steps) => {
        const plan = planChain(steps, sequentialIds('chain'));
        expect(new Set(plan.ids).size).toBe(plan.ids.length);
        plan.jobs.forEach((job, index) => {
          const previous = index > 0 ? plan.ids[index - 1] : null;
          expect(dependenciesOf(job)).toEqual(previous ? [previous] : []);
          expect((job.input.data as Record<string, unknown>).__flowParentId).toBe(previous);
        });
      }),
      campaign(250)
    );
  });

  test('fan-in links every parallel job reciprocally to the final job', () => {
    fc.assert(
      fc.property(fc.array(step, { maxLength: 30 }), step, (parallel, final) => {
        const plan = planBulkThen(parallel, final, sequentialIds('fanin'));
        const finalJob = plan.jobs.at(-1)!;
        expect(childrenOf(finalJob)).toEqual(plan.parallelIds);
        expect(dependenciesOf(finalJob)).toEqual(plan.parallelIds);
        expect((finalJob.input.data as Record<string, unknown>).__flowParentIds).toEqual(
          plan.parallelIds
        );
        plan.jobs.slice(0, -1).forEach((job) => {
          expect(job.input.parentId).toBe(plan.finalId);
          expect((job.input.data as Record<string, unknown>).__parentId).toBe(plan.finalId);
          expect(childrenOf(finalJob)).toContain(job.id);
        });
      }),
      campaign(250)
    );
  });
});
