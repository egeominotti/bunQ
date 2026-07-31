import { expect, test } from 'bun:test';
import { planFlows } from '../src/flow-plan.js';
import { planBulkThen, planChain } from '../src/flow-plan-legacy.js';
import type { FlowJob, FlowStep } from '../src/flow-types.js';

function ids(): () => string {
  let index = 0;
  return () => `limit-${index++}`;
}

function step(index: number): FlowStep {
  return { name: `job-${index}`, queueName: 'queue' };
}

test('flow planners accept exact name, queue, ID and depth limits', () => {
  const atTextLimits = planFlows([{ name: 'n'.repeat(256), queueName: 'q'.repeat(256) }], {}, () =>
    'i'.repeat(1_024)
  );
  expect(atTextLimits.jobs[0].id).toHaveLength(1_024);

  let deep: FlowJob = { name: 'leaf', queueName: 'queue' };
  for (let depth = 0; depth < 100; depth++) {
    deep = { name: `level-${depth}`, queueName: 'queue', children: [deep] };
  }
  expect(planFlows([deep], undefined, ids()).jobs).toHaveLength(101);
});

test('flow tree planner enforces the exact 10000-job batch bound', () => {
  const atLimit = Array.from({ length: 10_000 }, (_, index) => ({
    name: `job-${index}`,
    queueName: 'queue',
  }));
  expect(planFlows(atLimit, undefined, ids()).jobs).toHaveLength(10_000);
  expect(() =>
    planFlows([...atLimit, { name: 'overflow', queueName: 'queue' }], undefined, ids())
  ).toThrow('flow exceeds the 10000 job limit');
});

test('flat planners enforce total batch bounds without off-by-one errors', () => {
  const atChainLimit = Array.from({ length: 10_000 }, (_, index) => step(index));
  expect(planChain(atChainLimit, ids()).jobs).toHaveLength(10_000);
  expect(() => planChain([...atChainLimit, step(10_000)], ids())).toThrow(
    'flow exceeds the 10000 job limit'
  );

  const atFanInLimit = Array.from({ length: 9_999 }, (_, index) => step(index));
  expect(planBulkThen(atFanInLimit, step(9_999), ids()).jobs).toHaveLength(10_000);
  expect(() => planBulkThen([...atFanInLimit, step(10_000)], step(10_001), ids())).toThrow(
    'flow exceeds the 10000 job limit'
  );
});
