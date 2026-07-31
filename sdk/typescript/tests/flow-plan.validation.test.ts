import { describe, expect, test } from 'bun:test';
import { flowData, planFlows } from '../src/flow-plan.js';
import { planBulkThen, planChain } from '../src/flow-plan-legacy.js';
import type { FlowJob, FlowStep } from '../src/flow-types.js';
import type { JobOptions } from '../src/types.js';

function ids(prefix = 'id'): () => string {
  let index = 0;
  return () => `${prefix}-${index++}`;
}

describe('atomic flow planner validation', () => {
  test('validates names, queues, children and generated IDs before I/O', () => {
    const invalidNodes: Array<[unknown, string]> = [
      [null, 'flow node must be an object'],
      [{ name: '', queueName: 'queue' }, 'flow job name must be'],
      [{ name: 42, queueName: 'queue' }, 'flow job name must be'],
      [{ name: 'x'.repeat(257), queueName: 'queue' }, 'flow job name must be'],
      [{ name: 'job', queueName: '' }, 'flow queueName is invalid'],
      [{ name: 'job', queueName: 42 }, 'flow queueName is invalid'],
      [{ name: 'job', queueName: 'has spaces' }, 'flow queueName is invalid'],
      [{ name: 'job', queueName: 'q'.repeat(257) }, 'flow queueName is invalid'],
      [{ name: 'job', queueName: 'queue', children: {} }, 'flow children must be an array'],
    ];
    for (const [node, message] of invalidNodes) {
      expect(() => planFlows([node as FlowJob], undefined, ids())).toThrow(message);
    }
    for (const generated of ['', 'has:colon', 'x'.repeat(1_025)]) {
      expect(() =>
        planFlows([{ name: 'job', queueName: 'queue' }], undefined, () => generated)
      ).toThrow(/jobId/);
    }
  });

  test('rejects duplicate IDs, cycles and shared nodes', () => {
    expect(() =>
      planFlows(
        [
          { name: 'one', queueName: 'queue', opts: { jobId: 'same' } },
          { name: 'two', queueName: 'queue', opts: { jobId: 'same' } },
        ],
        undefined,
        ids()
      )
    ).toThrow(/duplicate/);

    const cycle: FlowJob = { name: 'cycle', queueName: 'queue' };
    cycle.children = [cycle];
    expect(() => planFlows([cycle], undefined, ids())).toThrow(/cycle or shared/);

    const shared: FlowJob = { name: 'shared', queueName: 'queue' };
    expect(() =>
      planFlows(
        [
          { name: 'one', queueName: 'queue', children: [shared] },
          { name: 'two', queueName: 'queue', children: [shared] },
        ],
        undefined,
        ids()
      )
    ).toThrow(/cycle or shared/);
  });

  test('enforces depth and caller-owned topology boundaries', () => {
    let deep: FlowJob = { name: 'leaf', queueName: 'queue' };
    for (let depth = 0; depth < 101; depth++) {
      deep = { name: `level-${depth}`, queueName: 'queue', children: [deep] };
    }
    expect(() => planFlows([deep], undefined, ids())).toThrow(/depth limit/);

    for (const opts of [
      { parentId: 'parent' },
      { dependsOn: ['dependency'] },
      { childrenIds: ['child'] },
    ] satisfies JobOptions[]) {
      expect(() => planFlows([{ name: 'job', queueName: 'queue', opts }])).toThrow(/topology/);
    }
  });

  test('wraps scalar payloads and never lets user data replace markers', () => {
    expect(flowData('undefined', undefined)).toEqual({ name: 'undefined' });
    expect(flowData('scalar', 42)).toEqual({ name: 'scalar', payload: 42 });
    expect(flowData('empty', null)).toEqual({ name: 'empty' });
    expect(flowData('array', [1, 2])).toEqual({ name: 'array', payload: [1, 2] });
    expect(flowData('object', { value: 1 }, { __marker: true })).toEqual({
      value: 1,
      name: 'object',
      __marker: true,
    });
    expect(() => flowData('job', { name: 'override' })).toThrow(/reserved/);
    expect(() => flowData('job', { __parentId: 'override' })).toThrow(/reserved/);
  });

  test('preserves the complete supported option surface', () => {
    const options: JobOptions = {
      priority: 7,
      delay: 8,
      attempts: 9,
      backoff: { type: 'exponential', delay: 10, maxDelay: 11 },
      ttl: 12,
      timeout: 13,
      tags: ['one', 'two'],
      groupId: 'group',
      lifo: false,
      removeOnComplete: false,
      removeOnFail: true,
      stallTimeout: 14,
      durable: true,
      stackTraceLimit: 15,
      keepLogs: 16,
      sizeLimit: 17,
      timestamp: 18,
    };
    const plan = planFlows(
      [
        {
          name: 'parent',
          queueName: 'queue',
          opts: options,
          children: [
            {
              name: 'child',
              queueName: 'queue',
              opts: { continueParentOnFailure: true },
            },
          ],
        },
      ],
      undefined,
      ids()
    );
    expect(plan.jobs[1].input).toMatchObject({
      priority: 7,
      delay: 8,
      maxAttempts: 9,
      backoff: options.backoff,
      ttl: 12,
      timeout: 13,
      tags: ['one', 'two'],
      groupId: 'group',
      lifo: false,
      removeOnComplete: false,
      removeOnFail: true,
      stallTimeout: 14,
      durable: true,
      stackTraceLimit: 15,
      keepLogs: 16,
      sizeLimit: 17,
      timestamp: 18,
    });
    expect(plan.jobs[0].input.continueParentOnFailure).toBe(true);
  });
});

describe('flat flow planner boundaries', () => {
  test('accepts an empty fan-in and preserves explicit IDs', () => {
    const plan = planBulkThen([], {
      name: 'final',
      queueName: 'queue',
      opts: { jobId: 'final-id' },
    });
    expect(plan).toMatchObject({ parallelIds: [], finalId: 'final-id' });
    expect(plan.jobs[0].input).not.toHaveProperty('dependsOn');
    expect(plan.jobs[0].input).not.toHaveProperty('childrenIds');
    expect(plan.jobs[0].input.customId).toBe('final-id');
  });

  test('rejects malformed steps, duplicate IDs and reserved chain data', () => {
    expect(() => planChain([null as unknown as FlowStep], ids())).toThrow(
      'flow step must be an object'
    );
    for (const queueName of ['!queue', 'queue!']) {
      expect(() => planChain([{ name: 'job', queueName }], ids())).toThrow(
        'flow queueName is invalid'
      );
    }
    expect(() =>
      planChain([{ name: 'job', queueName: 'queue', children: {} } as unknown as FlowStep], ids())
    ).toThrow('flow children must be an array');
    expect(() =>
      planChain(
        [
          { name: 'one', queueName: 'queue', opts: { jobId: 'same' } },
          { name: 'two', queueName: 'queue', opts: { jobId: 'same' } },
        ],
        ids()
      )
    ).toThrow(/duplicate/);
    expect(() =>
      planChain([{ name: 'one', queueName: 'queue', data: { __flowParentId: 'user' } }])
    ).toThrow(/reserved/);
  });
});
