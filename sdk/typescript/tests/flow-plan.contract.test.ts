import { describe, expect, test } from 'bun:test';

import { planFlows } from '../src/flow-plan.js';
import { planBulkThen, planChain } from '../src/flow-plan-legacy.js';
import type { FlowJob, FlowStep } from '../src/flow-types.js';

function ids(prefix = 'contract'): () => string {
  let index = 0;
  return () => `${prefix}-${index++}`;
}

describe('atomic flow validation contract', () => {
  test('reports the exact failing field for malformed runtime input', () => {
    expect(() => planFlows([42 as unknown as FlowJob], undefined, ids())).toThrow(
      'flow node must be an object'
    );
    expect(() => planChain([42 as unknown as FlowStep], ids())).toThrow(
      'flow step must be an object'
    );
    for (const name of [42, '', 'x'.repeat(257)]) {
      expect(() => planChain([{ name, queueName: 'queue' } as unknown as FlowStep], ids())).toThrow(
        'flow job name must be a non-empty string of at most 256 characters'
      );
    }
    for (const queueName of [42, '', 'q'.repeat(257), '!queue']) {
      expect(() => planChain([{ name: 'job', queueName } as unknown as FlowStep], ids())).toThrow(
        'flow queueName is invalid'
      );
    }
  });

  test('accepts exact flat-planner text limits and rejects a non-string generated ID', () => {
    const name = 'n'.repeat(256);
    const queueName = 'q'.repeat(256);
    expect(planChain([{ name, queueName }], ids()).jobs[0]).toMatchObject({
      queue: queueName,
      input: { data: { name } },
    });
    expect(() =>
      planFlows(
        [{ name: 'job', queueName: 'queue' }],
        undefined,
        (() => 42) as unknown as () => string
      )
    ).toThrow('flow jobId must be non-empty and cannot contain a colon');
  });

  test('omits the payload field when data is undefined', () => {
    const data = planFlows([{ name: 'job', queueName: 'queue' }], undefined, ids()).jobs[0].input
      .data;
    expect(data).toEqual({ name: 'job' });
    expect(data).not.toHaveProperty('payload');
  });
});

describe('legacy planner wire preservation', () => {
  test('preserves every chain step field and option', () => {
    const plan = planChain(
      [
        {
          name: 'first',
          queueName: 'queue-a',
          data: { value: 1 },
          opts: { priority: 3 },
        },
        {
          name: 'second',
          queueName: 'queue-b',
          data: { value: 2 },
          opts: { delay: 4 },
        },
      ],
      ids('chain')
    );
    expect(plan.jobs).toEqual([
      {
        id: 'chain-0',
        queue: 'queue-a',
        input: {
          data: { value: 1, name: 'first', __flowParentId: null },
          priority: 3,
        },
      },
      {
        id: 'chain-1',
        queue: 'queue-b',
        input: {
          data: { value: 2, name: 'second', __flowParentId: 'chain-0' },
          delay: 4,
          dependsOn: ['chain-0'],
        },
      },
    ]);
  });

  test('preserves every fan-in field, option and reciprocal marker', () => {
    const plan = planBulkThen(
      [
        {
          name: 'left',
          queueName: 'queue-left',
          data: { value: 1 },
          opts: { priority: 2 },
        },
      ],
      {
        name: 'final',
        queueName: 'queue-final',
        data: { value: 3 },
        opts: { timeout: 5 },
      },
      ids('fanin')
    );
    expect(plan.jobs).toEqual([
      {
        id: 'fanin-0',
        queue: 'queue-left',
        input: {
          data: {
            value: 1,
            name: 'left',
            __parentId: 'fanin-1',
            __parentQueue: 'queue-final',
          },
          priority: 2,
          parentId: 'fanin-1',
        },
      },
      {
        id: 'fanin-1',
        queue: 'queue-final',
        input: {
          data: {
            value: 3,
            name: 'final',
            __flowParentIds: ['fanin-0'],
            __childrenIds: ['fanin-0'],
          },
          timeout: 5,
          dependsOn: ['fanin-0'],
          childrenIds: ['fanin-0'],
        },
      },
    ]);
  });
});
