import { expect, test } from 'bun:test';
import fc from 'fast-check';

import { planFlows } from '../src/flow-plan.js';
import type { FlowOptions } from '../src/flow-types.js';

const seed = Number.parseInt(process.env.BUNQUEUE_FLOW_PBT_SEED ?? '20260730', 10);
const token = fc
  .array(fc.constantFrom(...'abcdefghijklmnopqrstuvwxyz0123456789-_'), {
    minLength: 1,
    maxLength: 12,
  })
  .map((parts) => parts.join(''));

test('queue defaults cannot define per-job identity', () => {
  fc.assert(
    fc.property(token, (jobId) => {
      let allocations = 0;
      expect(() =>
        planFlows(
          [{ name: 'job', queueName: 'queue' }],
          {
            queuesOptions: {
              queue: { jobId },
            },
          } as unknown as FlowOptions,
          () => {
            allocations += 1;
            return 'generated';
          }
        )
      ).toThrow('jobId cannot be a queue default');
      expect(allocations).toBe(0);
    }),
    { numRuns: 100, seed }
  );
});
