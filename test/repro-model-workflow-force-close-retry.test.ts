/**
 * Regression for Bun 1.4 retry timers surviving an in-process forced Engine close.
 *
 * Found by the workflow command model with seed -795204925 and path 10. The old
 * Engine was sleeping between attempts when close(true) returned. Its retry then
 * raced recovery in the replacement Engine and dispatched the same idempotent
 * operation four times even though the persisted retry budget was three.
 */

import { afterAll, describe, test } from 'bun:test';
import { disposeCampaign, RealWorkflow } from './model-based/workflow-model-harness';
import type { WorkflowSpec } from './model-based/workflow-spec';

const spec: WorkflowSpec = {
  name: 'force-close-retry-budget',
  nodes: [
    {
      kind: 'parallel',
      steps: [
        { name: 'pa887_0', behavior: 'ok', retry: 1, compensation: 'none' },
        { name: 'pa184_1', behavior: 'flaky', retry: 3, compensation: 'fail-once' },
      ],
    },
    {
      kind: 'step',
      step: { name: 'st398_2', behavior: 'flaky', retry: 3, compensation: 'none' },
    },
    { kind: 'pivot' },
    {
      kind: 'step',
      step: { name: 'st512_3', behavior: 'ok', retry: 3, compensation: 'none' },
    },
    {
      kind: 'branch',
      paths: [
        {
          name: 'a',
          steps: [
            {
              name: 'br646_4',
              behavior: 'ok',
              retry: 1,
              compensation: 'always-fail',
            },
          ],
        },
      ],
      pick: 'a',
    },
    {
      kind: 'subWorkflow',
      step: { name: 'sw848_5', behavior: 'ok', retry: 3, compensation: 'ok' },
    },
  ],
};

afterAll(() => {
  disposeCampaign();
});

describe('workflow retries across a forced in-process restart', () => {
  test('does not exceed the persisted retry budget', async () => {
    const real = RealWorkflow.create(spec, 'seed--795204925-path-10');
    try {
      await real.start();
      await real.settle(166);
      await real.resumeCompensation();
      await real.resumeCompensation();
      await real.restart();
      await real.assertSettles();
    } finally {
      await real.dispose();
    }
  }, 20_000);
});
