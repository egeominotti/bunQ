import { beforeAll, describe, expect, test } from 'bun:test';
import { runBunqueueContract } from './contracts/bunqueue-contract';
import { runFlowProducerContract } from './contracts/flow-producer-contract';
import { runJobContract } from './contracts/job-contract';
import { runQueueControlContract } from './contracts/queue-control-contract';
import { runQueueCronContract } from './contracts/queue-cron-contract';
import { runQueueDlqContract } from './contracts/queue-dlq-contract';
import { runQueueEventsContract } from './contracts/queue-events-contract';
import { runQueueFlowContract } from './contracts/queue-flow-contract';
import { runQueueForwardContract } from './contracts/queue-forward-contract';
import { runQueueGroupContract } from './contracts/queue-group-contract';
import { runQueueLimitsWorkerContract } from './contracts/queue-limits-worker-contract';
import { runQueueQueryContract } from './contracts/queue-query-contract';
import { runSandboxedWorkerContract } from './contracts/sandboxed-worker-contract';
import { runTcpConnectionPoolContract } from './contracts/tcp-connection-pool-contract';
import { runWorkerContract } from './contracts/worker-contract';
import { runWorkflowContract } from './contracts/workflow-contract';
import { runWorkflowEmitterContract } from './contracts/workflow-emitter-contract';
import type { CoreE2eMode } from './support/harness';
import {
  buildCoverageMatrix,
  type CoverageMatrixRow,
  writeCoverageMatrix,
} from './support/matrix-report';
import { expectedCoverageKeys } from './support/public-surface';
import { CoverageTracker } from './support/tracker';

const CONTRACTS = [
  runQueueQueryContract,
  runQueueControlContract,
  runQueueLimitsWorkerContract,
  runQueueCronContract,
  runQueueDlqContract,
  runQueueFlowContract,
  runQueueForwardContract,
  runFlowProducerContract,
  runWorkerContract,
  runSandboxedWorkerContract,
  runJobContract,
  runQueueEventsContract,
  runQueueGroupContract,
  runBunqueueContract,
  runWorkflowContract,
] as const;

const EXPECTED_KEYS = expectedCoverageKeys();
const MODES = ['embedded', 'tcp'] as const satisfies readonly CoreE2eMode[];
const EXPECTED_KEYS_BY_MODE = {
  embedded: expectedCoverageKeys('embedded'),
  tcp: expectedCoverageKeys('tcp'),
} as const;
const trackers: Partial<Record<CoreE2eMode, CoverageTracker>> = {};
let matrix: CoverageMatrixRow[] = [];

async function executeSurface(mode: CoreE2eMode): Promise<CoverageTracker> {
  const aggregate = new CoverageTracker();
  for (const contract of CONTRACTS) aggregate.merge(await contract(mode));
  aggregate.merge(runWorkflowEmitterContract(mode));
  if (mode === 'tcp') aggregate.merge(await runTcpConnectionPoolContract(mode));
  return aggregate;
}

function trackerFor(mode: CoreE2eMode): CoverageTracker {
  const tracker = trackers[mode];
  if (!tracker) throw new Error(`${mode} core E2E surface did not execute`);
  return tracker;
}

function reportTrackers(): Record<CoreE2eMode, CoverageTracker> {
  return {
    embedded: trackers.embedded ?? new CoverageTracker(),
    tcp: trackers.tcp ?? new CoverageTracker(),
  };
}

describe('real core public API E2E matrix', () => {
  beforeAll(async () => {
    matrix = buildCoverageMatrix(EXPECTED_KEYS, reportTrackers());
    await writeCoverageMatrix(matrix);
    for (const mode of MODES) {
      trackers[mode] = await executeSurface(mode);
      matrix = buildCoverageMatrix(EXPECTED_KEYS, reportTrackers());
      await writeCoverageMatrix(matrix);
    }
  }, 480_000);

  for (const mode of MODES) {
    describe(`${mode} method-by-method evidence`, () => {
      test('matches the exact compiler-discovered public surface', () => {
        expect(trackerFor(mode).covered()).toEqual(EXPECTED_KEYS_BY_MODE[mode]);
      });

      for (const key of EXPECTED_KEYS_BY_MODE[mode]) {
        test(`${key} [${mode}]`, () => {
          const records = trackerFor(mode).recordsFor(key);
          expect(records.length).toBeGreaterThan(0);
          expect(records.every((record) => record.mode === mode)).toBe(true);
          expect(
            records.every((record) => record.source.startsWith('test/core-e2e/contracts/'))
          ).toBe(true);
        });
      }
    });
  }

  test('writes one complete human-readable matrix row per public method', () => {
    expect(matrix).toHaveLength(EXPECTED_KEYS.length);
    expect(
      matrix.every(
        (row) =>
          (row.applicable.embedded ? row.embedded.length > 0 : row.embedded.length === 0) &&
          (row.applicable.tcp ? row.tcp.length > 0 : row.tcp.length === 0)
      )
    ).toBe(true);
  });

  test('contracts contain no test doubles', async () => {
    const glob = new Bun.Glob('{contracts,support,fixtures}/**/*.ts');
    const forbidden = new RegExp(
      `\\b(?:${['mo' + 'ck', 'spy' + 'On', 'st' + 'ub'].join('|')})\\s*\\(`
    );
    const violations: string[] = [];
    for await (const relativePath of glob.scan(import.meta.dir)) {
      const text = await Bun.file(`${import.meta.dir}/${relativePath}`).text();
      if (forbidden.test(text)) violations.push(relativePath);
    }
    expect(violations).toEqual([]);
  });
});
