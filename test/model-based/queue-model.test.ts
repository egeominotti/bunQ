import { describe, test } from 'bun:test';
import fc from 'fast-check';
import { queueCommandArbitraries } from './queue-commands';
import { RealQueue, type QueueModel } from './queue-model-harness';

const DEFAULT_RUNS = 150;
const DEFAULT_COMMANDS = 80;

describe('Queue state-machine model against the real broker', () => {
  test('generated lifecycle, batch, dependency, control and SIGKILL sequences preserve invariants', async () => {
    const seed = optionalInteger(Bun.env.BUNQUEUE_MODEL_SEED);
    const numRuns = optionalInteger(Bun.env.BUNQUEUE_MODEL_RUNS) ?? DEFAULT_RUNS;
    const maxCommands = optionalInteger(Bun.env.BUNQUEUE_MODEL_COMMANDS) ?? DEFAULT_COMMANDS;
    let run = 0;

    await fc.assert(
      fc.asyncProperty(
        fc.commands(queueCommandArbitraries(), { maxCommands }),
        async (commands) => {
          const real = await RealQueue.create(`${seed ?? 'random'}-${run++}`);
          const model: QueueModel = {
            accepted: 0,
            concurrency: null,
            generations: new Map(),
            jobs: new Map(),
            paused: false,
            rateLimit: null,
            rateRemaining: 0,
            removed: 0,
            terminalGenerations: new Set(),
          };
          try {
            await fc.asyncModelRun(() => ({ model, real }), commands);
          } finally {
            await real.dispose();
          }
        }
      ),
      {
        endOnFailure: true,
        interruptAfterTimeLimit: 120000,
        numRuns,
        seed,
        verbose: 2,
      }
    );
  }, 130000);
});

function optionalInteger(value: string | undefined): number | undefined {
  if (value === undefined || value === '') return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`expected a signed safe integer, received ${value}`);
  }
  return parsed;
}
