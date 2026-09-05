import { expect, test } from 'bun:test';
import fc from 'fast-check';
import * as native from '../src/client';
import { advance, model, ParityBroker, type Operation } from './client-parity-model-harness';

// Public declarations are checked separately; private class fields are nominal
// across independent builds, so one adapter runs the same real-broker history.
const portable = (await import('../sdk/typescript/dist/index.js')) as unknown as typeof native;

function integer(name: string, fallback?: number): number | undefined {
  const value = process.env[name];
  if (value === undefined) return fallback;
  const result = Number(value);
  if (!value || !Number.isSafeInteger(result)) throw new Error(`${name} must be a signed integer`);
  return result;
}

const operation: fc.Arbitrary<Operation> = fc.record({
  kind: fc.constantFrom(
    'add',
    'bulk',
    'update',
    'priority',
    'remove',
    'pause',
    'resume',
    'group',
    'query'
  ),
  slot: fc.integer({ min: 0, max: 5 }),
  value: fc.integer({ min: -100, max: 100 }),
  priority: fc.integer({ min: 0, max: 5 }),
});

test('generated histories preserve complete client parity and independent conservation', async () => {
  const seed = integer('BUNQUEUE_CLIENT_PARITY_SEED') ?? fc.sample(fc.integer(), 1)[0];
  const numRuns = integer('BUNQUEUE_CLIENT_PARITY_RUNS', 30);
  const maxLength = integer('BUNQUEUE_CLIENT_PARITY_COMMANDS', 30);
  if (!numRuns || numRuns < 1 || !maxLength || maxLength < 1) {
    throw new Error('Parity runs and commands must be positive');
  }
  const path = process.env.BUNQUEUE_CLIENT_PARITY_PATH;
  console.log(
    `Client parity seed=${seed} runs=${numRuns} maxCommands=${maxLength} path=${path ?? ''}`
  );
  await fc.assert(
    fc.asyncProperty(
      fc.array(operation, { minLength: 1, maxLength, size: 'max' }),
      async (history) => {
        const canonical = new ParityBroker(native);
        const network = new ParityBroker(portable);
        const expected = model();
        try {
          for (const command of history) {
            expect(await network.execute(command)).toEqual(await canonical.execute(command));
            advance(expected, command);
            expect(await network.snapshot(expected)).toEqual(await canonical.snapshot(expected));
          }
        } finally {
          await Promise.all([canonical.close(), network.close()]);
        }
      }
    ),
    { seed, numRuns, path, verbose: 2 }
  );
}, 120_000);

test('the differential oracle detects a client priority-option mutation', async () => {
  const canonical = new ParityBroker(native);
  const network = new ParityBroker(portable);
  const command: Operation = { kind: 'add', slot: 0, value: 1, priority: 2 };
  const expected = model();
  try {
    await canonical.execute(command);
    // Mutate the input at the adapter boundary, never production or broker code.
    await network.execute({ ...command, priority: 3 });
    advance(expected, command);
    await canonical.snapshot(expected);
    await expect(network.snapshot(expected)).rejects.toThrow();
  } finally {
    await Promise.all([canonical.close(), network.close()]);
  }
});
