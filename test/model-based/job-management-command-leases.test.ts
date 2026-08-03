import { describe, expect, test } from 'bun:test';
import { MoveToDelayedCommand, MoveToWaitCommand } from './job-management-commands';
import { MODEL_JOB_IDS } from './model-ids';
import type { ModelState, QueueModel, RealQueue } from './queue-model-harness';

const JOB_ID = MODEL_JOB_IDS[0];
const LEASE_TOKEN = 'model-lease-token';

function createModel(state: ModelState): QueueModel {
  return {
    accepted: 1,
    concurrency: null,
    generations: new Map([[JOB_ID, 1]]),
    jobs: new Map([
      [
        JOB_ID,
        {
          attempts: 0,
          diskState: state === 'active' ? 'active' : state === 'delayed' ? 'delayed' : 'waiting',
          generation: 1,
          maxAttempts: 3,
          priority: 0,
          progress: 0,
          progressMessage: null,
          stallCount: 0,
          state,
        },
      ],
    ]),
    paused: false,
    rateLimit: null,
    rateRemaining: 0,
    removed: 0,
    terminalGenerations: new Set(),
  };
}

function createReal(withToken: boolean): {
  commands: Record<string, unknown>[];
  real: RealQueue;
} {
  const commands: Record<string, unknown>[] = [];
  const real = {
    assertConsistent: async () => undefined,
    queue: 'model-based',
    send: async (command: Record<string, unknown>) => {
      commands.push(command);
      return { ok: true };
    },
    tokens: new Map(withToken ? [[JOB_ID, LEASE_TOKEN]] : []),
  } as unknown as RealQueue;
  return { commands, real };
}

describe('model job-management commands preserve lease ownership', () => {
  test('active MoveToWait forwards the token captured by pull', async () => {
    const model = createModel('active');
    const { commands, real } = createReal(true);

    await new MoveToWaitCommand(0).run(model, real);

    expect(commands).toEqual([{ cmd: 'MoveToWait', id: JOB_ID, token: LEASE_TOKEN }]);
  });

  for (const change of [false, true]) {
    const name = change ? 'ChangeDelay' : 'MoveToDelayed';
    test(`active ${name} forwards the token captured by pull`, async () => {
      const model = createModel('active');
      const { commands, real } = createReal(true);

      await new MoveToDelayedCommand(0, change).run(model, real);

      expect(commands).toEqual([{ cmd: name, delay: 60_000, id: JOB_ID, token: LEASE_TOKEN }]);
    });
  }

  test('administrative transitions without an active lease omit the token', async () => {
    const cases: [ModelState, MoveToDelayedCommand | MoveToWaitCommand][] = [
      ['delayed', new MoveToWaitCommand(0)],
      ['failed', new MoveToWaitCommand(0)],
      ['waiting', new MoveToDelayedCommand(0, false)],
      ['waiting', new MoveToDelayedCommand(0, true)],
      ['prioritized', new MoveToDelayedCommand(0, false)],
      ['prioritized', new MoveToDelayedCommand(0, true)],
      ['delayed', new MoveToDelayedCommand(0, false)],
      ['delayed', new MoveToDelayedCommand(0, true)],
    ];

    for (const [state, command] of cases) {
      const model = createModel(state);
      const { commands, real } = createReal(false);
      await command.run(model, real);
      expect(commands[0]).not.toHaveProperty('token');
    }
  });
});
