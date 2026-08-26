import { describe, expect, test } from 'bun:test';
import {
  createServerShutdown,
  type ServerShutdownResources,
  type ServerShutdownRuntime,
} from '../src/infrastructure/server/shutdownCoordinator';

interface Harness {
  readonly resources: ServerShutdownResources;
  readonly runtime: Partial<ServerShutdownRuntime>;
  readonly exits: number[];
  readonly actions: string[];
}

function harness(
  overrides: Partial<ServerShutdownResources> = {},
  runtimeOverrides: Partial<ServerShutdownRuntime> = {}
): Harness {
  const exits: number[] = [];
  const actions: string[] = [];
  return {
    exits,
    actions,
    resources: {
      shutdownTimeoutMs: 0,
      stopStats: () => actions.push('stats'),
      stopTcp: () => actions.push('tcp'),
      stopHttp: () => actions.push('http'),
      getActiveJobs: () => 0,
      emitShutdown: (signal) => actions.push(`emit:${signal}`),
      shutdownStorage: async () => {
        actions.push('storage');
      },
      ...overrides,
    },
    runtime: {
      exit: (code) => exits.push(code),
      stopRateLimiter: () => actions.push('rate-limiter'),
      info: () => undefined,
      warn: () => undefined,
      error: () => undefined,
      cloudTimeoutMs: 20,
      storageAttemptTimeoutMs: 20,
      ...runtimeOverrides,
    },
  };
}

describe('server shutdown coordinator', () => {
  test('retries one transient storage failure and exits successfully', async () => {
    let attempts = 0;
    const state = harness({
      shutdownStorage: async () => {
        attempts++;
        if (attempts === 1) throw new Error('transient');
      },
    });

    await createServerShutdown(state.resources, state.runtime)('SIGTERM');

    expect(attempts).toBe(2);
    expect(state.exits).toEqual([0]);
    expect(state.actions).toEqual(['stats', 'rate-limiter', 'tcp', 'http', 'emit:SIGTERM']);
  });

  test('bounds permanent storage failure and exits with an error', async () => {
    let attempts = 0;
    const state = harness({
      shutdownStorage: async () => {
        attempts++;
        throw new Error('permanent');
      },
    });

    await createServerShutdown(state.resources, state.runtime)('SIGINT');

    expect(attempts).toBe(2);
    expect(state.exits).toEqual([1]);
  });

  test('continues through backup and Cloud failures before storage', async () => {
    let storageCalls = 0;
    const state = harness({
      stopBackup: () => {
        throw new Error('backup');
      },
      stopCloud: async () => {
        throw new Error('cloud');
      },
      shutdownStorage: async () => {
        storageCalls++;
      },
    });

    await createServerShutdown(state.resources, state.runtime)('SIGTERM');

    expect(storageCalls).toBe(1);
    expect(state.exits).toEqual([0]);
  });

  test('coalesces duplicate signals and preserves the first signal', async () => {
    const state = harness();
    const shutdown = createServerShutdown(state.resources, state.runtime);
    const first = shutdown('SIGINT');
    const second = shutdown('SIGTERM');

    expect(second).toBe(first);
    await first;
    expect(state.actions.filter((action) => action === 'storage')).toHaveLength(1);
    expect(state.actions).toContain('emit:SIGINT');
    expect(state.actions).not.toContain('emit:SIGTERM');
    expect(state.exits).toEqual([0]);
  });

  test('times out pending Cloud and storage cleanup without rejecting', async () => {
    let storageCalls = 0;
    const pending = () => new Promise<void>(() => undefined);
    const state = harness(
      {
        stopCloud: pending,
        shutdownStorage: () => {
          storageCalls++;
          return pending();
        },
      },
      { cloudTimeoutMs: 5, storageAttemptTimeoutMs: 5 }
    );

    await createServerShutdown(state.resources, state.runtime)('SIGTERM');

    expect(storageCalls).toBe(2);
    expect(state.exits).toEqual([1]);
  });
});
