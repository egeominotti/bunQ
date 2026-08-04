/**
 * Regression for cancellation registrations left behind when a Simple Mode
 * processor throws before returning a Promise.
 */
import { afterEach, describe, expect, test } from 'bun:test';
import { Bunqueue, shutdownManager } from '../src/client';

let app: Bunqueue<unknown, unknown> | null = null;

async function waitForState(
  instance: Bunqueue<unknown, unknown>,
  jobId: string,
  expected: 'completed' | 'failed'
): Promise<string> {
  const deadline = Date.now() + 5_000;
  let state = await instance.queue.getJobState(jobId);
  while (state !== expected && Date.now() < deadline) {
    await Bun.sleep(10);
    state = await instance.queue.getJobState(jobId);
  }
  return state;
}

async function waitForCircuitState(
  instance: Bunqueue<unknown, unknown>,
  expected: 'closed' | 'open' | 'half-open'
): Promise<string> {
  const deadline = Date.now() + 5_000;
  let state = instance.getCircuitState();
  while (state !== expected && Date.now() < deadline) {
    await Bun.sleep(10);
    state = instance.getCircuitState();
  }
  return state;
}

function expectNoCancellationRegistrations(
  instance: Bunqueue<unknown, unknown>,
  jobId: string
): void {
  expect(instance.getSignal(jobId)).toBeNull();
  const cancellation = (
    instance as unknown as {
      cancellation: {
        currentByJob: Map<string, unknown>;
        registrations: Map<AbortController, unknown>;
      };
    }
  ).cancellation;
  expect(cancellation.currentByJob.size).toBe(0);
  expect(cancellation.registrations.size).toBe(0);
}

afterEach(async () => {
  await app?.close(true);
  app = null;
  shutdownManager();
});

describe('Bunqueue synchronous processor cancellation cleanup', () => {
  test('a terminal synchronous throw releases every cancellation generation', async () => {
    shutdownManager();
    app = new Bunqueue(`sync-throw-cancellation-${process.pid}`, {
      embedded: true,
      heartbeatInterval: 0,
      processor: () => {
        throw new Error('synchronous processor failure');
      },
    });

    const job = await app.add('fail', {}, { attempts: 3, backoff: 1 });
    expect(await waitForState(app, job.id, 'failed')).toBe('failed');
    expectNoCancellationRegistrations(app, job.id);
  });

  test('a throwing circuit-breaker hook cannot bypass cancellation cleanup', async () => {
    shutdownManager();
    app = new Bunqueue(`throwing-circuit-hook-${process.pid}`, {
      embedded: true,
      heartbeatInterval: 0,
      processor: () => {
        throw new Error('synchronous processor failure');
      },
      circuitBreaker: {
        threshold: 1,
        onOpen: () => {
          throw new Error('user circuit-breaker hook failure');
        },
      },
    });

    const job = await app.add('fail', {}, { attempts: 1 });
    expect(await waitForState(app, job.id, 'failed')).toBe('failed');
    expectNoCancellationRegistrations(app, job.id);
  });

  test('Simple Mode retry handles synchronous throws and releases its generation', async () => {
    shutdownManager();
    let processorCalls = 0;
    app = new Bunqueue(`sync-throw-retry-${process.pid}`, {
      embedded: true,
      heartbeatInterval: 0,
      processor: () => {
        processorCalls++;
        if (processorCalls < 3) throw new Error('retry this synchronous failure');
        return { ok: true };
      },
      retry: { maxAttempts: 3, delay: 1, strategy: 'fixed' },
    });

    const job = await app.add('retry', {}, { attempts: 1 });
    expect(await waitForState(app, job.id, 'completed')).toBe('completed');
    expect(processorCalls).toBe(3);
    expectNoCancellationRegistrations(app, job.id);
  }, 10_000);

  test('closing during retry backoff prevents processor calls after close', async () => {
    shutdownManager();
    let processorCalls = 0;
    app = new Bunqueue(`close-during-sync-retry-${process.pid}`, {
      embedded: true,
      heartbeatInterval: 0,
      processor: () => {
        processorCalls++;
        throw new Error('retry until the application closes');
      },
      retry: { maxAttempts: 3, delay: 250, strategy: 'fixed' },
    });

    await app.add('retry', {}, { attempts: 1 });
    const firstAttemptDeadline = Date.now() + 2_000;
    while (processorCalls === 0 && Date.now() < firstAttemptDeadline) await Bun.sleep(10);
    expect(processorCalls).toBe(1);

    await app.close(true);
    app = null;
    const callsAtClose = processorCalls;
    await Bun.sleep(700);

    expect(processorCalls).toBe(callsAtClose);
  });

  test('cancelling during retry backoff clears the pending attempt', async () => {
    shutdownManager();
    let processorCalls = 0;
    app = new Bunqueue(`cancel-during-retry-${process.pid}`, {
      embedded: true,
      heartbeatInterval: 0,
      processor: async () => {
        processorCalls++;
        throw new Error('retry until the job is cancelled');
      },
      retry: { maxAttempts: 3, delay: 250, strategy: 'fixed' },
    });

    const job = await app.add('retry', {}, { attempts: 1 });
    const firstAttemptDeadline = Date.now() + 2_000;
    while (processorCalls === 0 && Date.now() < firstAttemptDeadline) await Bun.sleep(10);
    expect(processorCalls).toBe(1);

    app.cancel(job.id);
    expect(app.isCancelled(job.id)).toBe(true);
    const callsAtCancel = processorCalls;
    await Bun.sleep(700);

    expect(processorCalls).toBe(callsAtCancel);
    expect(await waitForState(app, job.id, 'failed')).toBe('failed');
    expectNoCancellationRegistrations(app, job.id);
  });

  test('closing an aborted retry cannot reopen the circuit breaker', async () => {
    shutdownManager();
    let processorCalls = 0;
    let openHookCalls = 0;
    app = new Bunqueue(`close-during-circuit-retry-${process.pid}`, {
      embedded: true,
      heartbeatInterval: 0,
      processor: async () => {
        processorCalls++;
        throw new Error('retry until the application closes');
      },
      retry: { maxAttempts: 3, delay: 250, strategy: 'fixed' },
      circuitBreaker: {
        threshold: 1,
        resetTimeout: 5_000,
        onOpen: () => {
          openHookCalls++;
        },
      },
    });

    await app.add('retry', {}, { attempts: 1 });
    const firstAttemptDeadline = Date.now() + 2_000;
    while (processorCalls === 0 && Date.now() < firstAttemptDeadline) await Bun.sleep(10);
    expect(processorCalls).toBe(1);

    const closedApp = app;
    await closedApp.close(true);
    app = null;
    await Bun.sleep(20);

    expect(openHookCalls).toBe(0);
    expect(closedApp.getCircuitState()).toBe('closed');
    const circuitTimer = (
      closedApp as unknown as { cb: { timer: ReturnType<typeof setTimeout> | null } }
    ).cb.timer;
    expect(circuitTimer).toBeNull();
  });

  test('a processor that ignores explicit cancellation still reports circuit success', async () => {
    shutdownManager();
    let releaseSuccess!: () => void;
    let markSuccessStarted!: () => void;
    const successStarted = new Promise<void>((resolve) => {
      markSuccessStarted = resolve;
    });
    const successRelease = new Promise<void>((resolve) => {
      releaseSuccess = resolve;
    });
    let closeHookCalls = 0;
    app = new Bunqueue(`cancelled-circuit-success-${process.pid}`, {
      embedded: true,
      heartbeatInterval: 0,
      processor: async (job) => {
        if (job.name === 'trip') throw new Error('open the circuit');
        markSuccessStarted();
        await successRelease;
        return { ok: true };
      },
      circuitBreaker: {
        threshold: 1,
        resetTimeout: 50,
        onClose: () => {
          closeHookCalls++;
        },
      },
    });

    const failedJob = await app.add('trip', {}, { attempts: 1 });
    expect(await waitForState(app, failedJob.id, 'failed')).toBe('failed');
    expect(await waitForCircuitState(app, 'half-open')).toBe('half-open');

    const successfulJob = await app.add('succeed', {}, { attempts: 1 });
    await successStarted;
    app.cancel(successfulJob.id);
    expect(app.isCancelled(successfulJob.id)).toBe(true);
    releaseSuccess();

    expect(await waitForState(app, successfulJob.id, 'completed')).toBe('completed');
    expect(app.getCircuitState()).toBe('closed');
    expect(closeHookCalls).toBe(1);
  }, 10_000);
});
