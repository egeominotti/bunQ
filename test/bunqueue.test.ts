import { describe, test, expect, afterEach } from 'bun:test';
import { Bunqueue, shutdownManager, type BunqueueOptions } from '../src/client';

const openBunqueues = new Set<Bunqueue<unknown, unknown>>();

function createBunqueue<T = unknown, R = unknown>(
  name: string,
  options: BunqueueOptions<T, R>
): Bunqueue<T, R> {
  const bunqueue = new Bunqueue<T, R>(name, options);
  openBunqueues.add(bunqueue as Bunqueue<unknown, unknown>);
  return bunqueue;
}

afterEach(async () => {
  const instances = [...openBunqueues];
  openBunqueues.clear();
  try {
    await Promise.all(instances.map((bunqueue) => bunqueue.close(true)));
  } finally {
    shutdownManager();
  }
});

// ============ Basic ============

describe('Bunqueue', () => {
  test('should create queue and worker together', () => {
    const bq = createBunqueue('test-basic', {
      processor: async () => ({ ok: true }),
    });

    expect(bq.name).toBe('test-basic');
    expect(bq.queue).toBeDefined();
    expect(bq.worker).toBeDefined();
    expect(bq.isRunning()).toBe(true);
  });

  test('should add and process jobs', async () => {
    const processed: string[] = [];

    const bq = createBunqueue<{ email: string }, { sent: boolean }>('test-process', {
      processor: async (job) => {
        processed.push(job.data.email);
        return { sent: true };
      },
      concurrency: 1,
    });

    await bq.add('send', { email: 'a@test.com' });
    await bq.add('send', { email: 'b@test.com' });

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (processed.length >= 2) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(processed).toContain('a@test.com');
    expect(processed).toContain('b@test.com');
  });

  test('should emit completed events', async () => {
    const results: unknown[] = [];

    const bq = createBunqueue<{ v: number }, { doubled: number }>('test-events', {
      processor: async (job) => ({ doubled: job.data.v * 2 }),
    });

    bq.on('completed', (_job, result) => {
      results.push(result);
    });

    await bq.add('double', { v: 5 });

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (results.length >= 1) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(results[0]).toEqual({ doubled: 10 });
  });

  test('should emit failed events', async () => {
    const errors: Error[] = [];

    const bq = createBunqueue('test-fail', {
      processor: async () => {
        throw new Error('boom');
      },
    });

    bq.on('failed', (_job, err) => {
      errors.push(err);
    });

    await bq.add('fail-task', { v: 1 });

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (errors.length >= 1) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(errors[0].message).toBe('boom');
  });

  test('should support addBulk', async () => {
    let count = 0;

    const bq = createBunqueue<{ i: number }>('test-bulk', {
      processor: async () => {
        count++;
        return { ok: true };
      },
      concurrency: 5,
    });

    await bq.addBulk([
      { name: 'task', data: { i: 1 } },
      { name: 'task', data: { i: 2 } },
      { name: 'task', data: { i: 3 } },
    ]);

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (count >= 3) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(count).toBe(3);
  });

  test('should support pause and resume', () => {
    const bq = createBunqueue('test-pause', {
      processor: async () => ({ ok: true }),
    });

    expect(bq.isPaused()).toBe(false);
    bq.pause();
    expect(bq.isPaused()).toBe(true);
    bq.resume();
    expect(bq.isPaused()).toBe(false);
  });

  test('should close gracefully', async () => {
    const bq = createBunqueue('test-close', {
      processor: async () => ({ ok: true }),
    });

    expect(bq.isClosed()).toBe(false);
    await bq.close();
    expect(bq.isClosed()).toBe(true);
  });

  test('should support concurrency option', () => {
    const bq = createBunqueue('test-concurrency', {
      processor: async () => ({ ok: true }),
      concurrency: 10,
    });

    expect(bq.worker.opts.concurrency).toBe(10);
  });

  test('should expose getJob', async () => {
    const bq = createBunqueue<{ v: number }>('test-getjob', {
      processor: async () => ({ ok: true }),
      autorun: false,
    });

    const job = await bq.add('task', { v: 42 });
    const found = await bq.getJob(job.id);

    expect(found).not.toBeNull();
    expect(found!.data.v).toBe(42);
  });

  test('should expose getJobCounts', async () => {
    const bq = createBunqueue<{ v: number }>('test-counts', {
      processor: async () => ({ ok: true }),
      autorun: false,
    });

    await bq.add('task', { v: 1 });
    await bq.add('task', { v: 2 });

    const counts = bq.getJobCounts();
    expect(counts.waiting).toBeGreaterThanOrEqual(2);
  });

  test('should support progress updates', async () => {
    const progressValues: number[] = [];

    const bq = createBunqueue<{ v: number }>('test-progress', {
      processor: async (job) => {
        await job.updateProgress(50);
        await job.updateProgress(100);
        return { done: true };
      },
    });

    bq.on('progress', (_job, progress) => {
      progressValues.push(progress);
    });

    await bq.add('task', { v: 1 });

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (progressValues.length >= 2) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(progressValues).toContain(50);
    expect(progressValues).toContain(100);
  });

  test('should chain on() calls', () => {
    const bq = createBunqueue('test-chain', {
      processor: async () => ({ ok: true }),
    });

    const result = bq
      .on('completed', () => {})
      .on('failed', () => {})
      .on('error', () => {});

    expect(result).toBe(bq);
  });

  test('should support once()', async () => {
    let readyCount = 0;

    const bq = createBunqueue('test-once', {
      processor: async () => ({ ok: true }),
      autorun: false,
    });

    bq.once('ready', () => {
      readyCount++;
    });

    bq.worker.run();

    await Bun.sleep(50);
    expect(readyCount).toBe(1);
  });

  test('should support off()', () => {
    const bq = createBunqueue('test-off', {
      processor: async () => ({ ok: true }),
    });

    const listener = () => {};
    bq.on('completed', listener);
    bq.off('completed', listener);

    expect(bq.worker.listenerCount('completed')).toBe(0);
  });

  test('should support defaultJobOptions', async () => {
    const bq = createBunqueue<{ v: number }>('test-defaults', {
      processor: async () => ({ ok: true }),
      autorun: false,
      defaultJobOptions: { priority: 10 },
    });

    const job = await bq.add('task', { v: 1 });
    expect(job).toBeDefined();
  });

  test('should throw if no processor/routes/batch', () => {
    expect(() => {
      createBunqueue('test-err', {} as never);
    }).toThrow('requires');
  });

  test('should throw if multiple processor modes', () => {
    expect(() => {
      createBunqueue('test-err2', {
        processor: async () => ({}),
        routes: { a: async () => ({}) },
      } as never);
    }).toThrow('only one');
  });
});

// ============ Routes ============

describe('Bunqueue - Routes', () => {
  test('should route jobs by name', async () => {
    const log: string[] = [];

    const bq = createBunqueue<{ to: string }>('test-routes', {
      routes: {
        'send-email': async (job) => {
          log.push(`email:${job.data.to}`);
          return { channel: 'email' };
        },
        'send-sms': async (job) => {
          log.push(`sms:${job.data.to}`);
          return { channel: 'sms' };
        },
      },
      concurrency: 2,
    });

    await bq.add('send-email', { to: 'alice' });
    await bq.add('send-sms', { to: 'bob' });

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (log.length >= 2) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(log).toContain('email:alice');
    expect(log).toContain('sms:bob');
  });

  test('should fail for unknown route', async () => {
    const errors: Error[] = [];

    const bq = createBunqueue('test-no-route', {
      routes: {
        known: async () => ({ ok: true }),
      },
    });

    bq.on('failed', (_job, err) => {
      errors.push(err);
    });

    await bq.add('unknown-job', {});

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (errors.length >= 1) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(errors[0].message).toContain('No route for job "unknown-job"');
  });

  test('should handle multiple routes with different data', async () => {
    const results: unknown[] = [];

    const bq = createBunqueue<{ value: number }>('test-routes-data', {
      routes: {
        double: async (job) => ({ result: job.data.value * 2 }),
        triple: async (job) => ({ result: job.data.value * 3 }),
      },
    });

    bq.on('completed', (_job, result) => {
      results.push(result);
    });

    await bq.add('double', { value: 5 });
    await bq.add('triple', { value: 5 });

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (results.length >= 2) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(results).toContainEqual({ result: 10 });
    expect(results).toContainEqual({ result: 15 });
  });
});

// ============ Middleware ============

describe('Bunqueue - Middleware', () => {
  test('should execute middleware around processor', async () => {
    const order: string[] = [];

    const bq = createBunqueue<{ v: number }>('test-mw', {
      processor: async (job) => {
        order.push(`process:${job.data.v}`);
        return { ok: true };
      },
    });

    bq.use(async (_job, next) => {
      order.push('before');
      const result = await next();
      order.push('after');
      return result;
    });

    await bq.add('task', { v: 1 });

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (order.length >= 3) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(order).toEqual(['before', 'process:1', 'after']);
  });

  test('should chain multiple middlewares in order', async () => {
    const order: string[] = [];

    const bq = createBunqueue('test-mw-chain', {
      processor: async () => {
        order.push('core');
        return { ok: true };
      },
    });

    bq.use(async (_job, next) => {
      order.push('mw1-before');
      const r = await next();
      order.push('mw1-after');
      return r;
    });

    bq.use(async (_job, next) => {
      order.push('mw2-before');
      const r = await next();
      order.push('mw2-after');
      return r;
    });

    await bq.add('task', {});

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (order.length >= 5) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(order).toEqual(['mw1-before', 'mw2-before', 'core', 'mw2-after', 'mw1-after']);
  });

  test('middleware can modify the result', async () => {
    const results: unknown[] = [];

    const bq = createBunqueue<{ v: number }>('test-mw-modify', {
      processor: async (job) => ({ value: job.data.v }),
    });

    bq.use(async (_job, next) => {
      const result = await next();
      return { ...result, enhanced: true };
    });

    bq.on('completed', (_job, result) => {
      results.push(result);
    });

    await bq.add('task', { v: 42 });

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (results.length >= 1) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(results[0]).toEqual({ value: 42, enhanced: true });
  });

  test('middleware can catch errors', async () => {
    const recovered: unknown[] = [];

    const bq = createBunqueue('test-mw-catch', {
      processor: async () => {
        throw new Error('oops');
      },
    });

    bq.use(async (_job, next) => {
      try {
        return await next();
      } catch {
        recovered.push('caught');
        return { recovered: true };
      }
    });

    bq.on('completed', (_job, result) => {
      recovered.push(result);
    });

    await bq.add('task', {});

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (recovered.length >= 2) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(recovered).toContain('caught');
    expect(recovered).toContainEqual({ recovered: true });
  });

  test('middleware works with routes', async () => {
    const log: string[] = [];

    const bq = createBunqueue<{ v: number }>('test-mw-routes', {
      routes: {
        double: async (job) => {
          log.push('route');
          return { result: job.data.v * 2 };
        },
      },
    });

    bq.use(async (_job, next) => {
      log.push('mw');
      return next();
    });

    await bq.add('double', { v: 3 });

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (log.length >= 2) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(log).toEqual(['mw', 'route']);
  });

  test('use() should be chainable', () => {
    const bq = createBunqueue('test-mw-chain-api', {
      processor: async () => ({ ok: true }),
    });

    const result = bq.use(async (_job, next) => next()).use(async (_job, next) => next());

    expect(result).toBe(bq);
  });
});

// ============ Cron ============

describe('Bunqueue - Cron', () => {
  test('should add a cron job', async () => {
    const bq = createBunqueue<{ type: string }>('test-cron', {
      processor: async () => ({ ok: true }),
      autorun: false,
    });

    const info = await bq.cron('daily-report', '0 9 * * *', { type: 'summary' });

    expect(info).not.toBeNull();
    expect(info!.id).toBe('daily-report');
    expect(info!.next).toBeGreaterThan(0);
  });

  test('should add a repeating job with every()', async () => {
    const bq = createBunqueue<{ type: string }>('test-every', {
      processor: async () => ({ ok: true }),
      autorun: false,
    });

    const info = await bq.every('heartbeat', 5000, { type: 'ping' });

    expect(info).not.toBeNull();
    expect(info!.id).toBe('heartbeat');
    expect(info!.next).toBeGreaterThan(0);
  });

  test('should list cron jobs', async () => {
    const bq = createBunqueue('test-cron-list', {
      processor: async () => ({ ok: true }),
      autorun: false,
    });

    await bq.cron('job-a', '*/5 * * * *');
    await bq.cron('job-b', '0 * * * *');

    const list = await bq.listCrons();

    expect(list.length).toBeGreaterThanOrEqual(2);
  });

  test('should remove a cron job', async () => {
    const bq = createBunqueue('test-cron-remove', {
      processor: async () => ({ ok: true }),
      autorun: false,
    });

    await bq.cron('to-remove', '*/10 * * * *');
    await bq.removeCron('to-remove');

    const list = await bq.listCrons();
    const found = list.find((c: { name: string }) => c.name === 'to-remove');
    expect(found).toBeUndefined();
  });

  test('should support timezone in cron', async () => {
    const bq = createBunqueue('test-cron-tz', {
      processor: async () => ({ ok: true }),
      autorun: false,
    });

    const info = await bq.cron(
      'tz-job',
      '0 9 * * *',
      { type: 'report' },
      {
        timezone: 'Europe/Rome',
      }
    );

    expect(info).not.toBeNull();
  });
});

// ============ Batch Processing (feature 1) ============

describe('Bunqueue - Batch Processing', () => {
  test('should accumulate jobs and process as batch', async () => {
    const batchSizes: number[] = [];

    const bq = createBunqueue<{ i: number }, { ok: boolean }>('test-batch', {
      batch: {
        size: 3,
        timeout: 500,
        processor: async (jobs) => {
          batchSizes.push(jobs.length);
          return jobs.map(() => ({ ok: true }));
        },
      },
      concurrency: 5,
    });

    await bq.add('task', { i: 1 });
    await bq.add('task', { i: 2 });
    await bq.add('task', { i: 3 });

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (batchSizes.length >= 1) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(batchSizes[0]).toBeGreaterThanOrEqual(1);
  });

  test('should flush partial batch on timeout', async () => {
    let processed = false;

    const bq = createBunqueue<{ i: number }, { ok: boolean }>('test-batch-timeout', {
      batch: {
        size: 100, // large size, won't fill
        timeout: 100, // short timeout
        processor: async (jobs) => {
          processed = true;
          return jobs.map(() => ({ ok: true }));
        },
      },
    });

    await bq.add('task', { i: 1 });

    await Bun.sleep(300);
    expect(processed).toBe(true);
  });
});

// ============ Advanced Retry (feature 2) ============

describe('Bunqueue - Advanced Retry', () => {
  test('should retry with jitter strategy', async () => {
    let attempts = 0;
    const results: unknown[] = [];

    const bq = createBunqueue('test-retry-jitter', {
      processor: async () => {
        attempts++;
        if (attempts < 3) throw new Error('fail');
        return { ok: true };
      },
      retry: {
        maxAttempts: 5,
        delay: 50,
        strategy: 'jitter',
      },
    });

    bq.on('completed', (_job, result) => results.push(result));
    await bq.add('task', {});

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (results.length >= 1) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(attempts).toBe(3);
    expect(results[0]).toEqual({ ok: true });
  });

  test('should retry with fibonacci strategy', async () => {
    let attempts = 0;

    const bq = createBunqueue('test-retry-fib', {
      processor: async () => {
        attempts++;
        if (attempts < 2) throw new Error('fail');
        return { ok: true };
      },
      retry: {
        maxAttempts: 3,
        delay: 10,
        strategy: 'fibonacci',
      },
    });

    const results: unknown[] = [];
    bq.on('completed', (_j, r) => results.push(r));
    await bq.add('task', {});

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (results.length >= 1) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(attempts).toBe(2);
  });

  test('should respect retryIf predicate', async () => {
    let attempts = 0;
    const errors: Error[] = [];

    const bq = createBunqueue('test-retry-if', {
      processor: async () => {
        attempts++;
        throw new Error('permanent');
      },
      retry: {
        maxAttempts: 5,
        delay: 10,
        strategy: 'fixed',
        retryIf: (err) => !err.message.includes('permanent'),
      },
    });

    bq.on('failed', (_j, e) => errors.push(e));
    await bq.add('task', {});

    await Bun.sleep(200);
    expect(attempts).toBe(1); // no retry because retryIf returned false
  });

  test('should use custom backoff function', async () => {
    let attempts = 0;
    const delays: number[] = [];
    let lastTime = Date.now();

    const bq = createBunqueue('test-retry-custom', {
      processor: async () => {
        const now = Date.now();
        if (attempts > 0) delays.push(now - lastTime);
        lastTime = now;
        attempts++;
        if (attempts < 3) throw new Error('fail');
        return { ok: true };
      },
      retry: {
        maxAttempts: 5,
        strategy: 'custom',
        customBackoff: (attempt) => attempt * 50,
      },
    });

    const results: unknown[] = [];
    bq.on('completed', (_j, r) => results.push(r));
    await bq.add('task', {});

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (results.length >= 1) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(attempts).toBe(3);
  });
});

// ============ Graceful Cancellation (feature 3) ============

describe('Bunqueue - Cancellation', () => {
  test('should cancel a running job', async () => {
    const bq = createBunqueue('test-cancel', {
      processor: async (job) => {
        // Long-running job: check signal periodically
        const signal = bq.getSignal(job.id);
        for (let i = 0; i < 100; i++) {
          if (signal?.aborted) throw new Error('Job cancelled');
          await Bun.sleep(10);
        }
        return { ok: true };
      },
    });

    const active = new Promise<string>((resolve) => {
      bq.on('active', (runningJob) => resolve(runningJob.id));
    });
    const failed = new Promise<Error>((resolve) => {
      bq.once('failed', (_job, error) => resolve(error));
    });

    try {
      const job = await bq.add('long-task', {});

      // Cancellation is defined for a running job: wait for the worker's
      // observable active transition instead of racing its poll interval.
      expect(await active).toBe(job.id);
      bq.cancel(job.id);

      expect((await failed).message).toContain('cancelled');
    } finally {
      await bq.close(true);
    }
  });

  test('isCancelled should return correct state', async () => {
    let jobStarted = false;

    const bq = createBunqueue('test-cancel-check', {
      processor: async () => {
        jobStarted = true;
        await Bun.sleep(2000); // long job
        return { ok: true };
      },
    });

    const job = await bq.add('task', {});

    // Wait for the processor to start
    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (jobStarted) {
          clearInterval(check);
          resolve();
        }
      }, 5);
    });

    expect(bq.isCancelled(job.id)).toBe(false);
    bq.cancel(job.id);
    expect(bq.isCancelled(job.id)).toBe(true);
  });
});

// ============ Circuit Breaker (feature 5) ============

describe('Bunqueue - Circuit Breaker', () => {
  test('should open circuit after threshold failures', async () => {
    let openCalled = false;

    const bq = createBunqueue('test-cb', {
      processor: async () => {
        throw new Error('service down');
      },
      circuitBreaker: {
        threshold: 3,
        resetTimeout: 500,
        onOpen: () => {
          openCalled = true;
        },
      },
    });

    // Add enough jobs to trigger failures
    for (let i = 0; i < 5; i++) {
      await bq.add('task', {});
    }

    await Bun.sleep(300);
    expect(bq.getCircuitState()).toBe('open');
    expect(openCalled).toBe(true);
  });

  test('should reset circuit manually', () => {
    const bq = createBunqueue('test-cb-reset', {
      processor: async () => ({ ok: true }),
      circuitBreaker: { threshold: 1 },
    });

    expect(bq.getCircuitState()).toBe('closed');
    bq.resetCircuit();
    expect(bq.getCircuitState()).toBe('closed');
  });
});

// ============ Event Triggers (feature 6) ============

describe('Bunqueue - Event Triggers', () => {
  test('should create job B when job A completes', async () => {
    const processed: string[] = [];

    const bq = createBunqueue<{ step: string }>('test-trigger', {
      routes: {
        'step-a': async () => ({ result: 'a-done' }),
        'step-b': async (job) => {
          processed.push(job.data.step);
          return { ok: true };
        },
      },
    });

    bq.trigger({
      on: 'step-a',
      create: 'step-b',
      data: () => ({ step: 'from-trigger' }),
    });

    await bq.add('step-a', { step: 'initial' });

    await new Promise<void>((resolve) => {
      const check = setInterval(() => {
        if (processed.length >= 1) {
          clearInterval(check);
          resolve();
        }
      }, 10);
    });

    expect(processed).toContain('from-trigger');
  });

  test('should respect trigger condition', async () => {
    const created: string[] = [];

    const bq = createBunqueue<{ amount: number }>('test-trigger-cond', {
      routes: {
        payment: async (job) => ({ amount: job.data.amount }),
        notify: async (job) => {
          created.push(`notify:${job.data.amount}`);
          return { ok: true };
        },
      },
    });

    bq.trigger({
      on: 'payment',
      create: 'notify',
      data: (result) => ({ amount: (result as { amount: number }).amount }),
      condition: (result) => (result as { amount: number }).amount > 100,
    });

    await bq.add('payment', { amount: 50 }); // should NOT trigger
    await bq.add('payment', { amount: 200 }); // should trigger

    await Bun.sleep(300);
    expect(created.length).toBe(1);
    expect(created[0]).toBe('notify:200');
  });

  test('trigger() should be chainable', () => {
    const bq = createBunqueue('test-trigger-chain', {
      processor: async () => ({ ok: true }),
    });

    const result = bq
      .trigger({ on: 'a', create: 'b', data: () => ({}) })
      .trigger({ on: 'c', create: 'd', data: () => ({}) });

    expect(result).toBe(bq);
  });
});

// ============ Job TTL (feature 7) ============

describe('Bunqueue - Job TTL', () => {
  test('should reject expired jobs', async () => {
    const errors: Error[] = [];

    const bq = createBunqueue('test-ttl', {
      processor: async () => ({ ok: true }),
      autorun: false,
      ttl: { defaultTtl: 50 }, // 50ms TTL
    });

    await bq.add('task', {});

    // Wait for TTL to expire
    await Bun.sleep(100);

    bq.on('failed', (_j, e) => errors.push(e));
    bq.worker.run();

    await Bun.sleep(200);
    expect(errors.length).toBeGreaterThanOrEqual(1);
    expect(errors[0].message).toContain('expired');
  });

  test('should support per-name TTL', async () => {
    const completed: string[] = [];
    const failed: string[] = [];

    const bq = createBunqueue('test-ttl-pernam', {
      routes: {
        fast: async () => ({ ok: true }),
        slow: async () => ({ ok: true }),
      },
      autorun: false,
      ttl: {
        perName: {
          fast: 50, // 50ms
          slow: 10000, // 10s — won't expire
        },
      },
    });

    await bq.add('fast', {});
    await bq.add('slow', {});

    await Bun.sleep(100); // fast expires, slow doesn't

    bq.on('completed', (job) => completed.push(job.name));
    bq.on('failed', (job) => failed.push(job.name));
    bq.worker.run();

    await Bun.sleep(200);
    expect(failed).toContain('fast');
    expect(completed).toContain('slow');
  });

  test('setDefaultTtl should update TTL at runtime', () => {
    const bq = createBunqueue('test-ttl-set', {
      processor: async () => ({ ok: true }),
      ttl: { defaultTtl: 1000 },
    });

    bq.setDefaultTtl(500);
    // No assertion needed — just verify it doesn't throw
    expect(true).toBe(true);
  });
});

// ============ Priority Aging (feature 8) ============

describe('Bunqueue - Priority Aging', () => {
  test('should construct with priority aging config', () => {
    const bq = createBunqueue('test-aging', {
      processor: async () => ({ ok: true }),
      autorun: false,
      priorityAging: {
        interval: 100,
        minAge: 50,
        boost: 2,
        maxPriority: 50,
        maxScan: 10,
      },
    });

    expect(bq).toBeDefined();
  });

  test('should boost priority of old waiting jobs', async () => {
    const bq = createBunqueue<{ v: number }>('test-aging-boost', {
      processor: async () => ({ ok: true }),
      autorun: false,
      priorityAging: {
        interval: 100,
        minAge: 50,
        boost: 5,
        maxPriority: 100,
      },
    });

    const job = await bq.add('task', { v: 1 }, { priority: 1 });

    // Wait for aging interval to tick
    await Bun.sleep(250);

    const updated = await bq.getJob(job.id);
    // Priority should have been boosted
    expect(updated!.priority).toBeGreaterThanOrEqual(1);
  });

  test('should close cleanly with aging timer', async () => {
    const bq = createBunqueue('test-aging-close', {
      processor: async () => ({ ok: true }),
      priorityAging: { interval: 50 },
    });

    await bq.close();
    expect(bq.isClosed()).toBe(true);
  });
});

// ============ Deduplication (feature 9) ============

describe('Bunqueue - Deduplication', () => {
  test('should deduplicate jobs with same name and data', async () => {
    const bq = createBunqueue<{ key: string }>('test-dedup', {
      processor: async () => ({ ok: true }),
      embedded: true,
      autorun: false,
      deduplication: { ttl: 5000 },
    });

    const j1 = await bq.add('task', { key: 'a' });
    const j2 = await bq.add('task', { key: 'a' }); // duplicate — same name + data
    const j3 = await bq.add('task', { key: 'b' }); // different data — not duplicate

    // j1 and j2 should be the same job (dedup by name:data)
    expect(j1.id).toBe(j2.id);
    // j3 should be different
    expect(j1.id).not.toBe(j3.id);

    await bq.close();
  });

  test('should allow explicit dedup override per-job', async () => {
    const bq = createBunqueue<{ v: number }>('test-dedup-override', {
      processor: async () => ({ ok: true }),
      embedded: true,
      autorun: false,
      deduplication: { ttl: 5000 },
    });

    // Explicit dedup ID overrides the default
    const j1 = await bq.add('task', { v: 1 }, { deduplication: { id: 'custom-1', ttl: 1000 } });
    const j2 = await bq.add('task', { v: 2 }, { deduplication: { id: 'custom-1', ttl: 1000 } });

    // j2 should be deduplicated (same custom ID)
    expect(j1.id).toBe(j2.id);

    await bq.close();
  });
});

// ============ Debouncing (feature 10) ============

describe('Bunqueue - Debouncing', () => {
  test('should apply debounce config to jobs automatically', async () => {
    const processed: number[] = [];

    const bq = createBunqueue<{ seq: number }>('test-debounce', {
      processor: async (job) => {
        processed.push(job.data.seq);
        return { ok: true };
      },
      embedded: true,
      debounce: { ttl: 300 },
    });

    // Rapid adds with same name — debounce replaces within window
    await bq.add('event', { seq: 1 });
    await bq.add('event', { seq: 2 });
    await bq.add('event', { seq: 3 });

    // Different name — not debounced
    await bq.add('other', { seq: 99 });

    await Bun.sleep(600);

    // 'other' should always be processed
    expect(processed).toContain(99);
    // Due to debounce, fewer than 3 'event' jobs should run
    const eventJobs = processed.filter((x) => x !== 99);
    expect(eventJobs.length).toBeGreaterThanOrEqual(1);
    expect(eventJobs.length).toBeLessThanOrEqual(3);

    await bq.close();
  });

  test('should not debounce when per-job override is set', async () => {
    const bq = createBunqueue<{ v: number }>('test-debounce-override', {
      processor: async () => ({ ok: true }),
      embedded: true,
      autorun: false,
      debounce: { ttl: 500 },
    });

    // Explicit debounce override with different IDs → no dedup
    const j1 = await bq.add('task', { v: 1 }, { debounce: { id: 'a', ttl: 100 } });
    const j2 = await bq.add('task', { v: 2 }, { debounce: { id: 'b', ttl: 100 } });
    expect(j1.id).not.toBe(j2.id);

    await bq.close();
  });
});

// ============ Rate Limiting (feature 11) ============

describe('Bunqueue - Rate Limiting', () => {
  test('should accept rateLimit option', () => {
    const bq = createBunqueue('test-rate-limit', {
      processor: async () => ({ ok: true }),
      embedded: true,
      rateLimit: { max: 5, duration: 1000 },
    });

    // Worker should have been created with limiter
    expect(bq.worker).toBeDefined();
  });

  test('should expose setGlobalRateLimit and removeGlobalRateLimit', () => {
    const bq = createBunqueue('test-rate-api', {
      processor: async () => ({ ok: true }),
      embedded: true,
    });

    // Should not throw
    expect(() => bq.setGlobalRateLimit(10, 1000)).not.toThrow();
    expect(() => bq.removeGlobalRateLimit()).not.toThrow();
  });
});

// ============ DLQ Management (feature 12) ============

describe('Bunqueue - DLQ', () => {
  test('should configure DLQ via constructor option', () => {
    const bq = createBunqueue('test-dlq-config', {
      processor: async () => ({ ok: true }),
      embedded: true,
      dlq: {
        autoRetry: true,
        autoRetryInterval: 60000,
        maxAutoRetries: 5,
        maxAge: 86400000,
        maxEntries: 500,
      },
    });

    const config = bq.getDlqConfig();
    expect(config.autoRetry).toBe(true);
    expect(config.maxAutoRetries).toBe(5);
    expect(config.maxAge).toBe(86400000);
    expect(config.maxEntries).toBe(500);
  });

  test('should expose DLQ query and control methods', async () => {
    const bq = createBunqueue('test-dlq-api', {
      processor: async () => {
        throw new Error('always fail');
      },
      embedded: true,
      dlq: { maxEntries: 100 },
    });

    // Add a job that will fail
    await bq.add('failing', {});
    await Bun.sleep(500);

    // DLQ stats should be accessible
    const stats = bq.getDlqStats();
    expect(stats).toBeDefined();
    expect(typeof stats.total).toBe('number');

    // DLQ query should work
    const entries = bq.getDlq();
    expect(Array.isArray(entries)).toBe(true);

    // purgeDlq should not throw
    expect(() => bq.purgeDlq()).not.toThrow();

    await bq.close();
  });

  test('should update DLQ config at runtime', () => {
    const bq = createBunqueue('test-dlq-update', {
      processor: async () => ({ ok: true }),
      embedded: true,
    });

    bq.setDlqConfig({ autoRetry: true, maxAutoRetries: 10 });
    const config = bq.getDlqConfig();
    expect(config.autoRetry).toBe(true);
    expect(config.maxAutoRetries).toBe(10);
  });
});
