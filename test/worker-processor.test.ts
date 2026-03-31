/**
 * Worker Processor & AckBatcher Internals Tests
 * Unit tests for processJob() and AckBatcher class
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { EventEmitter } from 'events';
import { processJob, type ProcessorConfig } from '../src/client/worker/processor';
import { AckBatcher, type AckBatcherConfig } from '../src/client/worker/ackBatcher';
import { createJob, generateJobId, type Job as InternalJob } from '../src/domain/types/job';
import { shutdownManager } from '../src/client/manager';
import type { Job, Processor } from '../src/client/types';
import type { TcpConnection } from '../src/client/worker/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Create a minimal internal job for testing */
function makeJob(
  overrides: Partial<{ queue: string; data: unknown; priority: number }> = {},
): InternalJob {
  return createJob(generateJobId(), overrides.queue ?? 'test-q', {
    data: overrides.data ?? { name: 'send', email: 'test@test.com' },
    priority: overrides.priority ?? 0,
  });
}

/** Mock TCP connection that records sent commands */
function mockTcp(
  handler?: (cmd: Record<string, unknown>) => Promise<Record<string, unknown>>,
): { tcp: TcpConnection; sent: Record<string, unknown>[] } {
  const sent: Record<string, unknown>[] = [];
  const tcp: TcpConnection = {
    send: async (cmd) => {
      sent.push(cmd);
      if (handler) return handler(cmd);
      return { ok: true };
    },
  };
  return { tcp, sent };
}

/**
 * Build a ProcessorConfig in TCP mode with a mock connection.
 * This avoids needing real jobs in the QueueManager.
 */
function makeConfig<T = unknown, R = unknown>(
  processor: Processor<T, R>,
  overrides: Partial<ProcessorConfig<T, R>> = {},
): ProcessorConfig<T, R> {
  const { tcp } = mockTcp();
  const ackBatcher = new AckBatcher({
    batchSize: 10,
    interval: 50,
    embedded: false,
  });
  ackBatcher.setTcp(tcp);

  return {
    name: 'test-q',
    processor,
    embedded: false,
    tcp,
    ackBatcher,
    emitter: new EventEmitter(),
    token: undefined,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// processJob tests
// ---------------------------------------------------------------------------

describe('processJob', () => {
  afterEach(() => {
    shutdownManager();
  });

  // ------------------------------------------------------------------
  // Process single job
  // ------------------------------------------------------------------
  test('should process a single job and emit completed', async () => {
    const events: string[] = [];
    const emitter = new EventEmitter();
    emitter.on('active', () => events.push('active'));
    emitter.on('completed', () => events.push('completed'));

    const processor: Processor<unknown, string> = async () => 'done';
    const config = makeConfig(processor, { emitter });
    const job = makeJob();

    await processJob(job, config);

    expect(events).toEqual(['active', 'completed']);
  });

  // ------------------------------------------------------------------
  // Processor return value becomes job result
  // ------------------------------------------------------------------
  test('should pass return value to completed event', async () => {
    let capturedResult: unknown = undefined;
    const emitter = new EventEmitter();
    emitter.on('completed', (_job: Job, result: unknown) => {
      capturedResult = result;
    });

    const processor: Processor<unknown, { ok: boolean }> = async () => ({ ok: true });
    const config = makeConfig(processor, { emitter });

    await processJob(makeJob(), config);

    expect(capturedResult).toEqual({ ok: true });
  });

  test('should set returnvalue on the public job object', async () => {
    let capturedJob: Job | undefined;
    const emitter = new EventEmitter();
    emitter.on('completed', (job: Job) => {
      capturedJob = job;
    });

    const processor: Processor<unknown, number> = async () => 42;
    const config = makeConfig(processor, { emitter });

    await processJob(makeJob(), config);

    expect(capturedJob).toBeDefined();
    expect(capturedJob!.returnvalue).toBe(42);
  });

  // ------------------------------------------------------------------
  // Processor receives correct job object
  // ------------------------------------------------------------------
  test('should pass a Job with correct id, queueName and data to the processor', async () => {
    let receivedJob: Job | undefined;

    const processor: Processor<{ email: string }, void> = async (job) => {
      receivedJob = job as unknown as Job;
    };

    const internal = makeJob({ data: { name: 'send', email: 'hello@world.com' } });
    const config = makeConfig(processor);

    await processJob(internal, config);

    expect(receivedJob).toBeDefined();
    expect(receivedJob!.id).toBe(String(internal.id));
    expect(receivedJob!.queueName).toBe('test-q');
    // The job data should have the user data (with 'name' stripped by extractUserData)
    expect((receivedJob!.data as { email: string }).email).toBe('hello@world.com');
  });

  test('should use job data name as the public job name', async () => {
    let receivedJob: Job | undefined;
    const processor: Processor<unknown, void> = async (job) => {
      receivedJob = job as unknown as Job;
    };

    const internal = makeJob({ data: { name: 'my-task', payload: 1 } });
    const config = makeConfig(processor);

    await processJob(internal, config);

    expect(receivedJob!.name).toBe('my-task');
  });

  test('should default job name to "default" when data has no name', async () => {
    let receivedJob: Job | undefined;
    const processor: Processor<unknown, void> = async (job) => {
      receivedJob = job as unknown as Job;
    };

    const internal = makeJob({ data: { payload: 1 } });
    const config = makeConfig(processor);

    await processJob(internal, config);

    expect(receivedJob!.name).toBe('default');
  });

  // ------------------------------------------------------------------
  // Error handling in processor function
  // ------------------------------------------------------------------
  test('should emit failed when processor throws an Error', async () => {
    let failedErr: Error | undefined;
    let failedJob: Job | undefined;
    const emitter = new EventEmitter();
    emitter.on('failed', (job: Job, err: Error) => {
      failedJob = job;
      failedErr = err;
    });

    const { tcp } = mockTcp();
    const processor: Processor<unknown, void> = async () => {
      throw new Error('boom');
    };
    const config = makeConfig(processor, { emitter, tcp });

    await processJob(makeJob(), config);

    expect(failedErr).toBeDefined();
    expect(failedErr!.message).toBe('boom');
    expect(failedJob).toBeDefined();
  });

  test('should set failedReason on the job when processor throws', async () => {
    let failedJob: Job | undefined;
    const emitter = new EventEmitter();
    emitter.on('failed', (job: Job) => {
      failedJob = job;
    });

    const { tcp } = mockTcp();
    const processor: Processor<unknown, void> = async () => {
      throw new Error('something went wrong');
    };
    const config = makeConfig(processor, { emitter, tcp });

    await processJob(makeJob(), config);

    expect(failedJob!.failedReason).toBe('something went wrong');
  });

  test('should handle non-Error thrown values by wrapping them', async () => {
    let failedErr: Error | undefined;
    const emitter = new EventEmitter();
    emitter.on('failed', (_job: Job, err: Error) => {
      failedErr = err;
    });

    const { tcp } = mockTcp();
    const processor: Processor<unknown, void> = async () => {
      throw 'string-error'; // eslint-disable-line no-throw-literal
    };
    const config = makeConfig(processor, { emitter, tcp });

    await processJob(makeJob(), config);

    expect(failedErr).toBeInstanceOf(Error);
    expect(failedErr!.message).toBe('string-error');
  });

  test('should not emit completed when processor throws', async () => {
    const events: string[] = [];
    const emitter = new EventEmitter();
    emitter.on('completed', () => events.push('completed'));
    emitter.on('failed', () => events.push('failed'));

    const { tcp } = mockTcp();
    const processor: Processor<unknown, void> = async () => {
      throw new Error('fail');
    };
    const config = makeConfig(processor, { emitter, tcp });

    await processJob(makeJob(), config);

    expect(events).toEqual(['failed']);
    expect(events).not.toContain('completed');
  });

  test('should send FAIL command via TCP when processor throws', async () => {
    const { tcp, sent } = mockTcp();
    const emitter = new EventEmitter();
    // suppress unhandled error
    emitter.on('failed', () => {});

    const processor: Processor<unknown, void> = async () => {
      throw new Error('kaboom');
    };
    const config = makeConfig(processor, { emitter, tcp, embedded: false });

    await processJob(makeJob(), config);

    const failCmd = sent.find((c) => c.cmd === 'FAIL');
    expect(failCmd).toBeDefined();
    expect(failCmd!.error).toBe('kaboom');
  });

  test('should include token in FAIL command when provided', async () => {
    const { tcp, sent } = mockTcp();
    const emitter = new EventEmitter();
    emitter.on('failed', () => {});

    const processor: Processor<unknown, void> = async () => {
      throw new Error('err');
    };
    const config = makeConfig(processor, { emitter, tcp, token: 'my-lock-token' });

    await processJob(makeJob(), config);

    const failCmd = sent.find((c) => c.cmd === 'FAIL');
    expect(failCmd).toBeDefined();
    expect(failCmd!.token).toBe('my-lock-token');
  });

  test('should emit error event when FAIL command itself throws', async () => {
    const { tcp } = mockTcp(async (cmd) => {
      if (cmd.cmd === 'FAIL') throw new Error('tcp-down');
      return { ok: true };
    });
    const emitter = new EventEmitter();
    let errorEvent: Error | undefined;
    emitter.on('error', (err: Error) => {
      errorEvent = err;
    });
    emitter.on('failed', () => {});

    const processor: Processor<unknown, void> = async () => {
      throw new Error('original');
    };
    const config = makeConfig(processor, { emitter, tcp });

    await processJob(makeJob(), config);

    expect(errorEvent).toBeDefined();
    expect(errorEvent!.message).toBe('tcp-down');
  });

  // ------------------------------------------------------------------
  // Progress updates during processing
  // ------------------------------------------------------------------
  test('should emit progress event when updateProgress is called', async () => {
    let progressValue: number | undefined;
    const emitter = new EventEmitter();
    emitter.on('progress', (_job: Job, progress: number) => {
      progressValue = progress;
    });

    const { tcp } = mockTcp();
    const processor: Processor<unknown, void> = async (job) => {
      await job.updateProgress(75);
    };
    const config = makeConfig(processor, { emitter, tcp });

    await processJob(makeJob(), config);

    expect(progressValue).toBe(75);
  });

  test('should send Progress command via TCP when updateProgress is called', async () => {
    const { tcp, sent } = mockTcp();

    const processor: Processor<unknown, void> = async (job) => {
      await job.updateProgress(50, 'halfway');
    };
    const config = makeConfig(processor, { tcp });

    await processJob(makeJob(), config);

    const progressCmd = sent.find((c) => c.cmd === 'Progress');
    expect(progressCmd).toBeDefined();
    expect(progressCmd!.progress).toBe(50);
    expect(progressCmd!.message).toBe('halfway');
  });

  test('should emit progress event with the correct job reference', async () => {
    let progressJob: Job | undefined;
    const emitter = new EventEmitter();
    emitter.on('progress', (job: Job) => {
      progressJob = job;
    });

    const internal = makeJob();
    const { tcp } = mockTcp();
    const processor: Processor<unknown, void> = async (job) => {
      await job.updateProgress(50);
    };
    const config = makeConfig(processor, { emitter, tcp });

    await processJob(internal, config);

    expect(progressJob).toBeDefined();
    expect(progressJob!.id).toBe(String(internal.id));
  });

  // ------------------------------------------------------------------
  // Process with concurrency > 1 (parallel processJob calls)
  // ------------------------------------------------------------------
  test('should allow concurrent processJob calls to run in parallel', async () => {
    const order: number[] = [];
    const processor: Processor<{ idx: number }, number> = async (job) => {
      const idx = (job.data as { idx: number }).idx;
      await Bun.sleep(idx === 0 ? 100 : 10); // first job is slower
      order.push(idx);
      return idx;
    };

    const config = makeConfig(processor);
    const job0 = makeJob({ data: { name: 'task', idx: 0 } });
    const job1 = makeJob({ data: { name: 'task', idx: 1 } });

    await Promise.all([processJob(job0, config), processJob(job1, config)]);

    // job1 should finish before job0 since it sleeps less
    expect(order[0]).toBe(1);
    expect(order[1]).toBe(0);
  });

  // ------------------------------------------------------------------
  // Emit active event immediately
  // ------------------------------------------------------------------
  test('should emit active event before processor runs', async () => {
    const events: string[] = [];
    const emitter = new EventEmitter();
    emitter.on('active', () => events.push('active'));

    const processor: Processor<unknown, void> = async () => {
      events.push('processing');
    };
    const config = makeConfig(processor, { emitter });

    await processJob(makeJob(), config);

    expect(events[0]).toBe('active');
    expect(events[1]).toBe('processing');
  });

  // ------------------------------------------------------------------
  // Token forwarding in ackBatcher (TCP mode)
  // ------------------------------------------------------------------
  test('should queue ack with token via ackBatcher in TCP mode', async () => {
    let sentTokens: string[] = [];
    const { tcp } = mockTcp(async (cmd) => {
      if (cmd.cmd === 'ACKB') {
        sentTokens = cmd.tokens as string[];
      }
      return { ok: true };
    });
    const ackBatcher = new AckBatcher({
      batchSize: 1,
      interval: 50,
      embedded: false,
    });
    ackBatcher.setTcp(tcp);

    const emitter = new EventEmitter();
    const processor: Processor<unknown, string> = async () => 'ok';
    const config = makeConfig(processor, {
      emitter,
      tcp,
      ackBatcher,
      token: 'lock-token-abc',
    });

    await processJob(makeJob(), config);

    // Wait for ackBatcher flush
    await Bun.sleep(100);

    expect(sentTokens.length).toBeGreaterThan(0);
    expect(sentTokens[0]).toBe('lock-token-abc');

    ackBatcher.stop();
  });

  // ------------------------------------------------------------------
  // Synchronous processor
  // ------------------------------------------------------------------
  test('should support synchronous processor functions', async () => {
    let completed = false;
    const emitter = new EventEmitter();
    emitter.on('completed', () => {
      completed = true;
    });

    const processor: Processor<unknown, number> = () => 99;
    const config = makeConfig(processor, { emitter });

    await processJob(makeJob(), config);

    expect(completed).toBe(true);
  });

  // ------------------------------------------------------------------
  // Log handler
  // ------------------------------------------------------------------
  test('should send AddLog command via TCP when job.log is called', async () => {
    const { tcp, sent } = mockTcp();

    const processor: Processor<unknown, void> = async (job) => {
      await job.log('step 1 complete');
    };
    const config = makeConfig(processor, { tcp });

    await processJob(makeJob(), config);

    const logCmd = sent.find((c) => c.cmd === 'AddLog');
    expect(logCmd).toBeDefined();
    expect(logCmd!.message).toBe('step 1 complete');
  });

  // ------------------------------------------------------------------
  // Null/undefined data handling
  // ------------------------------------------------------------------
  test('should handle job data without name field gracefully', async () => {
    let receivedJob: Job | undefined;
    const processor: Processor<unknown, string> = async (job) => {
      receivedJob = job as unknown as Job;
      return 'ok';
    };

    // Create job with data that has no 'name' field at all
    const internal = createJob(generateJobId(), 'test-q', {
      data: { foo: 'bar' },
    });
    const config = makeConfig(processor);

    await processJob(internal, config);

    expect(receivedJob).toBeDefined();
    expect(receivedJob!.name).toBe('default');
  });
});

// ---------------------------------------------------------------------------
// AckBatcher tests
// ---------------------------------------------------------------------------

describe('AckBatcher', () => {
  afterEach(() => {
    shutdownManager();
  });

  // Helper to create a batcher for TCP mode with a mock tcp connection
  function makeTcpBatcher(
    opts: Partial<AckBatcherConfig> = {},
    tcpSend?: (cmd: Record<string, unknown>) => Promise<Record<string, unknown>>,
  ): AckBatcher {
    const batcher = new AckBatcher({
      batchSize: opts.batchSize ?? 5,
      interval: opts.interval ?? 50,
      embedded: false, // TCP mode so we can control the mock
      maxRetries: opts.maxRetries ?? 0,
      retryDelayMs: opts.retryDelayMs ?? 10,
    });
    batcher.setTcp({
      send: tcpSend ?? (async () => ({ ok: true })),
    });
    return batcher;
  }

  // ------------------------------------------------------------------
  // Batch ack accumulation
  // ------------------------------------------------------------------
  test('should accumulate acks and flush them', async () => {
    let batchIds: string[] = [];
    const batcher = makeTcpBatcher({ batchSize: 3 }, async (cmd) => {
      batchIds = cmd.ids as string[];
      return { ok: true };
    });

    // Queue 3 acks (reaches batch size -> immediate flush)
    const p1 = batcher.queue('j1', 'r1');
    const p2 = batcher.queue('j2', 'r2');
    const p3 = batcher.queue('j3', 'r3');

    await Promise.all([p1, p2, p3]);

    expect(batchIds).toEqual(['j1', 'j2', 'j3']);

    batcher.stop();
  });

  // ------------------------------------------------------------------
  // Flush at batch size
  // ------------------------------------------------------------------
  test('should flush immediately when batch size is reached', async () => {
    let flushCount = 0;
    const batcher = makeTcpBatcher({ batchSize: 2 }, async () => {
      flushCount++;
      return { ok: true };
    });

    // Queue exactly batchSize items
    const p1 = batcher.queue('a', 1);
    const p2 = batcher.queue('b', 2);

    await Promise.all([p1, p2]);

    expect(flushCount).toBe(1);

    batcher.stop();
  });

  // ------------------------------------------------------------------
  // Flush at interval
  // ------------------------------------------------------------------
  test('should flush after interval even if batch size not reached', async () => {
    let flushed = false;
    const batcher = makeTcpBatcher({ batchSize: 100, interval: 30 }, async () => {
      flushed = true;
      return { ok: true };
    });

    // Queue 1 ack (under batch size), wait for timer
    const p = batcher.queue('x', 'result');

    await p; // this will resolve once the timer fires and flushes

    expect(flushed).toBe(true);

    batcher.stop();
  });

  // ------------------------------------------------------------------
  // Mixed ack results in same batch
  // ------------------------------------------------------------------
  test('should send all items in a single batch with their results', async () => {
    let sentResults: unknown[] = [];
    const batcher = makeTcpBatcher({ batchSize: 3 }, async (cmd) => {
      sentResults = cmd.results as unknown[];
      return { ok: true };
    });

    const p1 = batcher.queue('j1', { status: 'ok' });
    const p2 = batcher.queue('j2', null);
    const p3 = batcher.queue('j3', 42);

    await Promise.all([p1, p2, p3]);

    expect(sentResults).toEqual([{ status: 'ok' }, null, 42]);

    batcher.stop();
  });

  // ------------------------------------------------------------------
  // Tokens are forwarded
  // ------------------------------------------------------------------
  test('should forward lock tokens in the batch', async () => {
    let sentTokens: string[] = [];
    const batcher = makeTcpBatcher({ batchSize: 2 }, async (cmd) => {
      sentTokens = cmd.tokens as string[];
      return { ok: true };
    });

    const p1 = batcher.queue('j1', 'r1', 'tok-1');
    const p2 = batcher.queue('j2', 'r2'); // no token

    await Promise.all([p1, p2]);

    expect(sentTokens).toEqual(['tok-1', '']);

    batcher.stop();
  });

  // ------------------------------------------------------------------
  // Empty batch flush (no-op)
  // ------------------------------------------------------------------
  test('should be a no-op when flushing with no pending acks', async () => {
    let sendCalled = false;
    const batcher = makeTcpBatcher({}, async () => {
      sendCalled = true;
      return { ok: true };
    });

    await batcher.flush();

    expect(sendCalled).toBe(false);

    batcher.stop();
  });

  // ------------------------------------------------------------------
  // hasPending
  // ------------------------------------------------------------------
  test('hasPending should reflect queued items', async () => {
    const batcher = makeTcpBatcher(
      { batchSize: 100, interval: 5000 },
      async () => ({ ok: true }),
    );

    expect(batcher.hasPending()).toBe(false);

    // Queue one item (won't flush yet because batchSize is 100 and interval is 5s)
    const p = batcher.queue('j1', 'r1');

    expect(batcher.hasPending()).toBe(true);

    // Manually flush to resolve the promise
    await batcher.flush();
    await p;

    expect(batcher.hasPending()).toBe(false);

    batcher.stop();
  });

  // ------------------------------------------------------------------
  // Stop/cleanup
  // ------------------------------------------------------------------
  test('stop should clear pending acks and timer', async () => {
    const batcher = makeTcpBatcher(
      { batchSize: 100, interval: 5000 },
      async () => ({ ok: true }),
    );

    // Queue items without awaiting (they won't flush due to large batchSize/interval)
    void batcher.queue('j1', 'r1');
    void batcher.queue('j2', 'r2');

    expect(batcher.hasPending()).toBe(true);

    batcher.stop();

    expect(batcher.hasPending()).toBe(false);
  });

  test('stop should reject in-flight retries', async () => {
    let attempt = 0;
    const batcher = makeTcpBatcher(
      { batchSize: 1, maxRetries: 5, retryDelayMs: 50 },
      async () => {
        attempt++;
        if (attempt <= 5) throw new Error('fail');
        return { ok: true };
      },
    );

    const p = batcher.queue('j1', 'r1');

    // Wait for first attempt to fail, then stop
    await Bun.sleep(20);
    batcher.stop();

    // The promise should reject because batcher was stopped during retries
    await expect(p).rejects.toThrow();
  });

  // ------------------------------------------------------------------
  // Retry logic
  // ------------------------------------------------------------------
  test('should retry on failure and succeed on later attempt', async () => {
    let attempts = 0;
    const batcher = makeTcpBatcher(
      { batchSize: 1, maxRetries: 3, retryDelayMs: 10 },
      async () => {
        attempts++;
        if (attempts < 3) throw new Error('transient');
        return { ok: true };
      },
    );

    await batcher.queue('j1', 'r1');

    expect(attempts).toBe(3);

    batcher.stop();
  });

  test('should reject all items in batch after exhausting retries', async () => {
    const batcher = makeTcpBatcher(
      { batchSize: 2, maxRetries: 1, retryDelayMs: 10 },
      async () => {
        throw new Error('persistent');
      },
    );

    const p1 = batcher.queue('j1', 'r1');
    const p2 = batcher.queue('j2', 'r2');

    // Both should reject
    let err1: Error | undefined;
    let err2: Error | undefined;
    try {
      await p1;
    } catch (e) {
      err1 = e as Error;
    }
    try {
      await p2;
    } catch (e) {
      err2 = e as Error;
    }

    expect(err1).toBeDefined();
    expect(err1!.message).toBe('persistent');
    expect(err2).toBeDefined();
    expect(err2!.message).toBe('persistent');

    batcher.stop();
  });

  // ------------------------------------------------------------------
  // Batch ACKB response not ok
  // ------------------------------------------------------------------
  test('should treat non-ok response as failure', async () => {
    const batcher = makeTcpBatcher({ batchSize: 1, maxRetries: 0 }, async () => {
      return { ok: false, error: 'server error' };
    });

    let err: Error | undefined;
    try {
      await batcher.queue('j1', 'r1');
    } catch (e) {
      err = e as Error;
    }

    expect(err).toBeDefined();
    expect(err!.message).toBe('server error');

    batcher.stop();
  });

  // ------------------------------------------------------------------
  // Overflow protection (MAX_PENDING_ACKS)
  // ------------------------------------------------------------------
  test('should drop oldest entries when buffer overflows', async () => {
    // Create a batcher that never sends (no TCP, large interval)
    const batcher = new AckBatcher({
      batchSize: 100_000,
      interval: 60_000,
      embedded: false,
      maxRetries: 0,
    });
    // Intentionally do NOT set TCP so sends would fail; but we stop before flushing.

    const rejected: string[] = [];

    // Queue MAX_PENDING_ACKS items (10000)
    for (let i = 0; i < 10_000; i++) {
      void batcher.queue(`j${i}`, null).catch(() => {
        rejected.push(`j${i}`);
      });
    }

    // Queue one more to trigger overflow
    void batcher.queue('overflow', null).catch(() => {});

    // Give microtasks a tick to process rejections
    await Bun.sleep(1);

    // 10% of oldest should have been dropped
    expect(rejected.length).toBe(1000);

    batcher.stop();
  });

  // ------------------------------------------------------------------
  // waitForInFlight
  // ------------------------------------------------------------------
  test('waitForInFlight should resolve immediately when no flushes in progress', async () => {
    const batcher = makeTcpBatcher();

    await batcher.waitForInFlight(); // should not hang

    batcher.stop();
  });

  test('waitForInFlight should wait for pending flush to complete', async () => {
    let resolveFlush: (() => void) | null = null;
    const batcher = makeTcpBatcher({ batchSize: 1 }, async () => {
      await new Promise<void>((r) => {
        resolveFlush = r;
      });
      return { ok: true };
    });

    void batcher.queue('j1', 'r1');

    // Give time for flush to start
    await Bun.sleep(10);

    let waited = false;
    const waitPromise = batcher.waitForInFlight().then(() => {
      waited = true;
    });

    // Not yet resolved
    await Bun.sleep(5);
    expect(waited).toBe(false);

    // Resolve the flush
    resolveFlush!();

    await waitPromise;
    expect(waited).toBe(true);

    batcher.stop();
  });

  // ------------------------------------------------------------------
  // Embedded mode with empty flush
  // ------------------------------------------------------------------
  test('should work in embedded mode with empty flush', async () => {
    const batcher = new AckBatcher({
      batchSize: 10,
      interval: 50,
      embedded: true,
    });

    // In embedded mode, flush calls manager.ackBatchWithResults directly.
    // With an empty batch, it should be a no-op.
    await batcher.flush();

    expect(batcher.hasPending()).toBe(false);

    batcher.stop();
  });

  // ------------------------------------------------------------------
  // Multiple flushes
  // ------------------------------------------------------------------
  test('should handle multiple sequential batches', async () => {
    const batches: string[][] = [];
    const batcher = makeTcpBatcher({ batchSize: 2 }, async (cmd) => {
      batches.push(cmd.ids as string[]);
      return { ok: true };
    });

    // First batch
    const p1 = batcher.queue('a', 1);
    const p2 = batcher.queue('b', 2);
    await Promise.all([p1, p2]);

    // Second batch
    const p3 = batcher.queue('c', 3);
    const p4 = batcher.queue('d', 4);
    await Promise.all([p3, p4]);

    expect(batches.length).toBe(2);
    expect(batches[0]).toEqual(['a', 'b']);
    expect(batches[1]).toEqual(['c', 'd']);

    batcher.stop();
  });

  // ------------------------------------------------------------------
  // ACKB command format
  // ------------------------------------------------------------------
  test('should send correct ACKB command format', async () => {
    let capturedCmd: Record<string, unknown> | undefined;
    const batcher = makeTcpBatcher({ batchSize: 2 }, async (cmd) => {
      capturedCmd = cmd;
      return { ok: true };
    });

    const p1 = batcher.queue('id-1', { result: 'a' }, 'token-a');
    const p2 = batcher.queue('id-2', { result: 'b' }, 'token-b');
    await Promise.all([p1, p2]);

    expect(capturedCmd).toBeDefined();
    expect(capturedCmd!.cmd).toBe('ACKB');
    expect(capturedCmd!.ids).toEqual(['id-1', 'id-2']);
    expect(capturedCmd!.results).toEqual([{ result: 'a' }, { result: 'b' }]);
    expect(capturedCmd!.tokens).toEqual(['token-a', 'token-b']);

    batcher.stop();
  });

  // ------------------------------------------------------------------
  // Timer only set once for consecutive sub-batch queues
  // ------------------------------------------------------------------
  test('should not create multiple timers for sub-batch queues', async () => {
    let flushCount = 0;
    const batcher = makeTcpBatcher({ batchSize: 10, interval: 30 }, async () => {
      flushCount++;
      return { ok: true };
    });

    // Queue 3 items (all below batchSize), timer should be set once
    const p1 = batcher.queue('a', 1);
    const p2 = batcher.queue('b', 2);
    const p3 = batcher.queue('c', 3);

    await Promise.all([p1, p2, p3]);

    // Should have flushed exactly once (via timer)
    expect(flushCount).toBe(1);

    batcher.stop();
  });

  // ------------------------------------------------------------------
  // setTcp allows changing connection
  // ------------------------------------------------------------------
  test('setTcp should update the TCP connection', async () => {
    const batcher = new AckBatcher({
      batchSize: 1,
      interval: 50,
      embedded: false,
    });

    let firstCalled = false;
    let secondCalled = false;

    batcher.setTcp({
      send: async () => {
        firstCalled = true;
        return { ok: true };
      },
    });

    await batcher.queue('j1', 'r1');
    expect(firstCalled).toBe(true);

    batcher.setTcp({
      send: async () => {
        secondCalled = true;
        return { ok: true };
      },
    });

    await batcher.queue('j2', 'r2');
    expect(secondCalled).toBe(true);

    batcher.stop();
  });
});
