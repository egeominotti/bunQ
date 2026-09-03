import { expect, test } from 'bun:test';
import { pack, unpack } from 'msgpackr';
import { Worker } from '../src/client/worker';
import { WORKER_CONSTANTS } from '../src/client/worker/constants';
import type { BatchWorkerOptions } from '../src/client/types';
import { closeAllSharedPools } from '../src/client/tcpPool';
import { FrameParser } from '../src/infrastructure/server/protocol';

interface SocketState {
  readonly parser: FrameParser;
}

interface AckScenario {
  jobs: number;
  expectedAcknowledgements?: number;
  concurrency: number;
  batchSize: number;
  batch?: BatchWorkerOptions;
  limiter?: { max: number; duration: number; groupKey?: string };
  sharedGroup?: string;
  distinctGroups?: boolean;
  failedBatchMember?: number;
  concurrencyAfterDelivery?: number;
}

async function runAckScenario(options: AckScenario): Promise<{
  completedBeforeFallback: boolean;
  acknowledgedBatches: string[][];
  errors: Error[];
}> {
  let delivered = 0;
  let acknowledged = 0;
  const acknowledgedBatches: string[][] = [];
  const errors: Error[] = [];
  let resolveProcessorStarted: (() => void) | undefined;
  let resolveAllAcknowledged: (() => void) | undefined;
  const processorStarted = new Promise<void>((resolve) => {
    resolveProcessorStarted = resolve;
  });
  const allAcknowledged = new Promise<void>((resolve) => {
    resolveAllAcknowledged = resolve;
  });

  const nextJobs = (count: number) =>
    Array.from({ length: Math.min(count, options.jobs - delivered) }, () => {
      const sequence = ++delivered;
      return {
        id: `ack-threshold-${sequence}`,
        data: {
          value: sequence,
          ...(options.sharedGroup ? { group: options.sharedGroup } : {}),
          ...(options.distinctGroups ? { group: `group-${sequence}` } : {}),
        },
      };
    });

  const server = Bun.listen<SocketState>({
    hostname: '127.0.0.1',
    port: 0,
    socket: {
      open(socket) {
        socket.data = { parser: new FrameParser() };
      },
      data(socket, data) {
        for (const frame of socket.data.parser.addData(new Uint8Array(data))) {
          const command = unpack(frame) as Record<string, unknown>;
          const response: Record<string, unknown> = { reqId: command.reqId, ok: true };
          if (command.cmd === 'PULLB') {
            const jobs = nextJobs(Number(command.count ?? 1));
            response.jobs = jobs;
            response.tokens = jobs.map((job) => `token-${job.id}`);
          } else if (command.cmd === 'PULL') {
            const [job] = nextJobs(1);
            response.job = job ?? null;
            response.token = job ? `token-${job.id}` : null;
          } else if (command.cmd === 'ACKB') {
            const ids = command.ids as string[];
            acknowledgedBatches.push(ids);
            acknowledged += ids.length;
            if (acknowledged === (options.expectedAcknowledgements ?? options.jobs)) {
              resolveAllAcknowledged?.();
            }
          }
          socket.write(FrameParser.frame(pack(response)));
        }
      },
      close() {},
      error(_socket, error) {
        errors.push(error);
      },
    },
  });

  const constants = WORKER_CONSTANTS as { DEFAULT_ACK_INTERVAL: number };
  const originalAckInterval = constants.DEFAULT_ACK_INTERVAL;
  constants.DEFAULT_ACK_INTERVAL = 5_000;
  let worker: Worker;
  try {
    worker = new Worker(
      `ack-threshold-${Bun.randomUUIDv7()}`,
      (job) => {
        resolveProcessorStarted?.();
        const batch = job.getBatch?.();
        if (batch && options.failedBatchMember !== undefined) {
          batch[options.failedBatchMember]?.setAsFailed?.(
            new Error('expected batch member failure')
          );
        }
        return { ok: true };
      },
      {
        embedded: false,
        autorun: false,
        concurrency: options.concurrency,
        batchSize: options.batchSize,
        batch: options.batch,
        limiter: options.limiter,
        connection: {
          host: '127.0.0.1',
          port: server.port,
          poolSize: 1,
          pingInterval: 0,
          commandTimeout: 2_000,
        },
      }
    );
  } finally {
    constants.DEFAULT_ACK_INTERVAL = originalAckInterval;
  }
  worker.on('error', (error) => errors.push(error));

  let completedBeforeFallback = false;
  try {
    worker.run();
    if (options.concurrencyAfterDelivery !== undefined) {
      await processorStarted;
      await Bun.sleep(10);
      worker.concurrency = options.concurrencyAfterDelivery;
    }
    completedBeforeFallback = await Promise.race([
      allAcknowledged.then(() => true),
      Bun.sleep(500).then(() => false),
    ]);
  } finally {
    await worker.close(true);
    server.stop(true);
    closeAllSharedPools();
  }
  return {
    completedBeforeFallback,
    acknowledgedBatches: acknowledgedBatches.map((batch) => [...batch]),
    errors: [...errors],
  };
}

test('TCP worker flushes when concurrency is below the ACK batch size', async () => {
  const result = await runAckScenario({ jobs: 1, concurrency: 1, batchSize: 10 });

  expect(result.completedBeforeFallback).toBe(true);
  expect(result.acknowledgedBatches).toEqual([['ack-threshold-1']]);
  expect(result.errors).toEqual([]);
});

test('TCP worker coalesces all concurrently attainable ACKs', async () => {
  const result = await runAckScenario({ jobs: 2, concurrency: 2, batchSize: 10 });

  expect(result.completedBeforeFallback).toBe(true);
  expect(result.acknowledgedBatches).toEqual([['ack-threshold-1', 'ack-threshold-2']]);
  expect(result.errors).toEqual([]);
});

test('TCP worker flushes the final scalar cohort below concurrency', async () => {
  const result = await runAckScenario({ jobs: 3, concurrency: 2, batchSize: 10 });

  expect(result.completedBeforeFallback).toBe(true);
  expect(result.acknowledgedBatches).toEqual([
    ['ack-threshold-1', 'ack-threshold-2'],
    ['ack-threshold-3'],
  ]);
  expect(result.errors).toEqual([]);
});

test('TCP worker flushes pending ACKs after concurrency is reduced', async () => {
  const result = await runAckScenario({
    jobs: 1,
    concurrency: 10,
    batchSize: 10,
    concurrencyAfterDelivery: 1,
  });

  expect(result.completedBeforeFallback).toBe(true);
  expect(result.acknowledgedBatches).toEqual([['ack-threshold-1']]);
  expect(result.errors).toEqual([]);
});

test('native batch workers retain member-level ACK coalescing', async () => {
  const result = await runAckScenario({
    jobs: 3,
    concurrency: 1,
    batchSize: 10,
    batch: { size: 3, minSize: 3 },
  });

  expect(result.completedBeforeFallback).toBe(true);
  expect(result.acknowledgedBatches).toEqual([
    ['ack-threshold-1', 'ack-threshold-2', 'ack-threshold-3'],
  ]);
  expect(result.errors).toEqual([]);
});

test('TCP worker flushes a partial native batch below its configured size', async () => {
  const result = await runAckScenario({
    jobs: 1,
    concurrency: 1,
    batchSize: 10,
    batch: { size: 3, minSize: 1 },
  });

  expect(result.completedBeforeFallback).toBe(true);
  expect(result.acknowledgedBatches).toEqual([['ack-threshold-1']]);
  expect(result.errors).toEqual([]);
});

test('TCP worker flushes at the capacity admitted by its rate limiter', async () => {
  const result = await runAckScenario({
    jobs: 1,
    concurrency: 4,
    batchSize: 10,
    limiter: { max: 1, duration: 5_000 },
  });

  expect(result.completedBeforeFallback).toBe(true);
  expect(result.acknowledgedBatches).toEqual([['ack-threshold-1']]);
  expect(result.errors).toEqual([]);
});

test('TCP worker excludes group-blocked jobs from the ACK frontier', async () => {
  const result = await runAckScenario({
    jobs: 2,
    concurrency: 2,
    batchSize: 10,
    limiter: { max: 1, duration: 5_000, groupKey: 'group' },
    sharedGroup: 'shared',
  });

  expect(result.completedBeforeFallback).toBe(true);
  expect(result.acknowledgedBatches).toEqual([['ack-threshold-1'], ['ack-threshold-2']]);
  expect(result.errors).toEqual([]);
});

test('TCP worker retains coalescing for independently eligible groups', async () => {
  const result = await runAckScenario({
    jobs: 2,
    concurrency: 2,
    batchSize: 10,
    limiter: { max: 1, duration: 5_000, groupKey: 'group' },
    distinctGroups: true,
  });

  expect(result.completedBeforeFallback).toBe(true);
  expect(result.acknowledgedBatches).toEqual([['ack-threshold-1', 'ack-threshold-2']]);
  expect(result.errors).toEqual([]);
});

test('TCP worker retires a failed native-batch member from the ACK frontier', async () => {
  const result = await runAckScenario({
    jobs: 3,
    expectedAcknowledgements: 2,
    concurrency: 1,
    batchSize: 10,
    batch: { size: 3, minSize: 3 },
    failedBatchMember: 2,
  });

  expect(result.completedBeforeFallback).toBe(true);
  expect(result.acknowledgedBatches).toEqual([['ack-threshold-1', 'ack-threshold-2']]);
  expect(result.errors).toEqual([]);
});
