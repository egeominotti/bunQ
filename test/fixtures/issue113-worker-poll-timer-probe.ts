import { QueueManager } from '../../src/application/queueManager';
import { Queue, Worker, shutdownManager } from '../../src/client';
import { createTcpServer } from '../../src/infrastructure/server/tcp';

const JOB_COUNT = 64;
const queueManager = new QueueManager();
const tcpServer = createTcpServer(queueManager, { hostname: '127.0.0.1', port: 0 });
const port = tcpServer.server.port;
const connection = { host: '127.0.0.1', port };
const queueName = `issue113-poll-timers-${process.pid}`;
const queue = new Queue(queueName, { connection, embedded: false });

const originalSetTimeout = globalThis.setTimeout;
const originalClearTimeout = globalThis.clearTimeout;
const tenMillisecondTimers = new Set<ReturnType<typeof setTimeout>>();
const capturedTimerDelays = new Map<ReturnType<typeof setTimeout>, number>();
let captureTimerDelays = false;

type TimerCallback = (...args: unknown[]) => void;

globalThis.setTimeout = ((callback: TimerCallback, delay = 0, ...args: unknown[]) => {
  let handle: ReturnType<typeof setTimeout>;
  handle = originalSetTimeout(() => {
    tenMillisecondTimers.delete(handle);
    capturedTimerDelays.delete(handle);
    callback(...args);
  }, delay);
  if (delay === 10) tenMillisecondTimers.add(handle);
  if (captureTimerDelays) capturedTimerDelays.set(handle, delay);
  return handle;
}) as typeof setTimeout;

globalThis.clearTimeout = ((handle: ReturnType<typeof setTimeout>) => {
  tenMillisecondTimers.delete(handle);
  capturedTimerDelays.delete(handle);
  originalClearTimeout(handle);
}) as typeof clearTimeout;

let worker: Worker | undefined;
let concurrentWorker: Worker | undefined;

try {
  await queue.addBulk(
    Array.from({ length: JOB_COUNT }, (_, index) => ({ name: 'job', data: { index } }))
  );

  let completed = 0;
  worker = new Worker(
    queueName,
    async () => {
      completed++;
    },
    {
      connection,
      embedded: false,
      concurrency: 8,
      batchSize: 8,
      pollTimeout: 3_000,
      heartbeatInterval: 0,
    }
  );

  const deadline = Date.now() + 10_000;
  while (completed < JOB_COUNT && Date.now() < deadline) await Bun.sleep(10);
  await Bun.sleep(150);

  const outstandingAfterDrain = tenMillisecondTimers.size;
  await worker.close(true);
  worker = undefined;

  concurrentWorker = new Worker('issue113-concurrent-scheduling', async () => undefined, {
    embedded: true,
    autorun: false,
    concurrency: 2,
    pollTimeout: 3_000,
    heartbeatInterval: 0,
  });
  const internals = concurrentWorker as unknown as {
    running: boolean;
    tryProcess: () => Promise<void>;
    doPullBatch: () => Promise<[]>;
    schedulePoll: (delay: number) => void;
    clearPollTimer: () => void;
  };
  let releasePulls!: () => void;
  const pullsReleased = new Promise<void>((resolve) => {
    releasePulls = resolve;
  });
  let waitingPulls = 0;
  internals.doPullBatch = async () => {
    waitingPulls++;
    if (waitingPulls === 2) releasePulls();
    await pullsReleased;
    return [];
  };
  internals.running = true;
  await Promise.all([internals.tryProcess(), internals.tryProcess()]);
  internals.clearPollTimer();

  captureTimerDelays = true;
  internals.schedulePoll(25);
  internals.schedulePoll(2_500);
  captureTimerDelays = false;
  const ownedDelaysAfterLaterSchedule = [...capturedTimerDelays.values()];
  internals.clearPollTimer();

  console.log(
    JSON.stringify({
      completed,
      expected: JOB_COUNT,
      outstandingAfterDrain,
      outstandingAfterConcurrentScheduling: tenMillisecondTimers.size,
      ownedDelaysAfterLaterSchedule,
    })
  );
} finally {
  await concurrentWorker?.close(true);
  await worker?.close(true);
  await queue.close();
  tcpServer.stop();
  queueManager.shutdown();
  shutdownManager();
  globalThis.setTimeout = originalSetTimeout;
  globalThis.clearTimeout = originalClearTimeout;
  for (const timer of tenMillisecondTimers) originalClearTimeout(timer);
}
