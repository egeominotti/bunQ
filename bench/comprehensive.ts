import { Queue as EmbeddedQueue, Worker as EmbeddedWorker } from '../src/client';
import { Queue as TcpQueue, Worker as TcpWorker } from '../src/client';
import { shutdownManager } from '../src/client/manager';
import { type BenchResult, printComparison, printResults } from './comprehensive-report';
import {
  assertExactCompletion,
  assertExactDeliveries,
  closeAll,
  positiveInteger,
  waitForAuthoritativeCompletion,
} from './native-benchmark-integrity';

const originalLog = console.log;
const originalInfo = console.info;
let suppressLogs = false;
const SCALES = [1000, 5000, 10000, 50000];
const BULK_SIZE = 100;
const CONCURRENCY = 10;
const PAYLOAD = { data: 'x'.repeat(100) };
const BENCH_HOST = process.env.BENCH_HOST ?? 'localhost';
const BENCH_PORT = Number.parseInt(process.env.BENCH_PORT ?? '6789', 10);
const PROCESS_TIMEOUT_MS = positiveInteger('BENCH_TIMEOUT_MS', 600_000);
if (!Number.isInteger(BENCH_PORT) || BENCH_PORT < 1 || BENCH_PORT > 65_535) {
  throw new Error(
    `BENCH_PORT must be an integer from 1 to 65535, received ${process.env.BENCH_PORT}`
  );
}
const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));
const log = (message: string): void => originalLog(message);
const connection = (poolSize = 32) => ({
  host: BENCH_HOST,
  port: BENCH_PORT,
  poolSize,
  pingInterval: 0,
  commandTimeout: 60_000,
});
async function runEmbeddedBenchmarks(): Promise<BenchResult[]> {
  log('\n📦 EMBEDDED MODE (In-memory)\n');
  log('═'.repeat(50));
  const results: BenchResult[] = [];
  suppressLogs = true;
  try {
    for (const scale of SCALES) {
      log(`\n🔄 Testing ${scale.toLocaleString()} jobs...`);
      const processQueueName = `emb-proc-${scale}-${Date.now()}`;
      let pushQueue: EmbeddedQueue | undefined;
      let bulkQueue: EmbeddedQueue | undefined;
      let processQueue: EmbeddedQueue | undefined;
      let worker: EmbeddedWorker | undefined;

      try {
        pushQueue = new EmbeddedQueue(`emb-push-${scale}-${Date.now()}`, { embedded: true });
        bulkQueue = new EmbeddedQueue(`emb-bulk-${scale}-${Date.now()}`, { embedded: true });
        processQueue = new EmbeddedQueue(processQueueName, { embedded: true });
        await sleep(50);
        const pushStart = performance.now();
        for (let index = 0; index < scale; index++) await pushQueue.add('job', PAYLOAD);
        const pushMs = performance.now() - pushStart;
        const pushOps = Math.round((scale / pushMs) * 1000);
        log(`  Push: ${pushOps.toLocaleString()} ops/sec`);

        const jobs = Array.from({ length: BULK_SIZE }, (_, index) => ({
          name: 'bulk-job',
          data: { ...PAYLOAD, i: index },
        }));
        const bulkIterations = Math.floor(scale / BULK_SIZE);
        const bulkStart = performance.now();
        for (let index = 0; index < bulkIterations; index++) await bulkQueue.addBulk(jobs);
        const bulkMs = performance.now() - bulkStart;
        const bulkPushOps = Math.round(((bulkIterations * BULK_SIZE) / bulkMs) * 1000);
        log(`  Bulk Push: ${bulkPushOps.toLocaleString()} ops/sec`);

        const acceptedIds = new Set<string>();
        const invokedIds = new Set<string>();
        let invocations = 0;
        let workerError: unknown;
        worker = new EmbeddedWorker(
          processQueueName,
          (job) => {
            invocations++;
            invokedIds.add(job.id);
            return { ok: true };
          },
          { embedded: true, concurrency: CONCURRENCY }
        );
        worker.on('error', (error) => {
          workerError = error;
        });
        await sleep(50);
        const processStart = performance.now();
        const deadline = Date.now() + PROCESS_TIMEOUT_MS;
        for (let index = 0; index < scale; index += 500) {
          const batch = Math.min(500, scale - index);
          const added = await Promise.all(
            Array.from({ length: batch }, () => processQueue.add('job', PAYLOAD))
          );
          for (const job of added) acceptedIds.add(job.id);
        }
        assertExactCompletion(`Embedded accepted ${scale}`, scale, acceptedIds.size, deadline);
        await waitForAuthoritativeCompletion({
          label: `Embedded process ${scale}`,
          expected: scale,
          deadline,
          getJobCounts: () => processQueue.getJobCounts(),
          getWorkerError: () => workerError,
        });
        assertExactCompletion(`Embedded invocations ${scale}`, scale, invocations, deadline);
        assertExactDeliveries(
          `Embedded process ${scale}`,
          acceptedIds,
          invokedIds,
          invocations,
          scale
        );
        const processMs = performance.now() - processStart;
        const processOps = Math.round((scale / processMs) * 1000);
        log(`  Process: ${processOps.toLocaleString()} ops/sec`);
        results.push({ scale, pushOps, bulkPushOps, processOps });
      } finally {
        await closeAll([worker, processQueue, bulkQueue, pushQueue]);
        shutdownManager();
      }
    }
    return results;
  } finally {
    suppressLogs = false;
    shutdownManager();
  }
}

async function runTcpBenchmarks(): Promise<BenchResult[]> {
  log('\n\n🌐 TCP MODE (Network + broker persistence)\n');
  log('═'.repeat(50));
  const results: BenchResult[] = [];
  suppressLogs = true;

  try {
    for (const scale of SCALES) {
      log(`\n🔄 Testing ${scale.toLocaleString()} jobs...`);
      const processQueueName = `tcp-proc-${scale}-${Date.now()}`;
      let pushQueue: TcpQueue | undefined;
      let bulkQueue: TcpQueue | undefined;
      let processQueue: TcpQueue | undefined;
      let worker: TcpWorker | undefined;

      try {
        pushQueue = new TcpQueue(`tcp-push-${scale}-${Date.now()}`, {
          embedded: false,
          connection: connection(),
        });
        bulkQueue = new TcpQueue(`tcp-bulk-${scale}-${Date.now()}`, {
          embedded: false,
          connection: connection(),
        });
        processQueue = new TcpQueue(processQueueName, {
          embedded: false,
          connection: connection(),
        });
        await sleep(200);
        const pushStart = performance.now();
        for (let index = 0; index < scale; index += 100) {
          const batch = Math.min(100, scale - index);
          await Promise.all(Array.from({ length: batch }, () => pushQueue.add('job', PAYLOAD)));
        }
        const pushMs = performance.now() - pushStart;
        const pushOps = Math.round((scale / pushMs) * 1000);
        log(`  Push: ${pushOps.toLocaleString()} ops/sec`);

        const jobs = Array.from({ length: BULK_SIZE }, (_, index) => ({
          name: 'bulk-job',
          data: { ...PAYLOAD, i: index },
        }));
        const bulkIterations = Math.floor(scale / BULK_SIZE);
        const bulkStart = performance.now();
        for (let index = 0; index < bulkIterations; index++) await bulkQueue.addBulk(jobs);
        const bulkMs = performance.now() - bulkStart;
        const bulkPushOps = Math.round(((bulkIterations * BULK_SIZE) / bulkMs) * 1000);
        log(`  Bulk Push: ${bulkPushOps.toLocaleString()} ops/sec`);

        const acceptedIds = new Set<string>();
        const invokedIds = new Set<string>();
        let invocations = 0;
        let workerError: unknown;
        worker = new TcpWorker(
          processQueueName,
          (job) => {
            invocations++;
            invokedIds.add(job.id);
            return { ok: true };
          },
          {
            embedded: false,
            connection: connection(),
            concurrency: CONCURRENCY,
            heartbeatInterval: 5000,
            batchSize: 20,
          }
        );
        worker.on('error', (error) => {
          workerError = error;
        });
        await sleep(300);
        const processStart = performance.now();
        const deadline = Date.now() + PROCESS_TIMEOUT_MS;
        for (let index = 0; index < scale; index += 500) {
          const batch = Math.min(500, scale - index);
          const added = await Promise.all(
            Array.from({ length: batch }, () => processQueue.add('job', PAYLOAD))
          );
          for (const job of added) acceptedIds.add(job.id);
        }
        assertExactCompletion(`TCP accepted ${scale}`, scale, acceptedIds.size, deadline);
        await waitForAuthoritativeCompletion({
          label: `TCP process ${scale}`,
          expected: scale,
          deadline,
          getJobCounts: () => processQueue.getJobCounts(),
          getWorkerError: () => workerError,
        });
        assertExactCompletion(`TCP invocations ${scale}`, scale, invocations, deadline);
        assertExactDeliveries(`TCP process ${scale}`, acceptedIds, invokedIds, invocations, scale);
        const processMs = performance.now() - processStart;
        const processOps = Math.round((scale / processMs) * 1000);
        log(`  Process: ${processOps.toLocaleString()} ops/sec`);
        results.push({ scale, pushOps, bulkPushOps, processOps });
      } finally {
        await closeAll([worker, processQueue, bulkQueue, pushQueue]);
      }
    }
    return results;
  } finally {
    suppressLogs = false;
  }
}

async function tcpIsAvailable(): Promise<boolean> {
  const queue = new TcpQueue(`bench-connect-${Date.now()}`, {
    embedded: false,
    connection: { ...connection(1), commandTimeout: 2000 },
  });
  try {
    await queue.getJobCounts();
    return true;
  } catch {
    return false;
  } finally {
    await closeAll([queue]);
  }
}

async function main(): Promise<void> {
  log('═══════════════════════════════════════════════════════════════');
  log('         bunqueue Comprehensive Benchmark');
  log('         Embedded vs TCP Mode');
  log('═══════════════════════════════════════════════════════════════');
  log(`\nScales: ${SCALES.map((scale) => scale.toLocaleString()).join(', ')} jobs`);
  log(`Bulk size: ${BULK_SIZE}`);
  log(`Concurrency: ${CONCURRENCY}`);
  log(`Payload: ${JSON.stringify(PAYLOAD).length} bytes`);
  log(`Process timeout: ${PROCESS_TIMEOUT_MS} ms`);

  const tcpAvailable = await tcpIsAvailable();
  log(
    tcpAvailable
      ? `\n✓ TCP server connected (${BENCH_HOST}:${BENCH_PORT})`
      : `\n✗ TCP server unavailable at ${BENCH_HOST}:${BENCH_PORT}; running Embedded only`
  );
  const embeddedResults = await runEmbeddedBenchmarks();
  const tcpResults = tcpAvailable ? await runTcpBenchmarks() : [];

  log('\n\n═══════════════════════════════════════════════════════════════');
  log('                         RESULTS');
  log('═══════════════════════════════════════════════════════════════');
  printResults(log, 'EMBEDDED', embeddedResults);
  if (tcpResults.length > 0) {
    printResults(log, 'TCP', tcpResults);
    printComparison(log, embeddedResults, tcpResults);
  }
}

if (import.meta.main) {
  console.log = (...args: unknown[]) => {
    if (!suppressLogs) originalLog(...args);
  };
  console.info = (...args: unknown[]) => {
    if (!suppressLogs) originalInfo(...args);
  };
  main()
    .catch((error) => {
      console.error(error);
      process.exitCode = 1;
    })
    .finally(() => {
      suppressLogs = false;
      console.log = originalLog;
      console.info = originalInfo;
    });
}
