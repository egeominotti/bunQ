/**
 * Comprehensive Benchmark: Embedded vs TCP Mode
 *
 * Tests both modes with identical workloads at different scales
 * Run: bun run bench/comprehensive.ts
 */

// Suppress console.log during benchmark
const originalLog = console.log;
const originalInfo = console.info;
let suppressLogs = false;

console.log = (...args: unknown[]) => {
  if (!suppressLogs) originalLog(...args);
};
console.info = (...args: unknown[]) => {
  if (!suppressLogs) originalInfo(...args);
};

import { Queue as EmbeddedQueue, Worker as EmbeddedWorker } from '../src/client';
import { Queue as TcpQueue, Worker as TcpWorker } from '../src/client';

const SCALES = [1000, 5000, 10000, 50000];
const BULK_SIZE = 100;
const CONCURRENCY = 10;
const PAYLOAD = { data: 'x'.repeat(100) };

interface BenchResult {
  scale: number;
  pushOps: number;
  bulkPushOps: number;
  processOps: number;
}

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function log(msg: string) {
  originalLog(msg);
}

// ============ EMBEDDED MODE BENCHMARKS ============

async function runEmbeddedBenchmarks(): Promise<BenchResult[]> {
  log('\n📦 EMBEDDED MODE (Direct SQLite)\n');
  log('═'.repeat(50));

  const results: BenchResult[] = [];
  suppressLogs = true;

  for (const scale of SCALES) {
    log(`\n🔄 Testing ${scale.toLocaleString()} jobs...`);

    // 1. Push benchmark
    const pushQueue = new EmbeddedQueue(`emb-push-${scale}-${Date.now()}`, { embedded: true });
    await sleep(50);

    const pushStart = performance.now();
    for (let i = 0; i < scale; i++) {
      await pushQueue.add('job', PAYLOAD);
    }
    const pushMs = performance.now() - pushStart;
    const pushOps = Math.round((scale / pushMs) * 1000);
    log(`  Push: ${pushOps.toLocaleString()} ops/sec`);

    // 2. Bulk Push benchmark
    const bulkQueue = new EmbeddedQueue(`emb-bulk-${scale}-${Date.now()}`, { embedded: true });
    const jobs = Array.from({ length: BULK_SIZE }, (_, i) => ({
      name: 'bulk-job',
      data: { ...PAYLOAD, i },
    }));

    const bulkIterations = Math.floor(scale / BULK_SIZE);
    const bulkStart = performance.now();
    for (let i = 0; i < bulkIterations; i++) {
      await bulkQueue.addBulk(jobs);
    }
    const bulkMs = performance.now() - bulkStart;
    const bulkPushOps = Math.round(((bulkIterations * BULK_SIZE) / bulkMs) * 1000);
    log(`  Bulk Push: ${bulkPushOps.toLocaleString()} ops/sec`);

    // 3. Process benchmark
    const embProcQueueName = `emb-proc-${scale}-${Date.now()}`;
    const processQueue = new EmbeddedQueue(embProcQueueName, { embedded: true });
    let processed = 0;

    const worker = new EmbeddedWorker(
      embProcQueueName,
      async () => {
        processed++;
        return { ok: true };
      },
      { embedded: true, concurrency: CONCURRENCY }
    );

    await sleep(50);
    const processStart = performance.now();

    // Push in batches
    for (let i = 0; i < scale; i += 500) {
      const batch = Math.min(500, scale - i);
      const promises = [];
      for (let j = 0; j < batch; j++) {
        promises.push(processQueue.add('job', PAYLOAD));
      }
      await Promise.all(promises);
    }

    while (processed < scale) {
      await sleep(5);
    }

    const processMs = performance.now() - processStart;
    const processOps = Math.round((scale / processMs) * 1000);
    log(`  Process: ${processOps.toLocaleString()} ops/sec`);

    await worker.close();

    results.push({ scale, pushOps, bulkPushOps, processOps });
  }

  suppressLogs = false;
  return results;
}

// ============ TCP MODE BENCHMARKS ============

async function runTcpBenchmarks(): Promise<BenchResult[]> {
  log('\n\n🌐 TCP MODE (Network + SQLite)\n');
  log('═'.repeat(50));

  const results: BenchResult[] = [];
  suppressLogs = true;

  for (const scale of SCALES) {
    log(`\n🔄 Testing ${scale.toLocaleString()} jobs...`);

    // 1. Push benchmark
    const pushQueue = new TcpQueue(`tcp-push-${scale}-${Date.now()}`, {
      connection: { host: 'localhost', port: 6789, poolSize: 32 },
    });
    await sleep(200);

    const pushStart = performance.now();
    for (let i = 0; i < scale; i += 100) {
      const batch = Math.min(100, scale - i);
      const promises = [];
      for (let j = 0; j < batch; j++) {
        promises.push(pushQueue.add('job', PAYLOAD));
      }
      await Promise.all(promises);
    }
    const pushMs = performance.now() - pushStart;
    const pushOps = Math.round((scale / pushMs) * 1000);
    log(`  Push: ${pushOps.toLocaleString()} ops/sec`);

    await sleep(50);
    await pushQueue.close();

    // 2. Bulk Push benchmark
    const bulkQueue = new TcpQueue(`tcp-bulk-${scale}-${Date.now()}`, {
      connection: { host: 'localhost', port: 6789, poolSize: 32 },
    });
    await sleep(200);

    const jobs = Array.from({ length: BULK_SIZE }, (_, i) => ({
      name: 'bulk-job',
      data: { ...PAYLOAD, i },
    }));

    const bulkIterations = Math.floor(scale / BULK_SIZE);
    const bulkStart = performance.now();
    for (let i = 0; i < bulkIterations; i++) {
      await bulkQueue.addBulk(jobs);
    }
    const bulkMs = performance.now() - bulkStart;
    const bulkPushOps = Math.round(((bulkIterations * BULK_SIZE) / bulkMs) * 1000);
    log(`  Bulk Push: ${bulkPushOps.toLocaleString()} ops/sec`);

    await sleep(50);
    await bulkQueue.close();

    // 3. Process benchmark
    let processed = 0;
    const processQueueName = `tcp-proc-${scale}-${Date.now()}`;

    const worker = new TcpWorker(
      processQueueName,
      async () => {
        processed++;
        return { ok: true };
      },
      {
        connection: {
          host: 'localhost',
          port: 6789,
          poolSize: 32,
          pingInterval: 0,
          commandTimeout: 60000,
        },
        concurrency: CONCURRENCY,
        heartbeatInterval: 0,
        batchSize: 20,
      }
    );

    const processQueue = new TcpQueue(processQueueName, {
      connection: {
        host: 'localhost',
        port: 6789,
        poolSize: 32,
        pingInterval: 0,
        commandTimeout: 60000,
      },
    });

    await sleep(300);
    const processStart = performance.now();

    // Push in batches
    for (let i = 0; i < scale; i += 500) {
      const batch = Math.min(500, scale - i);
      const promises = [];
      for (let j = 0; j < batch; j++) {
        promises.push(processQueue.add('job', PAYLOAD));
      }
      await Promise.all(promises);
    }

    while (processed < scale) {
      await sleep(5);
    }

    const processMs = performance.now() - processStart;
    const processOps = Math.round((scale / processMs) * 1000);
    log(`  Process: ${processOps.toLocaleString()} ops/sec`);

    await worker.close();
    await sleep(50);
    await processQueue.close();

    results.push({ scale, pushOps, bulkPushOps, processOps });
  }

  suppressLogs = false;
  return results;
}

// ============ MAIN ============

async function main() {
  log('═══════════════════════════════════════════════════════════════');
  log('         bunqueue Comprehensive Benchmark');
  log('         Embedded vs TCP Mode');
  log('═══════════════════════════════════════════════════════════════');
  log(`\nScales: ${SCALES.map((s) => s.toLocaleString()).join(', ')} jobs`);
  log(`Bulk size: ${BULK_SIZE}`);
  log(`Concurrency: ${CONCURRENCY}`);
  log(`Payload: ${JSON.stringify(PAYLOAD).length} bytes`);

  // Check TCP server
  let tcpAvailable = false;
  try {
    suppressLogs = true;
    const testQueue = new TcpQueue('test-conn', {
      connection: { host: 'localhost', port: 6789 },
    });
    await sleep(500);
    await testQueue.close();
    suppressLogs = false;
    tcpAvailable = true;
    log('\n✓ TCP server connected (port 6789)');
  } catch {
    suppressLogs = false;
    log('\n✗ TCP server not available. Start with: bun run start');
    log('Running EMBEDDED mode only...');
  }

  const embeddedResults = await runEmbeddedBenchmarks();

  let tcpResults: BenchResult[] = [];
  if (tcpAvailable) {
    tcpResults = await runTcpBenchmarks();
  }

  // Print summary
  log('\n\n');
  log('═══════════════════════════════════════════════════════════════');
  log('                         RESULTS');
  log('═══════════════════════════════════════════════════════════════');

  printResults('EMBEDDED', embeddedResults);
  if (tcpResults.length > 0) {
    printResults('TCP', tcpResults);
    printComparison(embeddedResults, tcpResults);
  }

  process.exit(0);
}

function printResults(mode: string, results: BenchResult[]) {
  log(`\n📊 ${mode} MODE RESULTS\n`);
  log('┌──────────┬────────────────┬────────────────┬────────────────┐');
  log('│ Scale    │ Push (ops/s)   │ Bulk (ops/s)   │ Process (ops/s)│');
  log('├──────────┼────────────────┼────────────────┼────────────────┤');

  for (const r of results) {
    const scale = r.scale.toLocaleString().padStart(8);
    const push = r.pushOps.toLocaleString().padStart(12);
    const bulk = r.bulkPushOps.toLocaleString().padStart(12);
    const proc = r.processOps.toLocaleString().padStart(12);
    log(`│ ${scale} │ ${push}   │ ${bulk}   │ ${proc}   │`);
  }

  log('└──────────┴────────────────┴────────────────┴────────────────┘');
}

function printComparison(embedded: BenchResult[], tcp: BenchResult[]) {
  log('\n📈 EMBEDDED vs TCP (Embedded is X times faster)\n');
  log('┌──────────┬────────────────┬────────────────┬────────────────┐');
  log('│ Scale    │ Push           │ Bulk           │ Process        │');
  log('├──────────┼────────────────┼────────────────┼────────────────┤');

  for (let i = 0; i < embedded.length; i++) {
    const e = embedded[i];
    const t = tcp[i];
    const scale = e.scale.toLocaleString().padStart(8);
    const pushRatio = (e.pushOps / t.pushOps).toFixed(1) + 'x';
    const bulkRatio = (e.bulkPushOps / t.bulkPushOps).toFixed(1) + 'x';
    const procRatio = (e.processOps / t.processOps).toFixed(1) + 'x';
    log(
      `│ ${scale} │ ${pushRatio.padStart(12)}   │ ${bulkRatio.padStart(12)}   │ ${procRatio.padStart(12)}   │`
    );
  }

  log('└──────────┴────────────────┴────────────────┴────────────────┘');
}

main().catch(console.error);
