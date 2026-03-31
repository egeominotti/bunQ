/**
 * Local Auto-Batch Benchmark
 * Compares queue.add() throughput with and without auto-batching
 * against a local server.
 *
 * Run: bun run bench/local-autobatch.ts
 */

import { Queue } from '../src/client';

const PORT = 16789;
const CONNECTION = { host: 'localhost', port: PORT, poolSize: 4, pingInterval: 0, commandTimeout: 30000 };

const PAYLOAD = { type: 'email', to: 'user@test.com', body: 'hello' };

function formatOps(n: number): string {
  return n.toLocaleString();
}

// ─── Sequential add() - worst case for non-batched ──────────────

async function benchSequential(scale: number, autoBatch: boolean) {
  const name = `seq-${autoBatch ? 'batch' : 'nobatch'}-${Date.now()}`;
  const queue = new Queue(name, {
    connection: CONNECTION,
    autoBatch: { enabled: autoBatch },
  });
  await Bun.sleep(200);

  const start = performance.now();
  for (let i = 0; i < scale; i++) {
    await queue.add('job', PAYLOAD);
  }
  const totalMs = performance.now() - start;
  const ops = Math.round((scale / totalMs) * 1000);

  console.log(
    `  [${autoBatch ? 'BATCH' : 'DIRECT'}] ${scale.toString().padStart(5)} sequential adds | ${formatOps(ops).padStart(8)} ops/s | ${totalMs.toFixed(0)}ms`
  );

  await queue.disconnect();
  return { ops, totalMs };
}

// ─── Concurrent add() - fire many, await all ────────────────────

async function benchConcurrent(scale: number, autoBatch: boolean) {
  const name = `conc-${autoBatch ? 'batch' : 'nobatch'}-${Date.now()}`;
  const queue = new Queue(name, {
    connection: CONNECTION,
    autoBatch: { enabled: autoBatch },
  });
  await Bun.sleep(200);

  const start = performance.now();
  const promises = [];
  for (let i = 0; i < scale; i++) {
    promises.push(queue.add('job', PAYLOAD));
  }
  await Promise.all(promises);
  const totalMs = performance.now() - start;
  const ops = Math.round((scale / totalMs) * 1000);

  console.log(
    `  [${autoBatch ? 'BATCH' : 'DIRECT'}] ${scale.toString().padStart(5)} concurrent adds | ${formatOps(ops).padStart(8)} ops/s | ${totalMs.toFixed(0)}ms`
  );

  await queue.disconnect();
  return { ops, totalMs };
}

// ─── addBulk() baseline ─────────────────────────────────────────

async function benchBulk(scale: number) {
  const name = `bulk-${Date.now()}`;
  const queue = new Queue(name, {
    connection: CONNECTION,
    autoBatch: { enabled: false },
  });
  await Bun.sleep(200);

  const batch = Array.from({ length: scale }, (_, i) => ({
    name: 'bulk',
    data: { ...PAYLOAD, i },
  }));

  const start = performance.now();
  await queue.addBulk(batch);
  const totalMs = performance.now() - start;
  const ops = Math.round((scale / totalMs) * 1000);

  console.log(
    `  [BULK  ] ${scale.toString().padStart(5)} addBulk          | ${formatOps(ops).padStart(8)} ops/s | ${totalMs.toFixed(0)}ms`
  );

  await queue.disconnect();
  return { ops, totalMs };
}

// ─── Main ───────────────────────────────────────────────────────

async function main() {
  console.log('');
  console.log('================================================================');
  console.log('   Auto-Batch Benchmark (localhost)');
  console.log(`   Server: localhost:${PORT}`);
  console.log('================================================================');

  // Warmup
  const warmup = new Queue('warmup', { connection: CONNECTION, autoBatch: { enabled: false } });
  await Bun.sleep(300);
  for (let i = 0; i < 50; i++) await warmup.add('w', { i });
  await warmup.disconnect();

  // Sequential adds - this is where auto-batch shines
  console.log('\n--- Sequential await queue.add() ---');
  const seqDirect = await benchSequential(500, false);
  const seqBatch = await benchSequential(500, true);
  console.log(`  -> Speedup: ${(seqDirect.totalMs / seqBatch.totalMs).toFixed(1)}x`);

  // Concurrent adds - fire all, await all
  console.log('\n--- Concurrent Promise.all(adds) ---');
  const concDirect = await benchConcurrent(1000, false);
  const concBatch = await benchConcurrent(1000, true);
  console.log(`  -> Speedup: ${(concDirect.totalMs / concBatch.totalMs).toFixed(1)}x`);

  console.log('\n--- Concurrent Promise.all(adds) x5000 ---');
  const concDirect5k = await benchConcurrent(5000, false);
  const concBatch5k = await benchConcurrent(5000, true);
  console.log(`  -> Speedup: ${(concDirect5k.totalMs / concBatch5k.totalMs).toFixed(1)}x`);

  // Baseline: addBulk
  console.log('\n--- addBulk() baseline ---');
  await benchBulk(1000);
  await benchBulk(5000);

  console.log('\n================================================================');
  console.log('Done.');
  process.exit(0);
}

main().catch((err) => {
  console.error('Benchmark failed:', err);
  process.exit(1);
});
