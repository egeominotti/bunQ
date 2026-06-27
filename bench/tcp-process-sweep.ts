/**
 * TCP process-throughput vs worker concurrency.
 * Confirms whether process throughput is bounded by the worker's
 * leased<=concurrency pull cap (slots), which serializes pulls to ~1/round-trip.
 * Requires a TCP server on :6789.  Run: bun run bench/tcp-process-sweep.ts
 */
import { Queue as TcpQueue, Worker as TcpWorker } from '../src/client';

const SCALE = 5000;
const PAYLOAD = { data: 'x'.repeat(100) };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function run(concurrency: number, batchSize: number): Promise<number> {
  const qn = `sweep-${concurrency}-${batchSize}-${Date.now()}`;
  let processed = 0;
  const worker = new TcpWorker(
    qn,
    async () => {
      processed++;
      return { ok: true };
    },
    {
      connection: { host: 'localhost', port: 6789, poolSize: 32, pingInterval: 0, commandTimeout: 60000 },
      concurrency,
      heartbeatInterval: 0,
      batchSize,
    }
  );
  const q = new TcpQueue(qn, {
    connection: { host: 'localhost', port: 6789, poolSize: 32, pingInterval: 0, commandTimeout: 60000 },
  });
  await sleep(300);
  const t = performance.now();
  for (let i = 0; i < SCALE; i += 500) {
    const p = [];
    for (let j = 0; j < Math.min(500, SCALE - i); j++) p.push(q.add('job', PAYLOAD));
    await Promise.all(p);
  }
  while (processed < SCALE) await sleep(2);
  const ops = Math.round((SCALE / (performance.now() - t)) * 1000);
  await worker.close();
  await sleep(50);
  await q.close();
  return ops;
}

async function main() {
  console.log(`\nTCP process throughput vs concurrency (scale=${SCALE})\n`);
  console.log('concurrency  batchSize   process(ops/s)');
  for (const [c, b] of [[10, 20], [50, 50], [100, 100], [200, 200]] as const) {
    const ops = await run(c, b);
    console.log(`${String(c).padStart(11)}  ${String(b).padStart(9)}   ${String(ops).padStart(13)}`);
  }
  console.log('');
  process.exit(0);
}
main();
