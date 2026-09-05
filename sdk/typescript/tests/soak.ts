/** Opt-in sustained producer soak. Run after `bun run build`. */

import { Queue } from '../dist/legacy.js';
import { cleanTempDirs, startServer } from './harness.ts';

const seconds = Math.max(1, Number(process.env.BUNQUEUE_SDK_SOAK_SECONDS ?? 300));
const batchSize = Math.max(1, Number(process.env.BUNQUEUE_SDK_SOAK_BATCH ?? 100));
const { port, proc } = await startServer();
const queue = new Queue<{ iteration: number; index: number }>(`ts-soak-${process.pid}`, {
  host: '127.0.0.1',
  port,
  maxInFlight: 32,
  poolSize: 4,
});
const startedAt = Date.now();
const startedRss = process.memoryUsage().rss;
let iterations = 0;
let jobs = 0;

try {
  while (Date.now() - startedAt < seconds * 1000) {
    const created = await queue.addBulk(
      Array.from({ length: batchSize }, (_, index) => ({
        name: 'soak',
        data: { iteration: iterations, index },
      }))
    );
    if (created.length !== batchSize) throw new Error('soak batch lost job ids');
    if ((await queue.count()) !== batchSize) throw new Error('soak queue count diverged');
    const first = await queue.getJob(created[0].id);
    const last = await queue.getJob(created[created.length - 1].id);
    if (!first || !last) throw new Error('soak query lost a visible job');
    await queue.obliterate();
    iterations += 1;
    jobs += created.length;
  }
  console.log(
    JSON.stringify({
      profile: 'typescript-soak',
      seconds,
      batchSize,
      iterations,
      jobs,
      rssStart: startedRss,
      rssEnd: process.memoryUsage().rss,
    })
  );
} finally {
  queue.close();
  proc.kill();
  cleanTempDirs();
}
