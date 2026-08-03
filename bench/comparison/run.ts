/** Native bunqueue/SQLite versus BullMQ/Redis comparison entrypoint. */
import IORedis from 'ioredis';
import { Queue } from '../../src/client';
import { runBullMqBenchmarks } from './bullmq';
import { runBunqueueBenchmarks } from './bunqueue';
import {
  BULK_SIZE,
  BUNQUEUE_HOST,
  BUNQUEUE_PORT,
  CONCURRENCY,
  ITERATIONS,
  PAYLOAD,
  REDIS_HOST,
  REDIS_PORT,
  RUN_ID,
  queueName,
} from './config';
import { printReport } from './report';

export async function main(): Promise<void> {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('           bunqueue vs BullMQ Comparison Benchmark');
  console.log('═══════════════════════════════════════════════════════════════');
  console.log(`\nIterations: ${ITERATIONS.toLocaleString()}`);
  console.log(`Bulk size: ${BULK_SIZE}`);
  console.log(`Concurrency: ${CONCURRENCY}`);
  console.log(`Payload: ${JSON.stringify(PAYLOAD).length} bytes`);
  console.log(`Run ID: ${RUN_ID}`);
  console.log(`bunqueue: ${BUNQUEUE_HOST}:${BUNQUEUE_PORT}`);
  console.log(`Redis: ${REDIS_HOST}:${REDIS_PORT}`);

  const redis = new IORedis({
    host: REDIS_HOST,
    port: REDIS_PORT,
    maxRetriesPerRequest: null,
  });

  try {
    await redis.ping();
    console.log('\n✓ Redis connected');

    const check = new Queue(queueName('connection-check'), {
      embedded: false,
      connection: { host: BUNQUEUE_HOST, port: BUNQUEUE_PORT },
    });
    try {
      await check.waitUntilReady();
      console.log(`✓ bunqueue server connected (port ${BUNQUEUE_PORT})`);
    } finally {
      await check.close();
    }

    const bunResults = await runBunqueueBenchmarks();
    const bullResults = await runBullMqBenchmarks(redis);
    printReport(bunResults, bullResults);
  } finally {
    await redis.quit().catch(() => undefined);
  }
}

if (import.meta.main) {
  main().catch((error) => {
    console.error(error);
    process.exitCode = 1;
  });
}
