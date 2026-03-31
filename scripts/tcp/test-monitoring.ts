#!/usr/bin/env bun
/**
 * Test Monitoring & Stats (TCP Mode): Stats, Metrics, Prometheus
 */

import { Queue, Worker } from '../../src/client';
import { pack, unpack } from 'msgpackr';
import { FrameParser } from '../../src/infrastructure/server/protocol';
import type { Socket } from 'bun';

const QUEUE_NAME = 'tcp-test-monitoring';
const TCP_PORT = parseInt(process.env.TCP_PORT ?? '16789');

/** Socket data context for TCP client */
interface SocketData {
  frameParser: FrameParser;
  resolve: ((value: Record<string, unknown>) => void) | null;
  reject: ((error: Error) => void) | null;
}

/** Create a TCP connection to the server */
async function createTcpClient(): Promise<{
  send: (cmd: Record<string, unknown>) => Promise<Record<string, unknown>>;
  close: () => void;
}> {
  return new Promise((resolve, reject) => {
    const socketData: SocketData = {
      frameParser: new FrameParser(),
      resolve: null,
      reject: null,
    };

    let socket: Socket<unknown> | null = null;

    const socketHandlers = {
      data(_sock: Socket<unknown>, data: Buffer) {
        const frames = socketData.frameParser.addData(new Uint8Array(data));
        for (const frame of frames) {
          if (socketData.resolve) {
            try {
              const response = unpack(frame) as Record<string, unknown>;
              socketData.resolve(response);
              socketData.resolve = null;
              socketData.reject = null;
            } catch {
              if (socketData.reject) {
                socketData.reject(new Error('Invalid response'));
                socketData.resolve = null;
                socketData.reject = null;
              }
            }
          }
        }
      },
      open(sock: Socket<unknown>) {
        socket = sock;
        resolve({
          send: (cmd: Record<string, unknown>) => {
            return new Promise((res, rej) => {
              socketData.resolve = res;
              socketData.reject = rej;
              sock.write(FrameParser.frame(pack(cmd)));
              setTimeout(() => {
                if (socketData.resolve === res) {
                  socketData.resolve = null;
                  socketData.reject = null;
                  rej(new Error('Command timeout'));
                }
              }, 10000);
            });
          },
          close: () => sock.end(),
        });
      },
      error(_sock: Socket<unknown>, error: Error) {
        reject(new Error(`Connection error: ${error.message}`));
      },
      connectError(_sock: Socket<unknown>, error: Error) {
        reject(new Error(`Failed to connect: ${error.message}`));
      },
    };

    void Bun.connect({
      hostname: 'localhost',
      port: TCP_PORT,
      socket: socketHandlers,
    });

    setTimeout(() => {
      if (!socket) {
        reject(new Error('Connection timeout'));
      }
    }, 5000);
  });
}

async function main() {
  console.log('=== Test Monitoring & Stats (TCP) ===\n');

  const queue = new Queue<{ value: number }>(QUEUE_NAME, {
    connection: { port: TCP_PORT },
  });

  let tcp: Awaited<ReturnType<typeof createTcpClient>> | null = null;
  let passed = 0;
  let failed = 0;

  try {
    tcp = await createTcpClient();
  } catch (e) {
    console.error(`Failed to connect to TCP server: ${e}`);
    console.error('Make sure the server is running on port', TCP_PORT);
    process.exit(1);
  }

  // Clean state
  queue.obliterate();
  await Bun.sleep(100);

  // Test 1: Stats - Get queue statistics via TCP
  console.log('1. Testing STATS (TCP command)...');
  try {
    const response = await tcp.send({ cmd: 'Stats' });
    const stats = response.stats as Record<string, unknown> | undefined;

    if (
      response.ok === true &&
      stats &&
      typeof stats.queued === 'number' &&
      typeof stats.processing === 'number' &&
      typeof stats.delayed === 'number' &&
      typeof stats.dlq === 'number' &&
      typeof stats.completed === 'number' &&
      typeof stats.uptime === 'number'
    ) {
      console.log(`   PASS Stats structure valid:`);
      console.log(`     - queued: ${stats.queued}`);
      console.log(`     - processing: ${stats.processing}`);
      console.log(`     - delayed: ${stats.delayed}`);
      console.log(`     - dlq: ${stats.dlq}`);
      console.log(`     - completed: ${stats.completed}`);
      console.log(`     - uptime: ${stats.uptime}ms`);
      passed++;
    } else {
      console.log('   FAIL Stats response invalid');
      console.log(`   Response: ${JSON.stringify(response)}`);
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Stats test failed: ${e}`);
    failed++;
  }

  // Test 2: Metrics - Get detailed metrics via TCP
  console.log('\n2. Testing METRICS (TCP command)...');
  try {
    const response = await tcp.send({ cmd: 'Metrics' });
    const metrics = response.metrics as Record<string, unknown> | undefined;

    if (
      response.ok === true &&
      metrics &&
      typeof metrics.totalPushed === 'number' &&
      typeof metrics.totalPulled === 'number' &&
      typeof metrics.totalCompleted === 'number' &&
      typeof metrics.totalFailed === 'number' &&
      typeof metrics.memoryUsageMb === 'number'
    ) {
      console.log(`   PASS Metrics structure valid:`);
      console.log(`     - totalPushed: ${metrics.totalPushed}`);
      console.log(`     - totalPulled: ${metrics.totalPulled}`);
      console.log(`     - totalCompleted: ${metrics.totalCompleted}`);
      console.log(`     - totalFailed: ${metrics.totalFailed}`);
      console.log(`     - memoryUsageMb: ${(metrics.memoryUsageMb as number).toFixed(2)}`);
      passed++;
    } else {
      console.log('   FAIL Metrics response invalid');
      console.log(`   Response: ${JSON.stringify(response)}`);
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Metrics test failed: ${e}`);
    failed++;
  }

  // Test 3: Prometheus - Get Prometheus format metrics via TCP
  console.log('\n3. Testing PROMETHEUS METRICS (TCP command)...');
  try {
    const response = await tcp.send({ cmd: 'Prometheus' });
    // Prometheus response is nested under data.metrics
    const data = response.data as Record<string, unknown> | undefined;
    const prometheusMetrics = data?.metrics as string | undefined;

    if (
      response.ok === true &&
      typeof prometheusMetrics === 'string' &&
      prometheusMetrics.includes('bunqueue_jobs_waiting') &&
      prometheusMetrics.includes('bunqueue_jobs_delayed') &&
      prometheusMetrics.includes('bunqueue_jobs_active') &&
      prometheusMetrics.includes('bunqueue_jobs_pushed_total') &&
      prometheusMetrics.includes('bunqueue_uptime_seconds')
    ) {
      console.log('   PASS Prometheus format valid');
      console.log('   Sample lines:');
      const lines = prometheusMetrics.split('\n').filter(l => !l.startsWith('#') && l.trim());
      for (const line of lines.slice(0, 5)) {
        console.log(`     ${line}`);
      }
      passed++;
    } else {
      console.log('   FAIL Prometheus format invalid');
      console.log(`   Response: ${JSON.stringify(response).substring(0, 200)}`);
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Prometheus test failed: ${e}`);
    failed++;
  }

  // Test 4: Stats after adding jobs
  console.log('\n4. Testing STATS AFTER ADDING JOBS...');
  try {
    const statsBefore = await tcp.send({ cmd: 'Stats' });
    const statsDataBefore = statsBefore.stats as Record<string, unknown>;
    const queuedBefore = (statsDataBefore?.queued as number) ?? 0;

    // Add some jobs
    await queue.add('job-1', { value: 1 });
    await queue.add('job-2', { value: 2 });
    await queue.add('job-3', { value: 3 });
    await Bun.sleep(100);

    const statsAfter = await tcp.send({ cmd: 'Stats' });
    const statsDataAfter = statsAfter.stats as Record<string, unknown>;
    const queuedAfter = (statsDataAfter?.queued as number) ?? 0;

    if (queuedAfter >= queuedBefore + 3) {
      console.log(`   PASS Stats updated after adding jobs:`);
      console.log(`     - queued: ${queuedBefore} -> ${queuedAfter}`);
      passed++;
    } else {
      console.log(`   FAIL Stats not updated correctly after adding jobs`);
      console.log(`     - queued: ${queuedBefore} -> ${queuedAfter} (expected >= ${queuedBefore + 3})`);
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Stats after adding jobs failed: ${e}`);
    failed++;
  }

  // Test 5: Stats after processing jobs
  console.log('\n5. Testing STATS AFTER PROCESSING JOBS...');
  try {
    const statsBefore = await tcp.send({ cmd: 'Stats' });
    const statsDataBefore = statsBefore.stats as Record<string, unknown>;
    const queuedBefore = (statsDataBefore?.queued as number) ?? 0;
    const completedBefore = (statsDataBefore?.completed as number) ?? 0;

    // Process jobs
    const worker = new Worker<{ value: number }>(QUEUE_NAME, async (job) => {
      return { processed: (job.data as { value: number }).value };
    }, { concurrency: 5, connection: { port: TCP_PORT }, useLocks: false });

    await Bun.sleep(1000);
    await worker.close();

    const statsAfter = await tcp.send({ cmd: 'Stats' });
    const statsDataAfter = statsAfter.stats as Record<string, unknown>;
    const queuedAfter = (statsDataAfter?.queued as number) ?? 0;
    const completedAfter = (statsDataAfter?.completed as number) ?? 0;

    if (queuedAfter < queuedBefore) {
      console.log(`   PASS Stats updated after processing:`);
      console.log(`     - queued: ${queuedBefore} -> ${queuedAfter}`);
      console.log(`     - completed: ${completedBefore} -> ${completedAfter}`);
      passed++;
    } else {
      console.log('   FAIL Stats not updated correctly after processing');
      console.log(`     - queued: ${queuedBefore} -> ${queuedAfter} (expected decrease)`);
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Stats after processing failed: ${e}`);
    failed++;
  }

  // Test 6: Memory usage in metrics
  console.log('\n6. Testing MEMORY USAGE IN METRICS...');
  try {
    const response = await tcp.send({ cmd: 'Metrics' });
    const metrics = response.metrics as Record<string, unknown> | undefined;

    if (
      response.ok === true &&
      metrics &&
      typeof metrics.memoryUsageMb === 'number' &&
      (metrics.memoryUsageMb as number) > 0
    ) {
      console.log(`   PASS Memory usage reported:`);
      console.log(`     - memoryUsageMb: ${(metrics.memoryUsageMb as number).toFixed(2)}`);
      console.log(`     - avgLatencyMs: ${metrics.avgLatencyMs}`);
      console.log(`     - avgProcessingMs: ${metrics.avgProcessingMs}`);
      console.log(`     - activeConnections: ${metrics.activeConnections}`);
      passed++;
    } else {
      console.log('   FAIL Memory usage not reported');
      console.log(`   Response: ${JSON.stringify(response)}`);
      failed++;
    }
  } catch (e) {
    console.log(`   FAIL Memory usage test failed: ${e}`);
    failed++;
  }

  // Cleanup
  queue.obliterate();
  queue.close();
  tcp.close();

  // Summary
  console.log('\n=== Summary ===');
  console.log(`Passed: ${passed}`);
  console.log(`Failed: ${failed}`);

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(console.error);
