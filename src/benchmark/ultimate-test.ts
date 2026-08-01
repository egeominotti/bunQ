#!/usr/bin/env bun

import { QueueManager } from '../application/queueManager';
import { testRaceConditions, testStallDetection } from './ultimate/concurrency';
import {
  testBasicApis,
  testBatchOperations,
  testCronJobs,
  testDelayedJobs,
  testDependencies,
  testPriorities,
  testQueueControl,
  testUniqueKeys,
} from './ultimate/core';
import { cleanup, TEST_DB, TestResults } from './ultimate/harness';
import { testClientDisconnect, testTcpMode, testTcpThroughput } from './ultimate/tcp';
import { testDataIntegrity, testHighVolume, testMemoryStability } from './ultimate/volume';

async function main(): Promise<void> {
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║          ULTIMATE TEST - Production Readiness              ║');
  console.log('╚════════════════════════════════════════════════════════════╝');
  await cleanup();
  const queueManager = new QueueManager({ dataPath: TEST_DB });
  const results = new TestResults();
  try {
    await testBasicApis(queueManager, results);
    await testBatchOperations(queueManager, results);
    await testPriorities(queueManager, results);
    await testDelayedJobs(queueManager, results);
    await testDependencies(queueManager, results);
    await testCronJobs(queueManager, results);
    await testUniqueKeys(queueManager, results);
    await testQueueControl(queueManager, results);
    await testTcpMode(queueManager, results);
    await testTcpThroughput(queueManager, results);
    await testRaceConditions(queueManager, results);
    await testClientDisconnect(queueManager, results);
    await testStallDetection(queueManager, results);
    await testMemoryStability(queueManager, results);
    await testDataIntegrity(queueManager, results);
    await testHighVolume(queueManager, results);
    process.exit(results.summary() ? 0 : 1);
  } catch (error) {
    console.error('\n❌ FATAL ERROR:', error);
    process.exit(1);
  } finally {
    queueManager.shutdown();
    await cleanup();
  }
}

main();
