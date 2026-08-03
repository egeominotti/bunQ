import { testDlqOperations, testFlowProducer } from './full-client/features';
import { printResults, sleep } from './full-client/harness';
import { shutdownManager } from '../client';
import {
  testConnectionModes,
  testQueueGroup,
  testRepeatJobs,
  testStallDetection,
} from './full-client/misc';
import { testJobOptions, testQueueBasicOps } from './full-client/queue';
import {
  testJobMethods,
  testQueueEvents,
  testWorkerOptions,
  testWorkerProcessing,
} from './full-client/workers';

async function main(): Promise<void> {
  try {
    console.log('╔══════════════════════════════════════════════════════════════╗');
    console.log('║          bunqueue Client SDK - Full Feature Test             ║');
    console.log('╚══════════════════════════════════════════════════════════════╝');
    const startTime = Date.now();
    await testQueueBasicOps();
    await testJobOptions();
    await testWorkerProcessing();
    await testQueueEvents();
    await testFlowProducer();
    await testDlqOperations();
    testStallDetection();
    await testQueueGroup();
    await testRepeatJobs();
    await testWorkerOptions();
    await testJobMethods();
    await testConnectionModes();
    const failures = printResults(Date.now() - startTime);
    await sleep(500);
    process.exitCode = failures > 0 ? 1 : 0;
  } finally {
    shutdownManager();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
