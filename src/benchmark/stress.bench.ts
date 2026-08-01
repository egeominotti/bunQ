import { testBatchOperations, testMemoryStability } from './stress/memory';
import { testPriorityStress, testRetryStorm } from './stress/scheduling';
import { testConcurrentQueues, testHighVolume, testLargePayloads } from './stress/volume';

async function main(): Promise<void> {
  console.log('╔════════════════════════════════════════════════════════════════╗');
  console.log('║                    bunQ STRESS TESTS                           ║');
  console.log('╚════════════════════════════════════════════════════════════════╝');
  const startTime = performance.now();
  await testHighVolume(100_000);
  await testConcurrentQueues(10, 10_000);
  await testLargePayloads(5_000, 10);
  await testPriorityStress(50_000);
  await testRetryStorm(10_000, 0.5);
  await testMemoryStability(100, 1_000);
  await testBatchOperations(100_000, 1_000);
  const totalTime = (performance.now() - startTime) / 1000;
  console.log(`\n${'═'.repeat(60)}`);
  console.log(`✅ All stress tests completed in ${totalTime.toFixed(1)}s`);
  console.log('═'.repeat(60));
}

main().catch(console.error);
