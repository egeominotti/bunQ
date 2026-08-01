import { runCoreFeatureSections } from './full-features/core';
import { queueManager, printSummary } from './full-features/harness';
import { runOperationalFeatureSections } from './full-features/operations';
import { runSchedulingFeatureSections } from './full-features/scheduling';

async function main(): Promise<void> {
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║              bunQ FULL FEATURES TEST                       ║');
  console.log('╚════════════════════════════════════════════════════════════╝');
  await runCoreFeatureSections();
  await runSchedulingFeatureSections();
  await runOperationalFeatureSections();
  printSummary();
  queueManager.shutdown();
}

main().catch(console.error);
