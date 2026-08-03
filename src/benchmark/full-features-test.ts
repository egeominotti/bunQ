import { runCoreFeatureSections } from './full-features/core';
import { queueManager, printSummary } from './full-features/harness';
import { runOperationalFeatureSections } from './full-features/operations';
import { runSchedulingFeatureSections } from './full-features/scheduling';

async function main(): Promise<void> {
  try {
    console.log('╔════════════════════════════════════════════════════════════╗');
    console.log('║              bunQ FULL FEATURES TEST                       ║');
    console.log('╚════════════════════════════════════════════════════════════╝');
    await runCoreFeatureSections();
    await runSchedulingFeatureSections();
    await runOperationalFeatureSections();
    if (!printSummary()) process.exitCode = 1;
  } finally {
    queueManager.shutdown();
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
