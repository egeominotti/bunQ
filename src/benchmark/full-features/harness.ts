import { QueueManager } from '../../application/queueManager';

export const queueManager = new QueueManager();

let passed = 0;
let failed = 0;

export function test(name: string, assertion: () => boolean | Promise<boolean>): Promise<void> {
  return Promise.resolve(assertion())
    .then((result) => {
      if (result) {
        console.log(`  ✅ ${name}`);
        passed++;
      } else {
        console.log(`  ❌ ${name}`);
        failed++;
      }
    })
    .catch((error: unknown) => {
      console.log(`  ❌ ${name}: ${(error as { message?: unknown }).message}`);
      failed++;
    });
}

export async function section(name: string, tests: () => Promise<void>): Promise<void> {
  console.log(`\n${'═'.repeat(50)}`);
  console.log(`📦 ${name}`);
  console.log('═'.repeat(50));
  await tests();
}

export function printSummary(): void {
  console.log(`\n${'═'.repeat(50)}`);
  console.log('📊 FINAL RESULTS');
  console.log('═'.repeat(50));
  console.log(`  Total tests: ${passed + failed}`);
  console.log(`  ✅ Passed:   ${passed}`);
  console.log(`  ❌ Failed:   ${failed}`);
  console.log('═'.repeat(50));
  if (failed === 0) console.log('\n🎉 ALL FEATURES WORKING CORRECTLY!\n');
  else console.log(`\n⚠️  ${failed} FEATURES NEED ATTENTION\n`);
}
