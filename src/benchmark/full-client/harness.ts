export const EMBEDDED = { embedded: true } as const;

let testsPassed = 0;
let testsFailed = 0;

export function sleep(ms: number): Promise<void> {
  return Bun.sleep(ms);
}

export function pass(name: string): void {
  testsPassed++;
  console.log(`  ✅ ${name}`);
}

export function fail(name: string, error?: unknown): void {
  testsFailed++;
  let errorMessage = '';
  if (error instanceof Error) errorMessage = error.message;
  else if (typeof error === 'string') errorMessage = error;
  else if (error !== undefined && error !== null) errorMessage = JSON.stringify(error);
  console.log(`  ❌ ${name}${errorMessage ? `: ${errorMessage}` : ''}`);
}

export function printResults(elapsed: number): number {
  console.log(`\n${'═'.repeat(60)}`);
  console.log('📊 RESULTS');
  console.log('═'.repeat(60));
  console.log(`  Total tests:  ${testsPassed + testsFailed}`);
  console.log(`  ✅ Passed:    ${testsPassed}`);
  console.log(`  ❌ Failed:    ${testsFailed}`);
  console.log(`  Time:         ${elapsed.toLocaleString('en-US')}ms`);
  console.log('═'.repeat(60));
  if (testsFailed === 0) console.log('\n🎉 ALL TESTS PASSED!\n');
  else console.log(`\n⚠️ ${testsFailed} test(s) failed\n`);
  return testsFailed;
}
