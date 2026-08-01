import { unlink } from 'fs/promises';

export const TEST_DB = './ultimate-test.db';
export const TCP_PORT = 16888;

export async function cleanup(): Promise<void> {
  for (const file of [TEST_DB, `${TEST_DB}-wal`, `${TEST_DB}-shm`]) {
    if (await Bun.file(file).exists()) await unlink(file);
  }
}

export function formatCount(value: number): string {
  return value.toLocaleString();
}

export function heapMegabytes(): number {
  return Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
}

export class TestResults {
  private readonly results: { name: string; passed: boolean; detail?: string }[] = [];
  private currentSection = '';

  section(name: string): void {
    this.currentSection = name;
    console.log(`\n${'═'.repeat(60)}`);
    console.log(`🔥 ${name}`);
    console.log('═'.repeat(60));
  }

  pass(name: string, detail?: string): void {
    this.results.push({ name: `${this.currentSection}: ${name}`, passed: true, detail });
    console.log(`  ✅ ${name}${detail ? ` - ${detail}` : ''}`);
  }

  fail(name: string, detail?: string): void {
    this.results.push({ name: `${this.currentSection}: ${name}`, passed: false, detail });
    console.log(`  ❌ ${name}${detail ? ` - ${detail}` : ''}`);
  }

  assert(condition: boolean, name: string, detail?: string): void {
    if (condition) this.pass(name, detail);
    else this.fail(name, detail);
  }

  summary(): boolean {
    const passed = this.results.filter((result) => result.passed).length;
    const failed = this.results.filter((result) => !result.passed).length;
    console.log(`\n${'═'.repeat(60)}`);
    console.log('📊 ULTIMATE TEST RESULTS');
    console.log('═'.repeat(60));
    console.log(`  Total:  ${this.results.length}`);
    console.log(`  ✅ Pass: ${passed}`);
    console.log(`  ❌ Fail: ${failed}`);
    console.log('═'.repeat(60));
    if (failed > 0) {
      console.log('\n❌ FAILED TESTS:');
      this.results
        .filter((result) => !result.passed)
        .forEach((result) =>
          console.log(`  - ${result.name}${result.detail ? `: ${result.detail}` : ''}`)
        );
    }
    if (failed === 0) console.log('\n🎉 ALL TESTS PASSED! PRODUCTION READY!');
    else console.log(`\n⚠️  ${failed} TEST(S) FAILED`);
    return failed === 0;
  }
}
