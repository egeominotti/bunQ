import type { BenchResult } from './config';

export function printReport(bun: BenchResult[], bull: BenchResult[]): void {
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log('                         RESULTS');
  console.log('═══════════════════════════════════════════════════════════════\n');
  console.log('┌─────────────┬──────────────────┬──────────────────┬──────────┐');
  console.log('│ Operation   │ bunqueue         │ BullMQ           │ Ratio    │');
  console.log('├─────────────┼──────────────────┼──────────────────┼──────────┤');

  for (let index = 0; index < bun.length; index++) {
    const left = bun[index];
    const right = bull[index];
    const ratio = (left.opsPerSec / right.opsPerSec).toFixed(2);
    console.log(
      `│ ${left.name.padEnd(11)} │ ${`${left.opsPerSec.toLocaleString()} ops/s`.padEnd(16)} │ ` +
        `${`${right.opsPerSec.toLocaleString()} ops/s`.padEnd(16)} │ ${`${ratio}x`.padEnd(8)} │`
    );
  }
  console.log('└─────────────┴──────────────────┴──────────────────┴──────────┘');

  console.log('\nBulk-call latency (p99):');
  for (let index = 0; index < bun.length; index++) {
    if (bun[index].p99Ms === undefined || bull[index].p99Ms === undefined) continue;
    console.log(
      `  ${bun[index].name}: bunqueue ${bun[index].p99Ms}ms; BullMQ ${bull[index].p99Ms}ms`
    );
  }
}
