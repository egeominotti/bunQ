export interface ScenarioResult {
  name: string;
  jobs: number;
  workers: number;
  concurrency: number;
  pushTimeMs: number;
  pushOps: number;
  drainTimeMs: number;
  drainOps: number;
  totalTimeMs: number;
  totalOps: number;
  perWorker: number[];
  wakeupP50: number;
  wakeupP95: number;
  wakeupP99: number;
  wakeupMax: number;
}

function formatOps(value: number): string {
  return value >= 1000 ? `${(value / 1000).toFixed(1)}k` : String(value);
}

export function printScenarioResults(results: ScenarioResult[]): void {
  console.log('\n══════════════════════════════════════════════════════════════');
  console.log('                        RESULTS');
  console.log('══════════════════════════════════════════════════════════════\n');

  for (const result of results) {
    const processed = result.perWorker.reduce((sum, count) => sum + count, 0);
    const min = Math.min(...result.perWorker);
    const max = Math.max(...result.perWorker);
    const fairness = min > 0 ? (min / max).toFixed(2) : '0.00';
    console.log(`─── ${result.name} ───\n`);
    console.log('  Throughput');
    console.log(`    Push:      ${formatOps(result.pushOps)} ops/s  (${result.pushTimeMs}ms)`);
    console.log(`    Drain:     ${formatOps(result.drainOps)} ops/s  (${result.drainTimeMs}ms)`);
    console.log(`    Total:     ${formatOps(result.totalOps)} ops/s  (${result.totalTimeMs}ms)\n`);
    console.log('  Wakeup latency');
    console.log(
      `    p50: ${result.wakeupP50}ms | p95: ${result.wakeupP95}ms | p99: ${result.wakeupP99}ms | max: ${result.wakeupMax}ms\n`
    );
    console.log(`  Distribution (${result.workers} workers)`);
    console.log(`    Processed: ${processed}/${result.jobs}`);
    console.log(`    Per worker: min=${min} max=${max} fairness=${fairness}`);
    console.log(`    Breakdown:  [${result.perWorker.join(', ')}]\n`);
  }

  console.log('══════════════════════════════════════════════════════════════');
  console.log('                      SUMMARY TABLE');
  console.log('══════════════════════════════════════════════════════════════\n');
  console.log('Scenario                              | Push    | Total   | p99    | Fairness');
  console.log('--------------------------------------|---------|---------|--------|--------');
  for (const result of results) {
    const min = Math.min(...result.perWorker);
    const max = Math.max(...result.perWorker);
    const fairness = min > 0 ? (min / max).toFixed(2) : '0.00';
    const name = result.name.padEnd(37).slice(0, 37);
    console.log(
      `${name} | ${formatOps(result.pushOps).padStart(7)} | ${formatOps(result.totalOps).padStart(7)} | ${`${result.wakeupP99}ms`.padStart(6)} | ${fairness}`
    );
  }
  console.log();
}
