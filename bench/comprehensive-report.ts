export interface BenchResult {
  scale: number;
  pushOps: number;
  bulkPushOps: number;
  processOps: number;
}

type Log = (message: string) => void;

export function printResults(log: Log, mode: string, results: BenchResult[]): void {
  log(`\n📊 ${mode} MODE RESULTS\n`);
  log('┌──────────┬────────────────┬────────────────┬────────────────┐');
  log('│ Scale    │ Push (ops/s)   │ Bulk (ops/s)   │ Process (ops/s)│');
  log('├──────────┼────────────────┼────────────────┼────────────────┤');

  for (const result of results) {
    const scale = result.scale.toLocaleString().padStart(8);
    const push = result.pushOps.toLocaleString().padStart(12);
    const bulk = result.bulkPushOps.toLocaleString().padStart(12);
    const process = result.processOps.toLocaleString().padStart(12);
    log(`│ ${scale} │ ${push}   │ ${bulk}   │ ${process}   │`);
  }

  log('└──────────┴────────────────┴────────────────┴────────────────┘');
}

export function printComparison(log: Log, embedded: BenchResult[], tcp: BenchResult[]): void {
  log('\n📈 EMBEDDED vs TCP (Embedded is X times faster)\n');
  log('┌──────────┬────────────────┬────────────────┬────────────────┐');
  log('│ Scale    │ Push           │ Bulk           │ Process        │');
  log('├──────────┼────────────────┼────────────────┼────────────────┤');

  for (let index = 0; index < embedded.length; index++) {
    const local = embedded[index];
    const remote = tcp[index];
    const scale = local.scale.toLocaleString().padStart(8);
    const push = `${(local.pushOps / remote.pushOps).toFixed(1)}x`.padStart(12);
    const bulk = `${(local.bulkPushOps / remote.bulkPushOps).toFixed(1)}x`.padStart(12);
    const process = `${(local.processOps / remote.processOps).toFixed(1)}x`.padStart(12);
    log(`│ ${scale} │ ${push}   │ ${bulk}   │ ${process}   │`);
  }

  log('└──────────┴────────────────┴────────────────┴────────────────┘');
}
