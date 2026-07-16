import type { Database } from 'bun:sqlite';
import { rmSync } from 'node:fs';
import type {
  BenchmarkResult,
  Distribution,
  Profile,
  QueueManagerLike,
  RuntimeModules,
  StorageLike,
} from './types';

function round(value: number): number {
  return Number(value.toFixed(3));
}

function median(values: number[]): number {
  const sorted = values.slice().sort((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
}

function distribution(samples: number[]): Distribution {
  const sorted = samples.slice().sort((a, b) => a - b);
  const middle = median(sorted);
  return {
    count: samples.length,
    min: round(sorted[0]),
    median: round(middle),
    p95: round(sorted[Math.max(0, Math.ceil(sorted.length * 0.95) - 1)]),
    max: round(sorted[sorted.length - 1]),
    mad: round(median(sorted.map((sample) => Math.abs(sample - middle)))),
  };
}

export class Harness {
  readonly results: BenchmarkResult[] = [];
  sink = 0;

  constructor(
    readonly runtime: RuntimeModules,
    readonly profile: Profile,
    readonly label: string
  ) {}

  addResult(result: Omit<BenchmarkResult, 'unit' | 'direction' | 'distribution'>): void {
    const complete: BenchmarkResult = {
      ...result,
      unit: 'ms',
      direction: 'lower-is-better',
      distribution: distribution(result.samples),
    };
    this.results.push(complete);
    console.log(
      `${complete.id.padEnd(34)} median=${complete.distribution.median.toFixed(3)} ms ` +
        `p95=${complete.distribution.p95.toFixed(3)} ms ` +
        `correct=${JSON.stringify(complete.correctness)}`
    );
  }
}

export function elapsedMs(startedAt: bigint): number {
  return Number(Bun.nanoseconds() - startedAt) / 1e6;
}

export function rounded(value: number): number {
  return round(value);
}

export function removeDatabase(dbPath: string): void {
  for (const suffix of ['', '-wal', '-shm']) {
    try {
      rmSync(dbPath + suffix, { force: true });
    } catch {
      // Best-effort cleanup for SQLite sidecars.
    }
  }
}

export function storageDb(storage: StorageLike): Database {
  return (storage as unknown as { db: Database }).db;
}

export function managerStorage(manager: QueueManagerLike): StorageLike {
  return (manager as unknown as { storage: StorageLike }).storage;
}

export function padded(prefix: string, index: number, width = 8): string {
  return `${prefix}${String(index).padStart(width, '0')}`;
}
