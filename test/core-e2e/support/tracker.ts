/** Runtime-discovered exported class or structural object name. */
export type CoreSurface = string;

export type CoverageMode = 'embedded' | 'tcp';
export type CoverageOperation = 'async-success' | 'sync-success' | 'expected-rejection';

export interface CoverageRecord {
  contract: string;
  durationMs: number;
  key: string;
  method: string;
  mode: CoverageMode;
  operation: CoverageOperation;
  source: string;
  surface: CoreSurface;
}

export function ensure(condition: unknown, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

export function ensureEqual(actual: unknown, expected: unknown, message: string): void {
  const left = JSON.stringify(actual);
  const right = JSON.stringify(expected);
  if (left !== right) throw new Error(`${message}: expected ${right}, received ${left}`);
}

export async function eventually(
  read: () => unknown | Promise<unknown>,
  predicate: (value: unknown) => boolean,
  message: string,
  timeoutMs = 10_000
): Promise<unknown> {
  const deadline = Date.now() + timeoutMs;
  let value = await read();
  while (!predicate(value) && Date.now() < deadline) {
    await Bun.sleep(20);
    value = await read();
  }
  ensure(predicate(value), `${message}: last value was ${JSON.stringify(value)}`);
  return value;
}

export class CoverageTracker {
  private readonly entries = new Map<string, CoverageRecord[]>();

  constructor(
    private readonly mode?: CoverageMode,
    private readonly contract?: string
  ) {}

  private record(
    surface: CoreSurface,
    method: string,
    operation: CoverageOperation,
    startedAt: number
  ): void {
    if (!this.mode || !this.contract) {
      throw new Error(`Coverage context missing for ${surface}.${method}`);
    }
    const key = `${surface}.${method}`;
    const stackLine = new Error().stack
      ?.split('\n')
      .find((line) => line.includes('/test/core-e2e/contracts/'));
    const source =
      stackLine?.match(/(test\/core-e2e\/contracts\/[^:)]+:\d+)(?::\d+)?/)?.[1] ??
      `test/core-e2e/contracts/${this.contract}.ts`;
    const records = this.entries.get(key) ?? [];
    records.push({
      contract: this.contract,
      durationMs: Math.max(0, performance.now() - startedAt),
      key,
      method,
      mode: this.mode,
      operation,
      source,
      surface,
    });
    this.entries.set(key, records);
  }

  async invoke<T>(surface: CoreSurface, method: string, operation: () => Promise<T>): Promise<T> {
    const startedAt = performance.now();
    const result = await operation();
    this.record(surface, method, 'async-success', startedAt);
    return result;
  }

  call<T>(surface: CoreSurface, method: string, operation: () => T): T {
    const startedAt = performance.now();
    const result = operation();
    this.record(surface, method, 'sync-success', startedAt);
    return result;
  }

  async rejects(
    surface: CoreSurface,
    method: string,
    operation: () => unknown | Promise<unknown>,
    expected: RegExp
  ): Promise<void> {
    const startedAt = performance.now();
    let failure: unknown;
    try {
      await operation();
    } catch (error) {
      failure = error;
    }
    ensure(failure instanceof Error, `${surface}.${method} did not reject`);
    ensure(
      expected.test(failure.message),
      `${surface}.${method}: unexpected error ${failure.message}`
    );
    this.record(surface, method, 'expected-rejection', startedAt);
  }

  merge(other: CoverageTracker): void {
    for (const record of other.records()) {
      const records = this.entries.get(record.key) ?? [];
      records.push(record);
      this.entries.set(record.key, records);
    }
  }

  covered(): string[] {
    return [...this.entries.keys()].sort();
  }

  records(): CoverageRecord[] {
    return [...this.entries.values()].flat();
  }

  recordsFor(key: string): CoverageRecord[] {
    return [...(this.entries.get(key) ?? [])];
  }
}
