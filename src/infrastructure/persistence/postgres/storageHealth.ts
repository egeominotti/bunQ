import type { PostgresStorageHealth, ResolvedPostgresStorageConfig } from './types';

/** Aggregate independent PostgreSQL subsystem failures without hiding older degradation. */
export class PostgresStorageHealthTracker {
  private readonly errors = new Map<string, { message: string; since: number }>();

  constructor(private readonly config: ResolvedPostgresStorageConfig) {}

  record(key: string, error: unknown, prefix = ''): void {
    const message = error instanceof Error ? error.message : String(error);
    const since = this.errors.get(key)?.since ?? Date.now();
    this.errors.set(key, { message: `${prefix}${message}`, since });
  }

  clear(key: string): void {
    this.errors.delete(key);
  }

  snapshot(): PostgresStorageHealth {
    const errors = [...this.errors.entries()].sort(([left], [right]) => left.localeCompare(right));
    const messages = errors.map(([, { message }]) => message);
    const sinceValues = errors.map(([, { since }]) => since);
    return {
      ok: messages.length === 0,
      error: messages.length > 0 ? messages.join('; ') : null,
      since: sinceValues.length > 0 ? Math.min(...sinceValues) : null,
      backend: 'postgres',
      brokerId: this.config.brokerId,
      namespace: this.config.namespace,
    };
  }
}
