import type { PostgresDeliveredStoreEvent } from '../../infrastructure/persistence/postgres';

const DEFAULT_STARTUP_EVENT_LIMIT = 256;

/** Bounded capture used to reconcile events with an in-flight authoritative snapshot. */
export class PostgresStartupEventBuffer {
  private readonly events: PostgresDeliveredStoreEvent[] = [];
  private didOverflow = false;

  constructor(private readonly limit = DEFAULT_STARTUP_EVENT_LIMIT) {
    if (!Number.isSafeInteger(limit) || limit < 1) {
      throw new RangeError('PostgreSQL startup event buffer limit must be a positive integer');
    }
  }

  get overflowed(): boolean {
    return this.didOverflow;
  }

  capture(event: PostgresDeliveredStoreEvent): void {
    if (this.didOverflow) return;
    if (this.events.length === this.limit) {
      this.didOverflow = true;
      this.events.length = 0;
      return;
    }
    this.events.push(event);
  }

  reset(): void {
    this.events.length = 0;
    this.didOverflow = false;
  }

  take(): readonly PostgresDeliveredStoreEvent[] | null {
    if (this.didOverflow) return null;
    const captured = this.events.splice(0);
    return captured;
  }
}
