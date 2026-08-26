interface PendingMaintenance {
  readonly operation: () => Promise<unknown>;
}

type MaintenanceReporter = (subsystem: string, error: unknown) => void;

/** Coalesce and retry idempotent work that follows an already committed transition. */
export class PostgresPostCommitMaintenance {
  private readonly pending = new Map<string, PendingMaintenance>();
  private timer: ReturnType<typeof setTimeout> | null = null;
  private closed = false;

  constructor(
    private readonly report: MaintenanceReporter,
    private readonly retryDelayMs: number
  ) {}

  async run(subsystem: string, operation: () => Promise<unknown>): Promise<void> {
    if (this.closed) return;
    const pending = { operation };
    this.pending.set(subsystem, pending);
    await this.attempt(subsystem, pending);
  }

  close(): void {
    this.closed = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
    this.pending.clear();
  }

  private async attempt(subsystem: string, pending: PendingMaintenance): Promise<void> {
    if (this.closed || this.pending.get(subsystem) !== pending) return;
    try {
      await pending.operation();
      if (this.pending.get(subsystem) !== pending) return;
      this.pending.delete(subsystem);
      this.report(subsystem, null);
    } catch (error) {
      if (this.pending.get(subsystem) !== pending) return;
      this.report(subsystem, error);
      this.scheduleRetry();
    }
  }

  private scheduleRetry(): void {
    if (this.closed || this.timer || this.pending.size === 0) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      void this.retryPending();
    }, this.retryDelayMs);
  }

  private async retryPending(): Promise<void> {
    const pending = [...this.pending.entries()];
    await Promise.all(pending.map(([subsystem, entry]) => this.attempt(subsystem, entry)));
    this.scheduleRetry();
  }
}
