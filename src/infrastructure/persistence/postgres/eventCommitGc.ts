const DEFAULT_BATCH_SIZE = 10_000;
const DEFAULT_MAX_BATCHES = 8;
const DEFAULT_IDLE_DELAY_MS = 60_000;
const DEFAULT_BACKLOG_DELAY_MS = 25;

type EventCommitPruner = (batchSize: number, maxBatches: number) => Promise<number>;
type MaintenanceReporter = (subsystem: string, error: unknown) => void;

export interface PostgresEventCommitGcOptions {
  readonly batchSize?: number;
  readonly maxBatches?: number;
  readonly idleDelayMs?: number;
  readonly backlogDelayMs?: number;
}

/** Adapt journal GC frequency to backlog while keeping each database turn bounded. */
export class PostgresEventCommitGc {
  private timer: ReturnType<typeof setTimeout> | null = null;
  private active: Promise<void> | null = null;
  private closed = false;
  private readonly batchSize: number;
  private readonly maxBatches: number;
  private readonly idleDelayMs: number;
  private readonly backlogDelayMs: number;

  constructor(
    private readonly prune: EventCommitPruner,
    private readonly report: MaintenanceReporter,
    options: PostgresEventCommitGcOptions = {}
  ) {
    this.batchSize = options.batchSize ?? DEFAULT_BATCH_SIZE;
    this.maxBatches = options.maxBatches ?? DEFAULT_MAX_BATCHES;
    this.idleDelayMs = options.idleDelayMs ?? DEFAULT_IDLE_DELAY_MS;
    this.backlogDelayMs = options.backlogDelayMs ?? DEFAULT_BACKLOG_DELAY_MS;
  }

  start(): void {
    this.schedule(this.idleDelayMs);
  }

  close(): void {
    this.closed = true;
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
  }

  async drain(): Promise<void> {
    await this.active;
  }

  private schedule(delayMs: number): void {
    if (this.closed || this.timer) return;
    this.timer = setTimeout(() => {
      this.timer = null;
      this.active = this.runOnce().finally(() => {
        this.active = null;
      });
    }, delayMs);
  }

  private async runOnce(): Promise<void> {
    let nextDelay = this.idleDelayMs;
    try {
      const deleted = await this.prune(this.batchSize, this.maxBatches);
      if (deleted >= this.batchSize * this.maxBatches) nextDelay = this.backlogDelayMs;
      this.report('event-commit-gc', null);
    } catch (error) {
      nextDelay = this.backlogDelayMs;
      this.report('event-commit-gc', error);
    } finally {
      this.schedule(nextDelay);
    }
  }
}
