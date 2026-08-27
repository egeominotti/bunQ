import { AsyncLocalStorage } from 'node:async_hooks';

interface PostgresOperationScope {
  active: boolean;
}

interface QueuedAdmission {
  readonly resolve: () => void;
}

export type PostgresSyncAdmission<T> =
  | { readonly accepted: true; readonly value: T }
  | { readonly accepted: false };

/** Stop new PostgreSQL operations and drain work admitted before shutdown. */
export class PostgresOperationGate {
  private readonly scopes = new AsyncLocalStorage<PostgresOperationScope>();
  private accepting = true;
  private active = 0;
  private running = 0;
  private readonly queued: QueuedAdmission[] = [];
  private drainPromise: Promise<void> | null = null;
  private resolveDrain: (() => void) | null = null;

  constructor(
    private readonly maxConcurrent = Number.POSITIVE_INFINITY,
    private readonly maxQueued = Number.POSITIVE_INFINITY
  ) {}

  async run<T>(operation: () => Promise<T>): Promise<T> {
    const parent = this.scopes.getStore();
    const nested = parent?.active === true;
    await this.admit(nested);
    const scope = { active: true };
    try {
      return await this.scopes.run(scope, operation);
    } finally {
      this.finish(scope, nested);
    }
  }

  runSync<T>(operation: () => T): T {
    const scope = this.enterSync();
    try {
      return this.scopes.run(scope, operation);
    } finally {
      this.finish(scope, true);
    }
  }

  tryRunSync<T>(operation: () => T): PostgresSyncAdmission<T> {
    if (!this.accepting && !this.scopes.getStore()?.active) return { accepted: false };
    return { accepted: true, value: this.runSync(operation) };
  }

  closeAndDrain(): Promise<void> {
    if (this.scopes.getStore()?.active) {
      throw new Error('Cannot shut down PostgreSQL from an active queue operation');
    }
    this.accepting = false;
    if (this.active === 0) return Promise.resolve();
    this.drainPromise ??= new Promise<void>((resolve) => {
      this.resolveDrain = resolve;
    });
    return this.drainPromise;
  }

  private async admit(nested: boolean): Promise<void> {
    if (!this.accepting && !this.scopes.getStore()?.active) {
      throw new Error('PostgreSQL queue manager is shutting down');
    }
    if (nested) {
      this.active++;
      return;
    }
    if (this.running < this.maxConcurrent) {
      this.running++;
      this.active++;
      return;
    }
    if (this.queued.length >= this.maxQueued) {
      throw new Error('PostgreSQL operation queue is saturated');
    }
    this.active++;
    await new Promise<void>((resolve) => this.queued.push({ resolve }));
  }

  private enterSync(): PostgresOperationScope {
    if (!this.accepting && !this.scopes.getStore()?.active) {
      throw new Error('PostgreSQL queue manager is shutting down');
    }
    this.active++;
    return { active: true };
  }

  private finish(scope: PostgresOperationScope, nested: boolean): void {
    scope.active = false;
    this.active--;
    if (!nested) {
      const next = this.queued.shift();
      if (next) next.resolve();
      else this.running--;
    }
    if (this.active !== 0) return;
    this.resolveDrain?.();
    this.resolveDrain = null;
  }
}
