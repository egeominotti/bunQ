import { AsyncLocalStorage } from 'node:async_hooks';

interface PostgresOperationScope {
  active: boolean;
}

export type PostgresSyncAdmission<T> =
  | { readonly accepted: true; readonly value: T }
  | { readonly accepted: false };

/** Stop new PostgreSQL operations and drain work admitted before shutdown. */
export class PostgresOperationGate {
  private readonly scopes = new AsyncLocalStorage<PostgresOperationScope>();
  private accepting = true;
  private active = 0;
  private drainPromise: Promise<void> | null = null;
  private resolveDrain: (() => void) | null = null;

  async run<T>(operation: () => Promise<T>): Promise<T> {
    const scope = this.enter();
    try {
      return await this.scopes.run(scope, operation);
    } finally {
      this.finish(scope);
    }
  }

  runSync<T>(operation: () => T): T {
    const scope = this.enter();
    try {
      return this.scopes.run(scope, operation);
    } finally {
      this.finish(scope);
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

  private enter(): PostgresOperationScope {
    if (!this.accepting && !this.scopes.getStore()?.active) {
      throw new Error('PostgreSQL queue manager is shutting down');
    }
    this.active++;
    return { active: true };
  }

  private finish(scope: PostgresOperationScope): void {
    scope.active = false;
    this.active--;
    if (this.active !== 0) return;
    this.resolveDrain?.();
    this.resolveDrain = null;
  }
}
