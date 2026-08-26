interface DeferredWriteFailure {
  readonly sequence: number;
  readonly error: unknown;
}

interface DrainCheckpoint {
  readonly sequence: number;
  readonly result: Promise<readonly unknown[]>;
}

export function throwDeferredWriteErrors(
  errors: readonly unknown[],
  message = 'Multiple deferred PostgreSQL writes failed'
): void {
  if (errors.length === 0) return;
  if (errors.length === 1) throw errors[0];
  throw new AggregateError(errors, message);
}

/** Serializes fire-and-forget writes while retaining failures until an observed drain. */
export class PostgresDeferredWriteQueue {
  private accepting = true;
  private tail: Promise<void> = Promise.resolve();
  private nextSequence = 0;
  private readonly failures: DeferredWriteFailure[] = [];
  private checkpoint: DrainCheckpoint | null = null;

  enqueue(operation: () => Promise<void>): void {
    if (!this.accepting) throw new Error('PostgreSQL queue manager is shutting down');
    const sequence = ++this.nextSequence;
    this.tail = this.tail.then(async () => {
      try {
        await operation();
      } catch (error) {
        this.failures.push({ sequence, error });
      }
    });
  }

  closeAndDrain(): Promise<unknown[]> {
    this.accepting = false;
    return this.drain();
  }

  async drain(): Promise<unknown[]> {
    const throughSequence = this.nextSequence;
    const pending = this.checkpoint;
    if (pending?.sequence === throughSequence) return [...(await pending.result)];

    const throughTail = this.tail;
    const checkpoint: DrainCheckpoint = {
      sequence: throughSequence,
      result: throughTail.then(() => {
        const firstLaterFailure = this.failures.findIndex(
          ({ sequence }) => sequence > throughSequence
        );
        const observed =
          firstLaterFailure === -1
            ? this.failures.splice(0)
            : this.failures.splice(0, firstLaterFailure);
        return observed.map(({ error }) => error);
      }),
    };
    this.checkpoint = checkpoint;
    try {
      return [...(await checkpoint.result)];
    } finally {
      if (this.checkpoint === checkpoint) this.checkpoint = null;
    }
  }

  async flush(): Promise<void> {
    throwDeferredWriteErrors(await this.drain());
  }
}
