import type { AsyncCommand } from 'fast-check';
import type { QueueModel, RealQueue } from './queue-model-harness';

export abstract class QueueCommand implements AsyncCommand<QueueModel, RealQueue> {
  abstract check(model: Readonly<QueueModel>): boolean;
  abstract run(model: QueueModel, real: RealQueue): Promise<void>;
  abstract toString(): string;

  protected async verify(model: QueueModel, real: RealQueue): Promise<void> {
    await real.assertConsistent(model);
  }
}
