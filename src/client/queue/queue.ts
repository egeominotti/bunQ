/** BullMQ-style queue for job management. */

import { Forwarder, type ForwardOptions, type RemoteQueueCtor } from '../forwarder';
import { QueueConnection } from './runtime/connection';

/** Public Queue façade; behavior is implemented by focused runtime layers. */
export class Queue<T = unknown> extends QueueConnection<T> {
  /**
   * Drain this queue's jobs to a remote bunqueue server (store-and-forward).
   * Remote failures leave jobs local and deterministic remote IDs prevent
   * duplicate delivery when a forward is retried.
   */
  forward(options: ForwardOptions): Forwarder {
    return new Forwarder(
      {
        name: this.name,
        queueKey: this.queueKey,
        prefixKey: this.prefixKey || undefined,
        embedded: this.embedded,
        dataPath: this.opts.dataPath,
        connection: this.opts.connection,
      },
      options,
      Queue as unknown as RemoteQueueCtor
    );
  }
}
