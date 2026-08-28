/**
 * Forwarder — store-and-forward from a local (edge) queue to a remote
 * bunqueue server.
 *
 * The edge pattern: jobs buffer in the local queue (typically embedded
 * SQLite on a gateway); the Forwarder drains them to a central server over
 * TCP/TLS. A remote failure makes the local job fail → local retry/backoff →
 * local DLQ. With a persistent local `dataPath`, queued work survives uplink
 * outages and process restarts; memory-only sources provide no such guarantee.
 *
 * Idempotency: every forwarded job carries the deterministic remote jobId
 * `fwd:<localQueueKey>:<localJobId>`. The server dedupes custom jobIds while
 * their ownership is retained; after bounded retention or removal, downstream
 * effects must still tolerate at-least-once delivery.
 */

import { EventEmitter } from 'events';
import { Worker } from './worker';
import type { ConnectionOptions, Job, QueueOptions } from './types';

/** Options for queue.forward() */
export interface ForwardOptions {
  /** Remote server connection (host/port/tls/token...) */
  to: ConnectionOptions;
  /** Remote queue name (default: same as the source queue) */
  queue?: string;
  /** Parallel forwards (default: 4) */
  concurrency?: number;
  /** Push with durable: true (bypasses SQLite buffering; PostgreSQL is already transactional). */
  durable?: boolean;
}

/** Source queue identity + config, provided by Queue.forward() */
export interface ForwardSource {
  /** Logical source queue name */
  name: string;
  /** Source queue key (prefixKey + name) — used in the deterministic jobId */
  queueKey: string;
  prefixKey?: string;
  embedded: boolean;
  dataPath?: string;
  connection?: ConnectionOptions;
}

/** Minimal remote-queue surface the Forwarder needs (avoids a Queue import cycle) */
interface RemoteQueueLike {
  add(
    name: string,
    data: unknown,
    opts?: { jobId?: string; priority?: number; durable?: boolean }
  ): Promise<unknown>;
  close(): Promise<void> | void;
}

/** Constructor for the remote queue, injected by Queue.forward() */
export type RemoteQueueCtor = new (name: string, opts: QueueOptions) => RemoteQueueLike;

/** Info emitted with each 'forwarded' event */
export interface ForwardedInfo {
  /** Local job id */
  id: string;
  /** Deterministic remote job id (fwd:<queueKey>:<id>) */
  remoteId: string;
  /** Job name */
  name: string;
}

/**
 * Drains a local queue into a remote bunqueue server.
 * Create via `queue.forward(options)`; stop with `close()`.
 */
export class Forwarder extends EventEmitter {
  on(event: 'forwarded', listener: (info: ForwardedInfo) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  on(event: string, listener: (...args: any[]) => void): this {
    return super.on(event, listener);
  }

  private readonly remote: RemoteQueueLike;
  private readonly worker: Worker<unknown, ForwardedInfo>;
  private closed = false;

  constructor(source: ForwardSource, options: ForwardOptions, RemoteQueue: RemoteQueueCtor) {
    super();

    this.remote = new RemoteQueue(options.queue ?? source.name, {
      embedded: false,
      connection: options.to,
      autoBatch: { enabled: false },
    });

    this.worker = new Worker<unknown, ForwardedInfo>(
      source.name,
      async (job: Job) => {
        const remoteId = `fwd:${source.queueKey}:${job.id}`;
        await this.remote.add(job.name, job.data, {
          jobId: remoteId,
          ...(job.opts.priority !== undefined && { priority: job.opts.priority }),
          ...(options.durable && { durable: true }),
        });
        const info: ForwardedInfo = { id: job.id, remoteId, name: job.name };
        // A throwing user listener must not fail a forward that already
        // succeeded remotely (it would trigger a local retry of a done job).
        try {
          this.emit('forwarded', info);
        } catch {
          /* listener error — forward itself succeeded */
        }
        return info;
      },
      {
        embedded: source.embedded,
        dataPath: source.dataPath,
        connection: source.connection,
        prefixKey: source.prefixKey,
        concurrency: options.concurrency ?? 4,
      }
    );

    // A remote failure is already handled by the local retry/DLQ path; the
    // event is observability. Guard: EventEmitter throws on 'error' without
    // listeners, and a transient uplink failure must never crash the process.
    this.worker.on('failed', (_job, err) => {
      if (this.listenerCount('error') > 0) this.emit('error', err);
    });
    this.worker.on('error', (err) => {
      if (this.listenerCount('error') > 0) this.emit('error', err);
    });
  }

  /** Stop forwarding and release connections. Safe to call more than once. */
  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await this.worker.close();
    await this.remote.close();
  }
}
