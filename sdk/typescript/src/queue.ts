/**
 * Queue: producer + management API over the bunqueue TCP protocol.
 * Method surface mirrors the official TS client (TCP mode).
 *
 * The class is composed from area modules kept under 250 lines each:
 * queue-query.ts (query/counts/logs), queue-control.ts (state control,
 * job mutations) and queue-admin.ts (DLQ, configs, schedulers, webhooks,
 * monitoring). Methods are merged onto the prototype; declaration merging
 * exposes them on the type.
 */

import { Connection, type Response, type TlsOption } from './connection.js';
import { Job } from './job.js';
import { adminMethods, type QueueAdminApi } from './queue-admin.js';
import { controlMethods, type QueueControlApi } from './queue-control.js';
import { type QueueQueryApi, queryMethods } from './queue-query.js';
import { type JobOptions, jobPayload, wireJobOptions } from './types.js';

export interface QueueOptions {
  host?: string;
  port?: number;
  token?: string;
  tls?: TlsOption;
  connection?: Connection;
  commandTimeoutMs?: number;
}

export interface BulkJobEntry<T = unknown> {
  name: string;
  data: T;
  opts?: JobOptions;
}

// biome-ignore lint/suspicious/noUnsafeDeclarationMerging: prototype-mixin composition — Object.assign at the bottom installs exactly the methods the merged interface declares
export class Queue<T = unknown> {
  readonly name: string;
  readonly connection: Connection;
  private readonly ownsConnection: boolean;

  constructor(name: string, opts: QueueOptions = {}) {
    this.name = name;
    this.connection =
      opts.connection ??
      new Connection({
        host: opts.host,
        port: opts.port,
        token: opts.token,
        tls: opts.tls,
        commandTimeoutMs: opts.commandTimeoutMs,
      });
    this.ownsConnection = opts.connection === undefined;
  }

  /** Send a raw command on this queue's connection (used by area modules). */
  call(command: Record<string, unknown> & { cmd: string }, timeoutMs?: number): Promise<Response> {
    return this.connection.call(command, timeoutMs);
  }

  // ------------------------------------------------------------------ produce

  /** Add a job; returns a Job stub carrying the assigned id. */
  async add(name: string, data: T, opts?: JobOptions): Promise<Job<T>> {
    const payload = jobPayload(name, data);
    const response = await this.call({
      cmd: 'PUSH',
      queue: this.name,
      data: payload,
      ...wireJobOptions(opts),
    });
    return new Job<T>({ id: response.id, queue: this.name, data: payload }, this.connection);
  }

  /** Add many jobs in one round-trip; returns Job stubs. */
  async addBulk(jobs: BulkJobEntry<T>[]): Promise<Job<T>[]> {
    const inputs = jobs.map((entry) => {
      const opts = wireJobOptions(entry.opts);
      // PUSHB entries are JobInput, whose custom-id field is `customId` —
      // unlike single PUSH which renames `jobId`->`customId` server-side.
      // Without this the batch custom id is silently dropped (idempotency /
      // getJobByCustomId broken).
      if (opts.jobId !== undefined) {
        opts.customId = opts.jobId;
        delete opts.jobId;
      }
      return { data: jobPayload(entry.name, entry.data), ...opts };
    });
    const response = await this.call({ cmd: 'PUSHB', queue: this.name, jobs: inputs });
    const ids = (response.ids ?? []) as string[];
    return ids.map(
      (id, i) => new Job<T>({ id, queue: this.name, data: inputs[i].data }, this.connection)
    );
  }

  // ---------------------------------------------------------------- lifecycle

  ping(): Promise<boolean> {
    return this.connection.ping();
  }

  async waitUntilReady(): Promise<void> {
    await this.connection.connect();
  }

  close(): void {
    if (this.ownsConnection) this.connection.close();
  }

  /** BullMQ v5 alias for close(). */
  async disconnect(): Promise<void> {
    this.close();
  }
}

/* Merge the area-module methods into the class (runtime + type level).
 * The declaration merging is intentional and safe: Object.assign below
 * installs exactly the methods the interface declares. The unused type
 * parameter is required — merged interfaces must repeat the class generics. */
// biome-ignore lint/correctness/noUnusedVariables: generic must match the class declaration
export interface Queue<T = unknown> extends QueueQueryApi, QueueControlApi, QueueAdminApi {}
Object.assign(Queue.prototype, queryMethods, controlMethods, adminMethods);
