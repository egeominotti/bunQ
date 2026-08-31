import type {
  ChangePriorityOpts,
  ConnectionOptions,
  GetDependenciesOpts,
  QueueOptions,
} from '../../types';
import { getSharedManager } from '../../manager';
import { resolveToken } from '../../resolveToken';
import { TcpConnectionPool, getSharedPool, releaseSharedPool } from '../../tcpPool';
import { AddBatcher } from '../addBatcher';
import { FORCE_EMBEDDED } from '../helpers';
import * as addOps from '../operations/add';
import type { QueueRuntime } from '../types';

/** Owns Queue connection state and builds the shared operation contexts. */
export class QueueState<T> {
  readonly name: string;
  protected readonly queueKey: string;
  protected readonly prefixKey: string;
  protected readonly opts: QueueOptions;
  protected readonly embedded: boolean;
  protected readonly tcpPool: TcpConnectionPool | null;
  protected readonly useSharedPool: boolean;
  protected readonly addBatcher: AddBatcher<unknown> | null;
  private connectionReleased = false;

  constructor(name: string, opts: QueueOptions = {}) {
    this.name = name;
    this.prefixKey = opts.prefixKey ?? '';
    this.queueKey = this.prefixKey + name;
    this.opts = opts;
    this.embedded = opts.embedded ?? FORCE_EMBEDDED;

    if (this.embedded) {
      getSharedManager(opts.dataPath);
      this.tcpPool = null;
      this.useSharedPool = false;
      this.addBatcher = null;
    } else {
      const connOpts: ConnectionOptions = opts.connection ?? {};
      const poolSize = connOpts.poolSize ?? 4;
      const token = resolveToken(connOpts.token);

      if (poolSize === 4 && !token) {
        this.tcpPool = getSharedPool({
          host: connOpts.host,
          port: connOpts.port,
          tls: connOpts.tls,
          poolSize,
          pingInterval: connOpts.pingInterval,
          commandTimeout: connOpts.commandTimeout,
          maxCommandTimeouts: connOpts.maxCommandTimeouts,
          pipelining: connOpts.pipelining,
          maxInFlight: connOpts.maxInFlight,
        });
        this.useSharedPool = true;
      } else {
        this.tcpPool = new TcpConnectionPool({
          host: connOpts.host ?? 'localhost',
          port: connOpts.port ?? 6789,
          token,
          tls: connOpts.tls,
          poolSize,
          pingInterval: connOpts.pingInterval,
          commandTimeout: connOpts.commandTimeout,
          maxCommandTimeouts: connOpts.maxCommandTimeouts,
          pipelining: connOpts.pipelining,
          maxInFlight: connOpts.maxInFlight,
        });
        this.useSharedPool = false;
      }

      const autoBatch = opts.autoBatch;
      if (autoBatch?.enabled === false) {
        this.addBatcher = null;
      } else {
        this.addBatcher = new AddBatcher(
          {
            maxSize: autoBatch?.maxSize ?? 50,
            maxDelayMs: autoBatch?.maxDelayMs ?? 5,
          },
          (jobs) => addOps.addBulk(this.addCtx, jobs)
        );
      }
    }
  }

  protected get ctx() {
    return {
      name: this.queueKey,
      embedded: this.embedded,
      tcp: this.tcpPool,
      prefixKey: this.prefixKey,
      dataPath: this.opts.dataPath,
    };
  }

  protected get addCtx() {
    const queue = this as unknown as QueueRuntime<T>;
    return {
      ...this.ctx,
      opts: this.opts,
      getJobState: (id: string) => queue.getJobState(id),
      removeAsync: (id: string) => queue.removeAsync(id),
      retryJob: (id: string) => queue.retryJob(id),
      getChildrenValues: (id: string) => queue.getChildrenValues(id),
      updateJobData: (id: string, data: unknown) => queue.updateJobData(id, data),
      promoteJob: (id: string) => queue.promoteJob(id),
      changeJobDelay: (id: string, delay: number) => queue.changeJobDelay(id, delay),
      changeJobPriority: (id: string, options: ChangePriorityOpts) =>
        queue.changeJobPriority(id, options),
      extendJobLock: (id: string, token: string, duration: number) =>
        queue.extendJobLock(id, token, duration),
      clearJobLogs: (id: string, keep?: number) => queue.clearJobLogs(id, keep),
      getJobDependencies: (id: string, options?: GetDependenciesOpts) =>
        queue.getJobDependencies(id, options),
      getJobDependenciesCount: (id: string, options?: GetDependenciesOpts) =>
        queue.getJobDependenciesCount(id, options),
      getJobCountsAsync: () => queue.getJobCountsAsync(),
      getJobsAsync: (options: {
        state?: string | string[];
        start?: number;
        end?: number;
        asc?: boolean;
      }) => queue.getJobsAsync(options),
      moveJobToCompleted: (id: string, result: unknown, token?: string) =>
        queue.moveJobToCompleted(id, result, token),
      moveJobToFailed: (id: string, error: Error, token?: string) =>
        queue.moveJobToFailed(id, error, token),
      moveJobToWait: (id: string, token?: string) => queue.moveJobToWait(id, token),
      moveJobToDelayed: (id: string, timestamp: number, token?: string) =>
        queue.moveJobToDelayed(id, timestamp, token),
      moveJobToWaitingChildren: (
        id: string,
        token?: string,
        options?: { child?: { id: string; queue: string } }
      ) => queue.moveJobToWaitingChildren(id, token, options),
      waitJobUntilFinished: (id: string, queueEvents: unknown, ttl?: number) =>
        queue.waitJobUntilFinished(id, queueEvents, ttl),
    };
  }

  protected get queryCtx() {
    const queue = this as unknown as QueueRuntime<T>;
    return {
      ...this.ctx,
      getJobState: (id: string) => queue.getJobState(id),
      removeAsync: (id: string) => queue.removeAsync(id),
      retryJob: (id: string) => queue.retryJob(id),
      getChildrenValues: (id: string) => queue.getChildrenValues(id),
      updateJobData: (id: string, data: unknown) => queue.updateJobData(id, data),
      promoteJob: (id: string) => queue.promoteJob(id),
      changeJobDelay: (id: string, delay: number) => queue.changeJobDelay(id, delay),
      changeJobPriority: (id: string, options: ChangePriorityOpts) =>
        queue.changeJobPriority(id, options),
      extendJobLock: (id: string, token: string, duration: number) =>
        queue.extendJobLock(id, token, duration),
      clearJobLogs: (id: string, keep?: number) => queue.clearJobLogs(id, keep),
      getJobDependencies: (id: string, options?: GetDependenciesOpts) =>
        queue.getJobDependencies(id, options),
      getJobDependenciesCount: (id: string, options?: GetDependenciesOpts) =>
        queue.getJobDependenciesCount(id, options),
      moveJobToCompleted: (id: string, result: unknown, token?: string) =>
        queue.moveJobToCompleted(id, result, token),
      moveJobToFailed: (id: string, error: Error, token?: string) =>
        queue.moveJobToFailed(id, error, token),
      moveJobToWait: (id: string, token?: string) => queue.moveJobToWait(id, token),
      moveJobToDelayed: (id: string, timestamp: number, token?: string) =>
        queue.moveJobToDelayed(id, timestamp, token),
      moveJobToWaitingChildren: (
        id: string,
        token?: string,
        options?: { child?: { id: string; queue: string } }
      ) => queue.moveJobToWaitingChildren(id, token, options),
      waitJobUntilFinished: (id: string, queueEvents: unknown, ttl?: number) =>
        queue.waitJobUntilFinished(id, queueEvents, ttl),
    };
  }

  protected get moveCtx() {
    const queue = this as unknown as QueueRuntime<T>;
    return {
      ...this.ctx,
      getJobState: (id: string) => queue.getJobState(id),
      getJobDependencies: (id: string) => queue.getJobDependencies(id),
    };
  }

  protected releaseConnection(): void {
    if (this.connectionReleased) return;
    this.connectionReleased = true;
    if (!this.tcpPool) return;
    if (this.useSharedPool) releaseSharedPool(this.tcpPool);
    else this.tcpPool.close();
  }
}
