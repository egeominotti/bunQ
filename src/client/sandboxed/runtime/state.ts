import { EventEmitter } from 'events';
import type { Job } from '../../types';
import { getSharedManager } from '../../manager';
import { getSharedPool, type TcpConnectionPool } from '../../tcpPool';
import { type QueueOps, createEmbeddedOps, createTcpOps } from '../queueOps';
import type {
  RequiredSandboxedWorkerOptions,
  SandboxedWorkerOptions,
  WorkerProcess,
} from '../types';

export abstract class SandboxedState<T = unknown> extends EventEmitter {
  on(event: 'ready' | 'closed', listener: () => void): this;
  on(event: 'active', listener: (job: Job<T>) => void): this;
  on(event: 'completed', listener: (job: Job<T>, result: unknown) => void): this;
  on(event: 'failed', listener: (job: Job<T>, error: Error) => void): this;
  on(event: 'progress', listener: (job: Job<T>, progress: number) => void): this;
  on(event: 'log', listener: (job: Job<T>, message: string) => void): this;
  on(event: 'error', listener: (error: Error) => void): this;
  // oxlint-disable-next-line typescript/no-explicit-any -- EventEmitter's fallback listener is intentionally untyped
  on(event: string, listener: (...args: any[]) => void): this {
    return super.on(event, listener);
  }

  once(event: 'ready' | 'closed', listener: () => void): this;
  once(event: 'active', listener: (job: Job<T>) => void): this;
  once(event: 'completed', listener: (job: Job<T>, result: unknown) => void): this;
  once(event: 'failed', listener: (job: Job<T>, error: Error) => void): this;
  once(event: 'progress', listener: (job: Job<T>, progress: number) => void): this;
  once(event: 'log', listener: (job: Job<T>, message: string) => void): this;
  once(event: 'error', listener: (error: Error) => void): this;
  // oxlint-disable-next-line typescript/no-explicit-any -- EventEmitter's fallback listener is intentionally untyped
  once(event: string, listener: (...args: any[]) => void): this {
    return super.once(event, listener);
  }

  protected readonly queueName: string;
  protected readonly options: RequiredSandboxedWorkerOptions;
  protected readonly workers: WorkerProcess[] = [];
  protected running = false;
  protected pullPromise: Promise<void> | null = null;
  protected wrapperPath: string | null = null;
  protected readonly ops: QueueOps;
  protected readonly tcp: TcpConnectionPool | null;
  protected readonly workerId: string;
  protected heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  protected readonly heartbeatInterval: number;
  protected readonly idleTimeout: number;
  protected readonly idleRecycleMs: number;
  protected readonly autoStart: boolean;
  protected readonly autoStartPollMs: number;
  protected lastActivityTime = 0;
  protected autoStartTimer: ReturnType<typeof setInterval> | null = null;

  constructor(queueName: string, options: SandboxedWorkerOptions) {
    super();
    this.queueName = queueName;
    this.workerId = `sandboxed-worker-${queueName}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;

    if (options.connection) {
      this.tcp = getSharedPool(options.connection);
      this.ops = createTcpOps(this.tcp);
      this.heartbeatInterval = options.heartbeatInterval ?? 10000;
    } else {
      this.tcp = null;
      this.ops = createEmbeddedOps(options.manager ?? getSharedManager());
      this.heartbeatInterval = options.heartbeatInterval ?? 5000;
    }

    this.idleTimeout = options.idleTimeout ?? 0;
    this.idleRecycleMs = options.idleRecycleMs ?? 30000;
    this.autoStart = options.autoStart ?? false;
    this.autoStartPollMs = options.autoStartPollMs ?? 5000;
    this.options = {
      processor: options.processor,
      concurrency: options.concurrency ?? 1,
      maxMemory: options.maxMemory ?? 256,
      timeout: options.timeout ?? 30000,
      autoRestart: options.autoRestart ?? true,
      maxRestarts: options.maxRestarts ?? 10,
      pollInterval: options.pollInterval ?? 10,
    };
  }
}
