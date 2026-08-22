import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  Queue,
  Worker,
  FlowProducer,
  closeAllSharedPools,
  shutdownManager,
} from '../../../src/client';
import type { Job, QueueOptions, WorkerOptions } from '../../../src/client';
import { QueueManager } from '../../../src/application/queueManager';
import { getSharedManager } from '../../../src/client/manager';
import { createTcpServer, type TcpServer } from '../../../src/infrastructure/server/tcp';
import { Engine } from '../../../src/client/workflow';

export type CoreE2eMode = 'embedded' | 'tcp';
type Cleanup = () => void | Promise<void>;

export class CoreE2eHarness {
  readonly dataDir: string;
  readonly dataPath: string;
  readonly mode: CoreE2eMode;
  private readonly cleanups: Cleanup[] = [];
  // oxlint-disable-next-line typescript/prefer-readonly -- initialized by the async start factory
  private manager: QueueManager | null = null;
  // oxlint-disable-next-line typescript/prefer-readonly -- initialized by the async start factory
  private server: TcpServer | null = null;
  // oxlint-disable-next-line typescript/prefer-readonly -- initialized by the async start factory
  private tcpPort: number | null = null;
  private sequence = 0;

  private constructor(mode: CoreE2eMode, prefix: string) {
    this.mode = mode;
    this.dataDir = mkdtempSync(join(tmpdir(), `bunqueue-core-e2e-${prefix}-${mode}-`));
    this.dataPath = join(this.dataDir, 'queue.db');
  }

  static async start(mode: CoreE2eMode, prefix: string): Promise<CoreE2eHarness> {
    shutdownManager();
    closeAllSharedPools();
    const harness = new CoreE2eHarness(mode, prefix);
    if (mode === 'tcp') {
      harness.manager = new QueueManager({ dataPath: harness.dataPath });
      harness.server = createTcpServer(harness.manager, { hostname: '127.0.0.1', port: 0 });
      harness.tcpPort = harness.server.server.port;
    }
    return harness;
  }

  unique(label: string): string {
    this.sequence++;
    return `core-e2e-${label}-${this.mode}-${this.sequence}-${crypto.randomUUID()}`;
  }

  queueOptions(extra: QueueOptions = {}): QueueOptions {
    if (this.mode === 'embedded') {
      return { ...extra, embedded: true, dataPath: this.dataPath };
    }
    return {
      ...extra,
      embedded: false,
      connection: {
        host: '127.0.0.1',
        port: this.tcpPort ?? 0,
        poolSize: 1,
        ...extra.connection,
      },
      autoBatch: extra.autoBatch ?? { enabled: false },
    };
  }

  workerOptions(extra: WorkerOptions = {}): WorkerOptions {
    if (this.mode === 'embedded') {
      return { ...extra, embedded: true, dataPath: this.dataPath };
    }
    return {
      ...extra,
      embedded: false,
      connection: {
        host: '127.0.0.1',
        port: this.tcpPort ?? 0,
        poolSize: 1,
        ...extra.connection,
      },
    };
  }

  queue<T = unknown>(label: string, options: QueueOptions = {}): Queue<T> {
    const queue = new Queue<T>(this.unique(label), this.queueOptions(options));
    this.cleanups.push(() => queue.close());
    return queue;
  }

  worker<T = unknown, R = unknown>(
    queueName: string,
    processor: (job: Job<T>) => R | Promise<R>,
    options: WorkerOptions = {}
  ): Worker<T, R> {
    const worker = new Worker<T, R>(queueName, processor, this.workerOptions(options));
    this.cleanups.push(() => worker.close(true));
    return worker;
  }

  flow(): FlowProducer {
    const flow = new FlowProducer(
      this.mode === 'embedded'
        ? { embedded: true }
        : {
            embedded: false,
            connection: { host: '127.0.0.1', port: this.tcpPort ?? 0, poolSize: 1 },
          }
    );
    this.cleanups.push(() => flow.close());
    return flow;
  }

  engine(queueLabel: string): Engine {
    const engine = new Engine({
      queueName: this.unique(queueLabel),
      embedded: this.mode === 'embedded',
      dataPath: join(this.dataDir, `${queueLabel}.db`),
      connection:
        this.mode === 'tcp'
          ? { host: '127.0.0.1', port: this.tcpPort ?? 0, poolSize: 1 }
          : undefined,
      concurrency: 2,
    });
    this.cleanups.push(() => engine.close(true));
    return engine;
  }

  connection(): { host: string; port: number; poolSize: number } {
    if (this.mode !== 'tcp' || this.tcpPort === null) {
      throw new Error('TCP connection requested from an embedded harness');
    }
    return { host: '127.0.0.1', port: this.tcpPort, poolSize: 1 };
  }

  brokerManager(): QueueManager {
    return this.manager ?? getSharedManager(this.dataPath);
  }

  /** Restart the broker over the same SQLite file and TCP endpoint. */
  async restartBroker(): Promise<void> {
    const port = this.tcpPort;
    this.server?.stop();
    this.server = null;
    this.manager?.shutdown();
    this.manager = null;
    closeAllSharedPools();
    shutdownManager();

    if (this.mode === 'tcp') {
      await Bun.sleep(20);
      this.manager = new QueueManager({ dataPath: this.dataPath });
      this.server = createTcpServer(this.manager, {
        hostname: '127.0.0.1',
        port: port ?? 0,
      });
      this.tcpPort = this.server.server.port;
    }
  }

  addCleanup(cleanup: Cleanup): void {
    this.cleanups.push(cleanup);
  }

  async close(): Promise<void> {
    const errors: unknown[] = [];
    for (const cleanup of this.cleanups.reverse()) {
      try {
        await cleanup();
      } catch (error) {
        errors.push(error);
      }
    }
    this.server?.stop();
    this.manager?.shutdown();
    closeAllSharedPools();
    shutdownManager();
    rmSync(this.dataDir, { recursive: true, force: true });
    if (errors.length > 0) throw new AggregateError(errors, 'core E2E cleanup failed');
  }
}
