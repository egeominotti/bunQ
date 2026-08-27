import { SQL } from 'bun';
import {
  FlowProducer,
  Queue,
  QueueEvents,
  Worker,
  type ConnectionOptions,
  type Processor,
  type WorkerOptions,
} from '../../src/client';
import {
  cleanupPostgresNamespace,
  crashPostgresProcessBroker,
  startPostgresProcessCluster,
  stopPostgresProcessCluster,
  type PostgresProcessBroker,
} from './postgres-process-cluster';

/** Four-process fixture for exercising public clients against one PostgreSQL namespace. */
export class PostgresPublicApiHarness {
  private readonly queues: Queue<unknown>[] = [];
  private readonly workers: Worker<unknown, unknown>[] = [];
  private readonly events: QueueEvents<unknown, unknown>[] = [];
  private readonly flows: FlowProducer[] = [];

  private constructor(
    private readonly url: string,
    readonly namespace: string,
    private readonly brokers: PostgresProcessBroker[]
  ) {}

  static async start(url: string, expectedVersion: string): Promise<PostgresPublicApiHarness> {
    const namespace = `test-public-api-${Date.now()}-${crypto.randomUUID()}`;
    const brokers = await startPostgresProcessCluster(url, namespace, 4, {
      pollIntervalMs: 25,
      poolSize: 8,
    });
    const sql = new SQL(url, { max: 1 });
    try {
      const [row] = await sql<{ server_version: string }[]>`SHOW server_version`;
      if (!row.server_version.startsWith(expectedVersion)) {
        throw new Error(`Expected PostgreSQL ${expectedVersion}, received ${row.server_version}`);
      }
      return new PostgresPublicApiHarness(url, namespace, brokers);
    } catch (error) {
      await stopPostgresProcessCluster(brokers);
      await cleanupPostgresNamespace(url, namespace);
      throw error;
    } finally {
      await sql.close({ timeout: 5 });
    }
  }

  unique(label: string): string {
    return `${label}-${crypto.randomUUID()}`;
  }

  connection(index: number): ConnectionOptions {
    const broker = this.brokers[index];
    if (!broker) throw new Error(`PostgreSQL public API broker ${index} is unavailable`);
    return {
      commandTimeout: 15_000,
      host: '127.0.0.1',
      pingInterval: 0,
      poolSize: 1,
      port: broker.port,
    };
  }

  queue<T>(name: string, broker: number): Queue<T> {
    const queue = new Queue<T>(name, {
      autoBatch: { enabled: false },
      connection: this.connection(broker),
      embedded: false,
    });
    this.queues.push(queue as Queue<unknown>);
    return queue;
  }

  worker<T, R>(
    name: string,
    broker: number,
    processor: Processor<T, R>,
    options: Omit<WorkerOptions, 'connection' | 'dataPath' | 'embedded'> = {}
  ): Worker<T, R> {
    const worker = new Worker<T, R>(name, processor, {
      ...options,
      concurrency: options.concurrency ?? 4,
      connection: this.connection(broker),
      embedded: false,
    });
    this.workers.push(worker as Worker<unknown, unknown>);
    return worker;
  }

  queueEvents<R, P>(name: string, broker: number): QueueEvents<R, P> {
    const events = new QueueEvents<R, P>(name, {
      connection: this.connection(broker),
      embedded: false,
    });
    this.events.push(events as QueueEvents<unknown, unknown>);
    return events;
  }

  flow(broker: number): FlowProducer {
    const flow = new FlowProducer({ connection: this.connection(broker), embedded: false });
    this.flows.push(flow);
    return flow;
  }

  async crashBroker(index: number): Promise<void> {
    const broker = this.brokers[index];
    if (!broker) throw new Error(`PostgreSQL public API broker ${index} is unavailable`);
    await crashPostgresProcessBroker(broker);
  }

  async closeClients(): Promise<void> {
    const resources = [
      ...this.workers
        .splice(0)
        .reverse()
        .map((worker) => () => worker.close(true)),
      ...this.events
        .splice(0)
        .reverse()
        .map((events) => async () => events.close()),
      ...this.flows
        .splice(0)
        .reverse()
        .map((flow) => () => flow.close()),
      ...this.queues
        .splice(0)
        .reverse()
        .map((queue) => async () => queue.close()),
    ];
    const results = await Promise.allSettled(resources.map((close) => close()));
    const errors = results.flatMap((result) =>
      result.status === 'rejected' ? [result.reason] : []
    );
    if (errors.length > 0) throw new AggregateError(errors, 'Public API client cleanup failed');
  }

  async close(): Promise<void> {
    let clientError: unknown;
    try {
      await this.closeClients();
    } catch (error) {
      clientError = error;
    }
    await stopPostgresProcessCluster(this.brokers);
    await cleanupPostgresNamespace(this.url, this.namespace);
    if (clientError) throw clientError;
  }
}

export async function waitFor(
  description: string,
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 20_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await Bun.sleep(25);
  }
  throw new Error(`Timed out waiting for ${description}`);
}
