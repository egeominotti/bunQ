import { FlowProducer } from '../../../client/flow';
import { TcpConnectionPool } from '../../../client/tcpPool';
import { normalizeLegacyJobPayload } from '../../../domain/types/job';
import type { SerializedJob } from '../../types/adapter';

export interface TcpBackendOptions {
  host?: string;
  port?: number;
  token?: string;
}

export class TcpBackendBase {
  protected readonly pool: TcpConnectionPool;
  private flowProducer: FlowProducer | null = null;
  private readonly connectionOptions: TcpBackendOptions;

  constructor(options: TcpBackendOptions) {
    this.connectionOptions = options;
    this.pool = new TcpConnectionPool({
      host: options.host ?? 'localhost',
      port: options.port ?? 6789,
      token: options.token,
      poolSize: Number(process.env.BUNQUEUE_POOL_SIZE) || 2,
    });
  }

  async connect() {
    await this.pool.connect();
  }

  protected getFlowProducer(): FlowProducer {
    this.flowProducer ??= new FlowProducer({
      connection: {
        host: this.connectionOptions.host,
        port: this.connectionOptions.port,
        token: this.connectionOptions.token,
      },
    });
    return this.flowProducer;
  }

  protected async send(command: Record<string, unknown>): Promise<Record<string, unknown>> {
    return this.pool.send(command);
  }

  protected parseJob(job: Record<string, unknown>): SerializedJob {
    const payload = normalizeLegacyJobPayload({ name: job.name, data: job.data });
    return {
      id: String(job.id),
      name: payload.name,
      queue: (job.queue as string) ?? '',
      data: payload.data,
      priority: (job.priority as number) ?? 0,
      progress: (job.progress as number) ?? 0,
      attempts: (job.attempts as number) ?? 0,
      maxAttempts: (job.maxAttempts as number) ?? 3,
      createdAt: job.createdAt
        ? new Date(job.createdAt as number).toISOString()
        : new Date().toISOString(),
      startedAt: job.startedAt ? new Date(job.startedAt as number).toISOString() : undefined,
    };
  }

  protected closeBackend(): void {
    this.flowProducer?.close();
    this.pool.close();
  }
}
