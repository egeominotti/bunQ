import type { JobId } from '../../domain/types/job';
import type { PostgresJobProjection } from '../../infrastructure/persistence/postgres/readModels';
import type { PostgresQueueSnapshot } from './snapshot';

export type { PostgresJobProjection } from '../../infrastructure/persistence/postgres/readModels';

export function applyPostgresJobProjection(
  snapshot: PostgresQueueSnapshot,
  activeTokens: Map<JobId, string>,
  id: JobId,
  projection: PostgresJobProjection
): void {
  snapshot.reconcile(id, projection.row, projection.completion);
  const token = activeTokens.get(id);
  if (token && (projection.row?.state !== 'active' || projection.row.token !== token)) {
    activeTokens.delete(id);
  }
}

interface PendingProjection {
  readonly queue: string;
  readonly generation: symbol;
}

export interface PostgresDirectProjectionTicket {
  readonly id: JobId;
  readonly generation: symbol;
}

type ProjectionLoader = (
  requests: readonly { id: JobId; queue: string }[]
) => Promise<ReadonlyMap<JobId, PostgresJobProjection>>;
type ProjectionApplier = (id: JobId, projection: PostgresJobProjection) => void;
type ProjectionReporter = (queue: string, id: JobId, error: unknown) => void;

/** Coalesced, generation-fenced repair of the local PostgreSQL read model. */
export class PostgresProjectionRefreshes {
  private static readonly MAX_ACTIVE_BATCHES = 2;
  private static readonly MAX_REQUESTS_PER_BATCH = 1_000;
  private readonly generations = new Map<JobId, symbol>();
  private readonly generationQueues = new Map<JobId, string>();
  private readonly pending = new Map<JobId, PendingProjection>();
  private readonly active = new Set<Promise<boolean>>();
  private timer: ReturnType<typeof setTimeout> | null = null;
  private accepting = true;
  private started = false;

  constructor(
    private readonly load: ProjectionLoader,
    private readonly apply: ProjectionApplier,
    private readonly report: ProjectionReporter,
    private readonly retryDelayMs: number
  ) {}

  request(id: JobId, queue: string): void {
    if (!this.accepting) return;
    const generation = this.beginGeneration(id, queue);
    this.pending.set(id, { queue, generation });
    this.schedule(0);
  }

  beginDirect(id: JobId, queue: string): PostgresDirectProjectionTicket {
    const generation = this.beginGeneration(id, queue);
    this.pending.delete(id);
    return { id, generation };
  }

  consumeDirect(ticket: PostgresDirectProjectionTicket): boolean {
    if (this.generations.get(ticket.id) !== ticket.generation) return false;
    this.endGeneration(ticket.id, ticket.generation);
    return true;
  }

  cancelDirect(ticket: PostgresDirectProjectionTicket): void {
    this.endGeneration(ticket.id, ticket.generation);
  }

  supersede(id: JobId): void {
    this.pending.delete(id);
    this.generations.delete(id);
    this.generationQueues.delete(id);
  }

  supersedeQueue(queue: string): void {
    for (const [id, generationQueue] of this.generationQueues) {
      if (generationQueue === queue) this.supersede(id);
    }
  }

  async refreshNow(id: JobId, queue: string): Promise<PostgresJobProjection> {
    const projections = await this.refreshManyNow([{ id, queue }]);
    return projections.get(id) ?? { row: null, completion: null };
  }

  async refreshManyNow(
    requests: readonly { id: JobId; queue: string }[]
  ): Promise<ReadonlyMap<JobId, PostgresJobProjection>> {
    let remaining = [...requests];
    const loaded = new Map<JobId, PostgresJobProjection>();
    for (let attempt = 0; attempt < 2 && remaining.length > 0; attempt++) {
      const generations = new Map<JobId, symbol>();
      for (const { id, queue } of remaining) {
        const generation = this.beginGeneration(id, queue);
        this.pending.delete(id);
        generations.set(id, generation);
      }
      try {
        const projections = await this.load(remaining);
        const invalidated: typeof remaining = [];
        for (const request of remaining) {
          const projection = projections.get(request.id) ?? { row: null, completion: null };
          loaded.set(request.id, projection);
          if (this.generations.get(request.id) !== generations.get(request.id)) {
            invalidated.push(request);
            continue;
          }
          this.apply(request.id, projection);
          this.report(request.queue, request.id, null);
          this.endGeneration(request.id, generations.get(request.id));
        }
        remaining = invalidated;
      } catch (error) {
        for (const { id, queue } of remaining) {
          const generation = generations.get(id);
          if (this.generations.get(id) === generation) {
            this.report(queue, id, error);
            this.endGeneration(id, generation);
          }
        }
        throw error;
      }
    }
    for (const { id, queue } of remaining) this.request(id, queue);
    return loaded;
  }

  start(): void {
    if (!this.accepting || this.started) return;
    this.started = true;
    this.schedule(0);
  }

  close(): void {
    this.accepting = false;
    this.pending.clear();
    this.generations.clear();
    this.generationQueues.clear();
    if (this.timer) clearTimeout(this.timer);
    this.timer = null;
  }

  async drain(): Promise<void> {
    await Promise.all([...this.active]);
  }

  private schedule(delayMs: number): void {
    if (
      !this.accepting ||
      !this.started ||
      this.timer ||
      this.active.size >= PostgresProjectionRefreshes.MAX_ACTIVE_BATCHES ||
      this.pending.size === 0
    ) {
      return;
    }
    this.timer = setTimeout(() => {
      this.timer = null;
      const execution = this.runBatch();
      this.active.add(execution);
      void execution.then((retry) => {
        this.active.delete(execution);
        this.schedule(retry ? this.retryDelayMs : 0);
      });
    }, delayMs);
  }

  private async runBatch(): Promise<boolean> {
    const batch = [...this.pending.entries()].slice(
      0,
      PostgresProjectionRefreshes.MAX_REQUESTS_PER_BATCH
    );
    for (const [id] of batch) this.pending.delete(id);
    if (batch.length === 0) return false;
    try {
      const projections = await this.load(batch.map(([id, { queue }]) => ({ id, queue })));
      for (const [id, request] of batch) {
        if (this.generations.get(id) !== request.generation) continue;
        this.apply(id, projections.get(id) ?? { row: null, completion: null });
        this.report(request.queue, id, null);
        this.endGeneration(id, request.generation);
      }
      return false;
    } catch (error) {
      for (const [id, request] of batch) {
        if (this.generations.get(id) === request.generation && this.accepting) {
          this.pending.set(id, request);
          this.report(request.queue, id, error);
        } else {
          this.endGeneration(id, request.generation);
        }
      }
      return true;
    }
  }

  private beginGeneration(id: JobId, queue: string): symbol {
    const generation = Symbol();
    this.generations.set(id, generation);
    this.generationQueues.set(id, queue);
    return generation;
  }

  private endGeneration(id: JobId, generation: symbol | undefined): void {
    if (generation && this.generations.get(id) === generation && !this.pending.has(id)) {
      this.generations.delete(id);
      this.generationQueues.delete(id);
    }
  }
}
