import { FailureReason } from '../../domain/types/dlq';
import type { JobId } from '../../domain/types/job';
import type { AckBatchOutcome, AckOutcome, CompletionOptions } from '../types/ack';
import { applyPostgresBatchCompletion } from './batchSnapshot';
import { PostgresQueueManagerQueries } from './queries';

/** Terminal delivery transitions drained before the PostgreSQL pool closes. */
export class PostgresQueueManagerTerminalDelivery extends PostgresQueueManagerQueries {
  override async ack(
    id: JobId,
    result?: unknown,
    token?: string,
    options: CompletionOptions = {}
  ): Promise<AckOutcome> {
    return await this.runPostgresOperation(() => this.ackPostgres(id, result, token, options));
  }

  override async ackBatch(ids: JobId[], tokens?: string[]): Promise<AckBatchOutcome> {
    return await this.runPostgresOperation(() =>
      this.completeBatch(ids.map((id, index) => ({ id, token: tokens?.[index] })))
    );
  }

  override async ackBatchWithResults(
    items: Array<{ id: JobId; result: unknown; token?: string } & CompletionOptions>
  ): Promise<AckBatchOutcome> {
    return await this.runPostgresOperation(() => this.completeBatch(items));
  }

  // oxlint-disable-next-line max-params -- public compatibility signature carries failure policy
  override async fail(
    id: JobId,
    error?: string,
    token?: string,
    unrecoverable = false,
    stack?: string[],
    removeOnFail?: boolean
  ): Promise<AckOutcome> {
    return await this.runPostgresOperation(() =>
      this.failPostgres(id, error, token, { unrecoverable, stack, removeOnFail })
    );
  }

  override async failWithReason(
    id: JobId,
    error: string | undefined,
    reason: FailureReason
  ): Promise<AckOutcome> {
    return await this.runPostgresOperation(() =>
      this.failPostgres(id, error, undefined, {
        unrecoverable: reason === FailureReason.Timeout,
        failureReason: reason,
      })
    );
  }

  private requireToken(id: JobId, supplied?: string): string {
    const token = this.tokenFor(id, supplied);
    if (!token) throw new Error(`Lock token required for job ${String(id)}`);
    return token;
  }

  private async ackPostgres(
    id: JobId,
    result: unknown,
    token: string | undefined,
    options: CompletionOptions
  ): Promise<AckOutcome> {
    await this.postgresReady;
    const transition = await this.postgresStore.complete(
      id,
      this.requireToken(id, token),
      result,
      options.removeOnComplete
    );
    this.forgetToken(id);
    await this.refreshJob(id);
    if (transition.applied) return;
    if (transition.alreadyFinalized) return { applied: false, reason: 'already-finalized' };
    throw new Error(`Invalid or expired lock token for job ${String(id)}`);
  }

  private async completeBatch(
    items: Array<{ id: JobId; result?: unknown; token?: string } & CompletionOptions>
  ): Promise<AckBatchOutcome> {
    if (items.length === 0) return;
    if (new Set(items.map((item) => String(item.id))).size === items.length) {
      await this.postgresReady;
      const transitions = await this.postgresStore.completeMany(
        items.map((item) => ({
          id: item.id,
          token: this.requireToken(item.id, item.token),
          result: item.result,
          removeOnComplete: item.removeOnComplete,
        }))
      );
      const repeatQueues = new Set<string>();
      const ignoredIds: JobId[] = [];
      const ignoredIndices: number[] = [];
      for (let index = 0; index < transitions.length; index++) {
        const transition = transitions[index];
        this.forgetToken(items[index].id);
        const repeatQueue = applyPostgresBatchCompletion(
          this.postgresSnapshot,
          items[index],
          transition
        );
        if (repeatQueue) repeatQueues.add(repeatQueue);
        if (!transition.applied) {
          ignoredIndices.push(index);
          ignoredIds.push(items[index].id);
        }
      }
      await Promise.all([
        ...ignoredIds.map((id) => this.refreshJob(id)),
        ...[...repeatQueues].map((queue) => this.refreshQueue(queue)),
      ]);
      return ignoredIndices.length === 0
        ? undefined
        : { ignoredIndices, ignoredIds: ignoredIndices.map((index) => items[index].id) };
    }

    const ignoredIndices: number[] = [];
    for (let index = 0; index < items.length; index++) {
      const outcome = await this.ackPostgres(
        items[index].id,
        items[index].result,
        items[index].token,
        items[index]
      );
      if (outcome) ignoredIndices.push(index);
    }
    return ignoredIndices.length === 0
      ? undefined
      : { ignoredIndices, ignoredIds: ignoredIndices.map((index) => items[index].id) };
  }

  private async failPostgres(
    id: JobId,
    error: string | undefined,
    token: string | undefined,
    options: {
      unrecoverable?: boolean;
      stack?: string[];
      removeOnFail?: boolean;
      failureReason?: FailureReason;
    }
  ): Promise<AckOutcome> {
    await this.postgresReady;
    const transition = await this.postgresStore.fail({
      id,
      token: this.requireToken(id, token),
      error,
      ...options,
    });
    this.forgetToken(id);
    await this.refreshJob(id);
    if (transition.applied) return;
    if (transition.alreadyFinalized) return { applied: false, reason: 'already-finalized' };
    throw new Error(`Invalid or expired lock token for job ${String(id)}`);
  }
}
