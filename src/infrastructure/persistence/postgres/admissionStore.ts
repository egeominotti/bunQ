import { validateAtomicFlowBatch } from '../../../application/operations/flowValidation';
import type { AtomicFlowBatchInput } from '../../../domain/types/flow';
import { createJob, type Job, type JobId } from '../../../domain/types/job';
import { admitPostgresJob } from './admission';
import type { PostgresAdmissionResult } from './admissionResult';
import { recordPostgresJobEvents } from './batchEvents';
import {
  admitPostgresJobsBatch,
  canBatchAdmitPostgresJobs,
  lockPostgresAdmissionKeys,
  PostgresBatchAdmissionConflict,
} from './batchAdmission';
import { retirePostgresCompletionGenerations } from './completionLifecycle';
import { databaseNow } from './context';
import {
  assertPostgresDependenciesExist,
  lockPostgresDependencyCompletions,
} from './dependencyPromotion';
import { lockPostgresFlowParents } from './flowFailures';
import { PostgresQueueStoreRuntime } from './runtime';
import { reconcilePostgresSerialAdmissions } from './serialAdmission';
import { lockPostgresAdmissionQueues, registerPostgresAdmissionQueues } from './queueLifecycle';
import { runPostgresTransactionWithRetry } from './transactionRetry';

function admissionDependencyIds(jobs: readonly Job[]): JobId[] {
  return jobs.flatMap((job) => [job.id, ...job.dependsOn]);
}

function admissionParentIds(jobs: readonly Job[]): JobId[] {
  return jobs.flatMap((job) => (job.parentId ? [job.parentId] : []));
}

function replaySafeAdmissionJob(job: Job): Job {
  return { ...job, timeline: [...job.timeline] };
}

/** Atomic single, batch, and flow admission strategy shared by the store facade. */
export class PostgresAdmissionStore extends PostgresQueueStoreRuntime {
  async insert(job: Job): Promise<{ job: Job; inserted: boolean }> {
    await this.initialize();
    return await runPostgresTransactionWithRetry(this.context, (tx) =>
      admitPostgresJob(tx, this.context, replaySafeAdmissionJob(job))
    );
  }

  async insertMany(jobs: readonly Job[], rejectExisting = false): Promise<Job[]> {
    await this.initialize();
    const insertSerially = () =>
      runPostgresTransactionWithRetry(this.context, async (tx) => {
        await lockPostgresAdmissionQueues(
          tx,
          this.context,
          jobs.map((job) => job.queue)
        );
        await lockPostgresDependencyCompletions(tx, this.context, admissionDependencyIds(jobs));
        await lockPostgresFlowParents(tx, this.context, admissionParentIds(jobs));
        await lockPostgresAdmissionKeys(tx, this.context, jobs);
        const admissions: PostgresAdmissionResult[] = [];
        const insertedJobIds: JobId[] = [];
        const insertedDependencies: JobId[] = [];
        for (const source of jobs) {
          const job = replaySafeAdmissionJob(source);
          const admitted = await admitPostgresJob(tx, this.context, job, {
            rejectExisting,
            dependencyLocksHeld: true,
            queueLifecycleLocksHeld: true,
            dependencyExistenceCheckDeferred: true,
            completionConsumerExemptions: insertedJobIds,
          });
          admissions.push(admitted);
          if (admitted.inserted) {
            insertedJobIds.push(job.id);
            insertedDependencies.push(...job.dependsOn);
          }
        }
        await assertPostgresDependenciesExist(tx, this.context, insertedDependencies);
        return await reconcilePostgresSerialAdmissions(tx, this.context, admissions);
      });
    if (!canBatchAdmitPostgresJobs(jobs)) return await insertSerially();
    try {
      return await runPostgresTransactionWithRetry(this.context, (tx) =>
        admitPostgresJobsBatch(tx, this.context, jobs)
      );
    } catch (error) {
      if (!(error instanceof PostgresBatchAdmissionConflict)) throw error;
      return await insertSerially();
    }
  }

  async insertFlow(batch: AtomicFlowBatchInput): Promise<Job[]> {
    validateAtomicFlowBatch(batch);
    await this.initialize();
    return await runPostgresTransactionWithRetry(this.context, async (tx) => {
      const now = await databaseNow(tx);
      const created = batch.jobs.map((planned) =>
        createJob(planned.id, planned.queue, planned.input, now)
      );
      await lockPostgresAdmissionQueues(
        tx,
        this.context,
        created.map((job) => job.queue)
      );
      await lockPostgresDependencyCompletions(tx, this.context, admissionDependencyIds(created));
      await retirePostgresCompletionGenerations(
        tx,
        this.context,
        created.map((job) => job.id)
      );
      await lockPostgresFlowParents(tx, this.context, admissionParentIds(created));
      await lockPostgresAdmissionKeys(tx, this.context, created);
      const admissionNow = await databaseNow(tx);
      const jobs: Job[] = [];
      const admissions: PostgresAdmissionResult[] = [];
      for (const job of created) {
        const admitted = await admitPostgresJob(tx, this.context, job, {
          rejectExisting: true,
          linkParent: false,
          dependencyLocksHeld: true,
          queueLifecycleLocksHeld: true,
          dependencyExistenceCheckDeferred: true,
          preparedFlow: { now: admissionNow },
        });
        admissions.push(admitted);
        jobs.push(admitted.job);
      }
      await assertPostgresDependenciesExist(
        tx,
        this.context,
        created.flatMap((job) => job.dependsOn)
      );
      const inserted = admissions.filter((admission) => admission.inserted);
      await registerPostgresAdmissionQueues(
        tx,
        this.context,
        inserted.map(({ job }) => job.queue)
      );
      await recordPostgresJobEvents(
        tx,
        this.context,
        inserted.map(({ job, state }) => ({ job, state, type: 'pushed' })),
        admissionNow
      );
      return jobs;
    });
  }
}
