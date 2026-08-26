/**
 * PUSHB Validation
 * Per-job validation for batch pushes with PUSH parity: the same
 * validateJobData/validateJobOptions bounds and the same dependsOn
 * existence gate handlePush enforces (core.ts).
 */

import {
  jobId,
  normalizeLegacyJobPayload,
  type JobId,
  type JobInput,
} from '../../../domain/types/job';
import type { HandlerContext } from '../types';
import { validateJobData, validateJobOptions } from '../protocol';

type DurableDependencyManager = HandlerContext['queueManager'] & {
  findMissingDependenciesDurable?: (ids: readonly JobId[]) => Promise<JobId[]>;
};

interface BatchDependencyState {
  readonly jobIndex: { has(id: JobId): boolean };
  readonly completedJobs: { has(id: JobId): boolean };
  readonly depCompletions: { has(id: JobId): boolean };
  readonly batchIds: ReadonlySet<string> | null;
  readonly missingDependencies?: ReadonlySet<JobId>;
}

function dependencyExistsLocally(id: JobId, ctx: HandlerContext): boolean {
  return (
    ctx.queueManager.getJobIndex().has(id) ||
    ctx.queueManager.getCompletedJobs().has(id) ||
    ctx.queueManager.getDepCompletions().has(id)
  );
}

/** Validate PUSH dependencies against the authoritative engine when it exposes one. */
export function validatePushDependencies(
  dependencies: readonly string[] | undefined,
  ctx: HandlerContext
): string | null | Promise<string | null> {
  if (!dependencies || dependencies.length === 0) return null;
  const manager = ctx.queueManager as DurableDependencyManager;
  if (!manager.findMissingDependenciesDurable) {
    for (const dependency of dependencies) {
      if (!dependencyExistsLocally(jobId(dependency), ctx)) {
        return `Dependency job not found: ${dependency}`;
      }
    }
    return null;
  }
  const ids = dependencies.map(jobId);
  return manager.findMissingDependenciesDurable(ids).then((missingIds) => {
    const missing = new Set(missingIds.map(String));
    const dependency = dependencies.find((id) => missing.has(String(id)));
    return dependency === undefined ? null : `Dependency job not found: ${dependency}`;
  });
}

/**
 * Validate every job of a PUSHB batch. Returns an error message naming the
 * offending index, or null when the whole batch is valid.
 *
 * The dependsOn gate is EXTENDED beyond the PUSH one: a dependency may also
 * reference ANY OTHER job of the same batch via its custom id (the only ids
 * a client can know before the batch is applied). Order within the batch is
 * deliberately irrelevant: the default-on auto-batcher groups concurrent
 * add() calls in arbitrary order, so a child can legitimately precede its
 * parent in the array, and the readiness layer (waitingDeps) resolves the
 * chain once the whole batch is applied. Self-references are rejected, they
 * can never resolve. jobIndex/completedJobs/depCompletions are hoisted out
 * of the loop because PUSHB is the bulk hot path.
 */
export function validatePushBatchJobs(
  jobs: JobInput[],
  ctx: HandlerContext
): string | null | Promise<string | null> {
  const jobIndex = ctx.queueManager.getJobIndex();
  const completedJobs = ctx.queueManager.getCompletedJobs();
  const depCompletions = ctx.queueManager.getDepCompletions();
  // Custom ids of ALL jobs in this batch: they become real job ids the
  // moment the batch is applied. Allocated lazily (most batches have neither
  // custom ids nor dependencies).
  let batchIds: Set<string> | null = null;
  for (const job of jobs) {
    if (job.customId) {
      batchIds ??= new Set();
      batchIds.add(job.customId);
    }
  }

  const manager = ctx.queueManager as DurableDependencyManager;
  if (manager.findMissingDependenciesDurable) {
    const dependencies = jobs.flatMap((job) =>
      (job.dependsOn ?? []).filter((id) => !(batchIds?.has(String(id)) ?? false))
    );
    if (dependencies.length === 0) {
      return validateJobs(jobs, { jobIndex, completedJobs, depCompletions, batchIds });
    }
    return manager.findMissingDependenciesDurable(dependencies).then((missingIds) =>
      validateJobs(jobs, {
        jobIndex,
        completedJobs,
        depCompletions,
        batchIds,
        missingDependencies: new Set(missingIds),
      })
    );
  }

  return validateJobs(jobs, { jobIndex, completedJobs, depCompletions, batchIds });
}

function validateJobs(jobs: JobInput[], state: BatchDependencyState): string | null {
  const { jobIndex, completedJobs, depCompletions, batchIds, missingDependencies } = state;
  for (let i = 0; i < jobs.length; i++) {
    const job = jobs[i];

    try {
      normalizeLegacyJobPayload(job);
    } catch (error) {
      return `jobs[${i}]: ${error instanceof Error ? error.message : String(error)}`;
    }

    const dataError = validateJobData(job.data);
    if (dataError) return `jobs[${i}]: ${dataError}`;

    const optionsError = validateJobOptions({
      priority: job.priority,
      delay: job.delay,
      timeout: job.timeout,
      maxAttempts: job.maxAttempts,
      backoff: job.backoff,
      ttl: job.ttl,
    });
    if (optionsError) return `jobs[${i}]: ${optionsError}`;

    if (job.dependsOn && job.dependsOn.length > 0) {
      for (const depId of job.dependsOn) {
        const key = String(depId);
        if (key === job.customId) {
          return `jobs[${i}]: Job cannot depend on itself: ${key}`;
        }
        const exists = missingDependencies
          ? !missingDependencies.has(depId)
          : jobIndex.has(depId) ||
            completedJobs.has(depId) ||
            // removeOnComplete parents leave only a bare completion id behind;
            // the gate must honor it exactly like PUSH does.
            depCompletions.has(depId) ||
            (batchIds?.has(key) ?? false);
        if (!exists) {
          return `jobs[${i}]: Dependency job not found: ${key}`;
        }
      }
    }
  }

  return null;
}
