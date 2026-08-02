import type { JobNode } from '../../client/flowTypes';
import type { CronJob } from '../../domain/types/cron';
import type { Job } from '../../domain/types/job';
import type { FlowNodeResult, SerializedCron, SerializedJob } from '../types/adapter';

export function serializeMcpJob(job: Job): SerializedJob {
  return {
    id: String(job.id),
    queue: job.queue,
    data: job.data,
    priority: job.priority,
    progress: job.progress,
    attempts: job.attempts,
    maxAttempts: job.maxAttempts,
    createdAt: new Date(job.createdAt).toISOString(),
    startedAt: job.startedAt ? new Date(job.startedAt).toISOString() : undefined,
  };
}

export function serializeMcpCron(
  cron: Pick<CronJob, 'name' | 'queue' | 'schedule' | 'repeatEvery' | 'nextRun' | 'executions'>
): SerializedCron {
  return {
    name: cron.name,
    queue: cron.queue,
    schedule: cron.schedule ?? undefined,
    repeatEvery: cron.repeatEvery ?? undefined,
    nextRun: cron.nextRun ? new Date(cron.nextRun).toISOString() : null,
    executions: cron.executions,
  };
}

export function serializeFlowNode(node: JobNode): FlowNodeResult {
  return {
    jobId: node.job.id,
    name: node.job.name,
    queueName: node.job.queueName,
    children: node.children?.map(serializeFlowNode),
  };
}
