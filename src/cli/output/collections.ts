import { color, colors, stringifyValue as str } from './style';

export function formatStats(stats: Record<string, unknown>): string {
  const lines = [
    color('Server Statistics:', colors.bold),
    '',
    `  ${color('Waiting:', colors.cyan)}     ${str(stats.waiting, '0')}`,
    `  ${color('Active:', colors.green)}      ${str(stats.active, '0')}`,
    `  ${color('Delayed:', colors.yellow)}     ${str(stats.delayed, '0')}`,
    `  ${color('Completed:', colors.dim)}   ${str(stats.completed, '0')}`,
    `  ${color('Failed:', colors.red)}      ${str(stats.failed, '0')}`,
    `  ${color('DLQ:', colors.red)}         ${str(stats.dlq, '0')}`,
  ];
  if (stats.totalPushed !== undefined) {
    lines.push('', `  Total Pushed:    ${str(stats.totalPushed)}`);
    lines.push(`  Total Pulled:    ${str(stats.totalPulled)}`);
    lines.push(`  Total Completed: ${str(stats.totalCompleted)}`);
    lines.push(`  Total Failed:    ${str(stats.totalFailed)}`);
  }
  if (stats.uptime !== undefined) {
    const uptimeSeconds =
      typeof stats.uptime === 'number' ? Math.floor(stats.uptime / 1000) : stats.uptime;
    lines.push('', `  ${color('Uptime:', colors.cyan)}      ${str(uptimeSeconds)}s`);
  }
  if (stats.pushPerSec !== undefined) lines.push(`  Push/sec:    ${str(stats.pushPerSec)}`);
  if (stats.pullPerSec !== undefined) lines.push(`  Pull/sec:    ${str(stats.pullPerSec)}`);
  return lines.join('\n');
}

export function formatCounts(counts: Record<string, number>): string {
  return Object.entries(counts)
    .map(([key, value]) => `  ${key}: ${value}`)
    .join('\n');
}

export function formatQueues(queues: string[]): string {
  if (queues.length === 0) return color('No queues found', colors.yellow);
  return queues.map((queue) => `  - ${queue}`).join('\n');
}

export function formatCronJobs(jobs: Record<string, unknown>[]): string {
  if (jobs.length === 0) return color('No cron jobs found', colors.yellow);
  return jobs
    .map((job) => {
      const schedule =
        job.schedule !== null && job.schedule !== undefined
          ? str(job.schedule)
          : `every ${str(job.repeatEvery)}ms`;
      let output = `  ${color(str(job.name), colors.bold)}\n    Queue: ${str(job.queue)}\n    Schedule: ${schedule}\n    Executions: ${str(job.executions)}`;
      if (typeof job.nextRun === 'number') {
        output += `\n    Next run: ${new Date(job.nextRun).toISOString()}`;
      }
      if (job.maxLimit !== null && job.maxLimit !== undefined) {
        output += `\n    Max: ${str(job.maxLimit)}`;
      }
      if (job.timezone !== null && job.timezone !== undefined) {
        output += `\n    Timezone: ${str(job.timezone)}`;
      }
      return output;
    })
    .join('\n\n');
}

export function formatWorkers(workers: Record<string, unknown>[]): string {
  if (workers.length === 0) return color('No workers registered', colors.yellow);
  return workers
    .map((worker) => {
      const queues = Array.isArray(worker.queues) ? (worker.queues as string[]).join(', ') : 'none';
      const status =
        worker.status === 'stale'
          ? color('[stale]', colors.red)
          : worker.status !== undefined
            ? color(`[${str(worker.status)}]`, colors.green)
            : '';
      const extra: string[] = [];
      if (worker.concurrency !== undefined) extra.push(`concurrency=${str(worker.concurrency)}`);
      if (worker.activeJobs !== undefined) extra.push(`active=${str(worker.activeJobs)}`);
      if (worker.processedJobs !== undefined) {
        extra.push(`processed=${str(worker.processedJobs)}/failed=${str(worker.failedJobs, '0')}`);
      }
      const extraText = extra.length > 0 ? `\n    ${extra.join(' ')}` : '';
      return `  ${color(str(worker.id), colors.bold)}: ${str(worker.name)} ${status} (${queues})${extraText}`;
    })
    .join('\n');
}

export function formatWebhooks(webhooks: Record<string, unknown>[]): string {
  if (webhooks.length === 0) return color('No webhooks registered', colors.yellow);
  return webhooks
    .map((webhook) => {
      const events = Array.isArray(webhook.events)
        ? (webhook.events as string[]).join(', ')
        : 'none';
      const enabled = webhook.enabled === false ? ` ${color('[disabled]', colors.yellow)}` : '';
      const queue =
        webhook.queue !== null && webhook.queue !== undefined
          ? `\n    Queue: ${str(webhook.queue)}`
          : '';
      const counters =
        webhook.successCount !== undefined || webhook.failureCount !== undefined
          ? `\n    Delivered: ${str(webhook.successCount, '0')} ok / ${str(webhook.failureCount, '0')} failed`
          : '';
      return `  ${color(str(webhook.id), colors.bold)}: ${str(webhook.url)}${enabled}\n    Events: ${events}${queue}${counters}`;
    })
    .join('\n\n');
}

export function formatDlqJobs(jobs: Record<string, unknown>[]): string {
  if (jobs.length === 0) return color('DLQ is empty', colors.green);
  return jobs
    .map((job) => {
      const id = str(job.jobId ?? job.id);
      const failedAt =
        job.failedAt !== null && job.failedAt !== undefined
          ? new Date(job.failedAt as number).toISOString()
          : job.createdAt
            ? new Date(job.createdAt as number).toISOString()
            : 'unknown';
      return `  ${color(id, colors.bold)}\n    Queue: ${str(job.queue)}\n    Error: ${color(str(job.error, 'Unknown'), colors.red)}\n    Failed: ${failedAt}`;
    })
    .join('\n\n');
}

export function formatLogs(logs: Record<string, unknown>[]): string {
  if (logs.length === 0) return color('No logs found', colors.yellow);
  return logs
    .map((log) => {
      const levelColor =
        log.level === 'error' ? colors.red : log.level === 'warn' ? colors.yellow : colors.dim;
      const timestamp =
        log.timestamp !== null && log.timestamp !== undefined
          ? new Date(log.timestamp as number).toISOString()
          : 'unknown';
      return `  [${timestamp}] ${color(str(log.level).toUpperCase(), levelColor)}: ${str(log.message)}`;
    })
    .join('\n');
}
