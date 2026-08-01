import {
  formatCounts,
  formatCronJobs,
  formatDlqJobs,
  formatLogs,
  formatQueues,
  formatStats,
  formatWebhooks,
  formatWorkers,
} from './collections';
import { formatJob, formatJobsTable } from './jobs';
import { color, colors, stringifyValue as str } from './style';

function unwrap(response: Record<string, unknown>): Record<string, unknown> {
  if (
    'data' in response &&
    response.data &&
    typeof response.data === 'object' &&
    !Array.isArray(response.data)
  ) {
    return { ...response, ...(response.data as Record<string, unknown>) };
  }
  return response;
}

function formatCollection(response: Record<string, unknown>, command: string): string | null {
  if ('job' in response) {
    return response.job === null
      ? color('No job available', colors.yellow)
      : formatJob(response.job as Record<string, unknown>);
  }
  if ('jobs' in response && Array.isArray(response.jobs)) {
    return command === 'dlq'
      ? formatDlqJobs(response.jobs as Record<string, unknown>[])
      : formatJobsTable(response.jobs as Record<string, unknown>[]);
  }
  if ('workers' in response && Array.isArray(response.workers)) {
    return formatWorkers(response.workers as Record<string, unknown>[]);
  }
  if ('webhooks' in response && Array.isArray(response.webhooks)) {
    return formatWebhooks(response.webhooks as Record<string, unknown>[]);
  }
  if ('crons' in response && Array.isArray(response.crons)) {
    return formatCronJobs(response.crons as Record<string, unknown>[]);
  }
  if ('cronJobs' in response && Array.isArray(response.cronJobs)) {
    return formatCronJobs(response.cronJobs as Record<string, unknown>[]);
  }
  if ('dlqJobs' in response && Array.isArray(response.dlqJobs)) {
    return formatDlqJobs(response.dlqJobs as Record<string, unknown>[]);
  }
  if ('logs' in response && Array.isArray(response.logs)) {
    return formatLogs(response.logs as Record<string, unknown>[]);
  }
  if ('stats' in response && typeof response.stats === 'object' && !Array.isArray(response.stats)) {
    return formatStats(response.stats as Record<string, unknown>);
  }
  if ('counts' in response) return formatCounts(response.counts as Record<string, number>);
  if ('queues' in response && Array.isArray(response.queues) && !('workerId' in response)) {
    return formatQueues(response.queues as string[]);
  }
  return null;
}

export function formatSuccess(
  rawResponse: Record<string, unknown>,
  command: string,
  subcommand?: string
): string {
  const response = unwrap(rawResponse);
  if ('id' in response && typeof response.id === 'string' && command === 'push') {
    return color(`Job created: ${response.id}`, colors.green);
  }
  if ('ids' in response && Array.isArray(response.ids)) {
    const count = response.ids.length;
    const ids = response.ids.join(', ');
    let verb = 'Affected';
    if (command === 'push') verb = 'Created';
    else if (command === 'queue') verb = subcommand === 'drain' ? 'Drained' : 'Cleaned';
    else if (command === 'dlq') {
      verb = subcommand === 'retry' ? 'Retried' : subcommand === 'purge' ? 'Purged' : 'Affected';
    }
    return color(`${verb} ${count} jobs${ids ? ': ' + ids : ''}`, colors.green);
  }

  const collection = formatCollection(response, command);
  if (collection !== null) return collection;
  if ('workerId' in response) {
    return color(`Worker registered: ${str(response.workerId)}`, colors.green);
  }
  if ('webhookId' in response) {
    return color(`Webhook added: ${str(response.webhookId)}`, colors.green);
  }
  if ('cron' in response && response.cron !== null && typeof response.cron === 'object') {
    const cron = response.cron as Record<string, unknown>;
    const next =
      typeof cron.nextRun === 'number'
        ? ` (next run: ${new Date(cron.nextRun).toISOString()})`
        : '';
    return color(`Cron scheduled: ${str(cron.name)}${next}`, colors.green);
  }
  if ('state' in response) return `State: ${str(response.state)}`;
  if ('result' in response) {
    if (response.result === undefined || response.result === null) {
      return color('No result available (job not completed or result was removed)', colors.yellow);
    }
    return `Result: ${JSON.stringify(response.result, null, 2)}`;
  }
  if ('progress' in response) {
    const message = response.message ? ` - ${str(response.message)}` : '';
    return `Progress: ${str(response.progress)}%${message}`;
  }
  if ('paused' in response) {
    return response.paused
      ? color('Queue is paused', colors.yellow)
      : color('Queue is active', colors.green);
  }
  if ('count' in response) return `Count: ${str(response.count)}`;
  if ('metrics' in response && typeof response.metrics === 'string') return response.metrics;
  return color('OK', colors.green);
}
