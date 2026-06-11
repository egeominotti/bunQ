/**
 * CLI Output Formatting
 * Formats command responses for terminal display
 */

/** ANSI color codes */
const colors = {
  reset: '\x1b[0m',
  bold: '\x1b[1m',
  dim: '\x1b[2m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  cyan: '\x1b[36m',
} as const;

/** Check if colors are supported */
const supportsColor = process.stdout.isTTY && Bun.env.NO_COLOR !== '1';

/** Apply color if supported */
function color(text: string, colorCode: string): string {
  return supportsColor ? `${colorCode}${text}${colors.reset}` : text;
}

/** Safely convert unknown value to string */
function str(value: unknown, fallback = ''): string {
  if (value === null || value === undefined) return fallback;
  if (typeof value === 'string') return value;
  if (typeof value === 'object') return JSON.stringify(value);
  // value is narrowed to number | boolean | bigint | symbol | function — all have a defined toString.
  return (value as { toString(): string }).toString();
}

/**
 * Derive a job's state from its fields when not explicitly provided.
 * The server-side Job domain object doesn't carry an explicit `state` field
 * (state is tracked separately in jobIndex), so responses like PULL omit it.
 *
 * Used only as a display fallback when the response includes a Job object
 * but no explicit state. Conservative: errs toward 'unknown' rather than
 * guessing 'active' for a job that may actually be in DLQ/failed/stalled.
 *   - explicit state field present → use it
 *   - completedAt → completed
 *   - attempts >= maxAttempts && no completedAt → likely failed / DLQ → unknown
 *   - startedAt && !completedAt → likely active, but not certain → unknown
 *     for the most common pull-response case the caller (PULL) knows it's
 *     active and will pass `state` explicitly in future iterations
 *   - runAt > now → delayed
 *   - else → waiting
 */
function deriveJobState(job: Record<string, unknown>): string {
  if (typeof job.state === 'string' && job.state.length > 0) return job.state;

  const completedAt = typeof job.completedAt === 'number' ? job.completedAt : 0;
  const startedAt = typeof job.startedAt === 'number' ? job.startedAt : 0;
  const attempts = typeof job.attempts === 'number' ? job.attempts : 0;
  const maxAttempts = typeof job.maxAttempts === 'number' ? job.maxAttempts : 0;
  const runAt = typeof job.runAt === 'number' ? job.runAt : 0;

  if (completedAt > 0) return 'completed';
  // A job whose retries are exhausted with no completion is in DLQ. We can't
  // tell from timestamps alone whether it's there or merely retrying, so
  // surface 'unknown' rather than confidently print 'active'.
  if (maxAttempts > 0 && attempts >= maxAttempts && startedAt > 0) return 'unknown';
  if (startedAt > 0) return 'active';
  if (runAt > Date.now()) return 'delayed';
  // If we have zero signal at all (no timestamps, no state), fall back to
  // 'unknown' rather than confidently print 'waiting'. Real responses from
  // the server include a `runAt` (set at push time), so 'waiting' inference
  // still triggers for real waiting jobs.
  if (runAt === 0 && startedAt === 0 && completedAt === 0) return 'unknown';
  return 'waiting';
}

/** Format a job object for display */
function formatJob(job: Record<string, unknown>): string {
  const lines = [
    `${color('Job:', colors.bold)} ${str(job.id)}`,
    `  Queue:      ${str(job.queue)}`,
    `  State:      ${deriveJobState(job)}`,
    `  Priority:   ${str(job.priority)}`,
    `  Attempts:   ${str(job.attempts)}/${str(job.maxAttempts)}`,
    `  Data:       ${JSON.stringify(job.data)}`,
  ];

  if (job.progress !== undefined && job.progress !== 0) {
    lines.push(`  Progress:   ${str(job.progress)}%`);
  }
  if (job.createdAt) {
    lines.push(`  Created:    ${new Date(job.createdAt as number).toISOString()}`);
  }
  if (job.startedAt) {
    lines.push(`  Started:    ${new Date(job.startedAt as number).toISOString()}`);
  }
  if (job.error) {
    lines.push(`  Error:      ${color(str(job.error), colors.red)}`);
  }

  return lines.join('\n');
}

/** Pad string to width accounting for ANSI codes */
function pad(text: string, width: number): string {
  const visualWidth = Bun.stringWidth(text);
  const padding = Math.max(0, width - visualWidth);
  return text + ' '.repeat(padding);
}

/** Format jobs as a table */
function formatJobsTable(jobs: Record<string, unknown>[]): string {
  if (jobs.length === 0) {
    return color('No jobs found', colors.yellow);
  }

  const header = [
    pad(color('ID', colors.bold), 20),
    pad(color('Queue', colors.bold), 15),
    pad(color('State', colors.bold), 12),
    pad(color('Priority', colors.bold), 10),
    color('Attempts', colors.bold),
  ].join(' ');

  const rows = jobs.map((job) =>
    [
      pad(str(job.id), 20),
      pad(str(job.queue), 15),
      pad(str(job.state, '-'), 12),
      pad(str(job.priority), 10),
      `${str(job.attempts)}/${str(job.maxAttempts)}`,
    ].join(' ')
  );

  return [header, '-'.repeat(75), ...rows].join('\n');
}

/** Format stats object */
function formatStats(stats: Record<string, unknown>): string {
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
    lines.push('', `  ${color('Uptime:', colors.cyan)}      ${str(stats.uptime)}s`);
  }
  if (stats.pushPerSec !== undefined) {
    lines.push(`  Push/sec:    ${str(stats.pushPerSec)}`);
  }
  if (stats.pullPerSec !== undefined) {
    lines.push(`  Pull/sec:    ${str(stats.pullPerSec)}`);
  }

  return lines.join('\n');
}

/** Format counts object */
function formatCounts(counts: Record<string, number>): string {
  return Object.entries(counts)
    .map(([key, value]) => `  ${key}: ${value}`)
    .join('\n');
}

/** Format queues list */
function formatQueues(queues: string[]): string {
  if (queues.length === 0) {
    return color('No queues found', colors.yellow);
  }
  return queues.map((q) => `  - ${q}`).join('\n');
}

/** Format cron jobs */
function formatCronJobs(jobs: Record<string, unknown>[]): string {
  if (jobs.length === 0) {
    return color('No cron jobs found', colors.yellow);
  }

  const lines = jobs.map((job) => {
    const schedule =
      job.schedule !== null && job.schedule !== undefined
        ? str(job.schedule)
        : `every ${str(job.repeatEvery)}ms`;
    let out = `  ${color(str(job.name), colors.bold)}\n    Queue: ${str(job.queue)}\n    Schedule: ${schedule}\n    Executions: ${str(job.executions)}`;
    if (typeof job.nextRun === 'number') {
      out += `\n    Next run: ${new Date(job.nextRun).toISOString()}`;
    }
    if (job.maxLimit !== null && job.maxLimit !== undefined) {
      out += `\n    Max: ${str(job.maxLimit)}`;
    }
    if (job.timezone !== null && job.timezone !== undefined) {
      out += `\n    Timezone: ${str(job.timezone)}`;
    }
    return out;
  });

  return lines.join('\n\n');
}

/** Format workers list */
function formatWorkers(workers: Record<string, unknown>[]): string {
  if (workers.length === 0) {
    return color('No workers registered', colors.yellow);
  }

  return workers
    .map((w) => {
      const queues = Array.isArray(w.queues) ? (w.queues as string[]).join(', ') : 'none';
      // status matters operationally: a stale worker must be visible
      const status =
        w.status === 'stale'
          ? color('[stale]', colors.red)
          : w.status !== undefined
            ? color(`[${str(w.status)}]`, colors.green)
            : '';
      const extra: string[] = [];
      if (w.concurrency !== undefined) extra.push(`concurrency=${str(w.concurrency)}`);
      if (w.activeJobs !== undefined) extra.push(`active=${str(w.activeJobs)}`);
      if (w.processedJobs !== undefined) {
        extra.push(`processed=${str(w.processedJobs)}/failed=${str(w.failedJobs, '0')}`);
      }
      const extraStr = extra.length > 0 ? `\n    ${extra.join(' ')}` : '';
      return `  ${color(str(w.id), colors.bold)}: ${str(w.name)} ${status} (${queues})${extraStr}`;
    })
    .join('\n');
}

/** Format webhooks list */
function formatWebhooks(webhooks: Record<string, unknown>[]): string {
  if (webhooks.length === 0) {
    return color('No webhooks registered', colors.yellow);
  }

  return webhooks
    .map((w) => {
      const events = Array.isArray(w.events) ? (w.events as string[]).join(', ') : 'none';
      const enabled = w.enabled === false ? ` ${color('[disabled]', colors.yellow)}` : '';
      const queue = w.queue !== null && w.queue !== undefined ? `\n    Queue: ${str(w.queue)}` : '';
      const counters =
        w.successCount !== undefined || w.failureCount !== undefined
          ? `\n    Delivered: ${str(w.successCount, '0')} ok / ${str(w.failureCount, '0')} failed`
          : '';
      return `  ${color(str(w.id), colors.bold)}: ${str(w.url)}${enabled}\n    Events: ${events}${queue}${counters}`;
    })
    .join('\n\n');
}

/** Format DLQ jobs */
function formatDlqJobs(jobs: Record<string, unknown>[]): string {
  if (jobs.length === 0) {
    return color('DLQ is empty', colors.green);
  }

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

/** Format logs */
function formatLogs(logs: Record<string, unknown>[]): string {
  if (logs.length === 0) {
    return color('No logs found', colors.yellow);
  }

  return logs
    .map((log) => {
      const levelColor =
        log.level === 'error' ? colors.red : log.level === 'warn' ? colors.yellow : colors.dim;
      return `  [${log.timestamp !== null && log.timestamp !== undefined ? new Date(log.timestamp as number).toISOString() : 'unknown'}] ${color(str(log.level).toUpperCase(), levelColor)}: ${str(log.message)}`;
    })
    .join('\n');
}

/** Unwrap response data - server may nest content in 'data' wrapper */
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

/** Try to format collection-type responses (jobs, workers, webhooks, crons, etc.) */
function formatCollection(r: Record<string, unknown>, command: string): string | null {
  if ('job' in r) {
    return r.job === null
      ? color('No job available', colors.yellow)
      : formatJob(r.job as Record<string, unknown>);
  }
  if ('jobs' in r && Array.isArray(r.jobs)) {
    return command === 'dlq'
      ? formatDlqJobs(r.jobs as Record<string, unknown>[])
      : formatJobsTable(r.jobs as Record<string, unknown>[]);
  }
  // Workers/webhooks before stats (their responses also contain stats)
  if ('workers' in r && Array.isArray(r.workers)) {
    return formatWorkers(r.workers as Record<string, unknown>[]);
  }
  if ('webhooks' in r && Array.isArray(r.webhooks)) {
    return formatWebhooks(r.webhooks as Record<string, unknown>[]);
  }
  if ('crons' in r && Array.isArray(r.crons)) {
    return formatCronJobs(r.crons as Record<string, unknown>[]);
  }
  if ('cronJobs' in r && Array.isArray(r.cronJobs)) {
    return formatCronJobs(r.cronJobs as Record<string, unknown>[]);
  }
  if ('dlqJobs' in r && Array.isArray(r.dlqJobs)) {
    return formatDlqJobs(r.dlqJobs as Record<string, unknown>[]);
  }
  if ('logs' in r && Array.isArray(r.logs)) {
    return formatLogs(r.logs as Record<string, unknown>[]);
  }
  if ('stats' in r && typeof r.stats === 'object' && !Array.isArray(r.stats)) {
    return formatStats(r.stats as Record<string, unknown>);
  }
  if ('counts' in r) {
    return formatCounts(r.counts as Record<string, number>);
  }
  if ('queues' in r && Array.isArray(r.queues) && !('workerId' in r)) {
    return formatQueues(r.queues as string[]);
  }
  return null;
}

/** Format a successful response based on its content */
function formatSuccess(
  response: Record<string, unknown>,
  command: string,
  subcommand?: string
): string {
  // Unwrap nested data wrapper (server wraps workers, webhooks, logs, metrics in {data: {...}})
  const r = unwrap(response);

  // Job created
  if ('id' in r && typeof r.id === 'string' && command === 'push') {
    return color(`Job created: ${r.id}`, colors.green);
  }
  // Batch jobs IDs returned. Wording depends on command + subcommand.
  if ('ids' in r && Array.isArray(r.ids)) {
    const n = r.ids.length;
    const idsStr = r.ids.join(', ');
    let verb = 'Affected';
    if (command === 'push') verb = 'Created';
    else if (command === 'queue') {
      verb = subcommand === 'drain' ? 'Drained' : 'Cleaned';
    } else if (command === 'dlq') {
      verb = subcommand === 'retry' ? 'Retried' : subcommand === 'purge' ? 'Purged' : 'Affected';
    }
    return color(`${verb} ${n} jobs${idsStr ? ': ' + idsStr : ''}`, colors.green);
  }

  // Try collection/list formats
  const collection = formatCollection(r, command);
  if (collection !== null) return collection;

  // Worker registered
  if ('workerId' in r) return color(`Worker registered: ${str(r.workerId)}`, colors.green);
  // Webhook added — show the id, it is needed for `webhook remove`
  if ('webhookId' in r) return color(`Webhook added: ${str(r.webhookId)}`, colors.green);
  // Cron scheduled — surface nextRun
  if ('cron' in r && r.cron !== null && typeof r.cron === 'object') {
    const c = r.cron as Record<string, unknown>;
    const next =
      typeof c.nextRun === 'number' ? ` (next run: ${new Date(c.nextRun).toISOString()})` : '';
    return color(`Cron scheduled: ${str(c.name)}${next}`, colors.green);
  }
  // State
  if ('state' in r) return `State: ${str(r.state)}`;
  // Result. undefined → no result stored yet (job not completed or removeOnComplete).
  if ('result' in r) {
    if (r.result === undefined || r.result === null) {
      return color('No result available (job not completed or result was removed)', colors.yellow);
    }
    return `Result: ${JSON.stringify(r.result, null, 2)}`;
  }
  // Progress
  if ('progress' in r) {
    const msg = r.message ? ` - ${str(r.message)}` : '';
    return `Progress: ${str(r.progress)}%${msg}`;
  }
  // Paused status
  if ('paused' in r) {
    return r.paused
      ? color('Queue is paused', colors.yellow)
      : color('Queue is active', colors.green);
  }
  // Count
  if ('count' in r) return `Count: ${str(r.count)}`;
  // Metrics (Prometheus format)
  if ('metrics' in r && typeof r.metrics === 'string') return r.metrics;

  return color('OK', colors.green);
}

/** Main output formatter */
export function formatOutput(
  response: Record<string, unknown>,
  command: string,
  asJson: boolean,
  subcommand?: string
): string {
  if (asJson) {
    return JSON.stringify(response, null, 2);
  }

  if (!response.ok) {
    return formatError(str(response.error, 'Unknown error'), false);
  }

  return formatSuccess(response, command, subcommand);
}

/** Format error message */
export function formatError(message: string, asJson: boolean): string {
  if (asJson) {
    return JSON.stringify({ ok: false, error: message });
  }
  return color(`Error: ${message}`, colors.red);
}
