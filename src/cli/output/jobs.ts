import { color, colors, pad, stringifyValue as str } from './style';

/** Infer a display state when the response does not include one. */
function deriveJobState(job: Record<string, unknown>): string {
  if (typeof job.state === 'string' && job.state.length > 0) return job.state;

  const completedAt = typeof job.completedAt === 'number' ? job.completedAt : 0;
  const startedAt = typeof job.startedAt === 'number' ? job.startedAt : 0;
  const attempts = typeof job.attempts === 'number' ? job.attempts : 0;
  const maxAttempts = typeof job.maxAttempts === 'number' ? job.maxAttempts : 0;
  const runAt = typeof job.runAt === 'number' ? job.runAt : 0;

  if (completedAt > 0) return 'completed';
  if (maxAttempts > 0 && attempts >= maxAttempts && startedAt > 0) return 'unknown';
  if (startedAt > 0) return 'active';
  if (runAt > Date.now()) return 'delayed';
  if (runAt === 0 && startedAt === 0 && completedAt === 0) return 'unknown';
  return 'waiting';
}

export function formatJob(job: Record<string, unknown>): string {
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
  if (job.createdAt) lines.push(`  Created:    ${new Date(job.createdAt as number).toISOString()}`);
  if (job.startedAt) lines.push(`  Started:    ${new Date(job.startedAt as number).toISOString()}`);
  if (job.error) lines.push(`  Error:      ${color(str(job.error), colors.red)}`);

  return lines.join('\n');
}

export function formatJobsTable(jobs: Record<string, unknown>[]): string {
  if (jobs.length === 0) return color('No jobs found', colors.yellow);

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
