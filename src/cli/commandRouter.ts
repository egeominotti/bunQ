import { CLI_COMMAND_SURFACE, type CliNetworkCommand } from './commandRegistry';

export const CLI_NETWORK_COMMANDS = Object.freeze(
  Object.keys(CLI_COMMAND_SURFACE) as CliNetworkCommand[]
);

/** Build the protocol command for a routed network CLI command. */
export async function buildCommand(
  command: string,
  args: string[]
): Promise<Record<string, unknown> | null> {
  if (!CLI_NETWORK_COMMANDS.includes(command as CliNetworkCommand)) return null;

  const { buildCoreCommand } = await import('./commands/core');
  const { buildJobCommand } = await import('./commands/job');
  const { buildQueueCommand } = await import('./commands/queue');
  const { buildDlqCommand } = await import('./commands/dlq');
  const { buildCronCommand } = await import('./commands/cron');
  const { buildWorkerCommand } = await import('./commands/worker');
  const { buildWebhookCommand } = await import('./commands/webhook');
  const { buildRateLimitCommand } = await import('./commands/rateLimit');
  const { buildMonitorCommand } = await import('./commands/monitor');

  switch (command) {
    case 'push':
    case 'pull':
    case 'ack':
    case 'fail':
      return buildCoreCommand(command, args);
    case 'job':
      return buildJobCommand(args);
    case 'queue':
      return buildQueueCommand(args);
    case 'dlq':
      return buildDlqCommand(args);
    case 'cron':
      return buildCronCommand(args);
    case 'worker':
      return buildWorkerCommand(args);
    case 'webhook':
      return buildWebhookCommand(args);
    case 'rate-limit':
    case 'concurrency':
      return buildRateLimitCommand(command, args);
    case 'stats':
    case 'metrics':
    case 'health':
    case 'ping':
      return buildMonitorCommand(command);
  }

  return null;
}
