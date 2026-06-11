/**
 * Webhook Command Builders
 * Webhook management operations
 */

import { parseArgs } from 'node:util';
import { CommandError, requireArg } from './types';
import { WEBHOOK_EVENTS } from '../../domain/types/webhook';

/** Valid webhook events — canonical list shared with server/MCP validation */
const VALID_EVENTS: readonly string[] = WEBHOOK_EVENTS;

/** Build a webhook subcommand */
export function buildWebhookCommand(args: string[]): Record<string, unknown> {
  const subcommand = args[0];
  if (!subcommand) {
    throw new CommandError('Missing subcommand. Use: list, add, remove');
  }
  const subArgs = args.slice(1);

  switch (subcommand) {
    case 'list':
      return { cmd: 'ListWebhooks' };
    case 'add':
      return buildAddWebhook(subArgs);
    case 'remove':
      return buildRemoveWebhook(subArgs);
    default:
      throw new CommandError(`Unknown webhook subcommand: ${subcommand}. Use: list, add, remove`);
  }
}

function buildAddWebhook(args: string[]): Record<string, unknown> {
  const { values, positionals } = parseArgs({
    args,
    options: {
      events: { type: 'string', short: 'e' },
      queue: { type: 'string', short: 'q' },
      secret: { type: 'string', short: 's' },
    },
    allowPositionals: true,
    strict: false,
  });

  const url = requireArg(positionals, 0, 'url');

  if (!values.events) {
    throw new CommandError(
      `--events (-e) is required (comma-separated). Valid events: ${VALID_EVENTS.join(', ')}`
    );
  }

  const events = (values.events as string).split(',').filter(Boolean);
  if (events.length === 0) {
    throw new CommandError('At least one event must be specified');
  }

  // Validate events
  for (const event of events) {
    if (!VALID_EVENTS.includes(event)) {
      throw new CommandError(`Invalid event: ${event}. Valid events: ${VALID_EVENTS.join(', ')}`);
    }
  }

  // Validate URL
  try {
    const parsed = new URL(url);
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      throw new CommandError(`Webhook URL must use http or https protocol: ${url}`);
    }
  } catch (err) {
    if (err instanceof CommandError) throw err;
    throw new CommandError(`Invalid URL: ${url}`);
  }

  const cmd: Record<string, unknown> = {
    cmd: 'AddWebhook',
    url,
    events,
  };

  if (values.queue) cmd.queue = values.queue;
  if (values.secret) cmd.secret = values.secret;

  return cmd;
}

function buildRemoveWebhook(args: string[]): Record<string, unknown> {
  const webhookId = requireArg(args, 0, 'webhookId');
  return { cmd: 'RemoveWebhook', webhookId };
}
