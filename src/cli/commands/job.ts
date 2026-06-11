/**
 * Job Command Builders
 * Job management subcommands
 */

import { parseArgs } from 'node:util';
import { CommandError, requireArg, parseJsonArg, parseNumberArg, parseBigIntArg } from './types';

/** Build a job subcommand */
export function buildJobCommand(args: string[]): Record<string, unknown> {
  const subcommand = args[0];
  if (!subcommand) {
    throw new CommandError(
      'Missing subcommand. Use: get, state, result, cancel, progress, update, priority, promote, delay, discard, logs, log, wait'
    );
  }
  const subArgs = args.slice(1);

  switch (subcommand) {
    case 'get':
      return buildGetJob(subArgs);
    case 'state':
      return buildGetState(subArgs);
    case 'result':
      return buildGetResult(subArgs);
    case 'cancel':
      return buildCancel(subArgs);
    case 'progress':
      return buildProgress(subArgs);
    case 'update':
      return buildUpdate(subArgs);
    case 'priority':
      return buildChangePriority(subArgs);
    case 'promote':
      return buildPromote(subArgs);
    case 'delay':
      return buildMoveToDelayed(subArgs);
    case 'discard':
      return buildDiscard(subArgs);
    case 'logs':
      return buildGetLogs(subArgs);
    case 'log':
      return buildAddLog(subArgs);
    case 'wait':
      return buildWaitJob(subArgs);
    default:
      throw new CommandError(
        `Unknown job subcommand: ${subcommand}. Use: get, state, result, cancel, progress, update, priority, promote, delay, discard, logs, log, wait`
      );
  }
}

function buildGetJob(args: string[]): Record<string, unknown> {
  const id = parseBigIntArg(requireArg(args, 0, 'id'), 'id');
  return { cmd: 'GetJob', id };
}

function buildGetState(args: string[]): Record<string, unknown> {
  const id = parseBigIntArg(requireArg(args, 0, 'id'), 'id');
  return { cmd: 'GetState', id };
}

function buildGetResult(args: string[]): Record<string, unknown> {
  const id = parseBigIntArg(requireArg(args, 0, 'id'), 'id');
  return { cmd: 'GetResult', id };
}

function buildCancel(args: string[]): Record<string, unknown> {
  const id = parseBigIntArg(requireArg(args, 0, 'id'), 'id');
  return { cmd: 'Cancel', id };
}

function buildProgress(args: string[]): Record<string, unknown> {
  const { values, positionals } = parseArgs({
    args,
    options: {
      message: { type: 'string', short: 'm' },
    },
    allowPositionals: true,
    strict: false,
  });

  const id = parseBigIntArg(requireArg(positionals, 0, 'id'), 'id');
  const progress = parseNumberArg(requireArg(positionals, 1, 'progress'), 'progress');

  if (progress === undefined || progress < 0 || progress > 100) {
    throw new CommandError('Progress must be a number between 0 and 100');
  }

  const cmd: Record<string, unknown> = {
    cmd: 'Progress',
    id,
    progress,
  };

  if (values.message) cmd.message = values.message;

  return cmd;
}

function buildUpdate(args: string[]): Record<string, unknown> {
  const id = parseBigIntArg(requireArg(args, 0, 'id'), 'id');
  const data = parseJsonArg(requireArg(args, 1, 'data'), 'data');
  return { cmd: 'Update', id, data };
}

function buildChangePriority(args: string[]): Record<string, unknown> {
  const id = parseBigIntArg(requireArg(args, 0, 'id'), 'id');
  const priority = parseNumberArg(requireArg(args, 1, 'priority'), 'priority');

  if (priority === undefined) {
    throw new CommandError('Priority must be a number');
  }

  return { cmd: 'ChangePriority', id, priority };
}

function buildPromote(args: string[]): Record<string, unknown> {
  const id = parseBigIntArg(requireArg(args, 0, 'id'), 'id');
  return { cmd: 'Promote', id };
}

function buildMoveToDelayed(args: string[]): Record<string, unknown> {
  const id = parseBigIntArg(requireArg(args, 0, 'id'), 'id');
  const delay = parseNumberArg(requireArg(args, 1, 'delay'), 'delay');

  if (delay === undefined || delay < 0) {
    throw new CommandError('Delay must be a positive number in milliseconds');
  }

  return { cmd: 'MoveToDelayed', id, delay };
}

function buildDiscard(args: string[]): Record<string, unknown> {
  const id = parseBigIntArg(requireArg(args, 0, 'id'), 'id');
  return { cmd: 'Discard', id };
}

function buildGetLogs(args: string[]): Record<string, unknown> {
  const id = parseBigIntArg(requireArg(args, 0, 'id'), 'id');
  return { cmd: 'GetLogs', id };
}

function buildAddLog(args: string[]): Record<string, unknown> {
  const { values, positionals } = parseArgs({
    args,
    options: {
      level: { type: 'string', short: 'l' },
    },
    allowPositionals: true,
    strict: false,
  });

  const id = parseBigIntArg(requireArg(positionals, 0, 'id'), 'id');
  const message = requireArg(positionals, 1, 'message');
  const level = (values.level as string) ?? 'info';

  if (!['info', 'warn', 'error'].includes(level)) {
    throw new CommandError(`Invalid log level: ${level}. Must be info, warn, or error`);
  }

  return {
    cmd: 'AddLog',
    id,
    message,
    level,
  };
}

function buildWaitJob(args: string[]): Record<string, unknown> {
  const { values, positionals } = parseArgs({
    args,
    options: {
      timeout: { type: 'string', short: 't' },
    },
    allowPositionals: true,
    strict: false,
  });

  const id = parseBigIntArg(requireArg(positionals, 0, 'id'), 'id');

  const cmd: Record<string, unknown> = {
    cmd: 'WaitJob',
    id,
  };

  const timeout = parseNumberArg(values.timeout as string | undefined, 'timeout');
  if (timeout !== undefined) {
    if (timeout < 0) {
      throw new CommandError(`Invalid timeout: ${timeout}. Must be >= 0 ms`);
    }
    cmd.timeout = timeout;
  }

  return cmd;
}
