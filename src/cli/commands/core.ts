/**
 * Core Command Builders
 * Push, Pull, Ack, Fail commands
 */

import { parseArgs } from 'node:util';
import { CommandError, requireArg, parseJsonArg, parseNumberArg, parseBigIntArg } from './types';

const PUSH_VALUE_FLAGS = new Set([
  '--priority',
  '-P',
  '--delay',
  '-d',
  '--max-attempts',
  '--backoff',
  '--ttl',
  '--timeout',
  '--unique-key',
  '-u',
  '--job-id',
  '--depends-on',
  '--tags',
  '--group-id',
  '-g',
]);

function protectNegativeJsonData(args: string[]): {
  args: string[];
  restore: Map<string, string>;
} {
  const protectedArgs = [...args];
  const restore = new Map<string, string>();
  let positionals = 0;

  for (let index = 0; index < args.length; index++) {
    const arg = args[index];
    if (arg === '--') break;
    if (PUSH_VALUE_FLAGS.has(arg)) {
      index++;
      continue;
    }
    if (arg.startsWith('-')) {
      if (positionals !== 1) continue;
      try {
        if (typeof JSON.parse(arg) !== 'number') continue;
      } catch {
        continue;
      }
      const sentinel = `bunqueue-negative-json-${index}`;
      protectedArgs[index] = sentinel;
      restore.set(sentinel, arg);
      positionals++;
      continue;
    }
    if (!arg.startsWith('--') && !/^-[a-zA-Z]/.test(arg)) positionals++;
  }

  return { args: protectedArgs, restore };
}

/** Build a core command from CLI args */
export function buildCoreCommand(command: string, args: string[]): Record<string, unknown> {
  switch (command) {
    case 'push':
      return buildPush(args);
    case 'pull':
      return buildPull(args);
    case 'ack':
      return buildAck(args);
    case 'fail':
      return buildFail(args);
    default:
      throw new CommandError(`Unknown core command: ${command}`);
  }
}

/** Build PUSH command */
function buildPush(args: string[]): Record<string, unknown> {
  const protectedInput = protectNegativeJsonData(args);
  const { values, positionals } = parseArgs({
    args: protectedInput.args,
    options: {
      priority: { type: 'string', short: 'P' },
      delay: { type: 'string', short: 'd' },
      'max-attempts': { type: 'string' },
      backoff: { type: 'string' },
      ttl: { type: 'string' },
      timeout: { type: 'string' },
      'unique-key': { type: 'string', short: 'u' },
      'job-id': { type: 'string' },
      'depends-on': { type: 'string' },
      tags: { type: 'string' },
      'group-id': { type: 'string', short: 'g' },
      lifo: { type: 'boolean' },
      'remove-on-complete': { type: 'boolean' },
      'remove-on-fail': { type: 'boolean' },
    },
    allowPositionals: true,
    strict: false,
  });

  const queue = requireArg(positionals, 0, 'queue');
  const protectedData = requireArg(positionals, 1, 'data');
  const data = parseJsonArg(protectedInput.restore.get(protectedData) ?? protectedData, 'data');

  const cmd: Record<string, unknown> = {
    cmd: 'PUSH',
    queue,
    data,
  };

  // Add optional fields only if provided
  const priority = parseNumberArg(values.priority as string | undefined, 'priority');
  if (priority !== undefined) cmd.priority = priority;

  const delay = parseNumberArg(values.delay as string | undefined, 'delay');
  if (delay !== undefined) {
    if (delay < 0) throw new CommandError('delay must be >= 0');
    cmd.delay = delay;
  }

  const maxAttempts = parseNumberArg(values['max-attempts'] as string | undefined, 'max-attempts');
  if (maxAttempts !== undefined) {
    if (maxAttempts < 1) throw new CommandError('max-attempts must be >= 1');
    cmd.maxAttempts = maxAttempts;
  }

  const backoff = parseNumberArg(values.backoff as string | undefined, 'backoff');
  if (backoff !== undefined) cmd.backoff = backoff;

  const ttl = parseNumberArg(values.ttl as string | undefined, 'ttl');
  if (ttl !== undefined) cmd.ttl = ttl;

  const timeout = parseNumberArg(values.timeout as string | undefined, 'timeout');
  if (timeout !== undefined) {
    if (timeout <= 0) throw new CommandError('timeout must be > 0');
    cmd.timeout = timeout;
  }

  if (values['unique-key']) cmd.uniqueKey = values['unique-key'];
  if (values['job-id']) cmd.jobId = values['job-id'];

  if (values['depends-on']) {
    cmd.dependsOn = (values['depends-on'] as string).split(',').filter(Boolean);
  }

  if (values.tags) {
    cmd.tags = (values.tags as string).split(',').filter(Boolean);
  }

  if (values['group-id']) cmd.groupId = values['group-id'];
  if (values.lifo) cmd.lifo = true;
  if (values['remove-on-complete']) cmd.removeOnComplete = true;
  if (values['remove-on-fail']) cmd.removeOnFail = true;

  return cmd;
}

/** Build PULL command */
function buildPull(args: string[]): Record<string, unknown> {
  const { values, positionals } = parseArgs({
    args,
    options: {
      timeout: { type: 'string', short: 't' },
    },
    allowPositionals: true,
    strict: false,
  });

  const queue = requireArg(positionals, 0, 'queue');

  const cmd: Record<string, unknown> = {
    cmd: 'PULL',
    queue,
    detach: true, // CLI: don't auto-release on disconnect so ack/fail work from separate connection
  };

  const timeout = parseNumberArg(values.timeout as string | undefined, 'timeout');
  if (timeout !== undefined) cmd.timeout = timeout;

  return cmd;
}

/** Build ACK command */
function buildAck(args: string[]): Record<string, unknown> {
  const { values, positionals } = parseArgs({
    args,
    options: {
      result: { type: 'string', short: 'r' },
    },
    allowPositionals: true,
    strict: false,
  });

  const id = parseBigIntArg(requireArg(positionals, 0, 'id'), 'id');

  const cmd: Record<string, unknown> = {
    cmd: 'ACK',
    id,
  };

  if (values.result) {
    cmd.result = parseJsonArg(values.result as string, 'result');
  }

  return cmd;
}

/** Build FAIL command */
function buildFail(args: string[]): Record<string, unknown> {
  const { values, positionals } = parseArgs({
    args,
    options: {
      error: { type: 'string', short: 'e' },
    },
    allowPositionals: true,
    strict: false,
  });

  const id = parseBigIntArg(requireArg(positionals, 0, 'id'), 'id');

  const cmd: Record<string, unknown> = {
    cmd: 'FAIL',
    id,
  };

  if (values.error) cmd.error = values.error;

  return cmd;
}
