/** Canonical CLI command surface used by routing, help conformance, and tests. */
export const CLI_COMMAND_SURFACE = {
  push: [],
  pull: [],
  ack: [],
  fail: [],
  job: [
    'get',
    'state',
    'result',
    'cancel',
    'progress',
    'update',
    'priority',
    'promote',
    'delay',
    'discard',
    'logs',
    'log',
    'wait',
  ],
  queue: ['list', 'pause', 'resume', 'drain', 'obliterate', 'clean', 'count', 'jobs', 'paused'],
  'rate-limit': ['set', 'clear'],
  concurrency: ['set', 'clear'],
  dlq: ['list', 'retry', 'purge'],
  cron: ['list', 'add', 'delete'],
  worker: ['list', 'register', 'unregister'],
  webhook: ['list', 'add', 'remove'],
  stats: [],
  metrics: [],
  health: [],
  ping: [],
} as const;

export type CliNetworkCommand = keyof typeof CLI_COMMAND_SURFACE;

/** Commands handled locally instead of being translated to a TCP command. */
export const CLI_LOCAL_COMMAND_SURFACE = {
  start: [],
  version: [],
  doctor: [],
  backup: ['now', 'create', 'list', 'restore', 'status'],
} as const;
