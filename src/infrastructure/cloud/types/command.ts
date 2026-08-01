import type { QueueManager } from '../../../application/queueManager';
import type { CloudSnapshot } from './snapshot';

/** Incoming command from the dashboard. */
export interface CloudCommand {
  type: 'command';
  id: string;
  action: string;
  queue?: string;
  jobId?: string;
  name?: string;
  schedule?: string;
  data?: unknown;
  config?: Record<string, unknown>;
  graceMs?: number;
  state?: string;
  limit?: number;
  offset?: number;
  priority?: number;
  delay?: number;
  url?: string;
  events?: string[];
  secret?: string;
  webhookId?: string;
  enabled?: boolean;
  keepLogs?: number;
  max?: number;
  concurrency?: number;
  sort?: string;
  search?: string;
}

/** Result sent back to the dashboard. */
export interface CloudCommandResult {
  type: 'command_result';
  id: string;
  success: boolean;
  data?: unknown;
  error?: string;
}

/** Optional context for commands that need more than QueueManager. */
export interface CommandContext {
  getSnapshot?: () => Promise<CloudSnapshot>;
  triggerBackup?: () => Promise<unknown>;
}

export type CloudCommandHandler = (
  queueManager: QueueManager,
  command: CloudCommand,
  context?: CommandContext
) => unknown;
