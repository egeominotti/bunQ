/**
 * Cloud Command Handler
 * Processes commands received from the dashboard via WebSocket.
 * Only active when BUNQUEUE_CLOUD_REMOTE_COMMANDS=true.
 *
 * All commands are whitelisted — unknown actions are rejected.
 */

import type { QueueManager } from '../../application/queueManager';
import { cloudLog } from './logger';
import { COMMANDS } from './commands';
import type { CloudCommand, CloudCommandResult, CommandContext } from './types/command';

export type { CloudCommand, CloudCommandResult, CommandContext } from './types/command';

/**
 * Normalize object keys: PascalCase/UpperCase → camelCase.
 * Skips user data fields (data, result, backoffConfig, repeat) — those are passed as-is.
 * NOTE: All command handlers already return camelCase keys, so this is only needed
 * for nested objects from QueueManager that may use PascalCase (e.g. getDlqConfig).
 * We check first char to skip already-camelCase objects (fast path).
 */
const USER_DATA_KEYS = new Set(['data', 'result', 'backoffConfig', 'repeat']);

function camelKeys(obj: unknown, isUserData = false): unknown {
  if (Array.isArray(obj)) {
    return obj.map((item) => camelKeys(item, isUserData));
  }
  if (obj && typeof obj === 'object' && !(obj instanceof Date)) {
    const entries = Object.entries(obj as Record<string, unknown>);
    const mapped = entries.map(([k, v]) => {
      const c = k.charCodeAt(0);
      const newKey = !isUserData && c >= 65 && c <= 90 ? k[0].toLowerCase() + k.slice(1) : k;
      const childIsUserData = USER_DATA_KEYS.has(newKey);
      const newV = childIsUserData ? v : camelKeys(v, isUserData);
      return [newKey, newV] as [string, unknown];
    });
    return Object.fromEntries(mapped);
  }
  return obj;
}

/** Process a command and return the result */
export async function handleCommand(
  queueManager: QueueManager,
  cmd: CloudCommand,
  context?: CommandContext
): Promise<CloudCommandResult> {
  // Handle snapshot:get directly — needs CloudAgent context
  if (cmd.action === 'snapshot:get' && context?.getSnapshot) {
    try {
      const snapshot = await context.getSnapshot();
      cloudLog.info('Remote command executed', { action: cmd.action, id: cmd.id });
      return { type: 'command_result', id: cmd.id, success: true, data: snapshot };
    } catch (err) {
      cloudLog.error('Remote command failed', {
        action: cmd.action,
        id: cmd.id,
        error: String(err),
      });
      return {
        type: 'command_result',
        id: cmd.id,
        success: false,
        error: err instanceof Error ? err.message : String(err),
      };
    }
  }

  const handler = COMMANDS[cmd.action];

  if (!handler) {
    cloudLog.warn('Unknown remote command', { action: cmd.action, id: cmd.id });
    return {
      type: 'command_result',
      id: cmd.id,
      success: false,
      error: `Unknown command: ${cmd.action}`,
    };
  }

  try {
    const raw = await handler(queueManager, cmd, context);
    const data = camelKeys(raw);
    cloudLog.info('Remote command executed', { action: cmd.action, id: cmd.id });
    return { type: 'command_result', id: cmd.id, success: true, data };
  } catch (err) {
    cloudLog.error('Remote command failed', {
      action: cmd.action,
      id: cmd.id,
      error: String(err),
    });
    return {
      type: 'command_result',
      id: cmd.id,
      success: false,
      error: err instanceof Error ? err.message : String(err),
    };
  }
}
