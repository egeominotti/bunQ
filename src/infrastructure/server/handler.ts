/**
 * Command Handler Router
 * Routes commands to appropriate handlers
 */

import type { Command } from '../../domain/types/command';
import type { Response } from '../../domain/types/response';
import * as resp from '../../domain/types/response';
import { constantTimeEqual } from '../../shared/hash';
import type { HandlerContext } from './types';
import { sanitizeServerError } from './errors';
import {
  routeCoreCommand,
  routeQueryCommand,
  routeManagementCommand,
  routeQueueControlCommand,
  routeDlqCommand,
  routeRateLimitCommand,
  routeConfigCommand,
  routeCronCommand,
  routeMonitoringCommand,
  routeDashboardCommand,
} from './handlerRoutes';

// Re-export types
export type { HandlerContext } from './types';

/**
 * Handle authentication command
 */
function handleAuth(
  cmd: Extract<Command, { cmd: 'Auth' }>,
  ctx: HandlerContext,
  reqId?: string
): Response {
  for (const token of ctx.authTokens) {
    if (constantTimeEqual(cmd.token, token)) {
      ctx.authenticated = true;
      return resp.ok(undefined, reqId);
    }
  }
  ctx.queueManager.emitDashboardEvent('auth:failed', { clientId: ctx.clientId });
  return resp.error('Invalid token', reqId);
}

/**
 * Main command handler - routes to specific handlers
 */
export async function handleCommand(cmd: Command, ctx: HandlerContext): Promise<Response> {
  const reqId = cmd.reqId;

  try {
    // Auth command is always allowed
    if (cmd.cmd === 'Auth') {
      return handleAuth(cmd, ctx, reqId);
    }

    // Check authentication if tokens are configured
    if (ctx.authTokens.size > 0 && !ctx.authenticated) {
      return resp.error('Not authenticated', reqId);
    }

    // Route through command groups
    let result: Response | null;

    result = await routeCoreCommand(cmd, ctx, reqId);
    if (result) return result;

    result = await routeQueryCommand(cmd, ctx, reqId);
    if (result) return result;

    result = await routeManagementCommand(cmd, ctx, reqId);
    if (result) return result;

    result = await routeQueueControlCommand(cmd, ctx, reqId);
    if (result) return result;

    result = await routeDlqCommand(cmd, ctx, reqId);
    if (result) return result;

    result = await routeRateLimitCommand(cmd, ctx, reqId);
    if (result) return result;

    result = await routeConfigCommand(cmd, ctx, reqId);
    if (result) return result;

    result = await routeCronCommand(cmd, ctx, reqId);
    if (result) return result;

    result = await routeMonitoringCommand(cmd, ctx, reqId);
    if (result) return result;

    result = await routeDashboardCommand(cmd, ctx, reqId);
    if (result) return result;

    return resp.error(`Unknown command: ${cmd.cmd}`, reqId);
  } catch (err) {
    return resp.error(sanitizeServerError(err), reqId);
  }
}
