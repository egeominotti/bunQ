import type { Command } from '../../../domain/types/command';
import type { Response } from '../../../domain/types/response';
import * as resp from '../../../domain/types/response';
import type { HandlerContext } from '../types';

/** Commit the whole graph in one broker operation; validation happens pre-mutation. */
export async function handlePushFlow(
  cmd: Extract<Command, { cmd: 'PUSHF' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  try {
    const result = await ctx.queueManager.pushFlow({ jobs: cmd.jobs });
    return resp.data(result, reqId);
  } catch (error) {
    return resp.error(error instanceof Error ? error.message : String(error), reqId);
  }
}
