import type { Command } from '../../../domain/types/command';
import type { Response } from '../../../domain/types/response';
import * as resp from '../../../domain/types/response';
import { normalizeLegacyJobPayload } from '../../../domain/types/job';
import type { HandlerContext } from '../types';
import { sanitizeServerError } from '../errors';

/** Commit the whole graph in one broker operation; validation happens pre-mutation. */
export async function handlePushFlow(
  cmd: Extract<Command, { cmd: 'PUSHF' }>,
  ctx: HandlerContext,
  reqId?: string
): Promise<Response> {
  try {
    const jobs = cmd.jobs.map((job) => {
      const payload = normalizeLegacyJobPayload(job.input);
      return { ...job, input: { ...job.input, name: payload.name, data: payload.data } };
    });
    const result = await ctx.queueManager.pushFlow({ jobs });
    return resp.data(result, reqId);
  } catch (error) {
    return resp.error(sanitizeServerError(error), reqId);
  }
}
