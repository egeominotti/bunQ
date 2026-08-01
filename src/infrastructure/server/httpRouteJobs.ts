import { handleCommand } from './handler';
import { routeJobAdvanced } from './http-routes/jobAdvanced';
import { routeJobManagement } from './http-routes/jobManagement';
import { jsonResponse, parseJsonBody } from './httpEndpoints';
import type { HandlerContext } from './types';

const RE_JOB_CUSTOM_ID = /^\/jobs\/custom\/([^/]+)$/;
const RE_JOB_BY_ID = /^\/jobs\/([^/]+)$/;
const RE_JOB_ACK = /^\/jobs\/([^/]+)\/ack$/;
const RE_JOB_FAIL = /^\/jobs\/([^/]+)\/fail$/;

export async function routeJobRoutes(
  request: Request,
  path: string,
  method: string,
  context: HandlerContext,
  cors: Set<string>
): Promise<Response | null> {
  if (path === '/jobs/ack-batch' && method === 'POST') {
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      {
        cmd: 'ACKB',
        ids: body['ids'] as string[],
        results: body['results'] as unknown[] | undefined,
        tokens: body['tokens'] as string[] | undefined,
      },
      context
    );
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }
  if (path === '/jobs/extend-locks' && method === 'POST') {
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      {
        cmd: 'ExtendLocks',
        ids: body['ids'] as string[],
        tokens: body['tokens'] as string[],
        durations: body['durations'] as number[],
      },
      context
    );
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }
  if (path === '/jobs/heartbeat-batch' && method === 'POST') {
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      {
        cmd: 'JobHeartbeatB',
        ids: body['ids'] as string[],
        tokens: body['tokens'] as string[] | undefined,
      },
      context
    );
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }

  const customIdMatch = path.match(RE_JOB_CUSTOM_ID);
  if (customIdMatch && method === 'GET') {
    const customId = decodeURIComponent(customIdMatch[1]);
    const result = await handleCommand({ cmd: 'GetJobByCustomId', customId }, context);
    return jsonResponse(result, result.ok ? 200 : 404, cors);
  }

  const jobMatch = path.match(RE_JOB_BY_ID);
  if (jobMatch) {
    const id = jobMatch[1];
    if (method === 'GET') {
      const result = await handleCommand({ cmd: 'GetJob', id }, context);
      return jsonResponse(result, result.ok ? 200 : 404, cors);
    }
    if (method === 'DELETE') {
      const result = await handleCommand({ cmd: 'Cancel', id }, context);
      return jsonResponse(result, 200, cors);
    }
  }

  const ackMatch = path.match(RE_JOB_ACK);
  if (ackMatch && method === 'POST') {
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      {
        cmd: 'ACK',
        id: ackMatch[1],
        result: body['result'],
        token: body['token'] as string | undefined,
      },
      context
    );
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }

  const failMatch = path.match(RE_JOB_FAIL);
  if (failMatch && method === 'POST') {
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      {
        cmd: 'FAIL',
        id: failMatch[1],
        error: body['error'] as string | undefined,
        token: body['token'] as string | undefined,
        unrecoverable: body['unrecoverable'] as boolean | undefined,
        stack: body['stack'] as string[] | undefined,
      },
      context
    );
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }

  const management = await routeJobManagement(request, path, method, context, cors);
  if (management) return management;
  return routeJobAdvanced(request, path, method, context, cors);
}
