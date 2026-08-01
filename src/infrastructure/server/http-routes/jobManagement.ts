import { handleCommand } from '../handler';
import { jsonResponse, parseJsonBody } from '../httpEndpoints';
import type { HandlerContext } from '../types';

const RE_JOB_PROMOTE = /^\/jobs\/([^/]+)\/promote$/;
const RE_JOB_DATA = /^\/jobs\/([^/]+)\/data$/;
const RE_JOB_STATE = /^\/jobs\/([^/]+)\/state$/;
const RE_JOB_RESULT = /^\/jobs\/([^/]+)\/result$/;
const RE_JOB_PROGRESS = /^\/jobs\/([^/]+)\/progress$/;
const RE_JOB_PRIORITY = /^\/jobs\/([^/]+)\/priority$/;
const RE_JOB_DISCARD = /^\/jobs\/([^/]+)\/discard$/;

export async function routeJobManagement(
  request: Request,
  path: string,
  method: string,
  context: HandlerContext,
  cors: Set<string>
): Promise<Response | null> {
  const promoteMatch = path.match(RE_JOB_PROMOTE);
  if (promoteMatch && method === 'POST') {
    const result = await handleCommand({ cmd: 'Promote', id: promoteMatch[1] }, context);
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }

  const dataMatch = path.match(RE_JOB_DATA);
  if (dataMatch && method === 'PUT') {
    let body: Record<string, unknown>;
    try {
      body = (await request.json()) as Record<string, unknown>;
    } catch {
      return jsonResponse({ ok: false, error: 'Invalid JSON body' }, 400, cors);
    }
    const result = await handleCommand(
      { cmd: 'Update', id: dataMatch[1], data: body['data'] },
      context
    );
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }

  const stateMatch = path.match(RE_JOB_STATE);
  if (stateMatch && method === 'GET') {
    const result = await handleCommand({ cmd: 'GetState', id: stateMatch[1] }, context);
    return jsonResponse(result, result.ok ? 200 : 404, cors);
  }

  const resultMatch = path.match(RE_JOB_RESULT);
  if (resultMatch && method === 'GET') {
    const result = await handleCommand({ cmd: 'GetResult', id: resultMatch[1] }, context);
    return jsonResponse(result, result.ok ? 200 : 404, cors);
  }

  const progressMatch = path.match(RE_JOB_PROGRESS);
  if (progressMatch && method === 'GET') {
    const result = await handleCommand({ cmd: 'GetProgress', id: progressMatch[1] }, context);
    return jsonResponse(result, result.ok ? 200 : 404, cors);
  }
  if (progressMatch && method === 'POST') {
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      {
        cmd: 'Progress',
        id: progressMatch[1],
        progress: body['progress'] as number,
        message: body['message'] as string | undefined,
      },
      context
    );
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }

  const priorityMatch = path.match(RE_JOB_PRIORITY);
  if (priorityMatch && method === 'PUT') {
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      {
        cmd: 'ChangePriority',
        id: priorityMatch[1],
        priority: body['priority'] as number,
        lifo: body['lifo'] as boolean | undefined,
      },
      context
    );
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }

  const discardMatch = path.match(RE_JOB_DISCARD);
  if (discardMatch && method === 'POST') {
    const result = await handleCommand({ cmd: 'Discard', id: discardMatch[1] }, context);
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }
  return null;
}
