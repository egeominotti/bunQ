import { handleCommand } from '../handler';
import { jsonResponse, parseJsonBody } from '../httpEndpoints';
import type { HandlerContext } from '../types';

const RE_JOB_MOVE_DELAYED = /^\/jobs\/([^/]+)\/move-to-delayed$/;
const RE_JOB_DELAY = /^\/jobs\/([^/]+)\/delay$/;
const RE_JOB_CHILDREN = /^\/jobs\/([^/]+)\/children$/;
const RE_JOB_LOGS = /^\/jobs\/([^/]+)\/logs$/;
const RE_JOB_HEARTBEAT = /^\/jobs\/([^/]+)\/heartbeat$/;
const RE_JOB_WAIT = /^\/jobs\/([^/]+)\/wait$/;
const RE_JOB_EXTEND_LOCK = /^\/jobs\/([^/]+)\/extend-lock$/;
const RE_JOB_MOVE_TO_WAIT = /^\/jobs\/([^/]+)\/move-to-wait$/;

export async function routeJobAdvanced(
  request: Request,
  path: string,
  method: string,
  context: HandlerContext,
  cors: Set<string>
): Promise<Response | null> {
  const moveDelayedMatch = path.match(RE_JOB_MOVE_DELAYED);
  if (moveDelayedMatch && method === 'POST') {
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      {
        cmd: 'MoveToDelayed',
        id: moveDelayedMatch[1],
        delay: body['delay'] as number,
        token: body['token'] as string | undefined,
      },
      context
    );
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }

  const changeDelayMatch = path.match(RE_JOB_DELAY);
  if (changeDelayMatch && method === 'PUT') {
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      { cmd: 'ChangeDelay', id: changeDelayMatch[1], delay: body['delay'] as number },
      context
    );
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }

  const childrenMatch = path.match(RE_JOB_CHILDREN);
  if (childrenMatch && method === 'GET') {
    const result = await handleCommand({ cmd: 'GetChildrenValues', id: childrenMatch[1] }, context);
    return jsonResponse(result, result.ok ? 200 : 404, cors);
  }

  const logsMatch = path.match(RE_JOB_LOGS);
  if (logsMatch && method === 'GET') {
    const result = await handleCommand({ cmd: 'GetLogs', id: logsMatch[1] }, context);
    return jsonResponse(result, 200, cors);
  }
  if (logsMatch && method === 'POST') {
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      {
        cmd: 'AddLog',
        id: logsMatch[1],
        message: body['message'] as string,
        level: body['level'] as 'info' | 'warn' | 'error' | undefined,
      },
      context
    );
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }
  if (logsMatch && method === 'DELETE') {
    const result = await handleCommand({ cmd: 'ClearLogs', id: logsMatch[1] }, context);
    return jsonResponse(result, 200, cors);
  }

  const heartbeatMatch = path.match(RE_JOB_HEARTBEAT);
  if (heartbeatMatch && method === 'POST') {
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      {
        cmd: 'JobHeartbeat',
        id: heartbeatMatch[1],
        token: body['token'] as string | undefined,
        duration: body['duration'] as number | undefined,
      },
      context
    );
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }

  const waitMatch = path.match(RE_JOB_WAIT);
  if (waitMatch && method === 'POST') {
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      { cmd: 'WaitJob', id: waitMatch[1], timeout: body['timeout'] as number | undefined },
      context
    );
    return jsonResponse(result, 200, cors);
  }

  const extendLockMatch = path.match(RE_JOB_EXTEND_LOCK);
  if (extendLockMatch && method === 'POST') {
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      {
        cmd: 'ExtendLock',
        id: extendLockMatch[1],
        duration: body['duration'] as number,
        token: body['token'] as string | undefined,
      },
      context
    );
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }

  const moveToWaitMatch = path.match(RE_JOB_MOVE_TO_WAIT);
  if (moveToWaitMatch && method === 'POST') {
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      {
        cmd: 'MoveToWait',
        id: moveToWaitMatch[1],
        token: body['token'] as string | undefined,
      },
      context
    );
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }
  return null;
}
