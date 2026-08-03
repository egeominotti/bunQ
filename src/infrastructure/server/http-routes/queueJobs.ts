import { handleCommand } from '../handler';
import { jsonResponse, parseJsonBody } from '../httpEndpoints';
import type { HandlerContext } from '../types';

const RE_QUEUE_JOBS = /^\/queues\/([^/]+)\/jobs$/;
const RE_QUEUE_JOBS_BULK = /^\/queues\/([^/]+)\/jobs\/bulk$/;
const RE_QUEUE_JOBS_PULL_BATCH = /^\/queues\/([^/]+)\/jobs\/pull-batch$/;
const RE_QUEUE_JOBS_LIST = /^\/queues\/([^/]+)\/jobs\/list$/;

export async function routeQueueJobOperations(
  request: Request,
  path: string,
  method: string,
  context: HandlerContext,
  cors: Set<string>
): Promise<Response | null> {
  const queueJobsMatch = path.match(RE_QUEUE_JOBS);
  if (queueJobsMatch) {
    const queue = decodeURIComponent(queueJobsMatch[1]);
    if (method === 'POST') {
      let body: Record<string, unknown>;
      try {
        body = (await request.json()) as Record<string, unknown>;
      } catch {
        return jsonResponse({ ok: false, error: 'Invalid JSON body' }, 400, cors);
      }
      const command = {
        cmd: 'PUSH' as const,
        queue,
        name: typeof body.name === 'string' ? body.name : 'default',
        data: body.data,
        priority: body.priority,
        delay: body.delay,
        maxAttempts: body.maxAttempts ?? body.attempts,
        backoff: body.backoff,
        timeout: body.timeout,
        jobId: body.jobId,
        removeOnComplete: body.removeOnComplete,
        removeOnFail: body.removeOnFail,
        durable: body.durable,
        ttl: body.ttl,
        uniqueKey: body.uniqueKey,
        groupId: body.groupId,
        dependsOn: body.dependsOn,
        tags: body.tags,
        lifo: body.lifo,
        repeat: body.repeat,
      } as Parameters<typeof handleCommand>[0];
      const result = await handleCommand(command, context);
      return jsonResponse(result, result.ok ? 200 : 400, cors);
    }

    if (method === 'GET') {
      const timeout = parseInt(new URL(request.url).searchParams.get('timeout') ?? '0', 10);
      const result = await handleCommand({ cmd: 'PULL', queue, timeout }, context);
      return jsonResponse(result, 200, cors);
    }
  }

  const bulkMatch = path.match(RE_QUEUE_JOBS_BULK);
  if (bulkMatch && method === 'POST') {
    const queue = decodeURIComponent(bulkMatch[1]);
    let body: Record<string, unknown>;
    try {
      body = (await request.json()) as Record<string, unknown>;
    } catch {
      return jsonResponse({ ok: false, error: 'Invalid JSON body' }, 400, cors);
    }
    const jobs = ((body['jobs'] as Record<string, unknown>[]) ?? []).map((job) => ({
      ...job,
      name: typeof job.name === 'string' ? job.name : 'default',
    }));
    const result = await handleCommand(
      { cmd: 'PUSHB', queue, jobs } as Parameters<typeof handleCommand>[0],
      context
    );
    return jsonResponse(result, result.ok ? 200 : 400, cors);
  }

  const pullBatchMatch = path.match(RE_QUEUE_JOBS_PULL_BATCH);
  if (pullBatchMatch && method === 'POST') {
    const queue = decodeURIComponent(pullBatchMatch[1]);
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      {
        cmd: 'PULLB',
        queue,
        count: body['count'] as number,
        timeout: body['timeout'] as number | undefined,
        owner: body['owner'] as string | undefined,
        lockTtl: body['lockTtl'] as number | undefined,
      },
      context
    );
    return jsonResponse(result, 200, cors);
  }

  const listMatch = path.match(RE_QUEUE_JOBS_LIST);
  if (listMatch && method === 'GET') {
    const queue = decodeURIComponent(listMatch[1]);
    const url = new URL(request.url);
    const stateValues = [
      ...url.searchParams.getAll('state'),
      ...url.searchParams.getAll('status'),
      ...url.searchParams.getAll('states'),
    ]
      .flatMap((value) => value.split(','))
      .map((state) => state.trim())
      .filter(Boolean);
    const state =
      stateValues.length === 0
        ? undefined
        : stateValues.length === 1
          ? stateValues[0]
          : stateValues;
    const limitParam = url.searchParams.get('limit');
    const offsetParam = url.searchParams.get('offset');
    const limit = limitParam ? parseInt(limitParam, 10) : undefined;
    const offset = offsetParam ? parseInt(offsetParam, 10) : undefined;
    const result = await handleCommand(
      { cmd: 'GetJobs', queue, state, limit, offset } as Parameters<typeof handleCommand>[0],
      context
    );
    return jsonResponse(result, 200, cors);
  }
  return null;
}
