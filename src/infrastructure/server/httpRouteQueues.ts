import { handleCommand } from './handler';
import { routeQueueJobOperations } from './http-routes/queueJobs';
import { jsonResponse, parseJsonBody } from './httpEndpoints';
import type { HandlerContext } from './types';

const RE_QUEUE_COUNTS = /^\/queues\/([^/]+)\/counts$/;
const RE_QUEUE_COUNT = /^\/queues\/([^/]+)\/count$/;
const RE_QUEUE_PRIORITY_COUNTS = /^\/queues\/([^/]+)\/priority-counts$/;
const RE_QUEUE_PAUSED = /^\/queues\/([^/]+)\/paused$/;
const RE_QUEUE_PAUSE = /^\/queues\/([^/]+)\/pause$/;
const RE_QUEUE_RESUME = /^\/queues\/([^/]+)\/resume$/;
const RE_QUEUE_DRAIN = /^\/queues\/([^/]+)\/drain$/;
const RE_QUEUE_OBLITERATE = /^\/queues\/([^/]+)\/obliterate$/;
const RE_QUEUE_CLEAN = /^\/queues\/([^/]+)\/clean$/;
const RE_QUEUE_PROMOTE_JOBS = /^\/queues\/([^/]+)\/promote-jobs$/;
const RE_QUEUE_RETRY_COMPLETED = /^\/queues\/([^/]+)\/retry-completed$/;
const RE_QUEUE_WORKERS = /^\/queues\/([^/]+)\/workers$/;

export async function routeQueueRoutes(
  request: Request,
  path: string,
  method: string,
  context: HandlerContext,
  cors: Set<string>
): Promise<Response | null> {
  if (path === '/queues' && method === 'GET') {
    return jsonResponse(await handleCommand({ cmd: 'ListQueues' }, context), 200, cors);
  }
  if (path === '/queues/summary' && method === 'GET') {
    return jsonResponse(context.queueManager.getQueuesSummary(), 200, cors);
  }

  const jobOperations = await routeQueueJobOperations(request, path, method, context, cors);
  if (jobOperations) return jobOperations;

  const queueWorkersMatch = path.match(RE_QUEUE_WORKERS);
  if (queueWorkersMatch && method === 'GET') {
    const queue = decodeURIComponent(queueWorkersMatch[1]);
    const workers = context.queueManager.workerManager.getForQueue(queue);
    return jsonResponse(
      {
        ok: true,
        workers: workers.map((worker) => ({
          id: worker.id,
          name: worker.name,
          queues: worker.queues,
          concurrency: worker.concurrency,
          registeredAt: worker.registeredAt,
          lastSeen: worker.lastSeen,
          activeJobs: worker.activeJobs,
          processedJobs: worker.processedJobs,
          failedJobs: worker.failedJobs,
        })),
      },
      200,
      cors
    );
  }

  const countsMatch = path.match(RE_QUEUE_COUNTS);
  if (countsMatch && method === 'GET') {
    const queue = decodeURIComponent(countsMatch[1]);
    return jsonResponse(await handleCommand({ cmd: 'GetJobCounts', queue }, context), 200, cors);
  }

  const countMatch = path.match(RE_QUEUE_COUNT);
  if (countMatch && method === 'GET') {
    const queue = decodeURIComponent(countMatch[1]);
    return jsonResponse(await handleCommand({ cmd: 'Count', queue }, context), 200, cors);
  }

  const priorityCountsMatch = path.match(RE_QUEUE_PRIORITY_COUNTS);
  if (priorityCountsMatch && method === 'GET') {
    const queue = decodeURIComponent(priorityCountsMatch[1]);
    return jsonResponse(
      await handleCommand({ cmd: 'GetCountsPerPriority', queue }, context),
      200,
      cors
    );
  }

  const pausedMatch = path.match(RE_QUEUE_PAUSED);
  if (pausedMatch && method === 'GET') {
    const queue = decodeURIComponent(pausedMatch[1]);
    return jsonResponse(await handleCommand({ cmd: 'IsPaused', queue }, context), 200, cors);
  }

  const controlRoutes = [
    [RE_QUEUE_PAUSE, 'POST', 'Pause'],
    [RE_QUEUE_RESUME, 'POST', 'Resume'],
    [RE_QUEUE_DRAIN, 'POST', 'Drain'],
    [RE_QUEUE_OBLITERATE, 'POST', 'Obliterate'],
  ] as const;
  for (const [pattern, expectedMethod, commandName] of controlRoutes) {
    const match = path.match(pattern);
    if (match && method === expectedMethod) {
      const queue = decodeURIComponent(match[1]);
      return jsonResponse(await handleCommand({ cmd: commandName, queue }, context), 200, cors);
    }
  }

  const cleanMatch = path.match(RE_QUEUE_CLEAN);
  if (cleanMatch && method === 'POST') {
    const queue = decodeURIComponent(cleanMatch[1]);
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      {
        cmd: 'Clean',
        queue,
        grace: typeof body['grace'] === 'number' ? body['grace'] : 0,
        state: body['state'] as string | undefined,
        limit: body['limit'] as number | undefined,
      } as Parameters<typeof handleCommand>[0],
      context
    );
    return jsonResponse(result, 200, cors);
  }

  const promoteJobsMatch = path.match(RE_QUEUE_PROMOTE_JOBS);
  if (promoteJobsMatch && method === 'POST') {
    const queue = decodeURIComponent(promoteJobsMatch[1]);
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      { cmd: 'PromoteJobs', queue, count: body['count'] as number | undefined },
      context
    );
    return jsonResponse(result, 200, cors);
  }

  const retryCompletedMatch = path.match(RE_QUEUE_RETRY_COMPLETED);
  if (retryCompletedMatch && method === 'POST') {
    const queue = decodeURIComponent(retryCompletedMatch[1]);
    const body = await parseJsonBody(request, cors);
    if (body instanceof Response) return body;
    const result = await handleCommand(
      { cmd: 'RetryCompleted', queue, id: body['id'] as string | undefined },
      context
    );
    return jsonResponse(result, 200, cors);
  }
  return null;
}
