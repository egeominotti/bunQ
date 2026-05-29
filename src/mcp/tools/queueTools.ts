/* eslint-disable @typescript-eslint/no-deprecated */
/**
 * MCP Tools - Queue Control
 * List, count, pause, resume, drain, obliterate, clean, get jobs
 */

import { z } from 'zod';
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import type { McpBackend } from '../adapter';
import { withErrorHandler } from './withErrorHandler';

export function registerQueueTools(server: McpServer, backend: McpBackend) {
  server.tool(
    'bunqueue_list_queues',
    'List all queues.',
    {},
    withErrorHandler('bunqueue_list_queues', async () => {
      const queues = await backend.listQueues();
      return { content: [{ type: 'text' as const, text: JSON.stringify({ queues }) }] };
    })
  );

  server.tool(
    'bunqueue_count_jobs',
    'Count total jobs in a queue (all states).',
    {
      queue: z.string().describe('Queue name'),
    },
    withErrorHandler('bunqueue_count_jobs', async ({ queue }) => {
      const count = await backend.countJobs(queue);
      return { content: [{ type: 'text' as const, text: JSON.stringify({ queue, count }) }] };
    })
  );

  server.tool(
    'bunqueue_get_jobs',
    'List jobs in a queue with optional state filter and pagination.',
    {
      queue: z.string().describe('Queue name'),
      state: z
        .enum(['waiting', 'delayed', 'active', 'completed', 'failed'])
        .optional()
        .describe('Filter by job state'),
      start: z.number().optional().describe('Start index for pagination (default: 0)'),
      end: z.number().optional().describe('End index for pagination (default: 20)'),
    },
    withErrorHandler('bunqueue_get_jobs', async ({ queue, state, start, end }) => {
      const jobs = await backend.getJobs(queue, { state, start: start ?? 0, end: end ?? 20 });
      return {
        content: [
          {
            type: 'text' as const,
            text: JSON.stringify({ queue, count: jobs.length, jobs }, null, 2),
          },
        ],
      };
    })
  );

  server.tool(
    'bunqueue_get_job_counts',
    'Get job counts per state for a queue (waiting, delayed, active, completed, failed).',
    {
      queue: z.string().describe('Queue name'),
    },
    withErrorHandler('bunqueue_get_job_counts', async ({ queue }) => {
      const counts = await backend.getJobCounts(queue);
      return { content: [{ type: 'text' as const, text: JSON.stringify({ queue, ...counts }) }] };
    })
  );

  server.tool(
    'bunqueue_pause_queue',
    'Pause job processing on a queue. No new jobs will be processed until resumed.',
    {
      queue: z.string().describe('Queue name'),
    },
    withErrorHandler('bunqueue_pause_queue', async ({ queue }) => {
      await backend.pauseQueue(queue);
      return {
        content: [
          {
            type: 'text' as const,
            text: JSON.stringify({ success: true, queue, message: 'Queue paused' }),
          },
        ],
      };
    })
  );

  server.tool(
    'bunqueue_resume_queue',
    'Resume job processing on a paused queue.',
    {
      queue: z.string().describe('Queue name'),
    },
    withErrorHandler('bunqueue_resume_queue', async ({ queue }) => {
      await backend.resumeQueue(queue);
      return {
        content: [
          {
            type: 'text' as const,
            text: JSON.stringify({ success: true, queue, message: 'Queue resumed' }),
          },
        ],
      };
    })
  );

  server.tool(
    'bunqueue_drain_queue',
    'Remove all waiting jobs from a queue. Active jobs continue processing.',
    {
      queue: z.string().describe('Queue name'),
    },
    withErrorHandler('bunqueue_drain_queue', async ({ queue }) => {
      const removed = await backend.drainQueue(queue);
      return {
        content: [
          { type: 'text' as const, text: JSON.stringify({ success: true, queue, removed }) },
        ],
      };
    })
  );

  server.tool(
    'bunqueue_obliterate_queue',
    'Remove ALL data from a queue (waiting, active, completed, failed). Destructive operation.',
    {
      queue: z.string().describe('Queue name'),
    },
    withErrorHandler('bunqueue_obliterate_queue', async ({ queue }) => {
      await backend.obliterateQueue(queue);
      return {
        content: [
          {
            type: 'text' as const,
            text: JSON.stringify({ success: true, queue, message: 'Queue obliterated' }),
          },
        ],
      };
    })
  );

  server.tool(
    'bunqueue_clean_queue',
    'Remove old jobs from a queue based on grace period and state.',
    {
      queue: z.string().describe('Queue name'),
      graceMs: z.number().min(0).describe('Grace period in ms - only remove jobs older than this'),
      state: z
        .enum(['completed', 'failed'])
        .optional()
        .describe('State to clean (default: both completed and failed)'),
      limit: z.number().optional().describe('Maximum number of jobs to remove'),
    },
    withErrorHandler('bunqueue_clean_queue', async ({ queue, graceMs, state, limit }) => {
      const ids = await backend.cleanQueue(queue, graceMs, state, limit);
      return {
        content: [
          {
            type: 'text' as const,
            text: JSON.stringify({ success: true, queue, removed: ids.length, ids }),
          },
        ],
      };
    })
  );

  server.tool(
    'bunqueue_is_paused',
    'Check if a queue is currently paused.',
    {
      queue: z.string().describe('Queue name'),
    },
    withErrorHandler('bunqueue_is_paused', async ({ queue }) => {
      const paused = await backend.isPaused(queue);
      return { content: [{ type: 'text' as const, text: JSON.stringify({ queue, paused }) }] };
    })
  );

  server.tool(
    'bunqueue_get_counts_per_priority',
    'Get job count breakdown by priority level for a queue. Counts only waiting/delayed (queued) jobs — active, completed and failed jobs are not included.',
    {
      queue: z.string().describe('Queue name'),
    },
    withErrorHandler('bunqueue_get_counts_per_priority', async ({ queue }) => {
      const counts = await backend.getCountsPerPriority(queue);
      return {
        content: [
          { type: 'text' as const, text: JSON.stringify({ queue, priorities: counts }, null, 2) },
        ],
      };
    })
  );
}
