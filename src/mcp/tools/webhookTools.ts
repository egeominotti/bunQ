/* eslint-disable @typescript-eslint/no-deprecated */
/**
 * MCP Tools - Webhook Management
 */

import { z } from 'zod';
import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import type { McpBackend } from '../adapter';
import { withErrorHandler } from './withErrorHandler';

export function registerWebhookTools(server: McpServer, backend: McpBackend) {
  server.tool(
    'bunqueue_add_webhook',
    'Add a webhook to receive notifications for job events.',
    {
      url: z.string().url().describe('Webhook URL to receive POST requests'),
      events: z
        .array(
          z.enum([
            'job.completed',
            'job.failed',
            'job.progress',
            'job.active',
            'job.waiting',
            'job.delayed',
          ])
        )
        .describe('Events to subscribe to'),
      queue: z.string().optional().describe('Limit to a specific queue (omit for all queues)'),
    },
    withErrorHandler('bunqueue_add_webhook', async ({ url, events, queue }) => {
      const webhook = await backend.addWebhook(url, events, queue);
      return {
        content: [
          { type: 'text' as const, text: JSON.stringify({ success: true, ...webhook }, null, 2) },
        ],
      };
    })
  );

  server.tool(
    'bunqueue_remove_webhook',
    'Remove a webhook by ID.',
    {
      id: z.string().describe('Webhook ID to remove'),
    },
    withErrorHandler('bunqueue_remove_webhook', async ({ id }) => {
      const success = await backend.removeWebhook(id);
      return { content: [{ type: 'text' as const, text: JSON.stringify({ success, id }) }] };
    })
  );

  server.tool(
    'bunqueue_list_webhooks',
    'List all registered webhooks.',
    {},
    withErrorHandler('bunqueue_list_webhooks', async () => {
      const webhooks = await backend.listWebhooks();
      return {
        content: [
          {
            type: 'text' as const,
            text: JSON.stringify({ count: webhooks.length, webhooks }, null, 2),
          },
        ],
      };
    })
  );

  server.tool(
    'bunqueue_set_webhook_enabled',
    'Enable or disable a webhook without removing it.',
    {
      id: z.string().describe('Webhook ID'),
      enabled: z.boolean().describe('Whether the webhook should be enabled'),
    },
    withErrorHandler('bunqueue_set_webhook_enabled', async ({ id, enabled }) => {
      const success = await backend.setWebhookEnabled(id, enabled);
      return {
        content: [{ type: 'text' as const, text: JSON.stringify({ success, id, enabled }) }],
      };
    })
  );
}
