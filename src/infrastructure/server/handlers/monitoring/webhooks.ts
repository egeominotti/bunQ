import type {
  AddWebhookCommand,
  ListWebhooksCommand,
  RemoveWebhookCommand,
  SetWebhookEnabledCommand,
} from '../../../../domain/types/command';
import type { Response } from '../../../../domain/types/response';
import * as response from '../../../../domain/types/response';
import { WEBHOOK_EVENTS } from '../../../../domain/types/webhook';
import { validateWebhookUrl } from '../../protocol';
import type { HandlerContext } from '../../types';

export function handleAddWebhook(
  command: AddWebhookCommand,
  context: HandlerContext,
  requestId?: string
): Response {
  const urlError = validateWebhookUrl(command.url);
  if (urlError) return response.error(urlError, requestId);

  const invalidEvents = command.events.filter(
    (event) => !(WEBHOOK_EVENTS as readonly string[]).includes(event)
  );
  if (invalidEvents.length > 0) {
    return response.error(
      `Invalid webhook event(s): ${invalidEvents.join(', ')}. Valid: ${WEBHOOK_EVENTS.join(', ')}`,
      requestId
    );
  }

  const webhook = context.queueManager.webhookManager.add(
    command.url,
    command.events,
    command.queue,
    command.secret
  );
  context.queueManager.emitDashboardEvent('webhook:added', {
    id: webhook.id,
    url: webhook.url,
    events: webhook.events,
  });
  return response.data(
    {
      webhookId: webhook.id,
      url: webhook.url,
      events: webhook.events,
      queue: webhook.queue,
      createdAt: webhook.createdAt,
    },
    requestId
  );
}

export function handleRemoveWebhook(
  command: RemoveWebhookCommand,
  context: HandlerContext,
  requestId?: string
): Response {
  const success = context.queueManager.webhookManager.remove(command.webhookId);
  if (success) {
    context.queueManager.emitDashboardEvent('webhook:removed', { id: command.webhookId });
    return response.data({ removed: true }, requestId);
  }
  return response.error('Webhook not found', requestId);
}

export function handleListWebhooks(
  _command: ListWebhooksCommand,
  context: HandlerContext,
  requestId?: string
): Response {
  const webhooks = context.queueManager.webhookManager.list();
  return response.data(
    {
      webhooks: webhooks.map((webhook) => ({
        id: webhook.id,
        url: webhook.url,
        events: webhook.events,
        queue: webhook.queue,
        createdAt: webhook.createdAt,
        lastTriggered: webhook.lastTriggered,
        successCount: webhook.successCount,
        failureCount: webhook.failureCount,
        enabled: webhook.enabled,
      })),
      stats: context.queueManager.webhookManager.getStats(),
    },
    requestId
  );
}

export function handleSetWebhookEnabled(
  command: SetWebhookEnabledCommand,
  context: HandlerContext,
  requestId?: string
): Response {
  const success = context.queueManager.webhookManager.setEnabled(command.id, command.enabled);
  return success
    ? response.ok(undefined, requestId)
    : response.error('Webhook not found', requestId);
}
