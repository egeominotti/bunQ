/**
 * Webhook domain types
 */

import { uuid } from '../../shared/hash';

/** Webhook ID type */
export type WebhookId = string;

/**
 * Canonical list of webhook events the server actually triggers — single
 * source of truth for CLI/TCP/HTTP/MCP validation. job.pushed/started/
 * completed/failed flow through eventsManager.mapEventToWebhook; job.progress
 * is triggered directly by updateProgress (jobManagement).
 */
export const WEBHOOK_EVENTS = [
  'job.pushed',
  'job.started',
  'job.completed',
  'job.failed',
  'job.progress',
] as const;

/**
 * Webhook event types. Includes 'job.stalled' for backward compatibility with
 * stored webhooks, but it is never emitted and not accepted on new ones.
 */
export type WebhookEvent = (typeof WEBHOOK_EVENTS)[number] | 'job.stalled';

/** Webhook configuration */
export interface Webhook {
  id: WebhookId;
  url: string;
  events: WebhookEvent[];
  queue: string | null; // null = all queues
  secret: string | null;
  createdAt: number;
  lastTriggered: number | null;
  successCount: number;
  failureCount: number;
  enabled: boolean;
}

/** Create a new webhook */
export function createWebhook(
  url: string,
  events: string[],
  queue?: string,
  secret?: string
): Webhook {
  return {
    id: uuid(),
    url,
    events: events as WebhookEvent[],
    queue: queue ?? null,
    secret: secret ?? null,
    createdAt: Date.now(),
    lastTriggered: null,
    successCount: 0,
    failureCount: 0,
    enabled: true,
  };
}

/** Webhook payload */
export interface WebhookPayload {
  event: WebhookEvent;
  timestamp: number;
  jobId: string;
  queue: string;
  data?: unknown;
  error?: string;
  progress?: number;
}
