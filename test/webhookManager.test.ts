/**
 * Webhook Manager Tests
 * Webhook registration and triggering
 */

import { describe, test, expect, beforeEach } from 'bun:test';
import { WebhookManager } from '../src/application/webhookManager';

describe('WebhookManager', () => {
  let manager: WebhookManager;

  beforeEach(() => {
    manager = new WebhookManager();
  });

  describe('add', () => {
    test('should add a webhook', () => {
      const webhook = manager.add('https://example.com/hook', ['job.completed']);

      expect(webhook.id).toBeDefined();
      expect(webhook.url).toBe('https://example.com/hook');
      expect(webhook.events).toEqual(['job.completed']);
      expect(webhook.enabled).toBe(true);
    });

    test('should add webhook with queue filter', () => {
      const webhook = manager.add('https://example.com/hook', ['job.completed'], 'emails');

      expect(webhook.queue).toBe('emails');
    });

    test('should add webhook with secret', () => {
      const webhook = manager.add(
        'https://example.com/hook',
        ['job.completed'],
        undefined,
        'my-secret'
      );

      expect(webhook.secret).toBe('my-secret');
    });

    test('should generate unique IDs', () => {
      const w1 = manager.add('https://example.com/hook1', ['job.completed']);
      const w2 = manager.add('https://example.com/hook2', ['job.completed']);

      expect(w1.id).not.toBe(w2.id);
    });
  });

  describe('remove', () => {
    test('should remove a webhook', () => {
      const webhook = manager.add('https://example.com/hook', ['job.completed']);

      const removed = manager.remove(webhook.id);
      expect(removed).toBe(true);

      expect(manager.get(webhook.id)).toBeUndefined();
    });

    test('should return false for non-existent webhook', () => {
      const removed = manager.remove('non-existent-id');
      expect(removed).toBe(false);
    });
  });

  describe('get', () => {
    test('should get webhook by ID', () => {
      const webhook = manager.add('https://example.com/hook', ['job.completed']);

      const found = manager.get(webhook.id);
      expect(found?.url).toBe('https://example.com/hook');
    });

    test('should return undefined for non-existent ID', () => {
      expect(manager.get('non-existent')).toBeUndefined();
    });
  });

  describe('list', () => {
    test('should return all webhooks', () => {
      manager.add('https://example.com/hook1', ['job.completed']);
      manager.add('https://example.com/hook2', ['job.failed']);
      manager.add('https://example.com/hook3', ['job.pushed']);

      const webhooks = manager.list();
      expect(webhooks.length).toBe(3);
    });

    test('should return empty array when no webhooks', () => {
      expect(manager.list()).toEqual([]);
    });
  });

  describe('setEnabled', () => {
    test('should disable webhook', () => {
      const webhook = manager.add('https://example.com/hook', ['job.completed']);
      expect(webhook.enabled).toBe(true);

      const result = manager.setEnabled(webhook.id, false);
      expect(result).toBe(true);
      expect(webhook.enabled).toBe(false);
    });

    test('should enable webhook', () => {
      const webhook = manager.add('https://example.com/hook', ['job.completed']);
      manager.setEnabled(webhook.id, false);

      const result = manager.setEnabled(webhook.id, true);
      expect(result).toBe(true);
      expect(webhook.enabled).toBe(true);
    });

    test('should return false for non-existent webhook', () => {
      const result = manager.setEnabled('non-existent', false);
      expect(result).toBe(false);
    });

    test('should not change counter if already in desired state', () => {
      manager.add('https://example.com/hook1', ['job.completed']);
      const w2 = manager.add('https://example.com/hook2', ['job.failed']);

      // Disable w2
      manager.setEnabled(w2.id, false);
      expect(manager.getStats().enabled).toBe(1);

      // Disable again - should not change counter
      manager.setEnabled(w2.id, false);
      expect(manager.getStats().enabled).toBe(1);

      // Enable twice - should not increase counter twice
      manager.setEnabled(w2.id, true);
      manager.setEnabled(w2.id, true);
      expect(manager.getStats().enabled).toBe(2);
    });

    test('should update enabled count correctly when removing disabled webhook', () => {
      const w1 = manager.add('https://example.com/hook1', ['job.completed']);
      const w2 = manager.add('https://example.com/hook2', ['job.failed']);

      manager.setEnabled(w2.id, false);
      expect(manager.getStats().enabled).toBe(1);

      // Remove disabled webhook - enabled count should stay same
      manager.remove(w2.id);
      expect(manager.getStats().enabled).toBe(1);

      // Remove enabled webhook - enabled count should decrease
      manager.remove(w1.id);
      expect(manager.getStats().enabled).toBe(0);
    });
  });

  describe('getStats', () => {
    test('should return correct stats', () => {
      manager.add('https://example.com/hook1', ['job.completed']);
      const w2 = manager.add('https://example.com/hook2', ['job.failed']);
      manager.add('https://example.com/hook3', ['job.pushed']);

      // Disable one webhook using setEnabled method
      manager.setEnabled(w2.id, false);

      const stats = manager.getStats();
      expect(stats.total).toBe(3);
      expect(stats.enabled).toBe(2);
    });

    test('should return zero stats when no webhooks', () => {
      const stats = manager.getStats();
      expect(stats.total).toBe(0);
      expect(stats.enabled).toBe(0);
    });
  });

  describe('trigger', () => {
    test('should not throw when no matching webhooks', async () => {
      // Should not throw
      await manager.trigger('job.completed', 'job-123', 'emails');
    });

    test('should filter by event type', async () => {
      manager.add('https://example.com/hook', ['job.failed']); // Only failed

      // Should not throw even though no matching webhook
      await manager.trigger('job.completed', 'job-123', 'emails');
    });

    test('should filter by queue', async () => {
      manager.add('https://example.com/hook', ['job.completed'], 'tasks'); // Only tasks queue

      // Should not throw even though queue doesn't match
      await manager.trigger('job.completed', 'job-123', 'emails');
    });
  });
});
