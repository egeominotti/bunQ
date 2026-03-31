/**
 * Webhook SSRF Prevention Tests
 *
 * Tests the validateWebhookUrl function and its enforcement in WebhookManager.add().
 * SSRF validation is now applied at the WebhookManager level, protecting both
 * TCP server mode and embedded SDK mode.
 */
import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { validateWebhookUrl } from '../src/infrastructure/server/protocol';
import { WebhookManager } from '../src/application/webhookManager';

describe('Webhook SSRF Prevention', () => {
  describe('validateWebhookUrl', () => {
    // ── Valid URLs that should be accepted ──

    test('accepts valid external HTTP URL', () => {
      const result = validateWebhookUrl('http://example.com/webhook');
      expect(result).toBeNull();
    });

    test('accepts valid external HTTPS URL', () => {
      const result = validateWebhookUrl('https://example.com/webhook');
      expect(result).toBeNull();
    });

    test('accepts URL with non-standard port', () => {
      const result = validateWebhookUrl('http://example.com:8080/webhook');
      expect(result).toBeNull();
    });

    test('accepts URL with path and query string', () => {
      const result = validateWebhookUrl('https://hooks.example.com/api/v1/webhook?token=abc');
      expect(result).toBeNull();
    });

    test('accepts URL with subdomain', () => {
      const result = validateWebhookUrl('https://hooks.slack.com/services/T00/B00/xxx');
      expect(result).toBeNull();
    });

    // ── Invalid URL formats ──

    test('rejects empty URL', () => {
      const result = validateWebhookUrl('');
      expect(result).toBe('Webhook URL is required');
    });

    test('rejects URL without protocol', () => {
      const result = validateWebhookUrl('example.com/webhook');
      expect(result).toBe('Invalid URL format');
    });

    test('rejects completely invalid URL format', () => {
      const result = validateWebhookUrl('not-a-url');
      expect(result).toBe('Invalid URL format');
    });

    test('rejects URL exceeding max length', () => {
      const longUrl = 'https://example.com/' + 'a'.repeat(2048);
      const result = validateWebhookUrl(longUrl);
      expect(result).toBe('Webhook URL too long (max 2048 characters)');
    });

    // ── Protocol restrictions ──

    test('rejects FTP protocol URL', () => {
      const result = validateWebhookUrl('ftp://example.com/file');
      expect(result).toBe('Webhook URL must use http or https protocol');
    });

    test('rejects file protocol URL', () => {
      const result = validateWebhookUrl('file:///etc/passwd');
      expect(result).toBe('Webhook URL must use http or https protocol');
    });

    test('rejects javascript protocol URL', () => {
      // URL constructor may throw on javascript: protocol
      const result = validateWebhookUrl('javascript:alert(1)');
      expect(result).not.toBeNull();
    });

    test('rejects data protocol URL', () => {
      const result = validateWebhookUrl('data:text/html,<h1>test</h1>');
      expect(result).not.toBeNull();
    });

    // ── Localhost blocking ──

    test('rejects localhost URL', () => {
      const result = validateWebhookUrl('http://localhost/webhook');
      expect(result).toBe('Webhook URL cannot point to localhost');
    });

    test('rejects localhost with port', () => {
      const result = validateWebhookUrl('http://localhost:3000/webhook');
      expect(result).toBe('Webhook URL cannot point to localhost');
    });

    test('rejects 127.0.0.1', () => {
      const result = validateWebhookUrl('http://127.0.0.1/webhook');
      expect(result).toBe('Webhook URL cannot point to localhost');
    });

    test('rejects 127.0.0.1 with port', () => {
      const result = validateWebhookUrl('http://127.0.0.1:8080/webhook');
      expect(result).toBe('Webhook URL cannot point to localhost');
    });

    test('rejects IPv6 loopback ::1', () => {
      const result = validateWebhookUrl('http://[::1]/webhook');
      expect(result).not.toBeNull();
    });

    test('rejects subdomain of localhost', () => {
      const result = validateWebhookUrl('http://sub.localhost/webhook');
      expect(result).toBe('Webhook URL cannot point to localhost');
    });

    test('rejects other 127.x.x.x loopback addresses', () => {
      const result = validateWebhookUrl('http://127.0.0.2/webhook');
      expect(result).toBe('Webhook URL cannot point to loopback IP');
    });

    test('rejects 127.255.255.255', () => {
      const result = validateWebhookUrl('http://127.255.255.255/webhook');
      expect(result).toBe('Webhook URL cannot point to loopback IP');
    });

    // ── Private IP range blocking ──

    test('rejects 10.0.0.0/8 private range', () => {
      const result = validateWebhookUrl('http://10.0.0.1/webhook');
      expect(result).toBe('Webhook URL cannot point to private IP');
    });

    test('rejects 10.255.255.255', () => {
      const result = validateWebhookUrl('http://10.255.255.255/webhook');
      expect(result).toBe('Webhook URL cannot point to private IP');
    });

    test('rejects 172.16.0.0/12 private range (lower bound)', () => {
      const result = validateWebhookUrl('http://172.16.0.1/webhook');
      expect(result).toBe('Webhook URL cannot point to private IP');
    });

    test('rejects 172.16.0.0/12 private range (upper bound)', () => {
      const result = validateWebhookUrl('http://172.31.255.255/webhook');
      expect(result).toBe('Webhook URL cannot point to private IP');
    });

    test('accepts 172.15.x.x (not in private range)', () => {
      const result = validateWebhookUrl('http://172.15.0.1/webhook');
      expect(result).toBeNull();
    });

    test('accepts 172.32.x.x (not in private range)', () => {
      const result = validateWebhookUrl('http://172.32.0.1/webhook');
      expect(result).toBeNull();
    });

    test('rejects 192.168.0.0/16 private range', () => {
      const result = validateWebhookUrl('http://192.168.0.1/webhook');
      expect(result).toBe('Webhook URL cannot point to private IP');
    });

    test('rejects 192.168.255.255', () => {
      const result = validateWebhookUrl('http://192.168.255.255/webhook');
      expect(result).toBe('Webhook URL cannot point to private IP');
    });

    test('accepts 192.167.x.x (not in private range)', () => {
      const result = validateWebhookUrl('http://192.167.0.1/webhook');
      expect(result).toBeNull();
    });

    // ── Special IP addresses ──

    test('rejects 0.0.0.0 (unspecified)', () => {
      const result = validateWebhookUrl('http://0.0.0.0/webhook');
      expect(result).toBe('Webhook URL cannot point to unspecified IP');
    });

    test('rejects 169.254.x.x link-local range', () => {
      const result = validateWebhookUrl('http://169.254.1.1/webhook');
      expect(result).toBe('Webhook URL cannot point to link-local IP');
    });

    test('rejects 169.254.169.254 (cloud metadata)', () => {
      const result = validateWebhookUrl('http://169.254.169.254/latest/meta-data/');
      expect(result).not.toBeNull();
    });

    // ── Cloud metadata endpoint blocking ──

    test('rejects metadata.google.internal', () => {
      const result = validateWebhookUrl('http://metadata.google.internal/computeMetadata/v1/');
      expect(result).toBe('Webhook URL cannot point to cloud metadata endpoints');
    });

    test('rejects any .internal domain', () => {
      const result = validateWebhookUrl('http://some-service.internal/webhook');
      expect(result).toBe('Webhook URL cannot point to cloud metadata endpoints');
    });

    // ── URL with credentials ──

    test('handles URL with embedded credentials', () => {
      // URL with user:pass@host - the URL constructor parses this fine
      // The current implementation does not explicitly block credentials
      const result = validateWebhookUrl('http://user:pass@example.com/webhook');
      // Document current behavior: credentials in URL are accepted
      // (the hostname is still example.com, which is external)
      expect(result).toBeNull();
    });

    test('rejects URL with credentials pointing to private IP', () => {
      const result = validateWebhookUrl('http://user:pass@10.0.0.1/webhook');
      expect(result).toBe('Webhook URL cannot point to private IP');
    });

    test('rejects URL with credentials pointing to localhost', () => {
      const result = validateWebhookUrl('http://admin:secret@localhost/webhook');
      expect(result).toBe('Webhook URL cannot point to localhost');
    });
  });

  describe('WebhookManager.add() SSRF validation (embedded mode)', () => {
    let manager: WebhookManager;

    beforeEach(() => {
      manager = new WebhookManager();
    });

    afterEach(() => {
      for (const wh of manager.list()) {
        manager.remove(wh.id);
      }
    });

    test('accepts valid external URL', () => {
      const wh = manager.add('http://example.com/webhook', ['job.completed']);
      expect(wh.url).toBe('http://example.com/webhook');
      expect(wh.enabled).toBe(true);
    });

    test('accepts valid HTTPS URL', () => {
      const wh = manager.add('https://example.com/webhook', ['job.completed']);
      expect(wh.url).toBe('https://example.com/webhook');
    });

    test('accepts URL with non-standard port', () => {
      const wh = manager.add('http://example.com:9090/hook', ['job.completed']);
      expect(wh.url).toBe('http://example.com:9090/hook');
    });

    test('blocks localhost in embedded mode', () => {
      expect(() => manager.add('http://localhost:3000/hook', ['job.completed'])).toThrow(
        'Webhook URL cannot point to localhost'
      );
    });

    test('blocks private IPs (10.x.x.x) in embedded mode', () => {
      expect(() => manager.add('http://10.0.0.1/hook', ['job.completed'])).toThrow(
        'Webhook URL cannot point to private IP'
      );
    });

    test('blocks 192.168.x.x in embedded mode', () => {
      expect(() => manager.add('http://192.168.1.1/hook', ['job.completed'])).toThrow(
        'Webhook URL cannot point to private IP'
      );
    });

    test('blocks 172.16.x.x in embedded mode', () => {
      expect(() => manager.add('http://172.16.0.1/hook', ['job.completed'])).toThrow(
        'Webhook URL cannot point to private IP'
      );
    });

    test('rejects empty URL in embedded mode', () => {
      expect(() => manager.add('', ['job.completed'])).toThrow('Webhook URL is required');
    });

    test('rejects invalid protocol in embedded mode', () => {
      expect(() => manager.add('ftp://example.com/file', ['job.completed'])).toThrow(
        'Webhook URL must use http or https protocol'
      );
    });

    test('rejects URL without protocol in embedded mode', () => {
      expect(() => manager.add('example.com/hook', ['job.completed'])).toThrow('Invalid URL format');
    });

    test('accepts URL with credentials to external host', () => {
      const wh = manager.add('http://user:pass@example.com/hook', ['job.completed']);
      expect(wh.url).toBe('http://user:pass@example.com/hook');
    });

    test('blocks cloud metadata endpoint', () => {
      expect(() =>
        manager.add('http://169.254.169.254/latest/meta-data/', ['job.completed'])
      ).toThrow();
    });

    test('blocks .internal domains', () => {
      expect(() =>
        manager.add('http://some-service.internal/webhook', ['job.completed'])
      ).toThrow('Webhook URL cannot point to cloud metadata endpoints');
    });
  });
});
