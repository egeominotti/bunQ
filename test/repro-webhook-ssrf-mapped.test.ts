/**
 * REPRO — webhook SSRF guard misses IPv4-mapped IPv6 loopback/private addresses.
 *
 * Run: bun test test/repro-webhook-ssrf-mapped.test.ts
 *
 * validateWebhookUrl (src/shared/webhookValidation.ts) blocks literal localhost /
 * 127.0.0.1 / ::1 and IPv4 private/loopback octets, but NOT IPv4-mapped IPv6
 * (e.g. http://[::ffff:127.0.0.1]:8080/x, which the URL parser normalizes to the
 * hex form [::ffff:7f00:1] and which resolves to 127.0.0.1). An attacker can thus
 * smuggle a loopback/private target past the guard. Asserts the mapped forms are
 * REJECTED while a normal public host stays allowed.
 *
 * RED on current code (mapped loopback allowed → null) → GREEN once the validator
 * unwraps ::ffff: mapped IPv4 and runs it through the private/loopback check.
 *
 * Pure unit test; DOES NOT touch src/.
 */
import { describe, test, expect } from 'bun:test';
import { validateWebhookUrl } from '../src/shared/webhookValidation';

describe('REPRO: webhook SSRF — IPv4-mapped IPv6', () => {
  test('rejects IPv4-mapped IPv6 loopback', () => {
    const err = validateWebhookUrl('http://[::ffff:127.0.0.1]:8080/x');
    // RED today: returns null (allowed). Must return a truthy error string.
    expect(typeof err).toBe('string');
    expect(err).toBeTruthy();
  });

  test('rejects IPv4-mapped IPv6 private (10.x)', () => {
    const err = validateWebhookUrl('http://[::ffff:10.0.0.1]/x');
    expect(typeof err).toBe('string');
    expect(err).toBeTruthy();
  });

  test('rejects IPv4-mapped IPv6 private (192.168.x)', () => {
    const err = validateWebhookUrl('http://[::ffff:192.168.1.1]/x');
    expect(typeof err).toBe('string');
    expect(err).toBeTruthy();
  });

  test('still allows a normal public host', () => {
    expect(validateWebhookUrl('https://example.com/hook')).toBeNull();
  });
});
