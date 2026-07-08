/**
 * Bug: JobOptions.backoff typed as `number | BackoffOptions`, but the TCP
 * server rejected the object form with "backoff must be a number".
 *
 * Embedded mode already accepts the object (domain parseBackoff maps
 * type -> strategy). The TCP path went through validateJobOptions, which
 * only allowed a plain number, so `queue.add(n, d, { backoff: { type:
 * 'exponential', delay: 200 } })` threw over the wire while working
 * embedded. This is a parity gap: the public type promises the object form,
 * the server must honour it.
 *
 * RED before fix (validator returns the error string), GREEN after.
 */
import { describe, it, expect } from 'bun:test';
import { validateJobOptions } from '../src/infrastructure/server/protocol';

describe('bug: backoff object form over TCP', () => {
  it('accepts exponential backoff object', () => {
    const err = validateJobOptions({
      backoff: { type: 'exponential', delay: 200 },
    });
    expect(err).toBeNull();
  });

  it('accepts fixed backoff object', () => {
    const err = validateJobOptions({
      backoff: { type: 'fixed', delay: 1000 },
    });
    expect(err).toBeNull();
  });

  it('still accepts the plain-number backoff form', () => {
    expect(validateJobOptions({ backoff: 500 })).toBeNull();
  });

  it('rejects a malformed backoff object (bad type)', () => {
    const err = validateJobOptions({
      backoff: { type: 'linear', delay: 200 },
    });
    expect(err).not.toBeNull();
  });

  it('rejects a malformed backoff object (non-numeric delay)', () => {
    const err = validateJobOptions({
      backoff: { type: 'fixed', delay: 'soon' },
    });
    expect(err).not.toBeNull();
  });

  it('rejects a negative backoff number', () => {
    expect(validateJobOptions({ backoff: -1 })).not.toBeNull();
  });
});
