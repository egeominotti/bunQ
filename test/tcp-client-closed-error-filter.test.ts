/**
 * ClientClosedError sentinel subclass — verifies the dedicated class used by
 * TcpClient.close() to identify the synthetic rejection issued by rejectAll.
 *
 * Regression context: the original implementation matched `error.message ===
 * 'Client closed'` exactly, which would collide with third-party libraries
 * that throw the same string (Redis clients, DB pools). Skeptic flagged this
 * as a soundness issue; the fix introduces `ClientClosedError` so the filter
 * uses `instanceof` and is collision-free.
 *
 * We deliberately do NOT test process.on('unhandledRejection') event firing
 * here because Bun's test runner treats fire-and-forget rejections as test
 * failures, conflicting with the global event-loop semantics we'd be probing.
 * That interaction is covered by the TCP integration suite
 * (test-sandboxed-worker.ts) which exercises the full close-and-rejectAll
 * path against a live server and now exits cleanly with code 0.
 */

import { describe, test, expect } from 'bun:test';
import { ClientClosedError } from '../src/client/tcp/client';

describe('ClientClosedError sentinel subclass', () => {
  test('is a proper Error subclass with stable name', () => {
    const err = new ClientClosedError();
    expect(err).toBeInstanceOf(Error);
    expect(err).toBeInstanceOf(ClientClosedError);
    expect(err.name).toBe('ClientClosedError');
    expect(err.message).toBe('Client closed');
  });

  test('custom message is preserved', () => {
    const err = new ClientClosedError('Pool shutting down');
    expect(err.message).toBe('Pool shutting down');
    expect(err).toBeInstanceOf(ClientClosedError);
  });

  test('instanceof discriminates from a plain Error("Client closed") — no string collision', () => {
    const real = new ClientClosedError();
    const collision = new Error('Client closed');

    expect(real instanceof ClientClosedError).toBe(true);
    expect(collision instanceof ClientClosedError).toBe(false);
    // The old filter used `error.message === 'Client closed'`, which would
    // match BOTH and swallow third-party rejections by accident. The new
    // filter checks `reason instanceof ClientClosedError` and is safe.
  });

  test('stack trace points to construction site', () => {
    const err = new ClientClosedError();
    expect(typeof err.stack).toBe('string');
    expect(err.stack).toContain('ClientClosedError');
  });
});
