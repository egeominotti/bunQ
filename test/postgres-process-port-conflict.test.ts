import { describe, expect, test } from 'bun:test';
import { isBindCollision } from './support/bind-collision';

describe('broker bind collisions', () => {
  test('classifies the HTTP listener failure seen in CI', () => {
    // Verbatim from the PostgreSQL 15 job of CI run 33174590181.
    expect(
      isBindCollision('Failed to start server: Failed to start server. Is port 38018 in use?')
    ).toBe(true);
  });

  test('classifies the TCP listener failure, which words it differently', () => {
    // `Bun.listen` does not name the port, so matching only the HTTP wording
    // would miss every conflict on the base port of a reserved pair.
    expect(isBindCollision('Failed to start server: Failed to listen at 127.0.0.1')).toBe(true);
  });

  test('classifies the underlying socket errors', () => {
    expect(isBindCollision('listen EADDRINUSE: address already in use')).toBe(true);
    expect(isBindCollision('bind: Address already in use')).toBe(true);
  });

  test('does not classify unrelated startup failures', () => {
    expect(isBindCollision('Failed to initialize storage: too many clients')).toBe(false);
    expect(isBindCollision('PostgreSQL schema version 19 is newer')).toBe(false);
    expect(isBindCollision('broker exited with 1')).toBe(false);
  });
});
