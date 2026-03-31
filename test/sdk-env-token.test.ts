/**
 * SDK Client Environment Variable Token Resolution Tests
 *
 * Bug #42: BQ_TOKEN in .bunfig.toml doesn't work for SDK clients.
 *
 * The CLI reads BQ_TOKEN / BUNQUEUE_TOKEN from Bun.env, but the SDK
 * client (Queue, Worker) ignores environment variables entirely.
 * When a user sets BQ_TOKEN in .bunfig.toml [env] section, Bun makes
 * it available as Bun.env.BQ_TOKEN, but Queue/Worker never reads it.
 *
 * Expected priority: connection.token > BQ_TOKEN > BUNQUEUE_TOKEN
 */

import { describe, test, expect, afterEach, beforeEach } from 'bun:test';

describe('SDK client auto-reads BQ_TOKEN from environment', () => {
  const originalBqToken = Bun.env.BQ_TOKEN;
  const originalBunqueueToken = Bun.env.BUNQUEUE_TOKEN;
  const originalEmbedded = Bun.env.BUNQUEUE_EMBEDDED;

  beforeEach(() => {
    // Force TCP mode so we can inspect the pool's token
    Bun.env.BUNQUEUE_EMBEDDED = undefined;
  });

  afterEach(() => {
    if (originalBqToken !== undefined) {
      Bun.env.BQ_TOKEN = originalBqToken;
    } else {
      delete Bun.env.BQ_TOKEN;
    }
    if (originalBunqueueToken !== undefined) {
      Bun.env.BUNQUEUE_TOKEN = originalBunqueueToken;
    } else {
      delete Bun.env.BUNQUEUE_TOKEN;
    }
    if (originalEmbedded !== undefined) {
      Bun.env.BUNQUEUE_EMBEDDED = originalEmbedded;
    } else {
      delete Bun.env.BUNQUEUE_EMBEDDED;
    }
  });

  test('Queue resolves token from BQ_TOKEN env when no explicit token provided', async () => {
    delete Bun.env.BUNQUEUE_TOKEN;
    Bun.env.BQ_TOKEN = 'env-bq-token-42';

    // Import resolveToken helper (should be exported from client)
    const { resolveToken } = await import('../src/client/resolveToken');

    const token = resolveToken(undefined);
    expect(token).toBe('env-bq-token-42');
  });

  test('Queue resolves token from BUNQUEUE_TOKEN when no BQ_TOKEN', async () => {
    delete Bun.env.BQ_TOKEN;
    Bun.env.BUNQUEUE_TOKEN = 'env-bunqueue-token-42';

    const { resolveToken } = await import('../src/client/resolveToken');

    const token = resolveToken(undefined);
    expect(token).toBe('env-bunqueue-token-42');
  });

  test('explicit connection.token takes precedence over env vars', async () => {
    Bun.env.BQ_TOKEN = 'env-bq-token-42';
    Bun.env.BUNQUEUE_TOKEN = 'env-bunqueue-token-42';

    const { resolveToken } = await import('../src/client/resolveToken');

    const token = resolveToken('explicit-token');
    expect(token).toBe('explicit-token');
  });

  test('BQ_TOKEN takes precedence over BUNQUEUE_TOKEN', async () => {
    Bun.env.BQ_TOKEN = 'bq-wins';
    Bun.env.BUNQUEUE_TOKEN = 'bunqueue-loses';

    const { resolveToken } = await import('../src/client/resolveToken');

    const token = resolveToken(undefined);
    expect(token).toBe('bq-wins');
  });

  test('empty BQ_TOKEN falls through to BUNQUEUE_TOKEN', async () => {
    Bun.env.BQ_TOKEN = '';
    Bun.env.BUNQUEUE_TOKEN = 'fallback-token';

    const { resolveToken } = await import('../src/client/resolveToken');

    const token = resolveToken(undefined);
    expect(token).toBe('fallback-token');
  });

  test('no token when no explicit token and no env vars', async () => {
    delete Bun.env.BQ_TOKEN;
    delete Bun.env.BUNQUEUE_TOKEN;

    const { resolveToken } = await import('../src/client/resolveToken');

    const token = resolveToken(undefined);
    expect(token).toBeUndefined();
  });
});
