/**
 * CLI Environment Variable Token Resolution Tests
 *
 * Enhancement #13: Allow setting auth token via environment variables.
 *
 * Priority order:
 *   1. --token CLI flag (highest)
 *   2. BQ_TOKEN environment variable
 *   3. BUNQUEUE_TOKEN environment variable
 */

import { describe, test, expect, afterEach } from 'bun:test';

describe('CLI auth token from environment variables', () => {
  const originalArgv = process.argv;
  const originalBqToken = Bun.env.BQ_TOKEN;
  const originalBunqueueToken = Bun.env.BUNQUEUE_TOKEN;

  afterEach(() => {
    process.argv = originalArgv;
    // Restore original env vars
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
  });

  test('uses BQ_TOKEN when no --token flag is provided', async () => {
    const { parseGlobalOptions } = await import('../src/cli/index');

    delete Bun.env.BUNQUEUE_TOKEN;
    Bun.env.BQ_TOKEN = 'env-token-bq';
    process.argv = ['bun', 'script', 'stats'];

    const { options } = parseGlobalOptions();
    expect(options.token).toBe('env-token-bq');
  });

  test('uses BUNQUEUE_TOKEN when no --token flag and no BQ_TOKEN', async () => {
    const { parseGlobalOptions } = await import('../src/cli/index');

    delete Bun.env.BQ_TOKEN;
    Bun.env.BUNQUEUE_TOKEN = 'env-token-bunqueue';
    process.argv = ['bun', 'script', 'stats'];

    const { options } = parseGlobalOptions();
    expect(options.token).toBe('env-token-bunqueue');
  });

  test('--token flag takes precedence over BQ_TOKEN', async () => {
    const { parseGlobalOptions } = await import('../src/cli/index');

    Bun.env.BQ_TOKEN = 'env-token-bq';
    Bun.env.BUNQUEUE_TOKEN = 'env-token-bunqueue';
    process.argv = ['bun', 'script', '--token', 'cli-token', 'stats'];

    const { options } = parseGlobalOptions();
    expect(options.token).toBe('cli-token');
  });

  test('--token= flag takes precedence over env vars', async () => {
    const { parseGlobalOptions } = await import('../src/cli/index');

    Bun.env.BQ_TOKEN = 'env-token-bq';
    process.argv = ['bun', 'script', '--token=cli-token-eq', 'stats'];

    const { options } = parseGlobalOptions();
    expect(options.token).toBe('cli-token-eq');
  });

  test('BQ_TOKEN takes precedence over BUNQUEUE_TOKEN', async () => {
    const { parseGlobalOptions } = await import('../src/cli/index');

    Bun.env.BQ_TOKEN = 'bq-wins';
    Bun.env.BUNQUEUE_TOKEN = 'bunqueue-loses';
    process.argv = ['bun', 'script', 'stats'];

    const { options } = parseGlobalOptions();
    expect(options.token).toBe('bq-wins');
  });

  test('no token when no --token flag and no env vars set', async () => {
    const { parseGlobalOptions } = await import('../src/cli/index');

    delete Bun.env.BQ_TOKEN;
    delete Bun.env.BUNQUEUE_TOKEN;
    process.argv = ['bun', 'script', 'stats'];

    const { options } = parseGlobalOptions();
    expect(options.token).toBeUndefined();
  });

  test('empty BQ_TOKEN is ignored, falls through to BUNQUEUE_TOKEN', async () => {
    const { parseGlobalOptions } = await import('../src/cli/index');

    Bun.env.BQ_TOKEN = '';
    Bun.env.BUNQUEUE_TOKEN = 'fallback-token';
    process.argv = ['bun', 'script', 'stats'];

    const { options } = parseGlobalOptions();
    expect(options.token).toBe('fallback-token');
  });

  test('empty BQ_TOKEN and empty BUNQUEUE_TOKEN result in no token', async () => {
    const { parseGlobalOptions } = await import('../src/cli/index');

    Bun.env.BQ_TOKEN = '';
    Bun.env.BUNQUEUE_TOKEN = '';
    process.argv = ['bun', 'script', 'stats'];

    const { options } = parseGlobalOptions();
    expect(options.token).toBeUndefined();
  });
});
