import { afterEach, describe, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { cleanupSdkResources } from '../scripts/sdk-sandbox-cleanup';
import { runSdkSuitesSettled } from '../scripts/sdk-sandbox-orchestration';
import { startSdkPostgresInfrastructure } from '../scripts/sdk-sandbox-postgres';

type SpawnResult = ReturnType<typeof Bun.spawnSync>;

const originalSpawnSync = Bun.spawnSync;

function spawnResult(exitCode: number, stderr = ''): SpawnResult {
  return {
    exitCode,
    stderr: Buffer.from(stderr),
    stdout: Buffer.from(''),
  } as SpawnResult;
}

function replaceSpawnSync(handler: (command: string[]) => SpawnResult): void {
  Bun.spawnSync = ((command: string[]) => handler(command)) as typeof Bun.spawnSync;
}

afterEach(() => {
  Bun.spawnSync = originalSpawnSync;
});

describe('SDK PostgreSQL sandbox lifecycle', () => {
  test('failed teardown reports every Docker error and remains retryable', async () => {
    const calls: string[][] = [];
    let cleanupFails = true;
    replaceSpawnSync((command) => {
      calls.push(command);
      if (command[1] === 'rm')
        return spawnResult(cleanupFails ? 17 : 0, 'container removal failed');
      if (command[1] === 'network' && command[2] === 'rm')
        return spawnResult(cleanupFails ? 18 : 0, 'network removal failed');
      return spawnResult(0);
    });

    const infrastructure = await startSdkPostgresInfrastructure('/workspace');
    expect(() => infrastructure.stop()).toThrow('PostgreSQL sandbox cleanup failed');

    cleanupFails = false;
    expect(() => infrastructure.stop()).not.toThrow();
    const callsAfterCleanup = calls.length;
    expect(() => infrastructure.stop()).not.toThrow();
    expect(calls).toHaveLength(callsAfterCleanup);

    expect(calls.filter((command) => command[1] === 'rm')).toHaveLength(2);
    expect(calls.filter((command) => command[1] === 'network' && command[2] === 'rm')).toHaveLength(
      2
    );
  });

  test('startup failure aggregates cleanup errors for partially created resources', async () => {
    const calls: string[][] = [];
    replaceSpawnSync((command) => {
      calls.push(command);
      if (command[1] === 'start') return spawnResult(42, 'container start failed');
      if (command[1] === 'rm') return spawnResult(17, 'container removal failed');
      if (command[1] === 'network' && command[2] === 'rm')
        return spawnResult(18, 'network removal failed');
      return spawnResult(0);
    });

    await expect(startSdkPostgresInfrastructure('/workspace')).rejects.toThrow(
      'PostgreSQL sandbox startup and cleanup failed'
    );
    expect(calls.some((command) => command[1] === 'rm')).toBe(true);
    expect(calls.some((command) => command[1] === 'network' && command[2] === 'rm')).toBe(true);
  });

  test('a failed create never removes a container whose name was already owned', async () => {
    const calls: string[][] = [];
    replaceSpawnSync((command) => {
      calls.push(command);
      if (command[1] === 'create') return spawnResult(42, 'container name is already in use');
      if (command[1] === 'rm')
        return spawnResult(1, 'Error response from daemon: No such container: partial');
      return spawnResult(0);
    });

    await expect(startSdkPostgresInfrastructure('/workspace')).rejects.toThrow(
      'creating PostgreSQL container'
    );
    expect(calls.some((command) => command[1] === 'rm')).toBe(false);
  });

  test('the real SDK runner uses a settled orchestration cleanup boundary', () => {
    const source = readFileSync(new URL('../scripts/test-sdk-sandbox.ts', import.meta.url), 'utf8');
    expect(source).toContain('runSdkSuitesSettled');
  });

  test('a suite teardown failure waits for peers before cleaning every owned resource', async () => {
    const active = new Set(['sdk-failed', 'sdk-peer']);
    let peerFinished = false;
    let postgresStops = 0;
    replaceSpawnSync(() => spawnResult(0));

    const settled = await runSdkSuitesSettled(
      ['failed', 'peer'],
      async (name) => {
        if (name === 'failed') throw new Error('suite container removal failed');
        await Bun.sleep(10);
        peerFinished = true;
        return name;
      },
      false
    );

    expect(peerFinished).toBe(true);
    expect(settled.results).toEqual(['peer']);
    expect(settled.errors.map((error) => error.message)).toEqual([
      'suite container removal failed',
    ]);
    cleanupSdkResources(
      active,
      {
        container: 'postgres',
        network: 'network',
        stop() {
          postgresStops++;
        },
        url: 'postgres://postgres',
      },
      '/workspace'
    );
    expect(active.size).toBe(0);
    expect(postgresStops).toBe(1);
  });

  test('outer cleanup attempts every owned resource and retries only failures', () => {
    const active = new Set(['sdk-a', 'sdk-b']);
    let cleanupFails = true;
    let postgresStops = 0;
    replaceSpawnSync((command) => {
      if (command.at(-1) === 'sdk-a' && cleanupFails)
        return spawnResult(17, 'sdk-a removal failed');
      return spawnResult(0);
    });
    const postgres = {
      container: 'postgres',
      network: 'network',
      stop() {
        postgresStops++;
        if (cleanupFails) throw new Error('postgres removal failed');
      },
      url: 'postgres://postgres',
    };

    expect(() => cleanupSdkResources(active, postgres, '/workspace')).toThrow(
      'SDK sandbox cleanup failed'
    );
    expect([...active]).toEqual(['sdk-a']);
    expect(postgresStops).toBe(1);

    cleanupFails = false;
    expect(() => cleanupSdkResources(active, postgres, '/workspace')).not.toThrow();
    expect(active.size).toBe(0);
    expect(postgresStops).toBe(2);
  });
});
