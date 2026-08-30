import { describe, expect, test } from 'bun:test';
import { chmod, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import {
  runSelectedScenarios,
  scenarioNames,
  scenarioTimeoutMs,
  selectScenarioNames,
  type Scenario,
} from '../examples/postgres-multibroker/run';
import { settleCleanup, waitFor, withTimeout } from '../examples/postgres-multibroker/shared';
import { runTopologyExample, type HttpRequester } from '../examples/postgres-multibroker/topology';

const repositoryRoot = resolve(import.meta.dir, '..');

async function runExampleCli(selection: string) {
  const child = Bun.spawn([process.execPath, 'examples/postgres-multibroker/run.ts', selection], {
    cwd: repositoryRoot,
    stderr: 'pipe',
    stdout: 'pipe',
  });
  const [exitCode, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  return { exitCode, stderr, stdout };
}

function fakeRunners(calls: Scenario[]): Record<Scenario, () => void> {
  return Object.fromEntries(scenarioNames.map((name) => [name, () => calls.push(name)])) as Record<
    Scenario,
    () => void
  >;
}

describe('PostgreSQL multi-broker example runner', () => {
  test('rejects inherited and unknown scenario names through the real CLI', async () => {
    for (const selection of ['toString', 'constructor', '__proto__', 'missing']) {
      const result = await runExampleCli(selection);
      expect(result.exitCode).toBe(1);
      expect(result.stderr).toContain(`Unknown scenario "${selection}"`);
      expect(result.stdout).not.toContain('"status":"PASS"');
    }
  });

  test('selects every valid scenario and all in documented order', async () => {
    expect(scenarioNames).toEqual(['topology', 'multi-queue', 'reliability', 'flow']);
    for (const name of scenarioNames) expect(selectScenarioNames(name)).toEqual([name]);
    expect(selectScenarioNames('all')).toEqual(scenarioNames);

    const calls: Scenario[] = [];
    const reports: Scenario[] = [];
    await runSelectedScenarios('all', fakeRunners(calls), 100, ({ scenario }) => {
      reports.push(scenario);
    });
    expect(calls).toEqual(scenarioNames);
    expect(reports).toEqual(scenarioNames);
  });

  test('bounds a hung scenario and validates the configured deadline', async () => {
    const runners = fakeRunners([]);
    runners.topology = () => new Promise<void>(() => undefined);

    await expect(runSelectedScenarios('topology', runners, 20, () => undefined)).rejects.toThrow(
      'Timed out'
    );
    expect(scenarioTimeoutMs('1234')).toBe(1234);
    expect(() => scenarioTimeoutMs('0')).toThrow('must be a positive integer');
    expect(() => scenarioTimeoutMs('invalid')).toThrow('must be a positive integer');
  });
});

describe('PostgreSQL multi-broker example deadlines and cleanup', () => {
  test('runs every cleanup phase and aggregates sync, async, and non-Error failures', async () => {
    const order: string[] = [];
    const cleanup = settleCleanup(
      [
        () => {
          order.push('sync-failed');
          throw new Error('first cleanup failed');
        },
        async () => {
          await Bun.sleep(10);
          order.push('async-failed');
          throw 'second cleanup failed';
        },
      ],
      [() => order.push('phase-two')],
      [() => order.push('phase-three')]
    );

    const failure = await cleanup.catch((error: unknown) => error);
    expect(failure).toBeInstanceOf(AggregateError);
    expect((failure as AggregateError).errors.map((error) => (error as Error).message)).toEqual([
      'first cleanup failed',
      'second cleanup failed',
    ]);
    expect(order).toEqual(['sync-failed', 'async-failed', 'phase-two', 'phase-three']);
  });

  test('bounds a predicate that never settles', async () => {
    const startedAt = performance.now();
    await expect(
      waitFor('a hung predicate', () => new Promise<boolean>(() => undefined), 20)
    ).rejects.toThrow('Timed out');
    expect(performance.now() - startedAt).toBeLessThan(1_000);
  });

  test('withTimeout captures synchronous throws without waiting for its timer', async () => {
    await expect(
      withTimeout(
        'a synchronous operation',
        () => {
          throw new Error('synchronous failure');
        },
        1_000
      )
    ).rejects.toThrow('synchronous failure');
  });
});

describe('PostgreSQL multi-broker topology requests', () => {
  test('checks all broker endpoints with bounded requests and authenticated metrics', async () => {
    const calls: Array<{ init?: RequestInit; url: string }> = [];
    const request: HttpRequester = async (url, init) => {
      calls.push({ init, url });
      if (url.endsWith('/healthz')) return new Response('OK');
      if (url.endsWith('/ready')) return new Response('ready');
      const authenticated = new Headers(init?.headers).has('authorization');
      return authenticated
        ? new Response('# TYPE bunqueue_jobs gauge\nbunqueue_jobs 0\n')
        : new Response('unauthorized', { status: 401 });
    };

    await runTopologyExample(request, 100);

    expect(calls).toHaveLength(12);
    expect(calls.every(({ init }) => init?.signal instanceof AbortSignal)).toBe(true);
    expect(
      calls.filter(({ init }) => new Headers(init?.headers).has('authorization'))
    ).toHaveLength(3);
  });

  test('aborts an HTTP request that never settles', async () => {
    const request: HttpRequester = (_url, init) =>
      new Promise<Response>((_resolve, reject) => {
        init?.signal?.addEventListener('abort', () => reject(init.signal?.reason), { once: true });
      });

    await expect(runTopologyExample(request, 20)).rejects.toThrow();
  });
});

async function runVerifierWithFakeDocker(mode: string, project: string) {
  const directory = await mkdtemp(join(tmpdir(), 'bunqueue-example-verifier-'));
  const log = join(directory, 'docker.log');
  const docker = join(directory, 'docker');
  await writeFile(
    docker,
    `#!/bin/sh
printf '%s\\n' "$*" >> "$FAKE_DOCKER_LOG"
case "$FAKE_DOCKER_MODE:$*" in
  run-fail:*" run --rm --no-deps sdk-example topology") exit 8 ;;
  cleanup-fail:*" down --volumes --remove-orphans") exit 9 ;;
esac
exit 0
`
  );
  await chmod(docker, 0o755);

  try {
    const child = Bun.spawn(['sh', 'examples/postgres-multibroker/verify.sh'], {
      cwd: repositoryRoot,
      env: {
        ...process.env,
        BUNQUEUE_EXAMPLE_PROJECT: project,
        FAKE_DOCKER_LOG: log,
        FAKE_DOCKER_MODE: mode,
        PATH: `${directory}:${process.env.PATH ?? ''}`,
      },
      stderr: 'pipe',
      stdout: 'pipe',
    });
    const [exitCode, stderr] = await Promise.all([
      child.exited,
      new Response(child.stderr).text(),
      new Response(child.stdout).text(),
    ]);
    const calls = (await Bun.file(log).exists()) ? await readFile(log, 'utf8') : '';
    return { calls, exitCode, stderr };
  } finally {
    await rm(directory, { force: true, recursive: true });
  }
}

describe('PostgreSQL multi-broker verifier teardown', () => {
  test('preserves a scenario failure and still attempts both cleanup phases', async () => {
    const result = await runVerifierWithFakeDocker('run-fail', 'bunqueue-pg-example-test-failure');
    expect(result.exitCode).toBe(8);
    expect(result.calls.match(/ down --volumes --remove-orphans/g)).toHaveLength(2);
    expect(result.calls).toContain('down --volumes --remove-orphans --rmi local');
  });

  test('attempts image cleanup when resource cleanup fails', async () => {
    const result = await runVerifierWithFakeDocker(
      'cleanup-fail',
      'bunqueue-pg-example-test-cleanup'
    );
    expect(result.exitCode).toBe(1);
    expect(result.stderr).toContain('Example cleanup failed');
    expect(result.calls.match(/ down --volumes --remove-orphans/g)).toHaveLength(2);
    expect(result.calls).toContain('down --volumes --remove-orphans --rmi local');
  });

  test('rejects a destructive project override before calling Docker', async () => {
    const result = await runVerifierWithFakeDocker('pass', 'production');
    expect(result.exitCode).toBe(2);
    expect(result.calls).toBe('');
    expect(result.stderr).toContain('must start with bunqueue-pg-example-');
  });
});
