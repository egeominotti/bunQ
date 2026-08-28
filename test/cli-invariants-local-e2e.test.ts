import { describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { type HealthData, evaluateDoctor } from '../src/cli/commands/doctor';
import { createHttpServer } from '../src/infrastructure/server/http';
import { parseSingleJson, runCli, runCliRaw } from './cli-invariants/runtimeHarness';
import { isBindCollision } from './support/bind-collision';

function reservePort(): number {
  const reservation = Bun.listen({
    hostname: '127.0.0.1',
    port: 0,
    socket: {
      data() {
        // Port reservation only.
      },
    },
  });
  const { port } = reservation;
  reservation.stop();
  return port;
}

async function readUntilListening(stream: ReadableStream<Uint8Array>): Promise<string> {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  let output = '';
  const deadline = Date.now() + 20_000;
  while (!output.includes('TCP') && Date.now() < deadline) {
    const chunk = await Promise.race([
      reader.read(),
      Bun.sleep(100).then(() => ({ value: undefined, done: false })),
    ]);
    if (chunk.done) break;
    if (chunk.value) output += decoder.decode(chunk.value);
  }
  return output;
}

async function bootServerOnce(): Promise<{ output: string; failure: string }> {
  const proc = Bun.spawn(
    [
      'bun',
      'src/main.ts',
      'start',
      '--host',
      '127.0.0.1',
      '--tcp-port',
      String(reservePort()),
      '--http-port',
      String(reservePort()),
    ],
    { cwd: `${import.meta.dir}/..`, stdout: 'pipe', stderr: 'pipe' }
  );
  const output = await readUntilListening(proc.stdout);
  proc.kill();
  await proc.exited;
  const stderr = await new Response(proc.stderr).text();
  return { output, failure: `${output}${stderr}`.trim() || 'the process produced no output' };
}

describe('complete local CLI functionality over the real executable', () => {
  test('--json emits one JSON document for every terminating local command', async () => {
    const cases = [
      ['--help', '--json'],
      ['--version', '--json'],
      ['start', '--help', '--json'],
      ['push', '--help', '--json'],
      ['cron', '--help', '--json'],
      ['version', '--json', '--port', '1'],
      ['doctor', '--json', '--port', '1'],
    ];

    for (const args of cases) {
      const result = await runCliRaw(args);
      expect(result.timedOut).toBe(false);
      expect(typeof parseSingleJson(result)).toBe('object');
      expect(result.stdout + result.stderr).not.toContain('\u001B');
    }
  });

  test('global help and version flags terminate successfully', async () => {
    const help = await runCliRaw(['--help']);
    expect(help.timedOut).toBe(false);
    expect(help.exitCode).toBe(0);
    expect(help.stdout).toContain('CORE COMMANDS');

    const version = await runCliRaw(['--version']);
    expect(version.timedOut).toBe(false);
    expect(version.exitCode).toBe(0);
    expect(version.stdout).toMatch(/bunqueue v\d+/);
  });

  test('version and doctor commands terminate deterministically', async () => {
    const version = await runCli(['version'], 1);
    expect(version.timedOut).toBe(false);
    expect(version.exitCode).toBe(0);
    expect(version.stdout).toContain('Client: bunqueue');
    expect(version.stdout).toContain('Server: not reachable');

    const doctor = await runCli(['doctor'], 1);
    expect(doctor.timedOut).toBe(false);
    expect(doctor.exitCode).toBe(1);
    expect(doctor.stdout).toContain('Cannot continue without server connection');
  });

  test('version and doctor consume the real HTTP health contract', async () => {
    const manager = new QueueManager();
    const http = createHttpServer(manager, { port: 0, hostname: '127.0.0.1' });
    try {
      const httpPort = Number(new URL(http.server.url).port);
      const tcpPort = httpPort - 1;
      const version = parseSingleJson(await runCli(['version', '--json'], tcpPort)) as Record<
        string,
        unknown
      >;
      const doctor = parseSingleJson(await runCli(['doctor', '--json'], tcpPort)) as Record<
        string,
        unknown
      > & { endpoint: string; health: HealthData; issues: number };
      expect(version).toMatchObject({ ok: true, mismatch: false });
      expect(doctor).toMatchObject({ reachable: true, fatal: false });

      // What this case is for: the CLI reached the REAL server over HTTP, on the port it
      // derived itself, and parsed the real payload.
      expect(doctor.endpoint).toContain(String(httpPort));
      expect(doctor.health.status).toBe('healthy');

      // And its verdict is the pure evaluator's verdict over the payload it reported.
      //
      // This used to assert a flat `issues: 0`, which asserted something else entirely.
      // The evaluator counts an issue at RSS above 512MB, so that number depended on the
      // memory footprint of whatever process ran the test. Run alone it was small and the
      // case passed; inside a full `bun test` run of 6000+ tests sharing one process it
      // was not, and the case failed on the suite's memory rather than on the CLI. An
      // oracle against the same payload has no such coupling, and unlike a re-implemented
      // count here it cannot drift from the evaluator it is checking. The thresholds
      // themselves are covered where they belong, over the pure function, in
      // `cli-doctor-logic.test.ts`.
      expect(doctor.issues).toBe(
        evaluateDoctor({ kind: 'ok', endpoint: doctor.endpoint, health: doctor.health }).issues
      );
    } finally {
      http.stop();
      manager.shutdown();
    }
  });

  test('every backup subcommand uses the local dispatcher and returns JSON', async () => {
    for (const subcommand of ['now', 'create', 'list', 'restore', 'status']) {
      const args =
        subcommand === 'restore'
          ? ['backup', subcommand, 'backup-key', '--force', '--json']
          : ['backup', subcommand, '--json'];
      const result = await runCliRaw(args);
      expect(result.timedOut).toBe(false);
      expect(result.exitCode).toBe(1);
      const parsed = parseSingleJson(result) as { success: boolean; message: string };
      expect(parsed.success).toBe(false);
      expect(parsed.message).toContain('BUNQUEUE_DATA_PATH');
    }
  });

  test('start boots a server and can be interrupted cleanly', async () => {
    // Reserving a port closes the probe socket before the server binds it, so a
    // concurrent worker can steal the pair. Retry a confirmed bind collision and
    // keep both streams, otherwise a lost race reports an empty stdout instead
    // of the real reason.
    let lastFailure = '';
    for (let attempt = 0; attempt < 5; attempt++) {
      const { output, failure } = await bootServerOnce();
      if (output.includes('TCP')) return;
      lastFailure = failure;
      if (!isBindCollision(failure)) break;
    }
    throw new Error(`bunqueue start never announced its TCP listener: ${lastFailure}`);
  }, 120_000);
});
