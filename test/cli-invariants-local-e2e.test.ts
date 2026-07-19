import { describe, expect, test } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import { createHttpServer } from '../src/infrastructure/server/http';
import { parseSingleJson, runCli, runCliRaw } from './cli-invariants/runtimeHarness';

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
      >;
      expect(version).toMatchObject({ ok: true, mismatch: false });
      expect(doctor).toMatchObject({ ok: true, reachable: true, issues: 0 });
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
    const tcpReservation = Bun.listen({
      hostname: '127.0.0.1',
      port: 0,
      socket: {
        data() {
          // Port reservation only.
        },
      },
    });
    const tcpPort = tcpReservation.port;
    tcpReservation.stop();
    const httpReservation = Bun.listen({
      hostname: '127.0.0.1',
      port: 0,
      socket: {
        data() {
          // Port reservation only.
        },
      },
    });
    const httpPort = httpReservation.port;
    httpReservation.stop();

    const proc = Bun.spawn(
      [
        'bun',
        'src/main.ts',
        'start',
        '--host',
        '127.0.0.1',
        '--tcp-port',
        String(tcpPort),
        '--http-port',
        String(httpPort),
      ],
      { cwd: `${import.meta.dir}/..`, stdout: 'pipe', stderr: 'pipe' }
    );
    const reader = proc.stdout.getReader();
    const decoder = new TextDecoder();
    let output = '';
    const deadline = Date.now() + 5000;
    while (!output.includes('TCP') && Date.now() < deadline) {
      const chunk = await Promise.race([
        reader.read(),
        Bun.sleep(100).then(() => ({ value: undefined, done: false })),
      ]);
      if (chunk.done) break;
      if (chunk.value) output += decoder.decode(chunk.value);
    }
    proc.kill();
    await proc.exited;
    expect(output).toContain('TCP');
  }, 10_000);
});
