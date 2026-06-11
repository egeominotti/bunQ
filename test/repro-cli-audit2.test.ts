/**
 * CLI audit 2026-06-10 — repro tests for the top findings (2 CRITICAL + 4 ALTA).
 *
 *  #1 CRITICAL main.ts dispatch: version/doctor/ping/typos fall through to
 *     startServer() — a typo (`bunqueue stast`) boots a full server (Docker entry).
 *  #2 CRITICAL `cron add --max-limit 0`: help says "0 = unlimited", server's
 *     isAtLimit treats 0 as "never run". CLI must translate 0 → omit (unlimited).
 *  #3 ALTA global `-t` (token) steals `-t` (timeout) from `pull` / `job wait`.
 *  #4 ALTA webhook VALID_EVENTS lists never-emitted events (job.active/waiting/
 *     delayed) and rejects the actually-emitted job.pushed/job.started.
 *  #5 ALTA `bunqueue backup` ignores BUNQUEUE_DATA_PATH/BQ_DATA_PATH.
 *  #6 ALTA CLI command timeout hardcoded 30s — kills long-poll pull/wait.
 */

import { describe, test, expect } from 'bun:test';
import { join } from 'node:path';
import { parseGlobalOptions } from '../src/cli/index';
import { buildCronCommand } from '../src/cli/commands/cron';
import { buildWebhookCommand } from '../src/cli/commands/webhook';
import { executeBackupCommand } from '../src/cli/commands/backup';

const REPO = join(import.meta.dir, '..');
const SERVER_BANNER = '(\\(\\'; // the rabbit — printed only when the server boots

/** Run `bun src/main.ts <args>`; kill after timeoutMs. */
async function runMain(
  args: string[],
  timeoutMs = 8000
): Promise<{ timedOut: boolean; exitCode: number | null; output: string }> {
  const proc = Bun.spawn(['bun', 'src/main.ts', ...args], {
    cwd: REPO,
    env: {
      ...process.env,
      TCP_PORT: '18941',
      HTTP_PORT: '18942',
      BUNQUEUE_DATA_PATH: '/tmp/bq-cli-audit2-dispatch.db',
    },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    proc.kill();
  }, timeoutMs);
  const exitCode = await proc.exited;
  clearTimeout(timer);
  const output =
    (await new Response(proc.stdout).text()) + (await new Response(proc.stderr).text());
  return { timedOut, exitCode: timedOut ? null : exitCode, output };
}

function parseWith(argv: string[]): { options: Record<string, unknown>; commandArgs: string[] } {
  const prev = process.argv;
  process.argv = ['bun', 'cli', ...argv];
  try {
    const { options, commandArgs } = parseGlobalOptions();
    return { options: options as unknown as Record<string, unknown>, commandArgs };
  } finally {
    process.argv = prev;
  }
}

describe('#1 main.ts dispatch never falls through to the server', () => {
  test('unknown command (typo) errors out instead of booting a server', async () => {
    const r = await runMain(['stast']);
    expect(r.timedOut).toBe(false); // currently: server boots and never exits
    expect(r.output).not.toContain(SERVER_BANNER);
    expect(r.exitCode).not.toBe(0);
    expect(r.output.toLowerCase()).toContain('unknown command');
  }, 15000);

  test('`version` routes to the CLI, not the server', async () => {
    const r = await runMain(['version']);
    expect(r.timedOut).toBe(false);
    expect(r.output).not.toContain(SERVER_BANNER);
    expect(r.output).toContain('Client: bunqueue');
  }, 15000);

  test('`doctor` routes to the CLI, not the server', async () => {
    const r = await runMain(['doctor']);
    expect(r.timedOut).toBe(false);
    expect(r.output).not.toContain(SERVER_BANNER);
  }, 15000);

  test('`ping` routes to the CLI, not the server', async () => {
    const r = await runMain(['ping']);
    expect(r.timedOut).toBe(false);
    expect(r.output).not.toContain(SERVER_BANNER);
  }, 15000);
});

describe('#2 cron add --max-limit 0 means unlimited', () => {
  test('--max-limit 0 is omitted from the Cron command (server: null = unlimited)', () => {
    const cmd = buildCronCommand([
      'add', 'job1', '-q', 'q1', '-d', '{}', '-s', '* * * * *', '--max-limit', '0',
    ]);
    expect(cmd.maxLimit).toBeUndefined();
  });

  test('--max-limit 5 still passes through', () => {
    const cmd = buildCronCommand([
      'add', 'job1', '-q', 'q1', '-d', '{}', '-s', '* * * * *', '--max-limit', '5',
    ]);
    expect(cmd.maxLimit).toBe(5);
  });
});

describe('#3 global -t does not steal pull/job-wait timeout', () => {
  test('pull q -t 5000: -t passes through to the subcommand', () => {
    const { options, commandArgs } = parseWith(['pull', 'q', '-t', '5000']);
    expect(options.token).toBeUndefined();
    expect(commandArgs).toEqual(['pull', 'q', '-t', '5000']);
  });

  test('job wait id -t 9000: -t passes through', () => {
    const { options, commandArgs } = parseWith(['job', 'wait', '123', '-t', '9000']);
    expect(options.token).toBeUndefined();
    expect(commandArgs).toEqual(['job', 'wait', '123', '-t', '9000']);
  });

  test('-t before the command is still the global token', () => {
    const { options, commandArgs } = parseWith(['-t', 'mytok', 'stats']);
    expect(options.token).toBe('mytok');
    expect(commandArgs).toEqual(['stats']);
  });

  test('commands without their own -t keep the global behavior', () => {
    const { options } = parseWith(['stats', '-t', 'mytok']);
    expect(options.token).toBe('mytok');
  });

  test('job subcommands other than wait keep -t as the global token', () => {
    const { options, commandArgs } = parseWith(['job', 'get', '123', '-t', 'SECRET']);
    expect(options.token).toBe('SECRET');
    expect(commandArgs).toEqual(['job', 'get', '123']);
  });
});

describe('global value flags do not swallow a following flag', () => {
  test('--token --json: token ignored with warning, --json still parsed', () => {
    const { options } = parseWith(['stats', '--token', '--json']);
    expect(options.token).toBeUndefined();
    expect(options.json).toBe(true);
  });

  test('-H --json and -p --json keep --json working', () => {
    const h = parseWith(['stats', '-H', '--json']);
    expect(h.options.host).toBe('localhost');
    expect(h.options.json).toBe(true);
    const p = parseWith(['stats', '-p', '--json']);
    expect(p.options.port).toBe(6789);
    expect(p.options.json).toBe(true);
  });
});

describe('builder validations (audit pass 2)', () => {
  test('cron add --every rejects non-positive intervals', () => {
    for (const bad of ['0', '-60000']) {
      expect(() =>
        buildCronCommand(['add', 'j', '-q', 'q1', '-d', '{}', '-e', bad])
      ).toThrow(/every/i);
    }
  });

  test('job wait rejects negative timeout', async () => {
    const { buildJobCommand } = await import('../src/cli/commands/job');
    expect(() => buildJobCommand(['wait', '123', '--timeout', '-5'])).toThrow(/timeout/i);
  });
});

describe('#4 webhook events match what the server actually emits', () => {
  test('accepts the five triggered events (incl. job.progress via updateProgress)', () => {
    const cmd = buildWebhookCommand([
      'add', 'http://example.com/hook',
      '-e', 'job.pushed,job.started,job.completed,job.failed,job.progress',
    ]);
    expect(cmd.cmd).toBe('AddWebhook');
    expect(cmd.events).toEqual([
      'job.pushed', 'job.started', 'job.completed', 'job.failed', 'job.progress',
    ]);
  });

  for (const dead of ['job.active', 'job.waiting', 'job.delayed']) {
    test(`rejects non-existent event ${dead}`, () => {
      expect(() =>
        buildWebhookCommand(['add', 'http://example.com/hook', '-e', dead])
      ).toThrow(/invalid event/i);
    });
  }
});

describe('#5 backup honors the canonical data-path env priority', () => {
  test('BUNQUEUE_DATA_PATH is recognized', async () => {
    const prev = {
      b: Bun.env.BUNQUEUE_DATA_PATH,
      q: Bun.env.BQ_DATA_PATH,
      d: Bun.env.DATA_PATH,
      s: Bun.env.SQLITE_PATH,
    };
    delete Bun.env.DATA_PATH;
    delete Bun.env.SQLITE_PATH;
    delete Bun.env.BQ_DATA_PATH;
    Bun.env.BUNQUEUE_DATA_PATH = '/tmp/bq-audit2-backup.db';
    try {
      const res = await executeBackupCommand(['status']);
      // It may fail on S3 config — but it must NOT claim the path is unset
      expect(res.message).not.toContain('DATA_PATH not set');
    } finally {
      if (prev.b === undefined) delete Bun.env.BUNQUEUE_DATA_PATH;
      else Bun.env.BUNQUEUE_DATA_PATH = prev.b;
      if (prev.q !== undefined) Bun.env.BQ_DATA_PATH = prev.q;
      if (prev.d !== undefined) Bun.env.DATA_PATH = prev.d;
      if (prev.s !== undefined) Bun.env.SQLITE_PATH = prev.s;
    }
  });
});

describe('#6 client timeout scales with long-poll commands', () => {
  test('clientTimeoutFor: base 30s, long-poll = command timeout + buffer', async () => {
    const mod = (await import('../src/cli/client')) as unknown as Record<string, unknown>;
    const fn = mod.clientTimeoutFor as ((cmd: Record<string, unknown>) => number) | undefined;
    expect(typeof fn).toBe('function');
    expect(fn?.({ cmd: 'Stats' })).toBe(30000);
    expect(fn?.({ cmd: 'PULL', timeout: 1000 })).toBe(30000); // small stays at base
    expect(fn?.({ cmd: 'PULL', timeout: 30000 })).toBeGreaterThanOrEqual(40000);
    expect(fn?.({ cmd: 'WaitJob', timeout: 60000 })).toBeGreaterThanOrEqual(70000);
    // On PUSH `timeout` is the JOB EXECUTION timeout — it must not stretch
    // the client wait (a dead connection would hang for the job's lifetime).
    expect(fn?.({ cmd: 'PUSH', timeout: 86400000 })).toBe(30000);
  });
});
