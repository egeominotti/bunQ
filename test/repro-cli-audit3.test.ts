/**
 * CLI audit pass 3 — remaining backlog, cross-layer.
 *
 *  #1 Short -h/-v after a command no longer suppress execution (typo of -H
 *     printed help and exited 0: false success in scripts). Long --help /
 *     --version stay global; --help after a command shows command help.
 *  #2 `--` separator: everything after it is opaque to the GLOBAL parser.
 *  #3 Attached short flags that shadow global letters warn instead of
 *     silently dropping data (push q '{}' -p10 → priority lost, job pushed).
 *  #4 Domain: cron maxLimit 0/negative → null (unlimited) at the single choke
 *     point createCronJob — fixes HTTP/TCP/MCP surfaces at once.
 *  #5 Server validates webhook events against the canonical live list
 *     (single source: WEBHOOK_EVENTS in domain) — HTTP/TCP/MCP aligned.
 *  #6 WaitJob server-side caps timeout (PULL already does) — no multi-day
 *     client hangs on a half-open connection.
 *  #7 Formatters stop dropping operational data: worker status (stale!),
 *     webhook enabled/counters, cron nextRun, stats uptime, webhookId on add.
 *  #8 `job state` of a missing job exits 1 (was: "State: unknown", exit 0).
 *  #9 `bunqueue start` boots the same FULL server as bare `bunqueue`
 *     (S3 backup, stats interval, crash handlers) — entry points unified.
 */

import { describe, test, expect, spyOn, beforeAll, afterAll } from 'bun:test';
import { join } from 'node:path';
import { parseGlobalOptions } from '../src/cli/index';
import { formatOutput } from '../src/cli/output';
import { createCronJob, isAtLimit } from '../src/domain/types/cron';

const LIVE_EVENTS = ['job.pushed', 'job.started', 'job.completed', 'job.failed', 'job.progress'];

async function loadCanonicalEvents(): Promise<readonly string[] | undefined> {
  const mod = (await import('../src/domain/types/webhook')) as Record<string, unknown>;
  return mod.WEBHOOK_EVENTS as readonly string[] | undefined;
}
import { handleCommand } from '../src/infrastructure/server/handler';
import type { HandlerContext } from '../src/infrastructure/server/handler';
import { QueueManager } from '../src/application/queueManager';
import { createTcpServer, type TcpServer } from '../src/infrastructure/server/tcp';

const REPO = join(import.meta.dir, '..');

let qm: QueueManager;
let server: TcpServer;
let ctx: HandlerContext;
const E2E_PORT = 18951;

beforeAll(() => {
  qm = new QueueManager();
  server = createTcpServer(qm, { port: E2E_PORT, hostname: '127.0.0.1' });
  ctx = { queueManager: qm, authTokens: new Set(), authenticated: true };
});

afterAll(() => {
  server.stop();
  qm.shutdown();
});

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

async function runCli(
  args: string[],
  timeoutMs = 10000
): Promise<{ exitCode: number | null; output: string }> {
  const proc = Bun.spawn(['bun', 'src/main.ts', ...args], {
    cwd: REPO,
    env: { ...process.env },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const timer = setTimeout(() => proc.kill(), timeoutMs);
  const exitCode = await proc.exited;
  clearTimeout(timer);
  const output =
    (await new Response(proc.stdout).text()) + (await new Response(proc.stderr).text());
  return { exitCode, output };
}

describe('#1 short -h/-v are positional-gated', () => {
  test('push q data -h host: -h passes through, command would execute', () => {
    const { options, commandArgs } = parseWith(['push', 'q', '{"a":1}', '-h', 'myhost']);
    expect(options.help).toBe(false);
    expect(commandArgs).toEqual(['push', 'q', '{"a":1}', '-h', 'myhost']);
  });

  test('-h before any command is still global help', () => {
    const { options } = parseWith(['-h']);
    expect(options.help).toBe(true);
  });

  test('-v after a command passes through; --version stays global', () => {
    const v = parseWith(['stats', '-v']);
    expect(v.options.version).toBe(false);
    const long = parseWith(['stats', '--version']);
    expect(long.options.version).toBe(true);
  });

  test('--help stays global anywhere', () => {
    const { options } = parseWith(['push', 'q', '{}', '--help']);
    expect(options.help).toBe(true);
  });
});

describe('#2 -- separator stops global parsing', () => {
  test('flags after -- are never consumed as global', () => {
    const { options, commandArgs } = parseWith(['job', 'log', '5', '--', '--json']);
    expect(options.json).toBe(false);
    expect(commandArgs).toEqual(['job', 'log', '5', '--json']);
  });

  test('-- as first arg passes everything through', () => {
    const { options, commandArgs } = parseWith(['--', 'stats', '--json', '-t', 'x']);
    expect(options.json).toBe(false);
    expect(options.token).toBeUndefined();
    expect(commandArgs).toEqual(['stats', '--json', '-t', 'x']);
  });
});

describe('#3 attached short flags warn instead of silent data loss', () => {
  test('push -p10 warns (priority would be silently dropped)', () => {
    const warnSpy = spyOn(console, 'warn').mockImplementation(() => {});
    try {
      const { commandArgs } = parseWith(['push', 'q', '{"a":1}', '-p10']);
      expect(commandArgs).toContain('-p10'); // still passed through
      expect(warnSpy).toHaveBeenCalled();
      const msg = String(warnSpy.mock.calls.map((c) => c.join(' ')).join('\n'));
      expect(msg).toContain('-p10');
    } finally {
      warnSpy.mockRestore();
    }
  });

  test('pull -t5000 does NOT warn (pull owns -t, attached form works)', () => {
    const warnSpy = spyOn(console, 'warn').mockImplementation(() => {});
    try {
      parseWith(['pull', 'q', '-t5000']);
      expect(warnSpy).not.toHaveBeenCalled();
    } finally {
      warnSpy.mockRestore();
    }
  });
});

describe('#4 domain: cron maxLimit 0/negative → unlimited (all surfaces)', () => {
  const base = { name: 'c1', queue: 'q1', data: {}, schedule: '* * * * *' };

  test('maxLimit 0 stores null (unlimited), cron can run', () => {
    const cron = createCronJob({ ...base, maxLimit: 0 });
    expect(cron.maxLimit).toBeNull();
    expect(isAtLimit(cron)).toBe(false);
  });

  test('negative maxLimit stores null too', () => {
    const cron = createCronJob({ ...base, maxLimit: -5 });
    expect(cron.maxLimit).toBeNull();
    expect(isAtLimit(cron)).toBe(false);
  });

  test('positive maxLimit still enforced', () => {
    const cron = createCronJob({ ...base, maxLimit: 2 });
    expect(cron.maxLimit).toBe(2);
  });
});

describe('#5 server validates webhook events (canonical list)', () => {
  test('canonical list is exactly the five live events', async () => {
    const events = await loadCanonicalEvents();
    expect(events).toBeDefined();
    expect([...(events ?? [])].sort()).toEqual([...LIVE_EVENTS].sort());
  });

  test('AddWebhook rejects unknown events', async () => {
    const resp = await handleCommand(
      {
        cmd: 'AddWebhook',
        url: 'http://example.com/hook',
        events: ['job.active'],
      } as never,
      ctx
    );
    expect(resp.ok).toBe(false);
    expect(String((resp as { error?: string }).error)).toMatch(/event/i);
  });

  test('AddWebhook accepts the live events', async () => {
    const resp = await handleCommand(
      {
        cmd: 'AddWebhook',
        url: 'http://example.com/hook2',
        events: [...LIVE_EVENTS],
      } as never,
      ctx
    );
    expect(resp.ok).toBe(true);
  });

  test('MCP webhook tool uses the canonical list (no dead events)', async () => {
    const src = await Bun.file(join(REPO, 'src/mcp/tools/webhookTools.ts')).text();
    expect(src).toContain('WEBHOOK_EVENTS');
    expect(src).not.toContain('job.active');
  });
});

describe('#6 WaitJob caps timeout server-side', () => {
  test('oversize timeout is rejected with a clear error', async () => {
    const resp = await handleCommand(
      { cmd: 'WaitJob', id: '1', timeout: 99999999 } as never,
      ctx
    );
    expect(resp.ok).toBe(false);
    expect(String((resp as { error?: string }).error)).toMatch(/timeout/i);
  });
});

describe('#7 formatters expose operational data', () => {
  test('worker list shows status (stale is visible)', () => {
    const out = formatOutput(
      {
        ok: true,
        data: {
          workers: [
            { id: 'w1', name: 'a', queues: ['q'], status: 'stale', concurrency: 4 },
            { id: 'w2', name: 'b', queues: ['q'], status: 'active', concurrency: 2 },
          ],
        },
      },
      'worker',
      false
    );
    expect(out).toContain('stale');
    expect(out).toContain('active');
  });

  test('webhook list shows enabled flag and counters', () => {
    const out = formatOutput(
      {
        ok: true,
        data: {
          webhooks: [
            {
              id: 'wh1',
              url: 'http://x',
              events: ['job.completed'],
              enabled: false,
              successCount: 3,
              failureCount: 7,
            },
          ],
        },
      },
      'webhook',
      false
    );
    expect(out.toLowerCase()).toContain('disabled');
    expect(out).toContain('3');
    expect(out).toContain('7');
  });

  test('cron list shows nextRun when present', () => {
    const out = formatOutput(
      {
        ok: true,
        crons: [
          {
            name: 'c1',
            queue: 'q',
            schedule: '* * * * *',
            executions: 2,
            nextRun: 1781200000000,
          },
        ],
      },
      'cron',
      false
    );
    expect(out).toMatch(/next/i);
  });

  test('stats shows uptime when present', () => {
    const out = formatOutput(
      { ok: true, stats: { waiting: 0, active: 0, uptime: 12345 } },
      'stats',
      false
    );
    expect(out).toMatch(/uptime/i);
  });

  test('webhook add shows the webhookId', () => {
    const out = formatOutput(
      { ok: true, data: { webhookId: 'wh-42', url: 'http://x', events: ['job.completed'] } },
      'webhook',
      false,
      'add'
    );
    expect(out).toContain('wh-42');
  });
});

describe('#8 job state of a missing job exits 1', () => {
  test('E2E: state unknown → exit 1, "not found"', async () => {
    const r = await runCli(['job', 'state', '999999999', '--port', String(E2E_PORT)]);
    expect(r.exitCode).toBe(1);
    expect(r.output.toLowerCase()).toContain('not found');
  }, 15000);
});

describe('#9 `start` boots the FULL server (entry parity)', () => {
  test('start banner includes the FULL-server S3 Backup line', async () => {
    const proc = Bun.spawn(
      ['bun', 'src/main.ts', 'start', '--tcp-port', '18961', '--http-port', '18962'],
      { cwd: REPO, env: { ...process.env }, stdout: 'pipe', stderr: 'pipe' }
    );
    let banner = '';
    const reader = proc.stdout.getReader();
    const deadline = Date.now() + 8000;
    const decoder = new TextDecoder();
    while (Date.now() < deadline && !banner.includes('Shards')) {
      const { value, done } = await Promise.race([
        reader.read(),
        Bun.sleep(300).then(() => ({ value: undefined, done: false })),
      ]);
      if (done) break;
      if (value) banner += decoder.decode(value);
    }
    proc.kill();
    await proc.exited;
    expect(banner).toContain('S3 Backup');
  }, 15000);
});
