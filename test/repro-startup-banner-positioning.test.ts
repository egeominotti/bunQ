import { expect, test } from 'bun:test';
import { renderHelp } from '../src/cli/help';

const REPO = `${import.meta.dir}/..`;

function probePort(port: number): { stop(): void } {
  return Bun.listen({
    hostname: '127.0.0.1',
    port,
    socket: {
      data() {
        return;
      },
    },
  });
}

function freePortPair(): number {
  for (let attempt = 0; attempt < 100; attempt++) {
    const port = 20_000 + Math.floor(Math.random() * 20_000);
    let tcpProbe: { stop(): void } | undefined;
    let httpProbe: { stop(): void } | undefined;
    try {
      tcpProbe = probePort(port);
      httpProbe = probePort(port + 1);
      return port;
    } catch {
      // Try another adjacent pair.
    } finally {
      httpProbe?.stop();
      tcpProbe?.stop();
    }
  }
  throw new Error('Unable to reserve a TCP/HTTP port pair');
}

function stripAnsi(text: string): string {
  return Bun.stripANSI(text);
}

test('help uses the polyglot product positioning', () => {
  const help = renderHelp(false);
  expect(help).toContain('One queue. Any language.');
  expect(help).not.toContain('job queue for Bun');
});

test('start banner is aligned and describes runtime state accurately', async () => {
  const tcpPort = freePortPair();
  const env = { ...process.env };
  for (const key of [
    'BUNQUEUE_DATA_PATH',
    'BQ_DATA_PATH',
    'DATA_PATH',
    'SQLITE_PATH',
    'S3_BACKUP_ENABLED',
  ]) {
    delete env[key];
  }

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
      String(tcpPort + 1),
    ],
    { cwd: REPO, env, stdout: 'pipe', stderr: 'pipe' }
  );
  const reader = proc.stdout.getReader();
  const decoder = new TextDecoder();
  const readBanner = async () => {
    let output = '';
    while (!output.includes('Shards')) {
      const { value, done } = await reader.read();
      if (done) break;
      output += decoder.decode(value, { stream: true });
    }
    return output;
  };

  let banner = '';
  try {
    banner = await Promise.race([
      readBanner(),
      Bun.sleep(8_000).then(() => {
        throw new Error('Timed out waiting for the startup banner');
      }),
    ]);
  } finally {
    proc.kill();
    await proc.exited;
  }

  const plain = stripAnsi(banner);
  expect(plain).toContain('One queue. Any language.');
  expect(plain).not.toContain('job queue for Bun');
  expect(plain).toMatch(/○ Unix socket\s+disabled/);
  expect(plain).toMatch(/• Storage\s+in-memory · ephemeral/);
  expect(plain).toMatch(/○ S3 Backup\s+disabled/);
  expect(plain).toMatch(/• Shards\s+16 · \d+ logical CPUs/);
}, 15_000);
