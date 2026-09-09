import { describe, expect, test } from 'bun:test';

const entry = `${import.meta.dir}/../src/main.ts`;

async function probe(args: string[], env: Record<string, string> = {}) {
  const child = Bun.spawn([process.execPath, entry, 'healthcheck', ...args], {
    env: { ...process.env, ...env },
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const [code, stdout, stderr] = await Promise.all([
    child.exited,
    new Response(child.stdout).text(),
    new Response(child.stderr).text(),
  ]);
  return { code, stdout, stderr };
}

describe('container HTTP health check', () => {
  test('uses HTTP_PORT, ignores TCP authentication, and never starts a broker', async () => {
    const server = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      fetch: (request) =>
        new URL(request.url).pathname === '/health'
          ? Response.json({ status: 'healthy' })
          : new Response('', { status: 404 }),
    });
    try {
      expect(await probe([], { HTTP_PORT: String(server.port), AUTH_TOKENS: 'test-only' })).toEqual(
        { code: 0, stdout: 'healthy\n', stderr: '' }
      );
    } finally {
      server.stop(true);
    }
  });

  for (const [name, status, body] of [
    ['degraded', 503, '{"status":"degraded"}'],
    ['false positive', 200, '{"status":"degraded"}'],
    ['invalid JSON', 200, 'private-response'],
    ['null', 200, 'null'],
    ['redirect', 302, ''],
  ] as const) {
    test(`fails closed for ${name}`, async () => {
      const server = Bun.serve({
        hostname: '127.0.0.1',
        port: 0,
        fetch: () => new Response(body, { status, headers: { location: '/health' } }),
      });
      try {
        expect(await probe([`http://127.0.0.1:${server.port}/health`])).toEqual({
          code: 1,
          stdout: '',
          stderr: 'Health check failed\n',
        });
      } finally {
        server.stop(true);
      }
    });
  }

  test('rejects unsupported URLs and extra arguments', async () => {
    for (const args of [['file:///etc/passwd'], ['invalid'], ['http://localhost', 'extra']]) {
      expect((await probe(args)).code).toBe(1);
    }
  });

  test('returns failure when the endpoint is unavailable', async () => {
    const server = Bun.serve({ hostname: '127.0.0.1', port: 0, fetch: () => new Response() });
    const port = server.port;
    server.stop(true);
    expect((await probe([`http://127.0.0.1:${port}/health`])).code).toBe(1);
  });

  test('times out an endpoint that never responds', async () => {
    const server = Bun.serve({
      hostname: '127.0.0.1',
      port: 0,
      fetch: () => new Promise<Response>(() => {}),
    });
    try {
      expect((await probe([`http://127.0.0.1:${server.port}/health`])).code).toBe(1);
    } finally {
      server.stop(true);
    }
  }, 10_000);
});
