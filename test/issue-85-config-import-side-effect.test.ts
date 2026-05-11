/**
 * Issue #85: defineConfig causes "Failed to start server" when used in config file
 *
 * Root cause:
 *   src/main.ts has top-level side effects that re-run the CLI/server on import.
 *   When `bunqueue start -c typed.ts` runs, the user config imports `defineConfig`
 *   from the `bunqueue` package — which resolves to src/main.ts. Re-importing
 *   main.ts re-executes its top-level startup code, attempting a second server
 *   bind on the same port → "Failed to listen at 0.0.0.0".
 *
 * Fix:
 *   Guard the top-level startup in src/main.ts with `import.meta.main` so that
 *   importing the module (e.g. for `defineConfig`) has no side effect.
 */

import { describe, test, expect } from 'bun:test';
import { mkdtempSync, writeFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';

const MAIN_PATH = resolve(import.meta.dir, '..', 'src', 'main.ts');

describe('Issue #85: importing main.ts must not trigger server/CLI startup', () => {
  test('importing src/main.ts with argv emulating `start` does NOT start a server', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'bunq-issue85-'));
    const script = join(dir, 'import-test.ts');

    // Emulate the real-world flow:
    //   `bunqueue start -c typed.ts` → loadConfigFile() → `import typed.ts`
    //   → typed.ts does `import { defineConfig } from 'bunqueue'`
    //   → resolves to src/main.ts → top-level code sees argv[2]==='start'
    //   → re-runs CLI → re-attempts server bind.
    //
    // Pre-fix: this script prints "bunqueue server started" (side effect).
    // Post-fix: nothing happens; only defineConfig is exported.
    writeFileSync(
      script,
      `
process.argv = ['bun', 'fake-script.ts', 'start'];
const mod = await import(${JSON.stringify(MAIN_PATH)});
console.log('defineConfig:' + typeof mod.defineConfig);
// Give any (buggy) async startup enough time to bind ports and emit logs.
await Bun.sleep(1500);
process.exit(0);
`,
    );

    // Use a unique port to avoid colliding with any local dev server.
    const tcpPort = 17890 + Math.floor(Math.random() * 1000);
    const httpPort = tcpPort + 1;

    try {
      const proc = Bun.spawn(['bun', 'run', script], {
        stdout: 'pipe',
        stderr: 'pipe',
        env: {
          ...process.env,
          BUNQUEUE_DATA_PATH: ':memory:',
          TCP_PORT: String(tcpPort),
          HTTP_PORT: String(httpPort),
        },
      });

      const timeoutPromise = new Promise<'timeout'>((res) =>
        setTimeout(() => res('timeout'), 8000),
      );
      const exitPromise = proc.exited.then((code) => ({ code }));
      const winner = await Promise.race([exitPromise, timeoutPromise]);

      if (winner === 'timeout') {
        proc.kill();
        throw new Error(
          'Subprocess hung — importing main.ts kept event loop alive (server started on import).',
        );
      }

      const stdout = await new Response(proc.stdout).text();
      const stderr = await new Response(proc.stderr).text();
      const combined = stdout + stderr;

      expect(stdout).toContain('defineConfig:function');

      // The smoking guns: any of these strings means main.ts ran startup on import.
      expect(combined).not.toContain('bunqueue server started');
      expect(combined).not.toContain('Failed to listen');
      expect(combined).not.toMatch(/●\s*TCP\s/);
      expect(winner.code).toBe(0);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  }, 20000);

  test('defineConfig is a pure passthrough (no side effects)', async () => {
    const mod = await import('../src/config/types');
    const input = { server: { tcpPort: 9999 } };
    const result = mod.defineConfig(input);
    expect(result).toBe(input);
  });
});
