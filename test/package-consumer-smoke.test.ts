import { afterAll, describe, expect, test } from 'bun:test';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  symlinkSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const root = join(import.meta.dir, '..');
const workspace = mkdtempSync(join(tmpdir(), 'bunqueue-package-consumer-'));

afterAll(() => {
  rmSync(workspace, { recursive: true, force: true });
});

function attempt(command: string[], cwd = root): { code: number | null; output: string } {
  const result = Bun.spawnSync(command, { cwd, stdout: 'pipe', stderr: 'pipe' });
  return {
    code: result.exitCode,
    output: `${result.stdout.toString()}${result.stderr.toString()}`.trim(),
  };
}

function run(command: string[], cwd = root): string {
  const { code, output } = attempt(command, cwd);
  if (code !== 0) {
    throw new Error(`${command.join(' ')} failed with exit ${code}:\n${output}`);
  }
  return output;
}

describe('published package consumer contract', () => {
  test('the packed tarball exposes every documented Bun entrypoint', () => {
    run([process.execPath, 'run', 'build:lib']);
    run([process.execPath, 'pm', 'pack', '--ignore-scripts', '--destination', workspace]);

    const archive = readdirSync(workspace).find((name) => name.endsWith('.tgz'));
    expect(archive).toBeDefined();

    // Reproduce the real installed layout instead of symlinking the package
    // itself: Bun resolves a dependency by walking up from the IMPORTING file's
    // realpath, so a symlinked package looks for `msgpackr` next to the
    // extraction directory and never sees the consumer's node_modules.
    // Unpacking in place is what a package manager does, and it is the only
    // arrangement that exercises the published resolution paths.
    const consumer = join(workspace, 'consumer');
    const modules = join(consumer, 'node_modules');
    const packageRoot = join(modules, 'bunqueue');
    mkdirSync(packageRoot, { recursive: true });
    run(
      ['tar', '-xzf', join(workspace, archive!), '-C', packageRoot, '--strip-components=1'],
      workspace
    );

    const manifest = JSON.parse(readFileSync(join(packageRoot, 'package.json'), 'utf8')) as {
      exports: Record<string, unknown>;
      dependencies: Record<string, string>;
    };
    // Membership, not JSON key order: reordering package.json keys is not a
    // contract change, but dropping or adding an entrypoint is.
    expect(Object.keys(manifest.exports).sort()).toEqual([
      '.',
      './client',
      './mcp',
      './package.json',
      './queue',
      './workflow',
    ]);
    expect(existsSync(join(packageRoot, 'dist/client/index.js'))).toBe(true);
    expect(existsSync(join(packageRoot, 'dist/client/workflow/index.js'))).toBe(true);

    // A real `bun add bunqueue` also installs the manifest's runtime
    // dependencies. Providing exactly those — and nothing else — keeps the
    // consumer offline while asserting the stronger property: the DECLARED
    // dependency set is sufficient to import every documented entrypoint. An
    // undeclared runtime import fails here instead of in a user's project.
    const declared = Object.keys(manifest.dependencies ?? {}).sort();
    expect(declared).toEqual(['croner', 'msgpackr']);
    for (const dependency of declared) {
      const source = join(root, 'node_modules', dependency);
      expect(existsSync(source)).toBe(true);
      symlinkSync(source, join(modules, dependency), 'dir');
    }

    // Every entrypoint the exports map advertises, not just the popular three.
    const probe = [
      "import { defineConfig } from 'bunqueue';",
      "import { Queue, Worker } from 'bunqueue/client';",
      "import { Engine } from 'bunqueue/workflow';",
      "import { QueueManager } from 'bunqueue/queue';",
      "console.log([typeof defineConfig, typeof Queue, typeof Worker, typeof Engine, typeof QueueManager].join(' '));",
    ].join('\n');

    expect(run([process.execPath, '-e', probe], consumer)).toBe(
      'function function function function function'
    );

    // `./mcp` is the one entrypoint with an optional peer. A consumer without
    // `@modelcontextprotocol/sdk` must get the actionable install message rather
    // than a module-resolution stack trace, so lock that contract here — the
    // published package is what users actually hit this on.
    // `src/mcp/index.ts` exits deterministically with 1, so assert exactly that:
    // `not.toBe(0)` would also accept a null exit code from a signal-killed
    // process and quietly green-light a future crash.
    const mcp = attempt([process.execPath, '-e', "await import('bunqueue/mcp');"], consumer);
    expect(mcp.code).toBe(1);
    expect(mcp.output).toContain('@modelcontextprotocol/sdk');
    expect(mcp.output).toContain('bun add @modelcontextprotocol/sdk');
  }, 60_000);
});
