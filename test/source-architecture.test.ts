import { describe, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const ROOT = join(import.meta.dir, '..');

function lineCount(path: string): number {
  const source = readFileSync(join(ROOT, path), 'utf8');
  return source.length === 0 ? 0 : source.split(/\r?\n/).length - (source.endsWith('\n') ? 1 : 0);
}

describe('source architecture boundaries', () => {
  test('every runtime source file stays at or below 300 lines', () => {
    const oversized = [...new Bun.Glob('src/**/*.{ts,tsx}').scanSync({ cwd: ROOT })]
      .map((path) => ({ path, lines: lineCount(path) }))
      .filter(({ lines }) => lines > 300)
      .sort((left, right) => right.lines - left.lines);

    expect(oversized).toEqual([]);
  });

  test('public orchestration entry points remain thin façades', () => {
    const facades = [
      'src/application/queueManager.ts',
      'src/client/queue/queue.ts',
      'src/client/worker/worker.ts',
      'src/client/sandboxed/worker.ts',
      'src/mcp/adapter.ts',
    ];
    const oversized = facades
      .map((path) => ({ path, lines: lineCount(path) }))
      .filter(({ lines }) => lines > 50);

    expect(oversized).toEqual([]);
  });

  test('core public types live in dedicated type modules', () => {
    const required = [
      'src/application/types/index.ts',
      'src/client/queue/types/index.ts',
      'src/client/worker/types/index.ts',
      'src/client/sandboxed/types/index.ts',
      'src/client/tcp/types/index.ts',
      'src/infrastructure/persistence/types/sqlite.ts',
      'src/infrastructure/server/types/protocol.ts',
      'src/mcp/types/adapter.ts',
    ];

    expect(required.filter((path) => !Bun.file(join(ROOT, path)).size)).toEqual([]);
  });

  test('internal docs do not pin implementation lines to thin façades', () => {
    const forbidden = [
      /(?:src\/application\/)?queueManager\.ts:\d/,
      /src\/client\/queue\/queue\.ts:\d/,
      /src\/client\/worker\/worker\.ts:\d/,
      /src\/client\/sandboxed\/worker\.ts:\d/,
      /src\/infrastructure\/persistence\/sqlite\.ts:\d/,
      /(?:src\/application\/)?backgroundTasks\.ts:\d/,
      /src\/application\/types\.ts:\d/,
    ];
    const staleReferences: string[] = [];
    const documents = [...new Bun.Glob('docs/**/*.{md,mdx}').scanSync({ cwd: ROOT })].filter(
      (path) => !path.startsWith('docs/public/')
    );

    expect(documents.length).toBeGreaterThan(0);
    for (const path of documents) {
      const lines = readFileSync(join(ROOT, path), 'utf8').split(/\r?\n/);
      for (let index = 0; index < lines.length; index++) {
        if (forbidden.some((pattern) => pattern.test(lines[index]))) {
          staleReferences.push(`${path}:${index + 1}`);
        }
      }
    }

    expect(staleReferences).toEqual([]);
  });
});
