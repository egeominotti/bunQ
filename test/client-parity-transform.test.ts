import { describe, expect, test } from 'bun:test';
import { portableSource } from '../scripts/client-portable/transform';

const path = '/repo/src/client/example.ts';
const runtime = '/repo/sdk/typescript/src/canonical-transport/runtime.ts';

describe('portable source runtime boundary', () => {
  for (const [name, source] of [
    ['computed primitive', "await Bun['sleep'](1);"],
    ['template-computed primitive', 'await Bun[`sleep`](1);'],
    ['destructured primitive', 'const { sleep } = Bun;'],
    ['aliased runtime', 'const runtime = Bun; runtime.sleep(1);'],
    ['unknown primitive', 'Bun.spawn(["echo"]);'],
    ['inherited mapping property', 'Bun.toString();'],
    ['global property', 'globalThis.Bun.sleep(1);'],
    ['computed global property', 'globalThis["Bun"].sleep(1);'],
    ['named import', 'import { sleep } from "bun";'],
    ['namespace import', 'import * as runtime from "bun";'],
    ['default import', 'import runtime from "bun";'],
    ['side-effect import', 'import "bun";'],
    ['type import', 'import type { Socket } from "bun";'],
    ['Bun module', 'import { Database } from "bun:sqlite";'],
    ['named re-export', 'export { sleep } from "bun";'],
    ['star re-export', 'export * from "bun:sqlite";'],
    ['namespace re-export', 'export * as runtime from "bun";'],
    ['dynamic import', 'await import("bun");'],
    ['dynamic Bun module', 'await import("bun:sqlite");'],
    ['template dynamic import', 'await import(`bun:sqlite`);'],
    ['concatenated dynamic import', 'await import("bun" + ":sqlite");'],
    [
      'constant-aliased dynamic import',
      'const moduleName = "bun:sqlite"; await import(moduleName);',
    ],
    ['interpolated dynamic import', 'await import(`bun:${driver}`);'],
    ['dynamic module suffix', 'await import("bun:" + driver);'],
    ['type query import', 'type Runtime = typeof import("bun");'],
    ['CommonJS resolve', 'require.resolve("bun:sqlite");'],
    ['CommonJS import', 'const runtime = require("bun");'],
    ['TypeScript import assignment', 'import runtime = require("bun");'],
  ]) {
    test(`rejects ${name}`, () => {
      expect(() => portableSource(path, source!, runtime)).toThrow(/Unreviewed Bun/);
    });
  }

  test('also validates arguments inside the worker-thread boundary', () => {
    expect(() =>
      portableSource(
        '/repo/src/client/sandboxed/runtime/pool.ts',
        'new Worker(Bun["file"]("processor.ts"));',
        runtime
      )
    ).toThrow(/Unreviewed Bun/);
  });

  test('preserves the approved primitive mappings', () => {
    const output = portableSource(
      path,
      `
const env = Bun.env;
await Bun.sleep(1);
Bun.hash('value');
Bun.randomUUIDv7();
Bun.file('file');
const cores = navigator.hardwareConcurrency;
`,
      runtime
    );
    for (const mapped of [
      'process.env',
      '__portableRuntime.sleep',
      '__portableRuntime.hash',
      '__portableRuntime.uuid',
      '__portableRuntime.file',
      '__portableRuntime.hardwareConcurrency',
    ]) {
      expect(output).toContain(mapped);
    }
    expect(output).not.toContain('Bun.');
  });

  test('preserves the worker mapping and transforms its approved arguments', () => {
    const output = portableSource(
      '/repo/src/client/sandboxed/runtime/pool.ts',
      'new Worker(Bun.env.PROCESSOR_PATH);',
      runtime
    );
    expect(output).toContain('new __portableRuntime.ThreadWorker(process.env.PROCESSOR_PATH)');
  });

  test('preserves ordinary module imports, comments, strings and dynamic processor paths', () => {
    const output = portableSource(
      path,
      `
import { readFile } from 'node:fs';
export * from './types';
const description = 'Bun.sleep is documented here';
const loaded = await import(processorPath);
const packageModule = await import('bun' + 'queue');
`,
      runtime
    );
    expect(output).toContain('node:fs');
    expect(output).toContain('Bun.sleep is documented here');
    expect(output).toContain('import(processorPath)');
    expect(output).toContain("import('bun' + 'queue')");
  });
});
