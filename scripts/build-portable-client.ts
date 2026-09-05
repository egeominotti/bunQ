/** Build the portable package directly from the canonical Bun client. */
import { dirname, resolve } from 'node:path';
import { mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { portableSource } from './client-portable/transform';
import { emitClientDeclarations } from './client-portable/declarations';

const root = resolve(import.meta.dir, '..');
const out = resolve(root, 'sdk/typescript/dist');
const adapter = resolve(root, 'sdk/typescript/src/canonical-transport');
const sources = new Map<string, string>();
rmSync(out, { recursive: true, force: true });
mkdirSync(out, { recursive: true });

function recordSource(path: string, content = readFileSync(path, 'utf8')): void {
  sources.set(path.slice(root.length + 1), createHash('sha256').update(content).digest('hex'));
}

const aliases = new Map([
  [resolve(root, 'src/client/manager.ts'), resolve(adapter, 'embedded.ts')],
  [resolve(root, 'src/application/dlqManager.ts'), resolve(adapter, 'dlq.ts')],
  [resolve(root, 'src/client/tcp/transport.ts'), resolve(adapter, 'transport.ts')],
]);

const portable = await Bun.build({
  entrypoints: ['index', 'legacy', 'frame', 'ack-batcher'].map((name) =>
    resolve(root, `sdk/typescript/src/${name}.ts`)
  ),
  outdir: out,
  target: 'node',
  format: 'esm',
  splitting: true,
  external: ['msgpackr'],
  plugins: [
    {
      name: 'canonical-client-runtime-boundaries',
      setup(build) {
        build.onResolve({ filter: /.*/ }, ({ path, importer }) => {
          if (!path.startsWith('.') && !path.startsWith('/')) return;
          const candidate = resolve(dirname(importer), path);
          for (const name of [candidate, `${candidate}.ts`, candidate.replace(/\.js$/, '.ts')]) {
            const replacement = aliases.get(name);
            if (replacement) return { path: replacement };
          }
        });
        build.onLoad({ filter: /\.ts$/ }, async ({ path }) => {
          if (path === resolve(root, 'src/require-bun.ts'))
            return { contents: 'export {};', loader: 'ts' };
          const selected = path;
          const content = readFileSync(selected, 'utf8');
          recordSource(selected, content);
          return {
            contents: portableSource(selected, content, resolve(adapter, 'runtime.ts')),
            loader: 'ts',
            resolveDir: resolve(selected, '..'),
          };
        });
      },
    },
  ],
});
if (!portable.success) throw new AggregateError(portable.logs, 'Portable client build failed');

const embedded = await Bun.build({
  entrypoints: [resolve(root, 'scripts/client-portable/embedded-entry.ts')],
  outdir: out,
  naming: 'embedded.js',
  target: 'bun',
  format: 'esm',
  external: ['msgpackr'],
  plugins: [
    {
      name: 'record-embedded-sources',
      setup(build) {
        build.onLoad({ filter: /\.ts$/ }, ({ path }) => {
          const content = readFileSync(path, 'utf8');
          recordSource(path, content);
          return { contents: content, loader: 'ts' };
        });
      },
    },
  ],
});
if (!embedded.success) throw new AggregateError(embedded.logs, 'Embedded backend build failed');
emitClientDeclarations(root, out);
for (const path of [
  ...aliases.keys(),
  resolve(root, 'src/client/workflow/index.ts'),
  import.meta.path,
  resolve(root, 'scripts/client-portable/transform.ts'),
  resolve(root, 'scripts/client-portable/declarations.ts'),
  resolve(root, 'scripts/client-portable/declaration-imports.ts'),
  resolve(root, 'sdk/typescript/package.json'),
  resolve(root, 'package.json'),
  resolve(root, 'tsconfig.json'),
  resolve(root, 'bun.lock'),
  resolve(root, 'sdk/typescript/tsconfig.json'),
  resolve(root, 'sdk/typescript/bun.lock'),
])
  recordSource(path);
const artifacts: Record<string, string> = {};
for (const entry of readdirSync(out, { recursive: true, withFileTypes: true })) {
  if (!entry.isFile()) continue;
  const path = resolve(entry.parentPath, entry.name);
  artifacts[path.slice(root.length + 1)] = createHash('sha256')
    .update(readFileSync(path))
    .digest('hex');
}
writeFileSync(
  resolve(out, 'canonical-manifest.json'),
  JSON.stringify(
    {
      schema: 1,
      canonicalEntry: 'src/client/index.ts',
      runtimeBoundaries: [...aliases.keys()].map((path) => path.slice(root.length + 1)),
      sources: Object.fromEntries([...sources].sort(([a], [b]) => a.localeCompare(b))),
      artifacts,
    },
    null,
    2
  ) + '\n'
);
console.log(`Built portable client from ${sources.size} shared source modules.`);
