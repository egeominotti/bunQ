import { createHash } from 'node:crypto';
import { readFileSync, readdirSync, realpathSync } from 'node:fs';
import { isAbsolute, relative, resolve } from 'node:path';

function record(value: unknown, name: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error(`Invalid parity manifest ${name}`);
  }
  return value as Record<string, unknown>;
}

function containedPath(root: string, file: string): string {
  const path = relative(root, resolve(root, file));
  if (!path || isAbsolute(file) || path.startsWith('..') || path !== file) {
    throw new Error(`Parity manifest path escapes or is not canonical: ${file}`);
  }
  const actual = realpathSync(resolve(root, file));
  const actualRelative = relative(root, actual);
  if (isAbsolute(actualRelative) || actualRelative.startsWith('..')) {
    throw new Error(`Parity manifest symlink escapes root: ${file}`);
  }
  return actual;
}

/** Signatures alone cannot detect a changed default, stale bundle, or changed build adapter. */
export function verifyManifest(
  repositoryRoot: string,
  manifestFile = 'sdk/typescript/dist/canonical-manifest.json'
): { sources: number; artifacts: number } {
  const root = realpathSync(repositoryRoot);
  const manifest = record(
    JSON.parse(readFileSync(containedPath(root, manifestFile), 'utf8')),
    'root'
  );
  if (manifest.schema !== 1 || manifest.canonicalEntry !== 'src/client/index.ts') {
    throw new Error('Unsupported parity manifest schema or canonical entry');
  }
  const sources = record(manifest.sources, 'sources');
  const artifacts = record(manifest.artifacts, 'artifacts');
  for (const file of ['src/client/index.ts', 'src/client/workflow/index.ts']) {
    if (!Object.hasOwn(sources, file))
      throw new Error(`Parity manifest omits canonical source: ${file}`);
  }
  for (const file of ['index.js', 'index.d.ts']) {
    const path = `sdk/typescript/dist/${file}`;
    if (!Object.hasOwn(artifacts, path))
      throw new Error(`Parity manifest omits published entry: ${path}`);
  }
  const outputDirectory = 'sdk/typescript/dist';
  for (const entry of readdirSync(resolve(root, outputDirectory), {
    recursive: true,
    withFileTypes: true,
  })) {
    if (entry.isDirectory()) continue;
    const path = relative(root, resolve(entry.parentPath, entry.name));
    if (path === manifestFile) continue;
    if (!Object.hasOwn(artifacts, path)) throw new Error(`Parity manifest omits artifact: ${path}`);
  }
  for (const [kind, entries] of [
    ['source', sources],
    ['artifact', artifacts],
  ] as const) {
    for (const [file, expected] of Object.entries(entries)) {
      if (typeof expected !== 'string' || !/^[a-f0-9]{64}$/.test(expected)) {
        throw new Error(`Invalid parity ${kind} checksum: ${file}`);
      }
      const actual = createHash('sha256')
        .update(readFileSync(containedPath(root, file)))
        .digest('hex');
      if (actual !== expected)
        throw new Error(`Stale portable client ${kind}: ${file}; rebuild the SDK`);
    }
  }
  return { sources: Object.keys(sources).length, artifacts: Object.keys(artifacts).length };
}
