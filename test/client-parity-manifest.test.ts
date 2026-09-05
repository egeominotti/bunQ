import { afterEach, describe, expect, test } from 'bun:test';
import { createHash } from 'node:crypto';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { verifyManifest } from '../scripts/client-parity/manifest';

const roots: string[] = [];
afterEach(() => {
  for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true });
});

function fixture() {
  const root = mkdtempSync(join(tmpdir(), 'client-parity-manifest-'));
  roots.push(root);
  const manifest = {
    schema: 1,
    canonicalEntry: 'src/client/index.ts',
    sources: {} as Record<string, string>,
    artifacts: {} as Record<string, string>,
  };
  for (const [entries, paths] of [
    [manifest.sources, ['src/client/index.ts', 'src/client/workflow/index.ts']],
    [manifest.artifacts, ['index.js', 'index.d.ts'].map((file) => `sdk/typescript/dist/${file}`)],
  ] as const) {
    for (const path of paths) {
      mkdirSync(dirname(join(root, path)), { recursive: true });
      const content = 'export const defaultConcurrency: number = 1;';
      writeFileSync(join(root, path), content);
      entries[path] = createHash('sha256').update(content).digest('hex');
    }
  }
  const save = () =>
    writeFileSync(
      join(root, 'sdk/typescript/dist/canonical-manifest.json'),
      JSON.stringify(manifest)
    );
  save();
  return { root, manifest, save };
}

describe('portable runtime freshness gate', () => {
  test('records compiler configuration and dependency locks in the real build', () => {
    const manifest = JSON.parse(
      readFileSync(join(import.meta.dir, '../sdk/typescript/dist/canonical-manifest.json'), 'utf8')
    );
    for (const path of [
      'tsconfig.json',
      'bun.lock',
      'sdk/typescript/tsconfig.json',
      'sdk/typescript/bun.lock',
    ]) {
      expect(manifest.sources[path]).toMatch(/^[a-f0-9]{64}$/);
    }
  });

  test('accepts a complete current source and artifact snapshot', () => {
    expect(verifyManifest(fixture().root)).toEqual({ sources: 2, artifacts: 2 });
  });

  test('rejects runtime-only default changes with identical public types', () => {
    const { root } = fixture();
    writeFileSync(
      join(root, 'src/client/index.ts'),
      'export const defaultConcurrency: number = 2;'
    );
    expect(() => verifyManifest(root)).toThrow('Stale portable client source');
  });

  test('rejects edited output bundles', () => {
    const { root } = fixture();
    writeFileSync(join(root, 'sdk/typescript/dist/index.js'), 'export {};');
    expect(() => verifyManifest(root)).toThrow('Stale portable client artifact');
  });

  test('rejects generated runtime files missing from the artifact inventory', () => {
    const { root } = fixture();
    writeFileSync(join(root, 'sdk/typescript/dist/embedded.js'), 'export {};');
    expect(() => verifyManifest(root)).toThrow('Parity manifest omits artifact');
  });

  test('rejects omitted entries and unsupported manifests', () => {
    const { root, manifest, save } = fixture();
    delete manifest.sources['src/client/index.ts'];
    save();
    expect(() => verifyManifest(root)).toThrow('omits canonical source');
    manifest.schema = 2;
    save();
    expect(() => verifyManifest(root)).toThrow('Unsupported parity manifest');
  });

  test('rejects paths outside the repository before reading their contents', () => {
    const { root, manifest, save } = fixture();
    manifest.sources['../outside.ts'] = 'a'.repeat(64);
    save();
    expect(() => verifyManifest(root)).toThrow('path escapes');
  });

  test('requires the manifest itself', () => {
    const { root } = fixture();
    rmSync(join(root, 'sdk/typescript/dist/canonical-manifest.json'));
    expect(() => verifyManifest(root)).toThrow();
  });
});
