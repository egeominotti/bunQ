/**
 * Guard the two ways the docs site builds locally and fails in CI.
 *
 *   bun scripts/check-docs-data.ts     # or: bun run check:docs-data
 *
 * 1. **Untracked assets.** A content page can reference a file that exists on the
 *    author's disk but never reaches git — `.gitignore`'s `data/` rule swallowed
 *    `docs/src/data/apiVersions.json` exactly this way, and the only symptom was
 *    `astro build` failing in a clean checkout with "Could not resolve".
 *
 * 2. **Stale generated version list.** `docs/src/data/apiVersions.json` is written
 *    by `scripts/build-api-reference.ts` but committed (see rule 1), so it can
 *    disagree with `package.json` and `docs/public/reference/`. The docs CI job
 *    never runs the generator, so a minor bump would otherwise silently publish
 *    the wrong "current" version. This recomputes the derivation and diffs it.
 *
 * A `dev` entry is rejected outright: `--dev` writes a local preview into the same
 * file, and committing it would advertise an unreleased tree on the public site.
 *
 * Scanning rules, because both directions of error are expensive: fenced and inline
 * code is stripped first (a doc page that SHOWS `import './x'` in a sample is not
 * importing it, and flagging it would turn main red over prose). A module specifier
 * that resolves to nothing is a hard failure, since the build cannot resolve it
 * either; a markdown/JSX asset reference is only reported when the file EXISTS but
 * is untracked, because those paths can legitimately resolve at runtime.
 *
 * Tests: test/check-docs-data.test.ts (the scanners are exported for that reason).
 */

import { readdir } from 'node:fs/promises';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

/** `fileURLToPath`, not `.pathname`: a checkout path with a space arrives encoded. */
const REPO = fileURLToPath(new URL('..', import.meta.url));
const CONTENT_DIR = join(REPO, 'docs/src/content/docs');
const REFERENCE_ROOT = join(REPO, 'docs/public/reference');
const API_VERSIONS = join(REPO, 'docs/src/data/apiVersions.json');

// ------------------------------------------------------------------ scanners

/**
 * Remove fenced blocks and inline spans. Line-driven rather than one regex: an
 * unterminated fence then swallows the rest of the page, which is the safe
 * direction (it can only cause a miss, never a false accusation).
 */
export function stripCode(source: string): string {
  const kept: string[] = [];
  let fence: string | null = null;
  for (const line of source.split('\n')) {
    const opener = /^\s*(```+|~~~+)/.exec(line);
    if (fence === null && opener) {
      fence = opener[1][0];
      continue;
    }
    if (fence !== null) {
      if (opener?.[1][0] === fence) fence = null;
      continue;
    }
    kept.push(line.replace(/`[^`\n]*`/g, ''));
  }
  return kept.join('\n');
}

/** `import x from './y'`, `export * from '../z'`, `import './y'`, `import('./y')`, `require('./y')`. */
const MODULE_SPECIFIER = /\b(?:from|import|require)\s*\(?\s*['"](\.[^'"]*)['"]/g;
/** `![alt](./img.png)` and `src="./img.svg"` — assets Astro resolves from the page. */
const ASSET_REFERENCE = /!\[[^\]]*\]\(\s*(\.[^)\s]+)\s*\)|\bsrc\s*=\s*['"](\.[^'"]+)['"]/g;

export interface Reference {
  specifier: string;
  /** A module specifier must resolve; an asset reference is only checked if it exists. */
  kind: 'module' | 'asset';
}

export function scanReferences(source: string): Reference[] {
  const body = stripCode(source);
  const found: Reference[] = [];
  for (const match of body.matchAll(MODULE_SPECIFIER)) {
    found.push({ specifier: match[1], kind: 'module' });
  }
  for (const match of body.matchAll(ASSET_REFERENCE)) {
    found.push({ specifier: match[1] ?? match[2], kind: 'asset' });
  }
  return found;
}

/** Remove bundler-only suffixes before resolving a module specifier on disk. */
export function moduleFilePath(specifier: string): string {
  return specifier.replace(/[?#].*$/, '');
}

/** Newest minor first, mirroring scripts/build-api-reference.ts. Non-`vN.N` names sort last. */
export function sortVersions(names: string[]): string[] {
  const rank = (name: string): [number, number] => {
    const [major, minor] = name.slice(1).split('.').map(Number);
    return Number.isFinite(major) && Number.isFinite(minor)
      ? [major, minor]
      : [Number.NEGATIVE_INFINITY, Number.NEGATIVE_INFINITY];
  };
  return [...names].sort((a, b) => {
    const [am, an] = rank(a);
    const [bm, bn] = rank(b);
    return bm - am || bn - an || a.localeCompare(b);
  });
}

/** `2.8.47` -> `v2.8`. Patch releases cannot change the public surface under semver. */
export function versionKey(version: string): string {
  const [major, minor] = version.split('.');
  return `v${major}.${minor}`;
}

// ------------------------------------------------------------------- checking

const failures: string[] = [];
const rel = (path: string) => relative(REPO, path);

/**
 * Extension and index candidates, in no meaningful order: EVERY candidate that
 * exists is checked, not just the first. Bundler resolution order differs between
 * tools (Vite prefers `.js` over `.ts`), so picking one would let a stale untracked
 * `x.js` hide behind a tracked `x.ts`.
 */
const CANDIDATES = [
  '',
  '.ts',
  '.tsx',
  '.js',
  '.jsx',
  '.json',
  '.astro',
  '.mdx',
  '.md',
  '/index.ts',
  '/index.tsx',
  '/index.js',
  '/index.astro',
];

/** Files that a page can reference; a component breaks the build the same way. */
const SCAN_ROOTS: { dir: string; extensions: string[] }[] = [
  { dir: CONTENT_DIR, extensions: ['.mdx', '.md'] },
  { dir: join(REPO, 'docs/src/components'), extensions: ['.astro', '.ts', '.tsx'] },
];

async function resolveCandidates(fromFile: string, specifier: string): Promise<string[]> {
  const found: string[] = [];
  const fileSpecifier = moduleFilePath(specifier);
  for (const extension of CANDIDATES) {
    const candidate = resolve(dirname(fromFile), fileSpecifier + extension);
    if (await Bun.file(candidate).exists()) found.push(candidate);
  }
  return found;
}

/** Tracked set for `paths`, or null when git itself could not answer. */
async function trackedPaths(paths: string[]): Promise<Set<string> | null> {
  const tracked = new Set<string>();
  // Chunked: one argv with every asset would eventually hit ARG_MAX. Diffing sets
  // rather than using `--error-unmatch`, which aborts on the first unknown path.
  for (let index = 0; index < paths.length; index += 200) {
    const batch = paths.slice(index, index + 200);
    const child = Bun.spawn(['git', 'ls-files', '-z', '--', ...batch], {
      cwd: REPO,
      stderr: 'pipe',
    });
    const listed = await new Response(child.stdout).text();
    if ((await child.exited) !== 0) {
      failures.push(`git ls-files failed: ${(await new Response(child.stderr).text()).trim()}`);
      return null;
    }
    for (const path of listed.split('\0').filter(Boolean)) tracked.add(resolve(REPO, path));
  }
  return tracked;
}

async function checkTrackedAssets(): Promise<void> {
  /** resolved absolute path -> files referencing it, for a useful error message. */
  const referenced = new Map<string, string[]>();

  for (const { dir, extensions } of SCAN_ROOTS) {
    let entries: string[];
    try {
      entries = await readdir(dir, { recursive: true });
    } catch {
      continue; // an optional root (components) may not exist
    }
    for (const entry of entries.filter((name) => extensions.some((ext) => name.endsWith(ext)))) {
      const filePath = join(dir, entry);
      for (const { specifier, kind } of scanReferences(await Bun.file(filePath).text())) {
        const resolved = await resolveCandidates(filePath, specifier);
        if (resolved.length === 0) {
          // Assets may resolve at runtime from public/; only imports must exist now.
          if (kind === 'module') {
            failures.push(`${rel(filePath)} imports '${specifier}', which does not exist`);
          }
          continue;
        }
        for (const path of resolved) {
          referenced.set(path, [...(referenced.get(path) ?? []), rel(filePath)]);
        }
      }
    }
  }

  if (referenced.size === 0) return;
  const tracked = await trackedPaths([...referenced.keys()]);
  if (!tracked) return;
  for (const [path, files] of referenced) {
    if (tracked.has(path)) continue;
    failures.push(
      `${rel(path)} is referenced by ${files.join(', ')} but is NOT tracked by git ` +
        `(a clean checkout cannot build: check .gitignore, then \`git add\` it)`
    );
  }
  console.log(`  assets: ${referenced.size} referenced, all tracked`);
}

async function checkApiVersions(): Promise<void> {
  const pkg = (await Bun.file(join(REPO, 'package.json')).json()) as { version: string };
  const current = versionKey(pkg.version);

  let directories: string[];
  try {
    directories = (await readdir(REFERENCE_ROOT, { withFileTypes: true }))
      .filter((entry) => entry.isDirectory())
      .map((entry) => entry.name);
  } catch {
    failures.push(`${rel(REFERENCE_ROOT)} is missing: run \`bun run docs:api\``);
    return;
  }

  const versions = sortVersions(directories.filter((name) => name !== 'dev'));
  if (!versions.includes(current)) {
    failures.push(
      `package.json is ${pkg.version} (${current}) but ${rel(REFERENCE_ROOT)}/${current}/ ` +
        `does not exist: a minor bump needs \`bun run docs:api\` and its output committed`
    );
    return;
  }

  let committed: { current?: string; versions?: string[] };
  try {
    committed = (await Bun.file(API_VERSIONS).json()) as { current?: string; versions?: string[] };
  } catch (error) {
    failures.push(
      `${rel(API_VERSIONS)} is missing or not valid JSON (${(error as Error).message}): ` +
        `run \`bun run docs:api\``
    );
    return;
  }

  if (committed.versions?.includes('dev')) {
    failures.push(
      `${rel(API_VERSIONS)} lists the "dev" preview: it must never be committed ` +
        `(re-run \`bun run docs:api\` without --dev)`
    );
  } else if (
    (await Bun.file(API_VERSIONS).text()) !== `${JSON.stringify({ current, versions }, null, 2)}\n`
  ) {
    failures.push(
      `${rel(API_VERSIONS)} is stale: expected ${JSON.stringify({ current, versions })}, ` +
        `found ${JSON.stringify(committed)} — run \`bun run docs:api\``
    );
  }

  // Mirror hole: the list may advertise a version whose 5 MB tree is untracked,
  // which publishes a /reference/ link straight to a 404.
  const tracked = await trackedPaths([join(REFERENCE_ROOT, current)]);
  if (tracked?.size === 0) {
    failures.push(
      `${rel(REFERENCE_ROOT)}/${current}/ is not tracked by git, so the published ` +
        `/reference/ listing would link to a 404`
    );
  }
  console.log(`  apiVersions.json: matches ${current}, tree tracked`);
}

if (import.meta.main) {
  await checkTrackedAssets();
  await checkApiVersions();

  if (failures.length > 0) {
    console.error(`docs data check FAILED (${failures.length}):`);
    for (const failure of failures) console.error(`  - ${failure}`);
    process.exit(1);
  }
  console.log('docs data OK');
}
