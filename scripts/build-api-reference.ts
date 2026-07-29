/**
 * Build the versioned API reference.
 *
 *   bun scripts/build-api-reference.ts            # build for the current package version
 *   bun scripts/build-api-reference.ts --dev      # build to /reference/dev/, not a release
 *
 * Output lands in `docs/public/reference/<version>/`, which Astro serves as-is.
 *
 * The version key is `major.minor`, so a tree is frozen ACROSS minors, not across
 * patches: re-running this on 2.8.48 overwrites the tree built for 2.8.47. That is
 * intended (under semver a patch cannot change the public surface) but it means the
 * page is only as accurate as the last patch that ran it, and a patch that DOES
 * change the surface silently rewrites history for the whole minor. A directory per
 * patch would instead put hundreds of near-identical 5 MB copies in git.
 *
 * A banner is injected into every generated page. TypeDoc has no slot for site chrome,
 * and without it a reader who lands on an old version from a search engine has no way
 * to tell it is old, nor a link back to the site.
 */

import { $ } from 'bun';
import { readdir, readFile, writeFile, mkdir } from 'node:fs/promises';
import { join } from 'node:path';

const REPO = new URL('..', import.meta.url).pathname;
const REFERENCE_ROOT = join(REPO, 'docs/public/reference');

const pkg = (await Bun.file(join(REPO, 'package.json')).json()) as { version: string };
const isDev = Bun.argv.includes('--dev');

/** `2.8.47` -> `v2.8`. Patch releases cannot change the public surface under semver. */
function versionKey(v: string): string {
  const [major, minor] = v.split('.');
  return `v${major}.${minor}`;
}

const current = versionKey(pkg.version);
const target = isDev ? 'dev' : current;
const outDir = join(REFERENCE_ROOT, target);

console.log(`bunqueue ${pkg.version} -> docs/public/reference/${target}/`);

await mkdir(REFERENCE_ROOT, { recursive: true });
const result = await $`bunx typedoc --out ${outDir}`.cwd(REPO).nothrow();
if (result.exitCode !== 0) {
  console.error(result.stderr.toString());
  process.exit(1);
}

// ---------------------------------------------------------------- banner

/**
 * Every generated page gets the same banner, injected right after <body>. It answers
 * the two questions TypeDoc's own chrome cannot: which version is this, and how do I
 * get back to the site. Which version is CURRENT is answered by `/reference/`, which
 * the banner links to, rather than by rewriting frozen trees on every build.
 */
function banner(version: string, depth: number): string {
  const root = '../'.repeat(depth);
  return (
    `<div class="bq-ref-banner">` +
    `<span><a href="https://bunqueue.dev/">bunqueue</a> ` +
    `<span class="bq-ref-version">${version}</span> API reference</span>` +
    `<span><a href="${root}../">all versions</a> · ` +
    `<a href="https://bunqueue.dev/guide/introduction/">guide</a></span>` +
    `</div>`
  );
}

async function injectBanner(dir: string, version: string, stale: boolean, depth = 0) {
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      await injectBanner(full, version, stale, depth + 1);
      continue;
    }
    if (!entry.name.endsWith('.html')) continue;
    const html = await readFile(full, 'utf8');
    if (html.includes('bq-ref-banner')) continue; // idempotent: safe to re-run
    await writeFile(
      full,
      html.replace(/<body([^>]*)>/, `<body$1>${banner(version, stale, depth)}`)
    );
  }
}

await injectBanner(outDir, target, false);

// ---------------------------------------------------------------- listing

/**
 * The version list is NOT written as `public/reference/index.html`. Astro copies
 * `public/` over the built output last, so such a file would clobber the Starlight
 * page that owns `/reference/` and the site would serve unstyled HTML there instead.
 *
 * That page reads the same directory listing at build time via
 * `docs/src/data/apiVersions.js`, so adding a version here makes it appear there
 * with no second place to update.
 */
const versions = (await readdir(REFERENCE_ROOT, { withFileTypes: true }))
  .filter((e) => e.isDirectory())
  .map((e) => e.name)
  .sort((a, b) => {
    if (a === 'dev') return -1;
    if (b === 'dev') return 1;
    const [am, an] = a.slice(1).split('.').map(Number);
    const [bm, bn] = b.slice(1).split('.').map(Number);
    return bm - am || bn - an;
  });

/**
 * Emit the list as JSON for the Starlight page at `/reference/` to import.
 *
 * The page cannot read this directory itself: Vite bundles page modules, and
 * `import.meta.url` no longer points at the source file once it does, so a
 * filesystem walk there silently yields an empty list and the page renders no
 * versions at all. A generated JSON import is resolved at build time and cannot
 * drift, because this script is the only thing that writes it.
 */
await writeFile(
  join(REPO, 'docs/src/data/apiVersions.json'),
  `${JSON.stringify({ current, versions }, null, 2)}\n`
);

console.log(`versions: ${versions.join(', ')}`);
console.log('listing:  /reference/ (Starlight page, imports docs/src/data/apiVersions.json)');
