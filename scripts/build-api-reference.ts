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
import { fileURLToPath } from 'node:url';

/** `fileURLToPath`, not `.pathname`: a checkout path with a space arrives encoded. */
const REPO = fileURLToPath(new URL('..', import.meta.url));
const REFERENCE_ROOT = join(REPO, 'docs/public/reference');

/** `2.8.47` -> `v2.8`. Patch releases cannot change the public surface under semver. */
export function versionKey(v: string): string {
  const [major, minor] = v.split('.');
  return `v${major}.${minor}`;
}

// ---------------------------------------------------------------- banner

/**
 * Every generated page gets the same banner, injected right after <body>. It answers
 * the two questions TypeDoc's own chrome cannot: which version is this, and how do I
 * get back to the site. Which version is CURRENT is answered by `/reference/`, which
 * the banner links to, rather than by rewriting frozen trees on every build.
 */
/**
 * `/reference/` relative to a page `depth` directories below the version root:
 * depth 0 (`/reference/v2.8/index.html`) is one level up, depth 1
 * (`/reference/v2.8/classes/Queue.html`) is two, and so on. Exported because this
 * was wrong on 234 published pages and nothing noticed: the caller passed a third
 * argument to a two-parameter `banner()`, so `depth` silently received a boolean
 * and every nested page linked back to its own version instead of the listing.
 */
export function allVersionsHref(depth: number): string {
  return `${'../'.repeat(depth)}../`;
}

export function banner(version: string, depth: number): string {
  return (
    `<div class="bq-ref-banner">` +
    `<span><a href="https://bunqueue.dev/">bunqueue</a> ` +
    `<span class="bq-ref-version">${version}</span> API reference</span>` +
    `<span><a href="${allVersionsHref(depth)}">all versions</a> · ` +
    `<a href="https://bunqueue.dev/guide/introduction/">guide</a></span>` +
    `</div>`
  );
}

export const REFERENCE_ROBOTS = '<meta name="robots" content="noindex, follow"/>';

/**
 * Only the current version tree belongs in the index. TypeDoc writes real per-page
 * titles (`Worker | bunqueue`), so an indexed current tree is a feature: a search for
 * a type name lands on that type. What must not accumulate is one near-identical tree
 * per released version, all sharing the same `Documentation for bunqueue` description
 * and no canonical, competing with each other and with the guide. `--dev` previews are
 * unreleased by definition and never index.
 *
 * `follow` keeps the tree's links counted either way. The `/reference/` listing is a
 * normal Starlight page and always stays indexed.
 */
export function shouldNoindex(version: string, current: string): boolean {
  return version !== current;
}

/**
 * Two independent, separately-guarded injections. A single shared guard would be wrong
 * in both directions: an already-bannered tree would never receive the meta, and a tree
 * that has the meta but predates the banner would never get chrome.
 */
export function injectHead(
  html: string,
  version: string,
  depth: number,
  noindex: boolean
): string {
  let out = html;
  if (!out.includes('bq-ref-banner')) {
    out = out.replace(/<body([^>]*)>/, `<body$1>${banner(version, depth)}`);
  }
  // Attribute-tolerant, like `<body>` above: TypeDoc emits `<head>` bare today, and a
  // release adding `lang` would otherwise drop the meta from every page with no error.
  // The `\s` is load-bearing — `<head([^>]*)>` also matches `<header class=…>`, which
  // TypeDoc emits on every page, and on a page without a `<head>` the meta would land
  // inside the header element instead.
  if (noindex && !/<meta name="robots"/.test(out)) {
    out = out.replace(/<head(\s[^>]*)?>/, (match) => `${match}${REFERENCE_ROBOTS}`);
  }
  return out;
}

/** The meta only counts if a crawler will read it as page-level: inside `<head>`. */
export function hasHeadRobots(html: string): boolean {
  const robots = html.indexOf('name="robots"');
  const headEnd = html.indexOf('</head>');
  return robots !== -1 && headEnd !== -1 && robots < headEnd;
}

async function injectBanner(dir: string, version: string, noindex: boolean, depth = 0) {
  let missing = 0;
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      missing += await injectBanner(full, version, noindex, depth + 1);
      continue;
    }
    if (!entry.name.endsWith('.html')) continue;
    const html = await readFile(full, 'utf8');
    const next = injectHead(html, version, depth, noindex); // idempotent: safe to re-run
    if (next !== html) await writeFile(full, next);
    // Post-condition, because a silently-unmatched replace is exactly how the wrong
    // `depth` shipped on 234 pages: assert the outcome, not the attempt. Placement, not
    // presence — a meta that landed outside `<head>` is not a directive, it is markup.
    if (noindex && !hasHeadRobots(next)) missing++;
  }
  return missing;
}

// ---------------------------------------------------------------- listing

/**
 * The version list is NOT written as `public/reference/index.html`. Astro copies
 * `public/` over the built output last, so such a file would clobber the Starlight
 * page that owns `/reference/` and the site would serve unstyled HTML there instead.
 *
 * That page imports `docs/src/data/apiVersions.json`, so adding a version here makes
 * it appear there with no second place to update. `bun run check:docs-data` asserts
 * the committed file still matches what this function derives.
 *
 * `dev` sorts first; anything that is not `vMAJOR.MINOR` sorts last rather than
 * producing a `NaN` comparator, which is not a total order and can reorder unrelated
 * entries.
 */
export function sortVersions(names: string[]): string[] {
  const rank = (name: string): [number, number] => {
    const [major, minor] = name.slice(1).split('.').map(Number);
    return Number.isFinite(major) && Number.isFinite(minor)
      ? [major, minor]
      : [Number.NEGATIVE_INFINITY, Number.NEGATIVE_INFINITY];
  };
  return [...names].sort((a, b) => {
    if (a === 'dev') return -1;
    if (b === 'dev') return 1;
    const [am, an] = rank(a);
    const [bm, bn] = rank(b);
    return bm - am || bn - an || a.localeCompare(b);
  });
}

async function main() {
  const pkg = (await Bun.file(join(REPO, 'package.json')).json()) as { version: string };
  const current = versionKey(pkg.version);
  const target = Bun.argv.includes('--dev') ? 'dev' : current;
  const outDir = join(REFERENCE_ROOT, target);

  console.log(`bunqueue ${pkg.version} -> docs/public/reference/${target}/`);

  await mkdir(REFERENCE_ROOT, { recursive: true });
  const result = await $`bunx typedoc --out ${outDir}`.cwd(REPO).nothrow();
  if (result.exitCode !== 0) {
    console.error(result.stderr.toString());
    process.exit(1);
  }

  const fail = (missing: number, where: string) => {
    if (missing === 0) return;
    console.error(`${missing} page(s) in ${where} ended without a robots meta inside <head>`);
    process.exit(1);
  };

  fail(await injectBanner(outDir, target, shouldNoindex(target, current)), target);

  const versions = sortVersions(
    (await readdir(REFERENCE_ROOT, { withFileTypes: true }))
      .filter((entry) => entry.isDirectory())
      .map((entry) => entry.name)
  );

  // Demote the trees this release just superseded. Without this pass the policy could
  // never fire for a released version: a tree is only ever written while it IS current,
  // so it would keep the indexable head it was born with and every release would add
  // another near-identical competitor. Idempotent, so re-running is free.
  const demoted: string[] = [];
  for (const version of versions) {
    if (!shouldNoindex(version, current)) continue;
    fail(await injectBanner(join(REFERENCE_ROOT, version), version, true), version);
    demoted.push(version);
  }
  console.log(
    `robots:   ${current} indexable${demoted.length ? `, noindex on ${demoted.join(', ')}` : ''}`
  );

  // The page cannot read this directory itself: Vite bundles page modules, and
  // `import.meta.url` no longer points at the source file once it does, so a
  // filesystem walk there silently yields an empty list and the page renders no
  // versions at all. A generated JSON import is resolved at build time.
  await writeFile(
    join(REPO, 'docs/src/data/apiVersions.json'),
    `${JSON.stringify({ current, versions }, null, 2)}\n`
  );

  console.log(`versions: ${versions.join(', ')}`);
  console.log('listing:  /reference/ (Starlight page, imports docs/src/data/apiVersions.json)');
}

// Guarded so the pure helpers above can be imported by test/build-api-reference.test.ts
// without running typedoc.
if (import.meta.main) await main();
