import { execSync } from 'node:child_process';

/**
 * Real per-page last-modified dates from git history, shared by the sitemap
 * (astro.config.mjs computes its own copy inline) and the structured-data in
 * Head.astro. A fake `new Date()` on every build teaches Google the field is
 * unreliable; pages with no known date simply omit the date field.
 *
 * Keys are normalized page ids: 'guide/queue', 'architecture', 'index'.
 *
 * cwd is process.cwd() (the docs/ project root during `astro build`), NOT a
 * path derived from import.meta.url: Vite rewrites import.meta.url in the
 * bundled server build, so an import.meta-relative cwd resolves wrong and the
 * git call silently fails, leaving every dateModified undefined.
 */
const docsDir = process.cwd();

const map = new Map<string, string>();
const created = new Map<string, string>();

try {
  const out = execSync('git log --format=%x00%cI --name-only -- src/content/docs', {
    cwd: docsDir,
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
  });
  let current: string | null = null;
  for (const line of out.split('\n')) {
    if (line.startsWith('\x00')) {
      current = line.slice(1).trim();
    } else {
      const f = line.trim();
      // git emits repo-root-relative paths (e.g. `docs/src/content/docs/x.md`)
      // even when run with cwd=docs/, so match/strip up to the content root
      // rather than anchoring at the string start.
      if (!f.includes('src/content/docs/')) continue;
      const norm = f
        .replace(/^.*src\/content\/docs\//, '')
        .replace(/\.(md|mdx)$/, '')
        .replace(/\/index$/, '');
      const key = norm === '' ? 'index' : norm;
      if (!current) continue;
      // git log walks newest-first: the first date seen for a file is its last
      // modification, the last one seen is when it first appeared.
      if (!map.has(key)) map.set(key, current);
      created.set(key, current);
    }
  }
} catch {
  // shallow clone or no git: dates are simply omitted downstream
}

/** Last-modified ISO date for a Starlight route id, or undefined if unknown. */
export function lastmodForId(id: string): string | undefined {
  const key = id === '' ? 'index' : id;
  return map.get(key) ?? map.get(`${key}/index`);
}

/**
 * First-commit ISO date for a Starlight route id — a real `datePublished` for
 * Article-family structured data, which Google expects alongside
 * dateModified. Same rule as above: unknown means the field is omitted, never
 * faked with a build-time now().
 */
export function createdForId(id: string): string | undefined {
  const key = id === '' ? 'index' : id;
  return created.get(key) ?? created.get(`${key}/index`);
}
