import { readdir, readFile } from 'node:fs/promises';
import { dirname, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { rawCodeSpecifiers } from '../src/lib/llms-full';
import { readSeoHead, referencePages } from '../src/lib/reference-seo';
import apiVersions from '../src/data/apiVersions.json';

const DOCS_ROOT = fileURLToPath(new URL('..', import.meta.url));
const CONTENT_ROOT = join(DOCS_ROOT, 'src/content/docs');
const DIST_ROOT = join(DOCS_ROOT, 'dist');
const SITE = 'https://bunqueue.dev';
const failures: string[] = [];

const asPosix = (path: string) => path.split(sep).join('/');

async function walk(directory: string): Promise<string[]> {
  const files: string[] = [];
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) files.push(...(await walk(path)));
    else files.push(path);
  }
  return files;
}

function contentId(file: string): string {
  return asPosix(relative(CONTENT_ROOT, file))
    .replace(/\.(?:md|mdx)$/, '')
    .replace(/\/index$/, '');
}

function canonicalUrl(id: string): string {
  return id === 'index' || id === '' ? `${SITE}/` : `${SITE}/${id}/`;
}

function locations(xml: string): string[] {
  return [...xml.matchAll(/<loc>([^<]+)<\/loc>/g)].map((match) => match[1]);
}

function compareSets(label: string, actual: string[], expected: string[]): void {
  const actualSet = new Set(actual);
  const expectedSet = new Set(expected);
  if (actualSet.size !== actual.length) failures.push(`${label} contains duplicate URLs`);
  if (expectedSet.size !== expected.length)
    failures.push(`content tree defines duplicate ${label} URLs`);
  for (const url of expectedSet) {
    if (!actualSet.has(url)) failures.push(`${label} is missing ${url}`);
  }
  for (const url of actualSet) {
    if (!expectedSet.has(url)) failures.push(`${label} contains unexpected URL ${url}`);
  }
}

const contentFiles = (await walk(CONTENT_ROOT)).filter((file) => /\.mdx?$/.test(file));
const pages = contentFiles.map((file) => ({ file, id: contentId(file) }));
const indexableUrls = pages.filter(({ id }) => id !== '404').map(({ id }) => canonicalUrl(id));
const currentReference = referencePages(
  join(DOCS_ROOT, 'public/reference'),
  apiVersions.current,
  SITE
);
indexableUrls.push(...currentReference.map(({ url }) => url));
const llmsUrlsExpected = pages
  .filter(({ id }) => id !== '404' && id !== 'blog' && !id.startsWith('blog/'))
  .map(({ id }) => canonicalUrl(id));

const llmsFull = await readFile(join(DIST_ROOT, 'llms-full.txt'), 'utf8');
const llmsUrls = [...llmsFull.matchAll(/^URL: (https:\/\/bunqueue\.dev\/.*)$/gm)].map(
  (match) => match[1]
);
compareSets('llms-full.txt', llmsUrls, llmsUrlsExpected);

const multiBrokerUrls = [
  `${SITE}/examples/postgres-multibroker/`,
  `${SITE}/examples/postgres-multibroker/docker/`,
  `${SITE}/examples/postgres-multibroker/queues-workers/`,
  `${SITE}/examples/postgres-multibroker/reliability/`,
  `${SITE}/examples/postgres-multibroker/flows/`,
  `${SITE}/examples/postgres-multibroker/operations/`,
  `${SITE}/examples/postgres-multibroker/validation/`,
];
const routePositions = multiBrokerUrls.map((url) => llmsFull.indexOf(`URL: ${url}`));
if (routePositions.some((position) => position < 0)) {
  failures.push('llms-full.txt is missing a PostgreSQL multi-broker section');
} else if (
  routePositions.some((position, index) => index > 0 && position <= routePositions[index - 1])
) {
  failures.push('llms-full.txt PostgreSQL multi-broker sections are out of reading order');
}

let rawSourceCount = 0;
for (const { file, id } of pages) {
  const body = await readFile(file, 'utf8');
  for (const specifier of rawCodeSpecifiers(body)) {
    const sourcePath = resolve(dirname(file), specifier.replace(/[?#].*$/, ''));
    const source = (await readFile(sourcePath, 'utf8')).replace(/\n+$/, '');
    const sectionStart = llmsFull.indexOf(`URL: ${canonicalUrl(id)}`);
    const sectionEnd = llmsFull.indexOf('\n\n---\n\n# ', sectionStart + 1);
    const section = llmsFull.slice(sectionStart, sectionEnd < 0 ? undefined : sectionEnd);
    rawSourceCount++;
    if (!source || sectionStart < 0 || !section.includes(source)) {
      failures.push(`llms-full.txt does not inline ${asPosix(relative(DOCS_ROOT, sourcePath))}`);
    }
  }
}
if (/<Code\b[^>]*\bcode=\{[A-Za-z_$][\w$]*\}/s.test(llmsFull)) {
  failures.push('llms-full.txt contains an unresolved executable-example Code component');
}

const curated = await readFile(join(DOCS_ROOT, 'public/llms.txt'), 'utf8');
const curatedInternalUrls = [...curated.matchAll(/https:\/\/bunqueue\.dev(?:\/[^\s)>\]]*)?/g)].map(
  (match) => match[0].replace(/#.*$/, '')
);
const indexableUrlSet = new Set(indexableUrls);
for (const url of new Set(curatedInternalUrls)) {
  if (!indexableUrlSet.has(url)) failures.push(`llms.txt links to non-canonical page ${url}`);
}
if (!curated.includes(`[PostgreSQL Multi-Broker Example](${multiBrokerUrls[0]})`)) {
  failures.push('llms.txt does not link the PostgreSQL multi-broker example hub');
}

const referenceTitles = new Set<string>();
const referenceDescriptions = new Set<string>();
for (const url of indexableUrls) {
  const route = new URL(url).pathname.slice(1);
  const file = join(DIST_ROOT, route.endsWith('/') || route === '' ? `${route}index.html` : route);
  const head = readSeoHead(await readFile(file, 'utf8'));
  if (head.canonicals.length !== 1 || head.canonicals[0] !== url) {
    failures.push(`${url} does not have exactly one matching canonical`);
  }
  if (head.noindex) failures.push(`Indexable sitemap page ${url} is noindex`);
  if (head.titles.length !== 1 || !head.titles[0].trim()) {
    failures.push(`${url} does not have exactly one nonempty title`);
  }
  if (head.descriptions.length !== 1 || !head.descriptions[0].trim()) {
    failures.push(`${url} does not have exactly one nonempty description`);
  }
  if (route.startsWith(`reference/${apiVersions.current}/`)) {
    const [title] = head.titles;
    const [description] = head.descriptions;
    if (referenceTitles.has(title)) failures.push(`Current reference repeats title ${title}`);
    if (referenceDescriptions.has(description) || description === 'Documentation for bunqueue') {
      failures.push(`Current reference has a generic or repeated description at ${url}`);
    }
    referenceTitles.add(title);
    referenceDescriptions.add(description);
  }
}
for (const entry of await readdir(join(DIST_ROOT, 'reference'), { withFileTypes: true })) {
  if (!entry.isDirectory() || entry.name === apiVersions.current) continue;
  for (const file of (await walk(join(DIST_ROOT, 'reference', entry.name))).filter((file) =>
    file.endsWith('.html')
  )) {
    if (!readSeoHead(await readFile(file, 'utf8')).noindex) {
      failures.push(
        `Historical or development reference is indexable: ${asPosix(relative(DIST_ROOT, file))}`
      );
    }
  }
}
const hosting = JSON.parse(await readFile(join(DOCS_ROOT, 'vercel.json'), 'utf8')) as {
  headers: { source: string; headers: { key: string; value: string }[] }[];
};
for (const rule of hosting.headers) {
  if (
    rule.source !== '/(.*).md' &&
    rule.headers.some(
      ({ key, value }) =>
        key.toLowerCase() === 'x-robots-tag' && /\b(?:noindex|none)\b/i.test(value)
    )
  )
    failures.push(`Hosting noindex header may block indexable HTML: ${rule.source}`);
}

const sitemap = await readFile(join(DIST_ROOT, 'sitemap-0.xml'), 'utf8');
const sitemapIndex = await readFile(join(DIST_ROOT, 'sitemap-index.xml'), 'utf8');
if (
  !sitemap.startsWith('<?xml') ||
  !sitemap.includes('xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"')
) {
  failures.push('sitemap-0.xml is not a Sitemap Protocol document');
}
compareSets('sitemap-0.xml', locations(sitemap), indexableUrls);
const indexLocations = locations(sitemapIndex);
if (indexLocations.length !== 1 || indexLocations[0] !== `${SITE}/sitemap-0.xml`) {
  failures.push('sitemap-index.xml does not point exclusively to the generated sitemap');
}
for (const match of sitemap.matchAll(/<lastmod>([^<]+)<\/lastmod>/g)) {
  if (Number.isNaN(Date.parse(match[1]))) failures.push(`sitemap has invalid lastmod ${match[1]}`);
}

const robots = await readFile(join(DOCS_ROOT, 'public/robots.txt'), 'utf8');
for (const required of [
  `Sitemap: ${SITE}/sitemap-index.xml`,
  `${SITE}/llms.txt`,
  `${SITE}/llms-full.txt`,
]) {
  if (!robots.includes(required)) failures.push(`robots.txt is missing ${required}`);
}

if (failures.length > 0) {
  console.error(`Discovery validation failed (${failures.length}):`);
  for (const failure of failures) console.error(`  - ${failure}`);
  process.exit(1);
}

console.log(
  `Discovery outputs OK: ${llmsUrls.length} full-text pages, ` +
    `${indexableUrls.length} sitemap URLs, ${rawSourceCount} inlined sources`
);
