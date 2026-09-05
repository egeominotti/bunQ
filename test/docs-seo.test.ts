import { expect, test } from 'bun:test';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  applyReferenceSeo,
  readSeoHead,
  referencePages,
  referenceSeo,
} from '../docs/src/lib/reference-seo';

test('hosting noindex headers apply only to raw Markdown, never current HTML references', () => {
  const config = JSON.parse(
    readFileSync(new URL('../docs/vercel.json', import.meta.url), 'utf8')
  ) as { headers: { source: string; headers: { key: string; value: string }[] }[] };
  const noindexRules = config.headers.filter((rule) =>
    rule.headers.some(
      (header) =>
        header.key.toLowerCase() === 'x-robots-tag' && /\b(?:noindex|none)\b/i.test(header.value)
    )
  );
  expect(noindexRules.map((rule) => rule.source)).toEqual(['/(.*).md']);
});

const SITE = 'https://bunqueue.dev';
const current = 'v2.9';
const html =
  '<!doctype html><html><head data-theme="light"><title>Worker | bunqueue</title>' +
  '<meta name="description" content="Documentation for bunqueue"/>' +
  '<script>const value = "$& $1";</script></head><body><h1>Worker</h1></body></html>';
const page = {
  file: 'classes/client.Worker.html',
  url: `${SITE}/reference/v2.9/classes/client.Worker.html`,
};

function fixture() {
  const root = mkdtempSync(join(tmpdir(), 'bunqueue-docs-seo-'));
  const publicRoot = join(root, 'public', 'reference');
  const distRoot = join(root, 'dist');
  const files = [
    'v2.9/index.html',
    'v2.9/classes/client.Worker.html',
    'v2.9/interfaces/client.Job.html',
    'v2.8/index.html',
    'dev/index.html',
  ];
  for (const base of [join(root, 'public'), distRoot]) {
    for (const file of files) {
      const target = join(base, 'reference', file);
      mkdirSync(dirname(target), { recursive: true });
      const content = file.startsWith(`${current}/`)
        ? html
        : html.replace('</head>', '<meta name="robots" content="noindex, follow"></head>');
      writeFileSync(target, content);
    }
  }
  writeFileSync(join(publicRoot, current, 'assets.js'), '');
  return {
    root,
    publicRoot,
    distRoot,
    cleanup: () => rmSync(root, { recursive: true, force: true }),
  };
}

test('sitemap reference URLs are deterministic, current-only, and canonicalize directory indexes', () => {
  const files = fixture();
  try {
    const pages = referencePages(files.publicRoot, current, SITE);
    expect(pages).toEqual([
      page,
      { file: 'index.html', url: `${SITE}/reference/v2.9/` },
      {
        file: 'interfaces/client.Job.html',
        url: `${SITE}/reference/v2.9/interfaces/client.Job.html`,
      },
    ]);
    expect(referencePages(files.publicRoot, current, SITE)).toEqual(pages);
    expect(pages.some(({ url }) => /\/v2\.8\/|\/dev\/|index\.html$/.test(url))).toBe(false);
    writeFileSync(join(files.publicRoot, current, 'classes/client.Query&#.html'), html);
    expect(referencePages(files.publicRoot, current, SITE).map(({ url }) => url)).toContain(
      `${SITE}/reference/v2.9/classes/client.Query%26%23.html`
    );
    expect(() => referencePages(files.publicRoot, 'dev', SITE)).toThrow(
      'Invalid current API version'
    );
    expect(() => referencePages(files.publicRoot, '../v2.9', SITE)).toThrow(
      'Invalid current API version'
    );
    rmSync(join(files.publicRoot, current, 'index.html'));
    expect(() => referencePages(files.publicRoot, current, SITE)).toThrow('has no index.html');
  } finally {
    files.cleanup();
  }
});

test('current reference metadata is specific, indexable, and distinguishes equal type names', () => {
  const result = applyReferenceSeo(html, page, current);
  const head = readSeoHead(result);
  expect(head).toEqual({
    titles: ['Worker class (client) | bunqueue v2.9 API'],
    descriptions: [
      'TypeScript API for the Worker class in bunqueue v2.9 (client). Explore its members, signatures, and related types.',
    ],
    canonicals: [page.url],
    noindex: false,
  });
  const alternative = applyReferenceSeo(
    html,
    {
      file: 'interfaces/domain_types_worker.Worker.html',
      url: `${SITE}/reference/v2.9/interfaces/domain_types_worker.Worker.html`,
    },
    current
  );
  expect(readSeoHead(alternative).titles[0]).not.toBe(head.titles[0]);
  expect(readSeoHead(alternative).descriptions[0]).not.toBe(head.descriptions[0]);
  expect(result).toContain(
    '<meta property="og:url" content="https://bunqueue.dev/reference/v2.9/classes/client.Worker.html"/>'
  );
  expect(result).toContain('<body><h1>Worker</h1></body>');
  expect(result).toContain('<script>const value = "$& $1";</script>');
});

test('metadata replacement is idempotent, escapes attributes, and replaces stale duplicates', () => {
  const special = {
    file: 'functions/client.run<&".html',
    url: `${SITE}/reference/v2.9/functions/test.html?a=1&b="two"`,
  };
  const stale = html.replace(
    '</head>',
    '<LINK HREF="https://old.example/" REL="canonical">' +
      '<meta content="Old description" name="DESCRIPTION">' +
      '<meta property="og:title" content="Old title"></head>'
  );
  const result = applyReferenceSeo(stale, special, current);
  expect(applyReferenceSeo(result, special, current)).toBe(result);
  expect(result).toContain('run&lt;&amp;&quot;');
  expect(result).toContain('test.html?a=1&amp;b=&quot;two&quot;');
  expect(readSeoHead(result).canonicals).toEqual([special.url]);
  expect(readSeoHead(result).descriptions).toHaveLength(1);
  expect(result).not.toContain('https://old.example/');
  expect(result).not.toContain('Old title');
});

test('missing heads and current noindex directives fail the build instead of publishing conflicts', () => {
  expect(() => applyReferenceSeo('<header>Not a head</header>', page, current)).toThrow(
    'no complete head'
  );
  for (const name of ['robots', 'googlebot', 'bingbot']) {
    const blocked = html.replace(
      '</head>',
      `<meta content='NOINDEX, follow' name='${name}'></head>`
    );
    expect(() => applyReferenceSeo(blocked, page, current)).toThrow('Current API page is noindex');
    expect(() =>
      applyReferenceSeo(blocked.replace('NOINDEX, follow', 'none'), page, current)
    ).toThrow('Current API page is noindex');
  }
});

test('reference build integration updates only built current pages and preserves frozen sources and history', async () => {
  const files = fixture();
  try {
    const source = readFileSync(join(files.publicRoot, current, page.file), 'utf8');
    const historical = readFileSync(join(files.distRoot, 'reference/v2.8/index.html'), 'utf8');
    const dev = readFileSync(join(files.distRoot, 'reference/dev/index.html'), 'utf8');
    const { integration, customPages } = referenceSeo(files.publicRoot, current, SITE);
    expect(customPages).toEqual([
      page.url,
      `${SITE}/reference/v2.9/`,
      `${SITE}/reference/v2.9/interfaces/client.Job.html`,
    ]);
    const hook = integration.hooks['astro:build:done']!;
    const context = { dir: pathToFileURL(`${files.distRoot}/`), logger: { info: () => {} } };
    await hook(context as Parameters<typeof hook>[0]);
    const result = readFileSync(join(files.distRoot, 'reference', current, page.file), 'utf8');
    expect(readSeoHead(result).canonicals).toEqual([page.url]);
    await hook(context as Parameters<typeof hook>[0]);
    expect(readFileSync(join(files.distRoot, 'reference', current, page.file), 'utf8')).toBe(
      result
    );
    expect(readFileSync(join(files.publicRoot, current, page.file), 'utf8')).toBe(source);
    expect(readFileSync(join(files.distRoot, 'reference/v2.8/index.html'), 'utf8')).toBe(
      historical
    );
    expect(readFileSync(join(files.distRoot, 'reference/dev/index.html'), 'utf8')).toBe(dev);
    expect(readSeoHead(historical).noindex).toBe(true);
    expect(readSeoHead(dev).noindex).toBe(true);
  } finally {
    files.cleanup();
  }
});
