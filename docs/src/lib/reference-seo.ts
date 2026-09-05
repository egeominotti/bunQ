import type { AstroIntegration } from 'astro';
import { readdirSync } from 'node:fs';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

interface ReferencePage {
  file: string;
  url: string;
}

const kindNames: Record<string, string> = {
  classes: 'class',
  enums: 'enumeration',
  functions: 'function',
  interfaces: 'interface',
  types: 'type alias',
  variables: 'variable',
};
const tagPattern = /<(?:meta|link)\b(?:[^"'<>]|"[^"]*"|'[^']*')*>/gi;

function decode(value: string): string {
  return value.replace(/&(?:amp|quot|apos|lt|gt|#\d+|#x[\da-f]+);/gi, (entity) => {
    const named: Record<string, string> = {
      '&amp;': '&',
      '&quot;': '"',
      '&apos;': "'",
      '&lt;': '<',
      '&gt;': '>',
    };
    if (!entity.startsWith('&#')) return named[entity.toLowerCase()];
    const hex = entity[2].toLowerCase() === 'x';
    const radix = hex ? 16 : 10;
    const point = Number.parseInt(entity.slice(hex ? 3 : 2, -1), radix);
    return point > 0 && point <= 0x10ffff ? String.fromCodePoint(point) : '\ufffd';
  });
}

function escape(value: string): string {
  const entities: Record<string, string> = {
    '&': '&amp;',
    '"': '&quot;',
    "'": '&#39;',
    '<': '&lt;',
    '>': '&gt;',
  };
  return value.replace(/[&"'<>]/g, (character) => entities[character]);
}

function attributes(tag: string): Record<string, string> {
  return Object.fromEntries(
    [...tag.matchAll(/([\w:-]+)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))/g)].map(
      ([, name, double, single, bare]) => [name.toLowerCase(), decode(double ?? single ?? bare)]
    )
  );
}

function headContent(html: string): string {
  const head = html.match(/<head(?:\s[^>]*)?>([\s\S]*?)<\/head>/i);
  if (!head) throw new Error('API reference HTML has no complete head element');
  return head[1];
}

export function readSeoHead(html: string) {
  const head = headContent(html);
  const tags = [...head.matchAll(tagPattern)].map(([tag]): Record<string, string> => ({
    ...attributes(tag),
    element: tag.slice(1, 5).toLowerCase(),
  }));
  return {
    titles: [...head.matchAll(/<title\b[^>]*>([\s\S]*?)<\/title>/gi)].map((match) =>
      decode(match[1])
    ),
    descriptions: tags
      .filter((tag) => tag.element === 'meta' && tag.name?.toLowerCase() === 'description')
      .map((tag) => tag.content ?? ''),
    canonicals: tags
      .filter(
        (tag) => tag.element === 'link' && tag.rel?.toLowerCase().split(/\s+/).includes('canonical')
      )
      .map((tag) => tag.href ?? ''),
    noindex: tags.some(
      (tag) =>
        tag.element === 'meta' &&
        /^(?:robots|googlebot|bingbot)$/i.test(tag.name ?? '') &&
        /\b(?:noindex|none)\b/i.test(tag.content ?? '')
    ),
  };
}

/** TypeDoc links retain .html, except index.html aliases canonicalize to directories. */
export function referencePages(root: string, current: string, site: string): ReferencePage[] {
  if (!/^v\d+\.\d+$/.test(current)) throw new Error(`Invalid current API version: ${current}`);
  const files: string[] = [];
  function walk(relative: string): void {
    for (const entry of readdirSync(join(root, current, relative), { withFileTypes: true })) {
      const file = relative ? `${relative}/${entry.name}` : entry.name;
      if (entry.isDirectory()) walk(file);
      else if (entry.isFile() && file.endsWith('.html')) files.push(file);
    }
  }
  walk('');
  if (!files.includes('index.html')) throw new Error(`API reference ${current} has no index.html`);
  return files.sort().map((file) => {
    const path = file
      .split('/')
      .map(encodeURIComponent)
      .join('/')
      .replace(/(^|\/)index\.html$/, '$1');
    return { file, url: new URL(`/reference/${current}/${path}`, site).href };
  });
}

function metadata(file: string, version: string): { title: string; description: string } {
  const project = `bunqueue ${version}`;
  if (file === 'index.html') {
    return {
      title: `${project} API reference`,
      description: `Explore the ${project} TypeScript API: queue and worker classes, workflow functions, public interfaces, and configuration types.`,
    };
  }
  if (file === 'hierarchy.html') {
    return {
      title: `Type hierarchy | ${project} API`,
      description: `Browse the ${project} class and interface hierarchy, including inheritance relationships and links to the full TypeScript API reference.`,
    };
  }
  const [directory, filename] = file.split('/');
  const qualified = filename?.replace(/\.html$/, '') ?? file;
  const separator = qualified.lastIndexOf('.');
  const name = separator < 0 ? qualified : qualified.slice(separator + 1);
  const module = (separator < 0 ? qualified : qualified.slice(0, separator)).replaceAll('_', '/');
  if (directory === 'modules') {
    return {
      title: `${module} module | ${project} API`,
      description: `TypeScript exports from the ${module} module in ${project}. Browse its public API, signatures, and related types.`,
    };
  }
  const kind = kindNames[directory];
  if (!kind || !filename || file.split('/').length !== 2) {
    throw new Error(`Unrecognized TypeDoc page: ${file}`);
  }
  return {
    title: `${name} ${kind} (${module}) | ${project} API`,
    description: `TypeScript API for the ${name} ${kind} in ${project} (${module}). Explore its members, signatures, and related types.`,
  };
}

/** Replace only metadata owned by this integration; repeated builds are byte-stable. */
export function applyReferenceSeo(html: string, page: ReferencePage, version: string): string {
  if (readSeoHead(html).noindex) throw new Error(`Current API page is noindex: ${page.url}`);
  const { title, description } = metadata(page.file, version);
  const values: Record<string, string> = {
    description,
    'og:title': title,
    'og:description': description,
    'og:url': page.url,
    'og:type': 'website',
    'og:site_name': 'bunqueue',
    'twitter:card': 'summary',
    'twitter:title': title,
    'twitter:description': description,
  };
  const clean = headContent(html)
    .replace(/<title\b[^>]*>[\s\S]*?<\/title>/gi, '')
    .replace(tagPattern, (tag) => {
      const attrs = attributes(tag);
      const key = (attrs.name ?? attrs.property ?? '').toLowerCase();
      const canonical =
        /^<link\b/i.test(tag) && attrs.rel?.toLowerCase().split(/\s+/).includes('canonical');
      return canonical || (/^<meta\b/i.test(tag) && Object.hasOwn(values, key)) ? '' : tag;
    });
  const injected =
    `<title>${escape(title)}</title><link rel="canonical" href="${escape(page.url)}"/>` +
    Object.entries(values)
      .map(
        ([key, value]) =>
          `<meta ${key.startsWith('og:') ? 'property' : 'name'}="${key}" content="${escape(value)}"/>`
      )
      .join('');
  return html.replace(
    /(<head(?:\s[^>]*)?>)[\s\S]*?(<\/head>)/i,
    (_match, open, close) => `${open}${clean}${injected}${close}`
  );
}

/** Public files bypass Astro routes, so expose their URLs and post-process only dist. */
export function referenceSeo(root: string, current: string, site: string) {
  const pages = referencePages(root, current, site);
  const integration: AstroIntegration = {
    name: 'bunqueue-reference-seo',
    hooks: {
      'astro:build:done': async ({ dir, logger }) => {
        for (const page of pages) {
          const target = join(fileURLToPath(dir), 'reference', current, page.file);
          const html = await readFile(target, 'utf8');
          const updated = applyReferenceSeo(html, page, current);
          if (updated !== html) await writeFile(target, updated);
        }
        logger.info(`Added canonical metadata to ${pages.length} current API reference pages`);
      },
    },
  };
  return { integration, customPages: pages.map((page) => page.url) };
}
