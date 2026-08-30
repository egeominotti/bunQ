import type { APIRoute } from 'astro';
import { getCollection } from 'astro:content';
import { readFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { inlineRawCodeImports } from '../lib/llms-full';

/**
 * Full-text dump of the docs for LLM grounding (https://llmstxt.org/).
 * Companion to the curated /llms.txt: this concatenates every page's title,
 * description, canonical URL and Markdown/MDX body into one file an LLM can
 * ingest without crawling. Tested sources imported with `?raw` are expanded
 * into fenced code blocks. Referenced from robots.txt.
 */
const SITE = 'https://bunqueue.dev';

// Rough sidebar order so the dump reads top-to-bottom like the docs.
const ORDER = [
  'index',
  'guide/introduction',
  'guide/installation',
  'guide/quickstart',
  'guide/mcp',
  'guide/use-cases',
  'guide/simple-mode',
  'guide/workflow',
  'guide/queue',
  'guide/worker',
  'guide/cpu-intensive-workers',
  'guide/queue-group',
  'guide/flow',
  'guide/stall-detection',
  'guide/dlq',
  'guide/sdks',
  'guide/sdk-benchmarks',
  'guide/server',
  'guide/configuration',
  'guide/cli',
  'guide/env-vars',
  'api/http',
  'api/tcp',
  'api/types',
  'guide/cron',
  'guide/backup',
  'guide/rate-limiting',
  'guide/webhooks',
  'guide/benchmarks',
  'guide/comparison',
  'guide/production',
  'guide/deployment',
  'guide/databases',
  'guide/tls',
  'guide/monitoring',
  'guide/telemetry',
  'guide/integrations',
  'guide/hono',
  'guide/elysia',
  'guide/iot-edge',
  'guide/migration',
  'examples',
  'examples/postgres-multibroker',
  'examples/postgres-multibroker/docker',
  'examples/postgres-multibroker/queues-workers',
  'examples/postgres-multibroker/reliability',
  'examples/postgres-multibroker/flows',
  'examples/postgres-multibroker/operations',
  'examples/postgres-multibroker/validation',
  'architecture',
  'architecture/client-sdk',
  'architecture/domain-layer',
  'architecture/application-layer',
  'architecture/tcp-protocol',
  'architecture/persistence',
  'architecture/data-structures',
  'architecture/cron-scheduler',
  'faq',
  'troubleshooting',
  'changelog',
  'security',
  'contributing',
];

const rank = (id: string) => {
  const i = ORDER.indexOf(id);
  return i === -1 ? ORDER.length + 1 : i;
};

const urlFor = (id: string) => (id === 'index' || id === '' ? `${SITE}/` : `${SITE}/${id}/`);

async function loadRawSource(pageFilePath: string | undefined, specifier: string): Promise<string> {
  if (!pageFilePath)
    throw new Error(`Cannot resolve ${specifier}: the content entry has no file path`);

  const relativeSource = specifier.replace(/[?#].*$/, '');
  const roots = [process.cwd(), resolve(process.cwd(), 'docs')];
  for (const root of roots) {
    const sourcePath = resolve(dirname(resolve(root, pageFilePath)), relativeSource);
    try {
      return await readFile(sourcePath, 'utf8');
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
  }
  throw new Error(`Cannot resolve raw documentation source ${specifier} from ${pageFilePath}`);
}

export const GET: APIRoute = async () => {
  const docs = await getCollection('docs');
  const pages = docs
    .filter((e) => e.id !== '404' && !e.id.startsWith('blog'))
    .sort((a, b) => rank(a.id) - rank(b.id) || a.id.localeCompare(b.id));

  const head =
    '# bunqueue: full documentation\n\n' +
    '> Concatenated full text of the bunqueue documentation for LLM grounding. ' +
    'bunqueue is a high-performance job queue for the Bun runtime, using in-memory storage ' +
    'by default, optional SQLite (WAL), or PostgreSQL 15-18 for multi-broker deployments, without Redis. ' +
    'It provides a BullMQ-compatible API and a native MCP server. ' +
    `Canonical site: ${SITE}. Curated index: ${SITE}/llms.txt\n`;

  const sections = await Promise.all(
    pages.map(async (e) => {
      const desc = e.data.description ? `${e.data.description}\n` : '';
      const raw = await inlineRawCodeImports(e.body ?? '', (specifier) =>
        loadRawSource(e.filePath, specifier)
      );
      return `\n\n---\n\n# ${e.data.title}\n${desc}URL: ${urlFor(e.id)}\n\n${raw}`;
    })
  );
  const body = sections.join('');

  return new Response(head + body, {
    headers: { 'Content-Type': 'text/plain; charset=utf-8' },
  });
};
