import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import sitemap from '@astrojs/sitemap';
import react from '@astrojs/react';
import { unified } from '@astrojs/markdown-remark';
import { fileURLToPath } from 'url';
import path from 'path';
import { readFileSync } from 'fs';
import { execSync } from 'child_process';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Read version from root package.json (with fallback for Vercel builds)
let pkg = { version: '0.0.0' };
try {
  pkg = JSON.parse(readFileSync(path.resolve(__dirname, '../package.json'), 'utf-8'));
} catch {
  // Fallback: try reading version from docs package.json
  try {
    pkg = JSON.parse(readFileSync(path.resolve(__dirname, 'package.json'), 'utf-8'));
  } catch {
    // Keep the explicit 0.0.0 fallback when neither manifest is available.
  }
}

// Real per-page lastmod for the sitemap, from git history. A fake
// `lastmod = new Date()` on every build teaches Google the field is
// unreliable and hurts crawl prioritization; pages with no known date
// simply omit lastmod. Keys are repo-relative paths.
const gitLastmod = {};
try {
  const out = execSync('git log --format=%x00%cI --name-only -- src/content/docs', {
    cwd: __dirname,
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024,
  });
  let current = null;
  for (const line of out.split('\n')) {
    if (line.startsWith('\x00')) current = line.slice(1).trim();
    else if (line.trim() && !(line.trim() in gitLastmod)) gitLastmod[line.trim()] = current;
  }
} catch {
  // shallow clone or no git: sitemap entries just omit lastmod
}

// Newest doc commit date, for a REAL SoftwareApplication.dateModified (never a
// build-time now(), which would churn the field on every build like the fake
// sitemap lastmod we deliberately avoid above).
const latestDocsDate = Object.values(gitLastmod).sort().pop();

function lastmodForUrl(url) {
  const rel = url.replace('https://bunqueue.dev', '').replace(/^\//, '').replace(/\/$/, '');
  const base = rel === '' ? 'index' : rel;
  for (const candidate of [`${base}.mdx`, `${base}.md`, `${rel}/index.mdx`, `${rel}/index.md`]) {
    const date = gitLastmod[`docs/src/content/docs/${candidate}`];
    if (date) return date;
  }
  return undefined;
}

export default defineConfig({
  site: 'https://bunqueue.dev',

  // The processor renders .md pages only. Starlight's MDX pipeline instead
  // reads the legacy top-level `markdown.gfm` flag, which has no default in
  // Astro 6 (z.boolean().optional()) — leave it out and every GFM table in a
  // .mdx page is emitted as literal |---| text. Keep both the processor and
  // the explicit flag until MDX inherits processor options. Do NOT follow the
  // deprecation warning's advice to move `gfm` onto unified() only: MDX
  // ignores processor options entirely and .mdx tables would break again.
  markdown: {
    gfm: true,
    processor: unified({ gfm: true }),
  },

  // Performance optimizations
  compressHTML: true,
  build: {
    inlineStylesheets: 'auto',
  },
  prefetch: {
    prefetchAll: true,
    defaultStrategy: 'viewport',
  },
  vite: {
    resolve: {
      alias: {
        '@components': path.resolve(__dirname, 'src/components'),
        '@root': path.resolve(__dirname, '..'),
      },
    },
    build: {},
  },

  integrations: [
    react(),
    starlight({
      // The docs collection provides its own /404 page. Let Starlight's
      // catch-all render that entry instead of also injecting a competing
      // framework route for the same path.
      disable404Route: true,
      title: 'bunqueue',
      description:
        'High-performance job queue for Bun & AI agents. Native MCP server, SQLite persistence, DLQ, cron jobs, S3 backups. Zero external infrastructure.',
      logo: {
        src: './src/assets/logo.svg',
        replacesTitle: false,
        alt: 'bunqueue',
      },
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/egeominotti/bunqueue' },
        { icon: 'npm', label: 'npm', href: 'https://www.npmjs.com/package/bunqueue' },
      ],
      components: {
        Header: './src/components/Header.astro',
        Head: './src/components/Head.astro',
        MobileMenuFooter: './src/components/MobileMenuFooter.astro',
        MarkdownContent: './src/components/MarkdownContent.astro',
        Footer: './src/components/Footer.astro',
      },
      editLink: {
        baseUrl: 'https://github.com/egeominotti/bunqueue/edit/main/docs/',
      },
      expressiveCode: {
        themes: ['catppuccin-latte', 'catppuccin-mocha'],
        styleOverrides: {
          borderRadius: '12px',
          borderColor: 'var(--bq-line, #e4e4e7)',
          codeFontFamily: "'IBM Plex Mono', ui-monospace, 'SF Mono', monospace",
          codeFontSize: '0.95rem',
          codeLineHeight: '1.8',
          codePaddingInline: '1.3rem',
          codePaddingBlock: '1rem',
          uiFontFamily: "'IBM Plex Mono', ui-monospace, monospace",
          frames: {
            frameBoxShadowCssValue: '0 6px 24px rgba(0, 0, 0, 0.08)',
          },
        },
      },
      customCss: [
        '@fontsource-variable/bricolage-grotesque',
        '@fontsource/ibm-plex-mono/400.css',
        '@fontsource/ibm-plex-mono/500.css',
        '@fontsource/inter/400.css',
        '@fontsource/inter/500.css',
        '@fontsource/inter/600.css',
        '@fontsource/inter/700.css',
        '@fontsource/inter/800.css',
        './src/styles/custom.css',
      ],
      defaultLocale: 'root',
      locales: {
        root: { label: 'English', lang: 'en' },
      },
      sidebar: [
        {
          label: 'Start Here',
          items: [
            { label: 'Introduction', link: '/guide/introduction/' },
            { label: 'Installation', link: '/guide/installation/' },
            { label: 'Quick Start', link: '/guide/quickstart/' },
            { label: 'Migrate from BullMQ', link: '/guide/migration/' },
            { label: 'FAQ', link: '/faq/' },
          ],
        },
        {
          label: 'Core Guide',
          items: [
            { label: 'Simple Mode', link: '/guide/simple-mode/' },
            { label: 'Queue', link: '/guide/queue/' },
            { label: 'Worker', link: '/guide/worker/' },
            { label: 'Cron Jobs', link: '/guide/cron/' },
            { label: 'Dead Letter Queue', link: '/guide/dlq/' },
            { label: 'Flow Producer', link: '/guide/flow/' },
          ],
        },
        {
          label: 'Workflow Engine',
          items: [
            { label: 'Overview', link: '/guide/workflow/' },
            { label: 'Quick Start', link: '/guide/workflow/quickstart/' },
            { label: 'Steps & Control Flow', link: '/guide/workflow/steps/' },
            { label: 'Rollback (Saga)', link: '/guide/workflow/rollback/' },
            { label: 'Durability & Idempotency', link: '/guide/workflow/durability/' },
            { label: 'Human Approval', link: '/guide/workflow/approval/' },
            { label: 'AI Agents (Vercel AI SDK)', link: '/guide/workflow/ai-agents/' },
            { label: 'Agent SDK Integrations', link: '/guide/workflow/agent-sdks/' },
            { label: 'API Reference', link: '/guide/workflow/api/' },
          ],
        },
        {
          label: 'SDKs · Any Language',
          items: [
            { label: 'TypeScript, Python, PHP, Go, Rust & Elixir', link: '/guide/sdks/' },
            { label: 'SDK Performance', link: '/guide/sdk-benchmarks/' },
          ],
        },
        {
          label: 'Run in Production',
          items: [
            { label: 'Running the Server', link: '/guide/server/' },
            { label: 'Deployment Guide', link: '/guide/deployment/' },
            { label: 'Configuration File', link: '/guide/configuration/' },
            { label: 'Environment Variables', link: '/guide/env-vars/' },
            { label: 'Native TLS', link: '/guide/tls/' },
            { label: 'Monitoring', link: '/guide/monitoring/' },
            { label: 'Telemetry', link: '/guide/telemetry/' },
            { label: 'S3 Backup', link: '/guide/backup/' },
            { label: 'Databases (Postgres/MySQL)', link: '/guide/databases/' },
            { label: 'Production Operations', link: '/guide/production/' },
          ],
        },
        {
          label: 'Advanced',
          items: [
            { label: 'Queue Group', link: '/guide/queue-group/' },
            { label: 'CPU-Intensive Workers', link: '/guide/cpu-intensive-workers/' },
            { label: 'Stall Detection', link: '/guide/stall-detection/' },
            { label: 'Rate Limiting', link: '/guide/rate-limiting/' },
            { label: 'Webhooks', link: '/guide/webhooks/' },
            { label: 'IoT & Edge (MQTT)', link: '/guide/iot-edge/' },
          ],
        },
        {
          label: 'Integrations',
          items: [
            { label: 'Overview', link: '/guide/integrations/' },
            { label: 'Hono', link: '/guide/hono/' },
            { label: 'Elysia', link: '/guide/elysia/' },
          ],
        },
        {
          label: 'AI Agents (MCP)',
          items: [
            { label: 'MCP Server', link: '/guide/mcp/' },
            { label: 'Production Patterns', link: '/guide/use-cases/' },
          ],
        },
        {
          label: 'Performance',
          items: [
            { label: 'Benchmarks', link: '/guide/benchmarks/' },
            { label: 'SDK Performance', link: '/guide/sdk-benchmarks/' },
            { label: 'bunqueue vs BullMQ', link: '/guide/comparison/' },
          ],
        },
        {
          label: 'Reference',
          collapsed: true,
          items: [
            { label: 'API Reference (by version)', link: '/reference/' },
            { label: 'CLI Commands', link: '/guide/cli/' },
            { label: 'HTTP API', link: '/api/http/' },
            { label: 'TCP Protocol', link: '/api/tcp/' },
            { label: 'TypeScript Types', link: '/api/types/' },
            { label: 'Glossary', link: '/guide/glossary/' },
            { label: 'Examples', link: '/examples/' },
          ],
        },
        {
          label: 'Architecture',
          collapsed: true,
          items: [
            { label: 'Overview', link: '/architecture/' },
            { label: 'Client SDK', link: '/architecture/client-sdk/' },
            { label: 'Domain Layer', link: '/architecture/domain-layer/' },
            { label: 'Application Layer', link: '/architecture/application-layer/' },
            { label: 'TCP Protocol', link: '/architecture/tcp-protocol/' },
            { label: 'Persistence', link: '/architecture/persistence/' },
            { label: 'Model-Based Testing', link: '/architecture/model-based-testing/' },
            { label: 'Data Structures', link: '/architecture/data-structures/' },
            { label: 'Cron Scheduler', link: '/architecture/cron-scheduler/' },
          ],
        },
        {
          label: 'Blog',
          collapsed: true,
          items: [
            { label: 'All Posts', link: '/blog/' },
            { label: 'Why bunqueue: SQLite Over Redis', link: '/blog/why-bunqueue/' },
            { label: 'Getting Started in 5 Minutes', link: '/blog/getting-started-five-minutes/' },
            { label: 'Sharding Architecture Deep Dive', link: '/blog/sharding-deep-dive/' },
            { label: 'bunqueue vs BullMQ Benchmarks', link: '/blog/benchmarks-vs-bullmq/' },
            { label: 'Reliable Workers & Stall Detection', link: '/blog/reliable-workers/' },
            { label: 'Dead Letter Queues', link: '/blog/dead-letter-queues/' },
            { label: 'Cron Jobs & Scheduling', link: '/blog/cron-scheduling/' },
            { label: 'Auto-Batching: 3x Throughput', link: '/blog/auto-batching/' },
            { label: 'Production Deployment', link: '/blog/production-deployment/' },
            { label: 'Hono & Elysia Integrations', link: '/blog/framework-integrations/' },
            { label: 'S3 Backup & Disaster Recovery', link: '/blog/s3-backup-recovery/' },
            { label: 'Job Pipelines with FlowProducer', link: '/blog/job-pipelines-flows/' },
            { label: 'Rate Limiting & Concurrency', link: '/blog/rate-limiting-concurrency/' },
            {
              label: 'Workflow Engine: Orchestration Without Temporal',
              link: '/blog/workflow-engine/',
            },
          ],
        },
        {
          label: 'Resources',
          collapsed: true,
          items: [
            { label: 'Troubleshooting', link: '/troubleshooting/' },
            { label: 'Changelog', link: '/changelog/' },
            { label: 'Security', link: '/security/' },
            { label: 'Contributing', link: '/contributing/' },
          ],
        },
      ],
      head: [
        // Primary Meta Tags
        {
          tag: 'meta',
          attrs: {
            name: 'keywords',
            content:
              'bun, job queue, message queue, task queue, background jobs, sqlite, redis alternative, bullmq alternative, typescript, cron, scheduler, worker, dlq, dead letter queue, ai agents, mcp server, model context protocol, agentic workflows, claude, cursor, windsurf, ai automation, ai task scheduler, llm tools, workflow engine, orchestration, saga pattern, compensation, human in the loop, step functions alternative, temporal alternative, inngest alternative, multi-step workflows, branching workflows',
          },
        },
        {
          tag: 'meta',
          attrs: {
            name: 'author',
            content: 'egeominotti',
          },
        },
        {
          tag: 'meta',
          attrs: {
            name: 'robots',
            content: 'index, follow, max-image-preview:large, max-snippet:-1, max-video-preview:-1',
          },
        },
        {
          tag: 'meta',
          attrs: {
            name: 'googlebot',
            content: 'index, follow',
          },
        },
        {
          tag: 'meta',
          attrs: {
            name: 'bingbot',
            content: 'index, follow',
          },
        },
        // Open Graph (title/description/url auto-generated by Starlight from frontmatter)
        {
          tag: 'meta',
          attrs: {
            property: 'og:type',
            content: 'website',
          },
        },
        {
          tag: 'meta',
          attrs: {
            property: 'og:site_name',
            content: 'bunqueue',
          },
        },
        {
          tag: 'meta',
          attrs: {
            property: 'og:image',
            content: 'https://bunqueue.dev/og-image.png',
          },
        },
        {
          tag: 'meta',
          attrs: {
            property: 'og:image:width',
            content: '1200',
          },
        },
        {
          tag: 'meta',
          attrs: {
            property: 'og:image:height',
            content: '630',
          },
        },
        {
          tag: 'meta',
          attrs: {
            property: 'og:locale',
            content: 'en_US',
          },
        },
        {
          tag: 'meta',
          attrs: {
            property: 'og:image:alt',
            content:
              'bunqueue - High-performance job queue for Bun & AI agents. Native MCP server.',
          },
        },
        // Twitter
        {
          tag: 'meta',
          attrs: {
            name: 'twitter:card',
            content: 'summary_large_image',
          },
        },
        {
          tag: 'meta',
          attrs: {
            name: 'twitter:creator',
            content: '@bunqueue',
          },
        },
        {
          tag: 'meta',
          attrs: {
            name: 'twitter:site',
            content: '@bunqueue',
          },
        },
        // JSON-LD Structured Data - SoftwareApplication
        {
          tag: 'script',
          attrs: {
            type: 'application/ld+json',
          },
          content: JSON.stringify({
            '@context': 'https://schema.org',
            '@type': 'SoftwareApplication',
            name: 'bunqueue',
            alternateName: 'bunQ',
            description:
              'High-performance job queue for Bun & AI agents. Workflow engine with saga compensation and branching. Native MCP server, SQLite persistence, DLQ, cron jobs, S3 backups.',
            applicationCategory: 'DeveloperApplication',
            operatingSystem: 'Cross-platform',
            softwareVersion: pkg.version,
            datePublished: '2024-01-01',
            dateModified: (latestDocsDate ?? '2026-07-12T00:00:00Z').split('T')[0],
            license: 'https://opensource.org/licenses/MIT',
            offers: {
              '@type': 'Offer',
              price: '0',
              priceCurrency: 'USD',
            },
            author: {
              '@type': 'Person',
              name: 'egeominotti',
              url: 'https://github.com/egeominotti',
            },
            publisher: {
              '@type': 'Person',
              name: 'egeominotti',
            },
            codeRepository: 'https://github.com/egeominotti/bunqueue',
            downloadUrl: 'https://www.npmjs.com/package/bunqueue',
            installUrl: 'https://www.npmjs.com/package/bunqueue',
            url: 'https://bunqueue.dev',
            softwareHelp: {
              '@type': 'CreativeWork',
              url: 'https://bunqueue.dev/guide/quickstart/',
            },
            sameAs: [
              'https://github.com/egeominotti/bunqueue',
              'https://www.npmjs.com/package/bunqueue',
              'https://www.npmjs.com/package/bunqueue-client',
            ],
            programmingLanguage: ['TypeScript', 'JavaScript'],
            runtimePlatform: 'Bun',
            keywords: [
              'job queue',
              'message queue',
              'bun',
              'sqlite',
              'typescript',
              'bullmq alternative',
              'ai agents',
              'mcp server',
              'agentic workflows',
              'workflow engine',
              'saga pattern',
              'orchestration',
              'temporal alternative',
              'step functions alternative',
            ],
          }),
        },
        // JSON-LD Structured Data - WebSite
        {
          tag: 'script',
          attrs: {
            type: 'application/ld+json',
          },
          content: JSON.stringify({
            '@context': 'https://schema.org',
            '@type': 'WebSite',
            name: 'bunqueue Documentation',
            url: 'https://bunqueue.dev/',
            description:
              'Official documentation for bunqueue - High-performance job queue for Bun & AI agents',
          }),
        },
        // Canonical will be auto-generated by Starlight
        // Theme color for mobile browsers
        {
          tag: 'meta',
          attrs: {
            name: 'theme-color',
            content: '#db2777',
          },
        },
        // Apple touch icon
        {
          tag: 'link',
          attrs: {
            rel: 'apple-touch-icon',
            href: '/apple-touch-icon.png',
          },
        },
        // Web App Manifest
        {
          tag: 'link',
          attrs: {
            rel: 'manifest',
            href: '/manifest.webmanifest',
          },
        },
        // DNS prefetch for external resources
        {
          tag: 'link',
          attrs: {
            rel: 'dns-prefetch',
            href: 'https://github.com',
          },
        },
        // :has() fallback for older Safari/Firefox (adds .bq-has-hero on main)
        {
          tag: 'script',
          attrs: {
            src: '/bq-compat.js',
            defer: true,
          },
        },
        // Preconnect to GitHub
        {
          tag: 'link',
          attrs: {
            rel: 'preconnect',
            href: 'https://github.com',
          },
        },
      ],
      lastUpdated: true,
      // Pagination enabled for better UX
      pagination: true,
      // Table of contents depth
      tableOfContents: { minHeadingLevel: 2, maxHeadingLevel: 3 },
    }),
    sitemap({
      serialize(item) {
        const lastmod = lastmodForUrl(item.url);
        if (lastmod) item.lastmod = lastmod;

        const url = item.url.replace('https://bunqueue.dev', '');

        // Homepage - highest priority
        if (url === '/' || url === '') {
          item.priority = 1.0;
          item.changefreq = 'weekly';
        }
        // Getting started guides - high priority
        else if (url.match(/^\/(guide\/(introduction|installation|quickstart))\//)) {
          item.priority = 0.9;
          item.changefreq = 'weekly';
        }
        // MCP, workflow, benchmarks, comparison, use-cases - high priority landing pages
        else if (url.match(/^\/(guide\/(mcp|workflow|benchmarks|comparison|use-cases))\//)) {
          item.priority = 0.9;
          item.changefreq = 'weekly';
        }
        // Core SDK docs and API reference - high priority
        else if (url.match(/^\/(guide\/(queue|worker|flow|server|cron|dlq)|api)\//)) {
          item.priority = 0.8;
          item.changefreq = 'weekly';
        }
        // Advanced guides, integrations, performance - medium priority
        else if (url.match(/^\/(guide\/|architecture\/)/)) {
          item.priority = 0.7;
          item.changefreq = 'monthly';
        }
        // Examples, migration, FAQ - medium priority
        else if (url.match(/^\/(examples|faq|troubleshooting)\//)) {
          item.priority = 0.6;
          item.changefreq = 'monthly';
        }
        // Blog posts - good for SEO, frequent updates
        else if (url.match(/^\/blog\//)) {
          item.priority = 0.7;
          item.changefreq = 'weekly';
        }
        // Changelog, security, contributing - lower priority
        else {
          item.priority = 0.5;
          item.changefreq = 'monthly';
        }

        return item;
      },
    }),
  ],
});
