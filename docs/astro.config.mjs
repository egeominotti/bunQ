import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import sitemap from '@astrojs/sitemap';
import react from '@astrojs/react';
import { fileURLToPath } from 'url';
import path from 'path';
import { readFileSync } from 'fs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Read version from root package.json (with fallback for Vercel builds)
let pkg = { version: '0.0.0' };
try {
  pkg = JSON.parse(readFileSync(path.resolve(__dirname, '../package.json'), 'utf-8'));
} catch {
  // Fallback: try reading version from docs package.json
  try {
    pkg = JSON.parse(readFileSync(path.resolve(__dirname, 'package.json'), 'utf-8'));
  } catch {}
}

export default defineConfig({
  site: 'https://bunqueue.dev',

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
      title: 'bunqueue',
      description: 'High-performance job queue for Bun & AI agents. Native MCP server, SQLite persistence, DLQ, cron jobs, S3 backups. Zero external dependencies.',
      logo: {
        src: './src/assets/logo.svg',
        replacesTitle: true,
      },
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/egeominotti/bunqueue' },
        { icon: 'npm', label: 'npm', href: 'https://www.npmjs.com/package/bunqueue' },
      ],
      components: {
        Header: './src/components/Header.astro',
      },
      editLink: {
        baseUrl: 'https://github.com/egeominotti/bunqueue/edit/main/docs/',
      },
      expressiveCode: {
        themes: ['github-light', 'github-dark'],
        styleOverrides: {
          borderRadius: '8px',
          borderColor: '#e4e4e7',
        },
      },
      customCss: [
        '@fontsource/inter/400.css',
        '@fontsource/inter/500.css',
        '@fontsource/inter/600.css',
        '@fontsource/inter/700.css',
        './src/styles/custom.css',
      ],
      defaultLocale: 'root',
      locales: {
        root: { label: 'English', lang: 'en' },
      },
      sidebar: [
        {
          label: 'Getting Started',
          items: [
            { label: 'Introduction', link: '/guide/introduction/' },
            { label: 'Installation', link: '/guide/installation/' },
            { label: 'Quick Start', link: '/guide/quickstart/' },
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
          label: 'Architecture',
          collapsed: true,
          items: [
            { label: 'Overview', link: '/architecture/' },
            { label: 'Client SDK', link: '/architecture/client-sdk/' },
            { label: 'Domain Layer', link: '/architecture/domain-layer/' },
            { label: 'Application Layer', link: '/architecture/application-layer/' },
            { label: 'TCP Protocol', link: '/architecture/tcp-protocol/' },
            { label: 'Persistence', link: '/architecture/persistence/' },
            { label: 'Data Structures', link: '/architecture/data-structures/' },
            { label: 'Cron Scheduler', link: '/architecture/cron-scheduler/' },
          ],
        },
        {
          label: 'Client SDK',
          items: [
            { label: 'Simple Mode', link: '/guide/simple-mode/' },
            { label: 'Workflow Engine', link: '/guide/workflow/' },
            { label: 'Queue', link: '/guide/queue/' },
            { label: 'Worker', link: '/guide/worker/' },
            { label: 'CPU-Intensive Workers', link: '/guide/cpu-intensive-workers/' },
            { label: 'Queue Group', link: '/guide/queue-group/' },
            { label: 'Flow Producer', link: '/guide/flow/' },
            { label: 'Stall Detection', link: '/guide/stall-detection/' },
            { label: 'Dead Letter Queue', link: '/guide/dlq/' },
          ],
        },
        {
          label: 'Server Mode',
          items: [
            { label: 'Running the Server', link: '/guide/server/' },
            { label: 'Configuration File', link: '/guide/configuration/' },
            { label: 'CLI Commands', link: '/guide/cli/' },
            { label: 'Environment Variables', link: '/guide/env-vars/' },
            { label: 'HTTP API', link: '/api/http/' },
            { label: 'TCP Protocol', link: '/api/tcp/' },
          ],
        },
        {
          label: 'Advanced',
          items: [
            { label: 'Cron Jobs', link: '/guide/cron/' },
            { label: 'S3 Backup', link: '/guide/backup/' },
            { label: 'Rate Limiting', link: '/guide/rate-limiting/' },
            { label: 'Webhooks', link: '/guide/webhooks/' },
          ],
        },
        {
          label: 'Performance',
          items: [
            { label: 'Benchmarks', link: '/guide/benchmarks/' },
            { label: 'bunqueue vs BullMQ', link: '/guide/comparison/' },
          ],
        },
        {
          label: 'Production',
          items: [
            { label: 'Deployment Guide', link: '/guide/deployment/' },
            { label: 'Databases (Postgres/MySQL)', link: '/guide/databases/' },
            { label: 'Native TLS', link: '/guide/tls/' },
            { label: 'Monitoring', link: '/guide/monitoring/' },
            { label: 'Telemetry', link: '/guide/telemetry/' },
          ],
        },
        {
          label: 'Integrations',
          items: [
            { label: 'Overview', link: '/guide/integrations/' },
            { label: 'Hono', link: '/guide/hono/' },
            { label: 'Elysia', link: '/guide/elysia/' },
            { label: 'IoT & Edge (MQTT)', link: '/guide/iot-edge/' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'TypeScript Types', link: '/api/types/' },
            { label: 'Examples', link: '/examples/' },
            { label: 'Migration from BullMQ', link: '/guide/migration/' },
          ],
        },
        {
          label: 'Blog',
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
            { label: 'Workflow Engine: Orchestration Without Temporal', link: '/blog/workflow-engine/' },
          ],
        },
        {
          label: 'Resources',
          items: [
            { label: 'FAQ', link: '/faq/' },
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
            content: 'bun, job queue, message queue, task queue, background jobs, sqlite, redis alternative, bullmq alternative, typescript, cron, scheduler, worker, dlq, dead letter queue, ai agents, mcp server, model context protocol, agentic workflows, claude, cursor, windsurf, ai automation, ai task scheduler, llm tools, workflow engine, orchestration, saga pattern, compensation, human in the loop, step functions alternative, temporal alternative, inngest alternative, multi-step workflows, branching workflows',
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
            content: 'bunqueue - High-performance job queue for Bun & AI agents. Native MCP server.',
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
            name: 'twitter:image',
            content: 'https://bunqueue.dev/og-image.png',
          },
        },
        {
          tag: 'meta',
          attrs: {
            name: 'twitter:creator',
            content: '@egeominotti',
          },
        },
        {
          tag: 'meta',
          attrs: {
            name: 'twitter:site',
            content: '@egeominotti',
          },
        },
        {
          tag: 'meta',
          attrs: {
            name: 'twitter:image:alt',
            content: 'bunqueue - High-performance job queue for Bun & AI agents',
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
            description: 'High-performance job queue for Bun & AI agents. Workflow engine with saga compensation and branching. Native MCP server, SQLite persistence, DLQ, cron jobs, S3 backups.',
            applicationCategory: 'DeveloperApplication',
            operatingSystem: 'Cross-platform',
            softwareVersion: pkg.version,
            datePublished: '2024-01-01',
            dateModified: new Date().toISOString().split('T')[0],
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
            programmingLanguage: ['TypeScript', 'JavaScript'],
            runtimePlatform: 'Bun',
            keywords: ['job queue', 'message queue', 'bun', 'sqlite', 'typescript', 'bullmq alternative', 'ai agents', 'mcp server', 'agentic workflows', 'workflow engine', 'saga pattern', 'orchestration', 'temporal alternative', 'step functions alternative'],
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
            description: 'Official documentation for bunqueue - High-performance job queue for Bun & AI agents',
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
        // Set lastmod for all pages
        item.lastmod = new Date();

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
