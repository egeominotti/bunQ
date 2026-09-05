import sitemap, { ChangeFreqEnum } from '@astrojs/sitemap';

/** Keep existing route priorities and real git dates when adding public reference pages. */
export function documentationSitemap(
  lastmodForUrl: (url: string) => string | undefined,
  customPages: string[]
) {
  return sitemap({
    customPages,
    serialize(item) {
      const lastmod = lastmodForUrl(item.url);
      if (lastmod) item.lastmod = lastmod;

      const url = new URL(item.url).pathname;

      // Homepage - highest priority
      if (url === '/' || url === '') {
        item.priority = 1.0;
        item.changefreq = ChangeFreqEnum.WEEKLY;
      }
      // Getting started guides - high priority
      else if (url.match(/^\/(guide\/(introduction|installation|quickstart))\//)) {
        item.priority = 0.9;
        item.changefreq = ChangeFreqEnum.WEEKLY;
      }
      // MCP, workflow, benchmarks, comparison, use-cases, and entry points for new readers.
      else if (
        url.match(
          /^\/(guide\/(mcp|workflow|benchmarks|comparison|use-cases|simple-mode|sdks|migration))\//
        )
      ) {
        item.priority = 0.9;
        item.changefreq = ChangeFreqEnum.WEEKLY;
      }
      // Core SDK docs and API reference - high priority
      else if (url.match(/^\/(guide\/(queue|worker|flow|server|cron|dlq)|api)\//)) {
        item.priority = 0.8;
        item.changefreq = ChangeFreqEnum.WEEKLY;
      }
      // Advanced guides, integrations, performance - medium priority
      else if (url.match(/^\/(guide\/|architecture\/)/)) {
        item.priority = 0.7;
        item.changefreq = ChangeFreqEnum.MONTHLY;
      }
      // Examples, migration, FAQ - medium priority
      else if (url.match(/^\/(examples|faq|troubleshooting)\//)) {
        item.priority = 0.6;
        item.changefreq = ChangeFreqEnum.MONTHLY;
      }
      // Blog posts - good for SEO, frequent updates
      else if (url.match(/^\/blog\//)) {
        item.priority = 0.7;
        item.changefreq = ChangeFreqEnum.WEEKLY;
      }
      // Changelog, security, contributing, and versioned references - lower priority
      else {
        item.priority = 0.5;
        item.changefreq = ChangeFreqEnum.MONTHLY;
      }

      return item;
    },
  });
}
