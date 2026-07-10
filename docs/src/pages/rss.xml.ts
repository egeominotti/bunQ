import type { APIRoute } from 'astro';
import { getCollection } from 'astro:content';

const SITE = 'https://bunqueue.dev';

function headContent(entry: any, property: string): string | undefined {
  for (const h of entry.data.head ?? []) {
    if (h.attrs?.property === property) return h.attrs.content;
  }
  return undefined;
}

const escapeXml = (s: string) =>
  s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');

export const GET: APIRoute = async () => {
  const entries = await getCollection(
    'docs',
    (e: any) => e.id.startsWith('blog/') && e.id !== 'blog'
  );

  const items = entries
    .map((e: any) => ({
      title: String(e.data.title).replace(/^"|"$/g, ''),
      description: String(e.data.description ?? ''),
      link: `${SITE}/${e.id}/`,
      date: headContent(e, 'article:published_time'),
    }))
    .sort((a, b) => new Date(b.date ?? 0).getTime() - new Date(a.date ?? 0).getTime());

  const xmlItems = items
    .map(
      (i) => `    <item>
      <title>${escapeXml(i.title)}</title>
      <link>${escapeXml(i.link)}</link>
      <guid isPermaLink="true">${escapeXml(i.link)}</guid>
      ${i.date ? `<pubDate>${new Date(i.date).toUTCString()}</pubDate>` : ''}
      <description>${escapeXml(i.description)}</description>
    </item>`
    )
    .join('\n');

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>bunqueue blog</title>
    <link>${SITE}/blog/</link>
    <atom:link href="${SITE}/rss.xml" rel="self" type="application/rss+xml"/>
    <description>Guides, benchmarks and architecture deep dives on bunqueue, the zero-infrastructure job queue for Bun.</description>
    <language>en</language>
${xmlItems}
  </channel>
</rss>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/rss+xml; charset=utf-8' },
  });
};
