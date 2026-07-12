import type { APIRoute, GetStaticPaths } from 'astro';
import { getCollection } from 'astro:content';
import { readFileSync } from 'node:fs';

/**
 * Per-page Open Graph card, generated at build time to /og/<slug>.png. Each doc
 * gets a distinct branded 1200x630 image (eyebrow + title + wordmark) instead of
 * sharing one generic card, which lifts CTR on social shares. Frontmatter
 * og:image points here (Starlight dedupes og:image, frontmatter wins).
 *
 * The heavy, native rendering deps (satori + resvg) and the font reads are
 * loaded lazily INSIDE the request path and inside try/catch. A font-read or
 * native-binding failure (e.g. a platform where resvg cannot load) therefore
 * degrades to the static fallback card and NEVER aborts the whole Astro build.
 */
const FALLBACK = 'public/og-image.png';

const el = (type: string, style: Record<string, unknown>, children: unknown) => ({
  type,
  props: { style, children },
});

// The bunqueue "q" mark as a self-contained data URI: chip bowl, lane
// descender, one node leaving the queue. Drawn as the wordmark lockup in the
// card footer. Geometry kept in sync with favicon.svg + .github/logo.svg.
const MARK_SVG =
  '<svg xmlns="http://www.w3.org/2000/svg" width="44" height="44" viewBox="0 0 48 48">' +
  '<defs><linearGradient id="bq" x1="0" y1="0" x2="1" y2="1">' +
  '<stop offset="0" stop-color="#ff4f9f"/><stop offset="1" stop-color="#d3156d"/></linearGradient></defs>' +
  '<rect width="48" height="48" rx="11" fill="url(#bq)"/>' +
  '<g transform="translate(8,8) scale(0.6667)">' +
  '<g fill="none" stroke="#fff" stroke-width="4.7" stroke-linecap="round" stroke-linejoin="round">' +
  '<rect x="13" y="8" width="20" height="20" rx="6.6"/><path d="M31 26V39.5"/></g>' +
  '<circle cx="31" cy="40" r="4" fill="#fff"/></g></svg>';
const MARK_URI = `data:image/svg+xml;base64,${Buffer.from(MARK_SVG).toString('base64')}`;

export const getStaticPaths: GetStaticPaths = async () => {
  const docs = await getCollection('docs');
  return docs
    .filter((e) => e.id !== '404')
    .map((e) => {
      const m = (e.body ?? '').match(/class="bq-eyebrow">([^<]+)</);
      const eyebrow = (m ? m[1] : e.id.replace(/\//g, ' · ')).trim();
      return { params: { slug: e.id }, props: { title: e.data.title, eyebrow } };
    });
};

type Renderer = {
  satori: (tree: unknown, opts: unknown) => Promise<string>;
  Resvg: new (svg: string, opts?: unknown) => { render: () => { asPng: () => Uint8Array } };
  fonts: unknown[];
};

let cache: Renderer | null | undefined;

async function renderer(): Promise<Renderer | null> {
  if (cache !== undefined) return cache;
  try {
    const [satoriMod, resvgMod] = await Promise.all([import('satori'), import('@resvg/resvg-js')]);
    const font = (w: number) =>
      readFileSync(`node_modules/@fontsource/inter/files/inter-latin-${w}-normal.woff`);
    cache = {
      satori: satoriMod.default as Renderer['satori'],
      Resvg: resvgMod.Resvg as unknown as Renderer['Resvg'],
      fonts: [
        { name: 'Inter', data: font(500), weight: 500, style: 'normal' },
        { name: 'Inter', data: font(700), weight: 700, style: 'normal' },
        { name: 'Inter', data: font(800), weight: 800, style: 'normal' },
      ],
    };
  } catch {
    cache = null;
  }
  return cache;
}

const fallback = () =>
  new Response(new Uint8Array(readFileSync(FALLBACK)), {
    headers: { 'Content-Type': 'image/png' },
  });

export const GET: APIRoute = async ({ props }) => {
  const r = await renderer();
  if (!r) return fallback();

  const title = String((props as { title: string }).title);
  const eyebrow = String((props as { eyebrow: string }).eyebrow);
  const size = title.length > 48 ? 54 : title.length > 34 ? 62 : 70;

  try {
    const tree = el(
      'div',
      {
        width: 1200,
        height: 630,
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'space-between',
        padding: 72,
        backgroundColor: '#141019',
        color: '#f2ecf6',
        fontFamily: 'Inter',
        overflow: 'hidden',
        backgroundImage:
          'radial-gradient(1100px 520px at 82% -12%, rgba(208,33,122,0.38), transparent)',
      },
      [
        el(
          'div',
          {
            display: 'flex',
            fontSize: 26,
            letterSpacing: 4,
            textTransform: 'uppercase',
            color: '#ff59a6',
            fontWeight: 600,
          },
          eyebrow,
        ),
        el(
          'div',
          {
            display: 'flex',
            fontSize: size,
            fontWeight: 800,
            lineHeight: 1.05,
            letterSpacing: -1.5,
            maxWidth: 1010,
            maxHeight: 360,
            overflow: 'hidden',
          },
          title,
        ),
        el(
          'div',
          { display: 'flex', alignItems: 'center', justifyContent: 'space-between', fontSize: 30 },
          [
            el('div', { display: 'flex', alignItems: 'center' }, [
              { type: 'img', props: { width: 42, height: 42, src: MARK_URI, style: { marginRight: 18 } } },
              el('div', { display: 'flex', fontWeight: 700 }, 'bunqueue'),
            ]),
            el('div', { display: 'flex', color: '#a89eb4', fontSize: 26 }, 'bunqueue.dev'),
          ],
        ),
      ],
    );

    const svg = await r.satori(tree, { width: 1200, height: 630, fonts: r.fonts });
    const png = new r.Resvg(svg, { fitTo: { mode: 'width', value: 1200 } }).render().asPng();
    return new Response(new Uint8Array(png), {
      headers: {
        'Content-Type': 'image/png',
        'Cache-Control': 'public, max-age=31536000, immutable',
      },
    });
  } catch {
    return fallback();
  }
};
