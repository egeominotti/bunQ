/**
 * bunqueue social cover generator: og images from hand-built SVG,
 * matching the site design system (dark surface, brand pink, Inter 800
 * display, IBM Plex Mono eyebrows, the simulator lane/chip motif).
 *
 * Usage (from docs/scripts/):
 *   bun generate-og.ts
 *   output lands in ./out/, copy to docs/public/ (og-image.png + og/*.png).
 *
 * Fonts: static TrueType files are vendored under ./fonts (Inter + IBM Plex
 * Mono, both OFL). resvg only lays out text from TTF/OTF, it does NOT decode
 * the .woff/.woff2 the site ships, so these are kept alongside the script to
 * make a regen self-contained (no network, no manual download).
 */
import { Resvg } from '@resvg/resvg-js';
import { writeFileSync, mkdirSync } from 'fs';
import { covers, type Cover } from './og-covers';

const PINK = '#f472b6';
const PINK_DEEP = '#ec4899';
const BG = '#0a0a0d';
const SURFACE = '#131318';
const LINE = '#26262c';
const TEXT = '#fafafa';
const GRAY = '#a1a1aa';
const GRAY_D = '#5b5b64';

interface LaneChip {
  readonly x: number;
  readonly w: number;
  readonly on?: boolean;
}

interface LaneRow {
  readonly y: number;
  readonly chips: readonly LaneChip[];
}

const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

// stylized simulator lanes anchored bottom: three lanes with chips,
// one accented chip with glow
function lanes(): string {
  const rows: readonly LaneRow[] = [
    {
      y: 470,
      chips: [
        { x: 60, w: 130 },
        { x: 210, w: 96, on: true },
        { x: 326, w: 118 },
      ],
    },
    {
      y: 522,
      chips: [
        { x: 60, w: 88 },
        { x: 168, w: 140 },
      ],
    },
    {
      y: 574,
      chips: [
        { x: 60, w: 118 },
        { x: 198, w: 86 },
        { x: 304, w: 132 },
      ],
    },
  ];
  let out = '';
  for (const r of rows) {
    out += `<line x1="48" y1="${r.y + 17}" x2="640" y2="${r.y + 17}" stroke="${LINE}" stroke-width="1"/>`;
    for (const c of r.chips) {
      if (c.on) {
        out += `<rect x="${c.x}" y="${r.y}" width="${c.w}" height="34" rx="9" fill="${PINK_DEEP}" opacity="0.9" filter="url(#glow)"/>`;
      } else {
        out += `<rect x="${c.x}" y="${r.y}" width="${c.w}" height="34" rx="9" fill="${SURFACE}" stroke="${LINE}" stroke-width="1.4"/>`;
        out += `<rect x="${c.x + 12}" y="${r.y + 14}" width="${Math.max(28, c.w - 52)}" height="6" rx="3" fill="${GRAY_D}" opacity="0.55"/>`;
      }
    }
  }
  return out;
}

function statChips(stats: [string, string, string]): string {
  let x = 1152; // right-aligned, build backwards
  let out = '';
  for (let i = stats.length - 1; i >= 0; i--) {
    const label = stats[i];
    const w = Math.round(label.length * 11.6) + 40;
    x -= w;
    out += `<rect x="${x}" y="551" width="${w}" height="42" rx="21" fill="none" stroke="${LINE}" stroke-width="1.6"/>`;
    out += `<text x="${x + w / 2}" y="578" font-family="IBM Plex Mono" font-weight="600" font-size="18" fill="${GRAY}" text-anchor="middle">${esc(label)}</text>`;
    x -= 14;
  }
  return out;
}

function svg(c: Cover): string {
  const titleY1 = 268;
  const titleY2 = 366;
  return `<svg width="1200" height="630" viewBox="0 0 1200 630" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <radialGradient id="pinkGlow" cx="82%" cy="0%" r="85%">
      <stop offset="0%" stop-color="${PINK_DEEP}" stop-opacity="0.22"/>
      <stop offset="45%" stop-color="${PINK_DEEP}" stop-opacity="0.05"/>
      <stop offset="100%" stop-color="${PINK_DEEP}" stop-opacity="0"/>
    </radialGradient>
    <filter id="glow" x="-60%" y="-60%" width="220%" height="220%">
      <feGaussianBlur stdDeviation="7" result="b"/>
      <feMerge><feMergeNode in="b"/><feMergeNode in="SourceGraphic"/></feMerge>
    </filter>
    <linearGradient id="fade" x1="0" y1="0" x2="1" y2="0">
      <stop offset="0%" stop-color="${BG}" stop-opacity="0"/>
      <stop offset="100%" stop-color="${BG}" stop-opacity="1"/>
    </linearGradient>
  </defs>

  <rect width="1200" height="630" fill="${BG}"/>
  <rect width="1200" height="630" fill="url(#pinkGlow)"/>

  <!-- wordmark: the q mark (chip bowl, lane descender, one node leaving the queue) -->
  <rect x="60" y="56" width="44" height="44" rx="12" fill="${PINK_DEEP}"/>
  <g transform="translate(67.33,63.33) scale(0.611)">
    <g fill="none" stroke="#fff" stroke-width="4.7" stroke-linecap="round" stroke-linejoin="round">
      <rect x="13" y="8" width="20" height="20" rx="6.6"/>
      <path d="M31 26V39.5"/>
    </g>
    <circle cx="31" cy="40" r="4" fill="#fff"/>
  </g>
  <text x="120" y="88" font-family="Inter" font-weight="800" font-size="34" fill="${TEXT}">bunqueue</text>
  <text x="1140" y="86" font-family="IBM Plex Mono" font-weight="600" font-size="20" fill="${PINK}" text-anchor="end">bunqueue.dev</text>

  <!-- eyebrow -->
  <text x="62" y="176" font-family="IBM Plex Mono" font-weight="600" font-size="21" letter-spacing="4.5" fill="${PINK}">${esc(c.eyebrow.toUpperCase())}</text>

  <!-- title -->
  <text x="58" y="${titleY1}" font-family="Inter" font-weight="800" font-size="92" letter-spacing="-2.5" fill="${TEXT}">${esc(c.l1)}</text>
  <text x="58" y="${titleY2}" font-family="Inter" font-weight="800" font-size="92" letter-spacing="-2.5" fill="${TEXT}">${esc(c.l2pre ?? '')}<tspan fill="${PINK}">${esc(c.l2)}</tspan></text>

  <!-- sub -->
  <text x="60" y="424" font-family="Inter" font-weight="400" font-size="27" fill="${GRAY}">${esc(c.sub)}</text>

  <!-- simulator lanes motif -->
  ${lanes()}
  <rect x="480" y="450" width="240" height="170" fill="url(#fade)"/>

  <!-- footer -->
  ${statChips(c.stats)}
</svg>`;
}

const fontFiles = [
  'fonts/Inter-ExtraBold.ttf', // Inter 800: title + wordmark
  'fonts/Inter-Regular.ttf', // Inter 400: sub copy
  'fonts/IBMPlexMono-SemiBold.ttf', // Plex 600: eyebrow, stats, url
];

mkdirSync('out/og', { recursive: true });
for (const c of covers) {
  const r = new Resvg(svg(c), {
    fitTo: { mode: 'width', value: 2400 },
    font: { fontFiles, loadSystemFonts: false, defaultFontFamily: 'Inter' },
  });
  writeFileSync(`out/${c.file}.png`, r.render().asPng());
  console.log('ok', c.file);
}
