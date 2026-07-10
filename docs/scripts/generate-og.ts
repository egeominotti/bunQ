/**
 * bunqueue social cover generator: og images from hand-built SVG,
 * matching the site design system (dark surface, brand pink, Inter 800
 * display, IBM Plex Mono eyebrows, the simulator lane/chip motif).
 *
 * Usage (from docs/scripts/):
 *   bun add @resvg/resvg-js
 *   download Inter static TTFs (rsms/inter release, extras/ttf/) and
 *   IBM Plex Mono TTFs (google/fonts ofl/ibmplexmono) into ./fonts/
 *   adjust fontFiles below, then: bun generate-og.ts
 *   output lands in ./out/, copy to docs/public/.
 */
import { Resvg } from '@resvg/resvg-js';
import { writeFileSync, mkdirSync } from 'fs';

const PINK = '#f472b6';
const PINK_DEEP = '#ec4899';
const BG = '#0a0a0d';
const SURFACE = '#131318';
const LINE = '#26262c';
const TEXT = '#fafafa';
const GRAY = '#a1a1aa';
const GRAY_D = '#5b5b64';

type Cover = {
  file: string;
  eyebrow: string;
  l1: string;
  l2: string; // accent segment rendered pink
  l2pre?: string; // optional white prefix on line 2
  sub: string;
  stats: [string, string, string];
};

const covers: Cover[] = [
  { file: 'og-image', eyebrow: 'job queue · zero infrastructure', l1: 'The queue', l2pre: 'is ', l2: 'a file.', sub: 'Priorities, retries, cron and DLQ in one process. No Redis.', stats: ['150K+ ops/sec', 'SQLite WAL', 'MIT'] },
  { file: 'og/getting-started', eyebrow: 'quickstart', l1: 'Working queue', l2pre: 'in ', l2: 'a minute.', sub: 'bun add bunqueue, write ten lines, run. Nothing to provision.', stats: ['1 dependency', '5.5 MB install', 'zero config'] },
  { file: 'og/queue', eyebrow: 'queue api', l1: 'Every job, exactly', l2pre: 'where it ', l2: 'belongs.', sub: 'Priorities, delays, deduplication, bulk and durable writes.', stats: ['idempotent IDs', 'auto-batching', 'BullMQ-familiar'] },
  { file: 'og/worker', eyebrow: 'workers', l1: 'Pull, process,', l2pre: 'ack, ', l2: 'repeat.', sub: 'Concurrency, heartbeats, stall detection, ACK batching.', stats: ['long polling', 'lock ownership', 'typed events'] },
  { file: 'og/server-mode', eyebrow: 'server mode', l1: 'One process', l2pre: 'serves ', l2: 'them all.', sub: 'TCP and HTTP, token auth, native TLS, Unix sockets.', stats: ['TCP :6789', 'HTTP :6790', 'native TLS'] },
  { file: 'og/client-sdk', eyebrow: 'sdks', l1: 'One protocol,', l2pre: 'every ', l2: 'runtime.', sub: 'Same Queue and Worker API on Node, Deno, Bun, Python, Workers.', stats: ['npm i bunqueue-client', '382 e2e scenarios', 'typed'] },
  { file: 'og/api-reference', eyebrow: 'api reference', l1: 'Every endpoint,', l2pre: '', l2: 'documented.', sub: 'TCP commands, HTTP routes and every type, spelled out.', stats: ['40+ commands', 'msgpack wire', 'JSON HTTP'] },
  { file: 'og/benchmarks', eyebrow: 'benchmarks', l1: 'Measured,', l2pre: 'not ', l2: 'marketed.', sub: 'Median of 3 runs, published methodology, reproducible.', stats: ['630K ops/s embedded', '3.5x BullMQ bulk', 'p99 < 1 ms'] },
  { file: 'og/integrations', eyebrow: 'integrations', l1: 'Plays well with', l2pre: 'your ', l2: 'stack.', sub: 'Hono, Elysia, MCP for AI agents, Prometheus, webhooks, S3.', stats: ['73 MCP tools', 'SSE + WebSocket', 'Prometheus'] },
  { file: 'og/advanced', eyebrow: 'advanced', l1: 'Sagas, cron,', l2pre: 'rate limits, ', l2: 'DLQ.', sub: 'Workflow engine with compensation, everything operational.', stats: ['saga rollback', 'cron + tz', 'S3 backups'] },
  { file: 'og/use-cases', eyebrow: 'production patterns', l1: 'What teams', l2pre: 'run ', l2: 'on it.', sub: 'Email, webhooks, payments, images, AI agents, cron.', stats: ['12+ patterns', 'saga rollback', '73 MCP tools'] },
  { file: 'og/workflow', eyebrow: 'workflow engine', l1: 'Multi-step, with', l2pre: 'a ', l2: 'rollback plan.', sub: 'Saga compensation, branching, signals and loops.', stats: ['11 event types', 'waitFor signals', '0 extra services'] },
  { file: 'og/production', eyebrow: 'guide · production', l1: 'What survives', l2pre: '', l2: 'a crash.', sub: 'At-least-once semantics, tested under kill -9. One file to back up.', stats: ['SIGKILL tested', 'fenced ACKs', 'S3 snapshots'] },
  { file: 'og/hono', eyebrow: 'integrations · hono', l1: 'Background jobs', l2pre: 'for ', l2: 'Hono.', sub: 'Return the response now, do the work in the background.', stats: ['embedded mode', 'zero config', 'typed'] },
  { file: 'og/elysia', eyebrow: 'integrations · elysia', l1: 'Background jobs', l2pre: 'for ', l2: 'Elysia.', sub: 'Return the response now, do the work in the background.', stats: ['embedded mode', 'zero config', 'typed'] },
];

const esc = (s: string) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

// stylized simulator lanes anchored bottom: three lanes with chips,
// one accented chip with glow
function lanes(): string {
  const rows = [
    { y: 470, chips: [{ x: 60, w: 130 }, { x: 210, w: 96, on: true }, { x: 326, w: 118 }] },
    { y: 522, chips: [{ x: 60, w: 88 }, { x: 168, w: 140 }] },
    { y: 574, chips: [{ x: 60, w: 118 }, { x: 198, w: 86 }, { x: 304, w: 132 }] },
  ];
  let out = '';
  for (const r of rows) {
    out += `<line x1="48" y1="${r.y + 17}" x2="640" y2="${r.y + 17}" stroke="${LINE}" stroke-width="1"/>`;
    for (const c of r.chips) {
      if ((c as any).on) {
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

  <!-- wordmark -->
  <rect x="60" y="56" width="44" height="44" rx="12" fill="${PINK_DEEP}"/>
  <rect x="71" y="68" width="22" height="5.5" rx="2.75" fill="#fff"/>
  <rect x="71" y="79" width="15" height="5.5" rx="2.75" fill="#fff" opacity="0.85"/>
  <rect x="71" y="90" width="19" height="5.5" rx="2.75" fill="#fff" opacity="0.7"/>
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
  'inter/extras/ttf/Inter-ExtraBold.ttf',
  'inter/extras/ttf/Inter-Regular.ttf',
  'PlexMono-SemiBold.ttf',
  'PlexMono-Regular.ttf',
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
