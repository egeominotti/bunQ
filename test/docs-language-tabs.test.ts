import { describe, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const DOCS_ROOT = join(import.meta.dir, '../docs/src/content/docs/guide');
const LANGUAGE_LABELS = ['Bun', 'Node.js / Deno', 'Python', 'PHP', 'Go', 'Rust', 'Elixir'];
const GUIDE_PATTERNS = [
  'queue/*.{md,mdx}',
  'worker/*.{md,mdx}',
  'cron/*.{md,mdx}',
  'dlq/*.{md,mdx}',
  'rate-limiting.mdx',
  'queue-group.md',
  'stall-detection.mdx',
  'cpu-intensive-workers.mdx',
];

function guideFiles(): string[] {
  const files = new Set<string>();
  for (const pattern of GUIDE_PATTERNS) {
    for (const file of new Bun.Glob(pattern).scanSync({ cwd: DOCS_ROOT })) files.add(file);
  }
  return [...files].sort();
}

describe('documented SDK language tabs', () => {
  const files = guideFiles();
  let languageGroupCount = 0;

  test('discovers every audited guide and language group', () => {
    expect(files).toHaveLength(33);
    expect(languageGroupCount).toBe(65);
  });

  for (const file of files) {
    const source = readFileSync(join(DOCS_ROOT, file), 'utf8');
    const groups = [
      ...source.matchAll(/<Tabs\b[^>]*syncKey=["']lang["'][^>]*>([\s\S]*?)<\/Tabs>/g),
    ];
    languageGroupCount += groups.length;

    for (const [index, group] of groups.entries()) {
      test(`${file} language group ${index + 1} contains every official SDK exactly once`, () => {
        const labels = [...group[1].matchAll(/<TabItem\b[^>]*label=["']([^"']+)["']/g)].map(
          (match) => match[1]
        );
        expect(labels).toEqual(LANGUAGE_LABELS);
      });
    }
  }
});
