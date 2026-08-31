import { describe, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import ts from 'typescript';

const DOCS_ROOT = join(import.meta.dir, '../docs/src/content/docs/guide');
const GUIDES = {
  'queue/index.mdx': 2,
  'queue/adding-jobs.mdx': 5,
  'queue/deduplication.mdx': 4,
  'queue/querying.mdx': 2,
  'queue/control.mdx': 2,
  'queue/progress.mdx': 2,
  'queue/limits.mdx': 1,
  'rate-limiting.mdx': 5,
  'queue/job-groups.mdx': 0,
  'queue-group.mdx': 4,
  'queue/metrics.mdx': 1,
  'queue/advanced.mdx': 4,
  'queue/options.mdx': 1,
} as const;

const LANGUAGES = ['Bun', 'Node.js / Deno', 'Python', 'PHP', 'Go', 'Rust', 'Elixir'];

interface Snippet {
  code: string;
  language: string;
  label: string;
}

function groups(source: string): string[] {
  return [...source.matchAll(/<Tabs\b[^>]*syncKey=["']lang["'][^>]*>([\s\S]*?)<\/Tabs>/g)].map(
    (match) => match[1]
  );
}

function snippets(group: string): Snippet[] {
  return [
    ...group.matchAll(/<TabItem\b[^>]*label=["']([^"']+)["'][^>]*>([\s\S]*?)<\/TabItem>/g),
  ].map((match) => {
    const fence = match[2].match(/```([^\n]*)\n([\s\S]*?)```/);
    if (!fence) throw new Error(`Missing code fence for ${match[1]}`);
    return { label: match[1], language: fence[1].trim(), code: fence[2] };
  });
}

function duplicateTopLevelConstants(code: string): string[] {
  const source = ts.createSourceFile('snippet.ts', code, ts.ScriptTarget.Latest, true);
  if (source.parseDiagnostics.length > 0) {
    throw new Error(
      source.parseDiagnostics.map((diagnostic) => String(diagnostic.messageText)).join('; ')
    );
  }
  const names = new Set<string>();
  const duplicates = new Set<string>();
  for (const statement of source.statements) {
    if (!ts.isVariableStatement(statement)) continue;
    if (!(statement.declarationList.flags & (ts.NodeFlags.Const | ts.NodeFlags.Let))) continue;
    for (const declaration of statement.declarationList.declarations) {
      if (!ts.isIdentifier(declaration.name)) continue;
      if (names.has(declaration.name.text)) duplicates.add(declaration.name.text);
      names.add(declaration.name.text);
    }
  }
  return [...duplicates];
}

describe('Queue guide snippets', () => {
  test('accounts for every requested guide, language group, and code fence', () => {
    let groupCount = 0;
    let tabFenceCount = 0;
    let standaloneFenceCount = 0;
    for (const [guide, expectedGroups] of Object.entries(GUIDES)) {
      const source = readFileSync(join(DOCS_ROOT, guide), 'utf8');
      const guideGroups = groups(source);
      expect(guideGroups, guide).toHaveLength(expectedGroups);
      groupCount += guideGroups.length;
      for (const group of guideGroups) {
        const entries = snippets(group);
        expect(
          entries.map((entry) => entry.label),
          guide
        ).toEqual(LANGUAGES);
        tabFenceCount += entries.length;
      }
      const allFences = [...source.matchAll(/^```[^\n]*\n[\s\S]*?^```$/gm)].length;
      standaloneFenceCount += allFences - guideGroups.length * LANGUAGES.length;
    }
    expect(groupCount).toBe(33);
    expect(tabFenceCount).toBe(231);
    expect(standaloneFenceCount).toBe(12);
  });

  test('TypeScript fences do not redeclare block-scoped alternatives', () => {
    for (const guide of Object.keys(GUIDES)) {
      const source = readFileSync(join(DOCS_ROOT, guide), 'utf8');
      for (const [groupIndex, group] of groups(source).entries()) {
        for (const snippet of snippets(group).filter((entry) => entry.language === 'typescript')) {
          expect(
            duplicateTopLevelConstants(snippet.code),
            `${guide} group ${groupIndex + 1} (${snippet.label})`
          ).toEqual([]);
        }
      }
    }
  });

  test('TTL deduplication keys are not documented as custom job ids', () => {
    const source = readFileSync(join(DOCS_ROOT, 'queue/deduplication.mdx'), 'utf8');
    for (const group of groups(source).slice(1, 3)) {
      const elixir = snippets(group).find((entry) => entry.label === 'Elixir');
      expect(elixir).toBeDefined();
      expect(elixir?.code).not.toContain('deduplication:');
      expect(elixir?.code).toContain('jobId:');
    }
    const management = groups(source)[3];
    expect(management).toBeDefined();
    const networkExamples = snippets(management).filter((entry) => entry.label !== 'Bun');
    for (const snippet of networkExamples) {
      expect(snippet.code, snippet.label).not.toMatch(
        /getDeduplicationJobId|get_deduplication_job_id|stored as custom ids/i
      );
    }
    expect(source).not.toContain('Deduplication ids are stored as custom ids on the broker.');
  });
});
