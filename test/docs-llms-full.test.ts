import { describe, expect, test } from 'bun:test';
import { inlineRawCodeImports } from '../docs/src/lib/llms-full';

describe('inlineRawCodeImports', () => {
  test('replaces single-line and multiline Code components with the loaded source', async () => {
    const body = [
      "import { Code } from '@astrojs/starlight/components';",
      "import composeSource from '../../../../examples/demo/compose.yaml?raw';",
      "import scenarioSource from '../../../../examples/demo/scenario.ts?raw';",
      '',
      '<Code code={composeSource} lang="yaml" meta=\'title="compose.yaml"\' />',
      '<Code',
      '  code={scenarioSource}',
      '  lang="typescript"',
      '/>',
    ].join('\n');
    const sources: Record<string, string> = {
      '../../../../examples/demo/compose.yaml?raw': 'services:\n  app: {}\n',
      '../../../../examples/demo/scenario.ts?raw': "const result = 'PASS';\n",
    };

    const expanded = await inlineRawCodeImports(body, async (specifier) => sources[specifier]);

    expect(expanded).toContain('```yaml title="compose.yaml"\nservices:\n  app: {}\n```');
    expect(expanded).toContain("```typescript\nconst result = 'PASS';\n```");
    expect(expanded).not.toContain('?raw');
    expect(expanded).not.toContain('code={composeSource}');
    expect(expanded).toContain("import { Code } from '@astrojs/starlight/components';");
  });

  test('uses a longer fence when the imported source contains backticks', async () => {
    const body = 'import source from \'./sample.md?raw\';\n<Code code={source} lang="markdown" />';
    const expanded = await inlineRawCodeImports(body, () => '```ts\nconst value = 1;\n```\n');

    expect(expanded).toContain('````markdown\n```ts\nconst value = 1;\n```\n````');
  });

  test('fails closed when a raw import has no Code destination', async () => {
    const body = "import source from './sample.ts?raw';\n\nNothing renders it.";

    expect(inlineRawCodeImports(body, () => 'const hidden = true;')).rejects.toThrow(
      'source is not rendered by a Code component'
    );
  });

  test('does not execute raw imports or Code components shown inside a fenced example', async () => {
    const body = [
      '````mdx',
      '```ts',
      "import fakeSource from './fake.ts?raw';",
      '```',
      '<Code code={fakeSource} lang="typescript" />',
      '````',
      "import realSource from './real.ts?raw';",
      '<Code code={realSource} lang="typescript" />',
    ].join('\n');
    const loaded: string[] = [];
    const expanded = await inlineRawCodeImports(body, (specifier) => {
      loaded.push(specifier);
      return 'const real = true;';
    });

    expect(loaded).toEqual(['./real.ts?raw']);
    expect(expanded).toContain("import fakeSource from './fake.ts?raw';");
    expect(expanded).toContain('<Code code={fakeSource} lang="typescript" />');
    expect(expanded).toContain('```typescript\nconst real = true;\n```');
  });
});
