import { describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import {
  scanReferences,
  sortVersions,
  stripCode,
  versionKey,
} from '../scripts/check-docs-data';

/**
 * The guard behind `bun run check:docs-data`. Both directions of error are
 * expensive: a miss lets an untracked import reach main and break the docs build
 * in a clean checkout only, while a false positive turns main red over prose in a
 * code sample. Every case below is one of those two directions.
 */

describe('stripCode', () => {
  test('drops fenced blocks so documented imports are not treated as real ones', () => {
    const page = ['import real from "./real";', '```ts', 'import x from "./sample";', '```'].join(
      '\n'
    );
    expect(stripCode(page)).toContain('./real');
    expect(stripCode(page)).not.toContain('./sample');
  });

  test('handles tilde fences and does not let one style close the other', () => {
    const page = ['~~~js', 'import x from "./tilde";', '~~~', 'import y from "./after";'].join('\n');
    expect(stripCode(page)).not.toContain('./tilde');
    expect(stripCode(page)).toContain('./after');
  });

  test('an unterminated fence swallows the rest (a miss, never a false accusation)', () => {
    const page = ['```', 'import x from "./inside";'].join('\n');
    expect(stripCode(page)).not.toContain('./inside');
  });

  test('drops inline spans', () => {
    expect(stripCode('use `import x from "./inline"` here')).not.toContain('./inline');
  });

  test('longer fences close correctly and code never leaks out of a block', () => {
    fc.assert(
      fc.property(fc.integer({ min: 3, max: 6 }), (length) => {
        const fence = '`'.repeat(length);
        const page = [fence, 'import x from "./fenced";', fence, 'import y from "./tail";'].join(
          '\n'
        );
        const stripped = stripCode(page);
        return !stripped.includes('./fenced') && stripped.includes('./tail');
      })
    );
  });
});

describe('scanReferences', () => {
  test('covers every import form that a clean checkout must resolve', () => {
    const page = [
      'import data from "./a.json";',
      "export * from '../b';",
      "import './side-effect.css';",
      "const mod = await import('./dynamic.ts');",
      "const legacy = require('./legacy.js');",
    ].join('\n');
    const modules = scanReferences(page)
      .filter((reference) => reference.kind === 'module')
      .map((reference) => reference.specifier);
    expect(modules).toEqual([
      './a.json',
      '../b',
      './side-effect.css',
      './dynamic.ts',
      './legacy.js',
    ]);
  });

  test('classifies markdown images and src attributes as assets, not modules', () => {
    const references = scanReferences('![shot](./shot.png)\n<img src="./icon.svg" />');
    expect(references).toEqual([
      { specifier: './shot.png', kind: 'asset' },
      { specifier: './icon.svg', kind: 'asset' },
    ]);
  });

  test('ignores bare and aliased specifiers, which are never repo files', () => {
    const page = ['import { Queue } from "bunqueue";', 'import x from "~/components/x.astro";'].join(
      '\n'
    );
    expect(scanReferences(page)).toEqual([]);
  });

  test('never reports a specifier that only appears inside a code sample', () => {
    fc.assert(
      fc.property(
        fc.stringMatching(/^[a-z][a-z0-9-]{0,12}$/),
        fc.constantFrom('from', 'import', 'require'),
        (name, keyword) => {
          const sample =
            keyword === 'from'
              ? `import v from './${name}';`
              : keyword === 'import'
                ? `import './${name}';`
                : `require('./${name}');`;
          return scanReferences(['```ts', sample, '```'].join('\n')).length === 0;
        }
      )
    );
  });
});

describe('sortVersions', () => {
  test('newest minor first, with numeric (not lexicographic) minors', () => {
    expect(sortVersions(['v2.8', 'v2.10', 'v3.0'])).toEqual(['v3.0', 'v2.10', 'v2.8']);
  });

  test('a stray non-version directory sorts last instead of producing NaN order', () => {
    expect(sortVersions(['v2.8', 'legacy', 'v3.0'])).toEqual(['v3.0', 'v2.8', 'legacy']);
  });

  test('is a total order: independent of input order, and stable', () => {
    fc.assert(
      fc.property(
        fc.uniqueArray(
          fc.tuple(fc.integer({ min: 0, max: 30 }), fc.integer({ min: 0, max: 30 })),
          { minLength: 1, maxLength: 8 }
        ),
        fc.array(fc.constantFrom('legacy', 'draft', 'old'), { maxLength: 2 }),
        (pairs, strays) => {
          const names = [...pairs.map(([major, minor]) => `v${major}.${minor}`), ...new Set(strays)];
          const sorted = sortVersions(names);
          const shuffled = sortVersions([...names].reverse());
          return (
            sorted.length === names.length && JSON.stringify(sorted) === JSON.stringify(shuffled)
          );
        }
      )
    );
  });
});

describe('versionKey', () => {
  test('keeps the minor tree across patch releases', () => {
    expect(versionKey('2.8.47')).toBe('v2.8');
    expect(versionKey('2.8.48')).toBe('v2.8');
    expect(versionKey('2.9.0')).toBe('v2.9');
  });
});
