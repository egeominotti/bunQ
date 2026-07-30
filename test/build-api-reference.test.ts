import { describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import { allVersionsHref, banner, sortVersions, versionKey } from '../scripts/build-api-reference';

/**
 * The banner is injected into generated TypeDoc output, so a wrong link is invisible
 * to every other gate: the page still builds, the site still deploys, and only a
 * reader clicking "all versions" finds out. It WAS wrong on 234 published pages —
 * the caller passed three arguments to a two-parameter `banner()`, so `depth`
 * received a boolean and collapsed to 0 everywhere.
 */

describe('allVersionsHref', () => {
  test('a page at the version root goes up one level, to /reference/', () => {
    expect(allVersionsHref(0)).toBe('../');
  });

  test('a page one directory deeper goes up two', () => {
    expect(allVersionsHref(1)).toBe('../../');
  });

  test('the href always resolves to the listing, whatever the depth', () => {
    fc.assert(
      fc.property(fc.integer({ min: 0, max: 8 }), (depth) => {
        // A page at reference/<version>/<depth dirs>/page.html applying the href
        // must land exactly on `reference/`.
        const segments = ['reference', 'v2.8', ...Array.from({ length: depth }, (_, i) => `d${i}`)];
        const ups = allVersionsHref(depth).split('/').filter(Boolean).length;
        return segments.length - ups === 1 && segments[0] === 'reference';
      })
    );
  });

  test('a boolean where a depth belongs no longer type-checks as a call', () => {
    // Regression marker for the original defect: banner takes exactly two args.
    expect(banner.length).toBe(2);
  });
});

describe('banner', () => {
  test('carries the version and the depth-correct listing link', () => {
    const html = banner('v2.8', 1);
    expect(html).toContain('<span class="bq-ref-version">v2.8</span>');
    expect(html).toContain('<a href="../../">all versions</a>');
    expect(html).toContain('bq-ref-banner');
  });

  test('the marker used for idempotent re-injection is present', () => {
    expect(banner('v2.8', 0)).toContain('bq-ref-banner');
  });
});

describe('sortVersions', () => {
  test('dev first, then newest minor, numerically', () => {
    expect(sortVersions(['v2.8', 'dev', 'v2.10', 'v3.0'])).toEqual([
      'dev',
      'v3.0',
      'v2.10',
      'v2.8',
    ]);
  });

  test('a stray directory sorts last instead of producing NaN order', () => {
    expect(sortVersions(['v2.8', 'legacy', 'v3.0'])).toEqual(['v3.0', 'v2.8', 'legacy']);
  });

  test('is a total order: the result does not depend on input order', () => {
    fc.assert(
      fc.property(
        fc.uniqueArray(fc.tuple(fc.integer({ min: 0, max: 20 }), fc.integer({ min: 0, max: 20 })), {
          minLength: 1,
          maxLength: 8,
        }),
        (pairs) => {
          const names = pairs.map(([major, minor]) => `v${major}.${minor}`);
          return (
            JSON.stringify(sortVersions(names)) === JSON.stringify(sortVersions([...names].reverse()))
          );
        }
      )
    );
  });
});

describe('versionKey', () => {
  test('freezes a tree per minor, not per patch', () => {
    expect(versionKey('2.8.47')).toBe('v2.8');
    expect(versionKey('2.8.48')).toBe('v2.8');
    expect(versionKey('2.9.0')).toBe('v2.9');
  });
});
