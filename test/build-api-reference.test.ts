import { describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import {
  allVersionsHref,
  banner,
  hasHeadRobots,
  injectHead,
  REFERENCE_ROBOTS,
  shouldNoindex,
  sortVersions,
  versionKey,
} from '../scripts/build-api-reference';

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

describe('shouldNoindex', () => {
  test('the current version is indexable, every other tree is not', () => {
    expect(shouldNoindex('v2.8', 'v2.8')).toBe(false);
    expect(shouldNoindex('v2.7', 'v2.8')).toBe(true);
    expect(shouldNoindex('dev', 'v2.8')).toBe(true);
  });
});

describe('injectHead', () => {
  const PAGE = '<!DOCTYPE html><html><head><title>Worker | bunqueue</title></head><body><p>x</p></body></html>';

  test('fresh typedoc output of an old version gets both banner and robots', () => {
    const out = injectHead(PAGE, 'v2.7', 0, true);
    expect(out).toContain('bq-ref-banner');
    expect(out).toContain(REFERENCE_ROBOTS);
  });

  test('the current version gets chrome but stays indexable', () => {
    const out = injectHead(PAGE, 'v2.8', 0, false);
    expect(out).toContain('bq-ref-banner');
    expect(out).not.toContain('robots');
  });

  test('an already-bannered page still receives the meta', () => {
    // The guards must be independent: the committed tree carries banners already, so
    // a shared guard would skip robots on every existing file.
    const bannered = injectHead(PAGE, 'v2.7', 0, false);
    expect(bannered).not.toContain('robots');
    const out = injectHead(bannered, 'v2.7', 0, true);
    expect(out).toContain(REFERENCE_ROBOTS);
    expect(out.match(/bq-ref-banner/g)).toHaveLength(1);
  });

  test('re-running over its own output changes nothing', () => {
    const once = injectHead(PAGE, 'v2.7', 0, true);
    expect(injectHead(once, 'v2.7', 0, true)).toBe(once);
  });

  test('a <head> carrying attributes still matches', () => {
    // A silently unmatched replace is how the wrong depth shipped on 234 pages: a
    // typedoc release emitting `<head lang="en">` must not quietly drop the meta.
    const out = injectHead(PAGE.replace('<head>', '<head lang="en">'), 'v2.7', 0, true);
    expect(out).toContain(REFERENCE_ROBOTS);
    expect(out).toContain('<head lang="en">');
    expect(hasHeadRobots(out)).toBe(true);
  });

  test('<header>, which typedoc emits on every page, is not mistaken for <head>', () => {
    // `<head([^>]*)>` matches `<header class=…>` too. On a page whose head is missing
    // the meta would land inside the header element: still present, no longer a
    // directive — which is why the post-condition asserts placement, not presence.
    const headless = '<html><body><header class="tsd-page-toolbar">t</header></body></html>';
    const out = injectHead(headless, 'v2.7', 0, true);
    expect(out).not.toContain('robots');
    expect(hasHeadRobots(out)).toBe(false);
  });

  test('the meta goes into the real head, leaving the toolbar header alone', () => {
    const page = PAGE.replace('<body>', '<body><header class="tsd-page-toolbar">t</header>');
    const out = injectHead(page, 'v2.7', 0, true);
    expect(out).toContain(`<head>${REFERENCE_ROBOTS}`);
    expect(out).toContain('<header class="tsd-page-toolbar">t</header>');
    expect(hasHeadRobots(out)).toBe(true);
  });

  test('the meta lands inside the first 1024 bytes, where crawlers still read it', () => {
    const out = injectHead(PAGE, 'v2.7', 0, true);
    expect(out.indexOf('name="robots"')).toBeLessThan(1024);
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
