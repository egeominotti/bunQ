# Generated API Reference

> **Category:** Documentation tooling · **Source:** `typedoc.json`, `tsconfig.typedoc.json`, `scripts/build-api-reference.ts`, `docs/typedoc-theme.css`, `docs/src/content/docs/reference.mdx`, `docs/src/data/apiVersions.json`, `docs/src/lib/reference-seo.ts`, `docs/src/lib/sitemap.ts`

## Purpose

A per-version reference of everything the package exports, generated from the source rather than written by hand, published at `https://bunqueue.dev/reference/<version>/`.

Hand-written API docs drift the moment a signature changes, and the repo already carries three hand-maintained reference pages (`/api/types/`, `/api/http/`, `/api/tcp/`) that must be kept honest by review. This one cannot drift: it is TypeDoc output over the real entry points.

Versioning is the point. A reader on `bunqueue@2.6` needs the surface 2.6 shipped, not the surface on `main` today, so each generated tree is frozen where it is written and older versions stay served.

## Running it

```bash
bun run docs:api              # build for the current package.json version
bun run docs:api -- --dev     # build to /reference/dev/, for unreleased work
```

Output: `docs/public/reference/<version>/`, which Astro copies into `dist/`.
The documentation build adds current-version search metadata to the copied HTML;
it never rewrites the committed reference trees.

## Version keying

The directory is `v<major>.<minor>`, so `2.8.47` writes `v2.8`. Patch releases share a page because under semver a patch cannot change the public surface, and a directory per patch would put hundreds of near-identical copies of a 5 MB tree in git.

The consequence is worth stating plainly: a tree is frozen **across minors, not across patches**. Re-running the build on 2.8.48 overwrites the 2.8.47 tree. If a patch ever does change the public surface, that rewrite is silent and retroactive for the whole minor, which is one more reason such a change belongs in a minor release.

## What is covered

`typedoc.json` `entryPoints` mirrors the package's `exports` map, so nothing is documented that a consumer cannot import:

| Entry point                       | `exports` key |
| --------------------------------- | ------------- |
| `src/main.ts`                     | `.`           |
| `src/client/index.ts`             | `./client`    |
| `src/client/workflow/index.ts`    | `./workflow`  |
| `src/application/queueManager.ts` | `./queue`     |
| `src/mcp/index.ts`                | `./mcp`       |

The remaining entries (`src/client/types.ts`, `src/domain/types/job.ts`, and the other type modules) are **not** additional package export paths. They hold types that appear in public signatures: event payloads, job options, DLQ and cron shapes. Without them TypeDoc emits `referenced but not included` and the reader hits a dead link on exactly the type they need to call the method. Directory-backed modules must name their real `index.ts`; a stale `types.ts` glob is a warning and silently omits those pages.

When a new type module starts appearing in those warnings, add it to `entryPoints` rather than ignoring it.

TypeDoc uses `tsconfig.typedoc.json`, not the production type-check config. The
production config deliberately excludes the optional MCP tree, while the package
exports `bunqueue/mcp` and the reference must include it. The docs config extends
the same compiler options but includes all of `src`. Private and protected members
are excluded: inherited implementation state is not callable public API and would
otherwise create dozens of misleading missing-type links.

The generator treats warnings as errors. Every exported type reachable from a
documented signature must either be an entry point or, for a deliberately private
structural helper such as `RemoteQueueLike`, be named in
`intentionallyNotExported`. A release cannot silently publish dead type links or
an entry-point glob that matched nothing.

## Two collisions the layout avoids

Both were hit while building this, and both fail silently rather than loudly.

**1. `/api/` is taken.** Starlight already owns `/api/http/`, `/api/tcp/` and `/api/types/` as content routes. Output therefore goes to `/reference/`, not `/api/`.

**2. `public/` overwrites built pages.** An earlier version of the script wrote `docs/public/reference/index.html` as the version listing. Astro copies `public/` over the built output _last_, so that file clobbered the Starlight page that owns `/reference/`, and the site served unstyled HTML there. The script no longer writes it; `/reference/` is `docs/src/content/docs/reference.mdx`.

## Why the version list is a generated JSON

`reference.mdx` imports `docs/src/data/apiVersions.json`, written by the build script.

The obvious alternative, having the page read `docs/public/reference/` itself, **does not work**: Vite bundles page modules, and once it does `import.meta.url` no longer points at the source file, so the directory walk resolves somewhere else and returns an empty array. The page then renders an empty list with no error at any stage of the build. A generated JSON import is resolved at build time and has one writer.

`apiVersions.json` is generated output and should not be edited by hand.

It is nevertheless **committed**, because the CI docs job builds the site without
running the generator: an untracked file makes `astro build` fail with
`Could not resolve "../../data/apiVersions.json"`. The repository ignores `data/`
for runtime SQLite directories, so `.gitignore` carries an explicit
`!docs/src/data/` negation — deleting that line reintroduces the failure, and it
only shows up in a clean checkout, never locally.

## The guard: `bun run check:docs-data`

Committing generated output trades one silent failure for another, so
`scripts/check-docs-data.ts` closes both. It runs in the CI docs job **before** the
build, and in `bun run check`:

- every relative module specifier and asset reference in `docs/src/content/docs/**`
  must resolve to a **git-tracked** file (fenced and inline code is stripped first,
  so a documented `import './x'` in a sample is not mistaken for a real one);
- `apiVersions.json` must equal what this script would derive from `package.json`
  and `docs/public/reference/`, and the tree for the current version must itself be
  tracked — otherwise the published listing links to a 404;
- a `dev` entry is rejected, so a local `--dev` preview cannot reach the site.

**Consequence for minor bumps.** `current` is `v<major>.<minor>`, so `2.8.x → 2.9.0`
fails the check until `bun run docs:api` has been run and its output committed
(a multi-megabyte TypeDoc tree). That is deliberate: the alternative is a
`/reference/` page advertising a version that does not exist. Patch bumps are
unaffected. Unit coverage for the scanners lives in `test/check-docs-data.test.ts`.

## Theming

`docs/typedoc-theme.css` maps the site palette onto TypeDoc's own `--light-*` / `--dark-*` variables instead of restyling its markup. TypeDoc's search, member filtering and navigation keep working, and a TypeDoc upgrade that renames a class cannot break the theme.

The version banner is the one piece of markup injected by the script, because TypeDoc has no slot for site chrome. It answers the two questions its own header cannot: which version am I reading, and how do I get back. Injection is idempotent, guarded by a check for `bq-ref-banner`, so re-running the build does not stack banners.

`injectHead()` adds a second, separately-guarded injection: `<meta name="robots" content="noindex, follow">`, but **only on trees that are not the current version** (`shouldNoindex(version, current)`). The current tree stays indexable on purpose: a search for a type name should be able to reach its reference page. Older trees and `--dev` previews never index, so near-identical pages do not compete across every released version.

Demotion happens on the release that supersedes a tree, not when the tree is written: `main()` injects into the tree TypeDoc just produced, then walks every sibling version directory and re-runs the same injection with `noindex` on each one that is no longer current. Without that second pass the policy could never fire for a released version — a tree is only ever generated while it _is_ current, so it would keep the indexable head it was born with and each release would add another near-identical competitor. Both passes are idempotent, so re-running costs nothing.

The two injections need independent guards. A shared one is wrong in both directions: the committed tree already carries banners, so a `bq-ref-banner` check would skip the meta on every existing file, and a tree that predates the banner would never get chrome. `injectBanner` also asserts a post-condition — a page that ends without a robots meta _inside its `<head>`_ exits the build non-zero, because a silently unmatched `replace` is exactly how the wrong `depth` shipped on 234 pages. Placement, not presence: `<head(\s[^>]*)?>` is attribute-tolerant like `<body>` above, and the `\s` is load-bearing, because `<head([^>]*)>` also matches the `<header class="tsd-page-toolbar">` TypeDoc puts on every page — on a page with no `<head>` the meta would land inside that element, still present and no longer a directive.

### Canonicals, metadata, and sitemap

TypeDoc's default metadata is incomplete for a published versioned site: every
page starts with the same generic description, no canonical, and some types
share a title. `docs/src/lib/reference-seo.ts` fixes the copied current-version
HTML in Astro's `astro:build:done` hook. Titles and descriptions identify the
symbol kind, module, and version; the index and hierarchy have separate metadata.
Open Graph and Twitter metadata use the same per-page values. Injection escapes
HTML attributes and replaces owned tags idempotently, preserving scripts, page
content, and unrelated head elements. A missing head or a current page carrying
`noindex` fails the build rather than publishing an indexing conflict.

The integration reads `apiVersions.json` through the Astro configuration and
enumerates only the current tree. It supplies those URLs through
[`@astrojs/sitemap`'s `customPages` option](https://docs.astro.build/en/guides/integrations-guide/sitemap/#custompages),
because copied public HTML is not an Astro route. Directory indexes canonicalize
to `/reference/v2.9/`; other TypeDoc pages retain their actual `.html` paths.
The sitemap contains one URL per current page and excludes historical and
development versions. The 262 current v2.9 pages join the 119 authored pages,
for 381 canonical URLs. `docs/src/lib/sitemap.ts` preserves the authored-route
priorities and git-derived modification dates; unknown dates remain absent.

`docs/vercel.json` applies a hosting-level `noindex` header only to raw Markdown.
A blanket `/reference/v:version/(.*)` header would also block the current tree,
overriding this policy, so it is explicitly prohibited by regression coverage.
Historical version exclusions come from their HTML robots metadata.

The post-build discovery validator checks exact sitemap membership, matching
canonical tags, titles and descriptions, unique current-reference metadata,
historical/development `noindex`, and the hosting policy. It also retains the
independent 104-page `llms-full.txt` coverage check; generated TypeDoc pages are
not added to that hand-authored document stream. Focused fixtures in
`test/docs-seo.test.ts` exercise URL selection, encoding, metadata escaping,
idempotence, build failures, and preservation of source and historical trees.

Its "all versions" link is depth-relative: `allVersionsHref(depth)` returns `../` for a
page at the version root and one more `../` per directory below it, so every page lands
on `/reference/`. This was **wrong on 234 published pages** until 2.8.48: `injectBanner`
passed three arguments to a two-parameter `banner()`, so `depth` silently received a
boolean, collapsed to `0`, and every nested page linked back to its own version tree
instead of the listing. Nothing caught it — the page still built, the site still
deployed, `scripts/` is outside `tsconfig.json`'s `include`, and only a reader clicking
the link would find out. The already-generated pages were repaired in place (the
injection guard means a re-run would not have touched them), and
`test/build-api-reference.test.ts` now pins the depth arithmetic, the two-parameter
signature, and the version ordering.

`scripts/` is still not type-checked: it carries a few hundred pre-existing errors, so
wiring a blanket gate is its own change. Unit-testing the pure helpers is the guard that
exists today — prefer exporting logic from a script over leaving it only reachable
through side effects.

## Verification

Do not verify with `bun run dev`. The Astro dev server does not resolve directory indexes under `public/`, so `/reference/v2.9/` returns **404** there while the identical path returns 200 in production. Build and preview instead:

```bash
cd docs && bun run build && bunx astro preview --port 4399
curl -o /dev/null -w '%{http_code}\n' http://localhost:4399/reference/v2.9/
```

## The generated tree is committed, deliberately

`docs/public/reference/<version>/` is **not** gitignored, and cannot be.

Generating at deploy time would only ever produce the _current_ version, because an older tree cannot be regenerated from today's source. Serving `/reference/v2.6/` after 2.8 ships requires the v2.6 tree to exist in the repo. Versioned references and build-time generation are mutually exclusive; this design chooses versioning.

The current cost is **about 5.25 MB and 271 files per minor version** (v2.9 measured). Ten similarly sized minor versions would be roughly 52.5 MB of mostly-static HTML.

If that becomes a problem, the options, in increasing effort:

1. Keep only the last N minor versions and delete older directories.
2. Publish the reference from a separate branch or repo, as BullMQ does with `api.bullmq.pro`.
3. Emit TypeDoc JSON (much smaller) and render it with an Astro route at request time, losing TypeDoc's built-in search.

## Backfilling an older version

A tree must be generated from the source of the version it claims to document. Building
`v2.8` from today's checkout produces a page that says 2.8 and describes 2.9, which is
worse than having no page for 2.8 at all.

To add one, check the tag out first:

```bash
git worktree add /tmp/bq-2.8 v2.8.46
cd /tmp/bq-2.8 && bun install && bun run docs:api
cp -r docs/public/reference/v2.8 <this-repo>/docs/public/reference/
```

The repository now ships the historical `v2.8` tree and the current `v2.9`
tree. Any older version must still be generated from its matching tag before it
can be added safely.

## Release integration

The reference is **not** wired into the release flow yet. `bun run docs:api` is a manual step; running it after a version bump and before publishing keeps `/reference/` current. Automating it belongs with the version-bump step in `CLAUDE.md`.

**Commit the whole diff, not just the new tree.** A minor bump makes the demotion pass rewrite every page of the previous version, so `bun run docs:api` for v2.9 also touches all ~238 files under `v2.8/`. Staging only `v2.9/` leaves the published v2.8 indexable with a green build: `check-docs-data` validates `apiVersions.json` and tree tracking, not robots state, so nothing else would catch it.

## Related Docs

- [Architecture](./architecture.md) — the modules the reference documents.
- [Data Model](./data-model.md) — hand-written reference for the wire shapes and SQLite schema, which TypeDoc does not cover.
- `docs/protocol.md` — normative wire spec for client authors, a different audience from this reference.
