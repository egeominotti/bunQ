# Documentation Tooling

The public documentation site is an Astro/Starlight project under
`docs/src/content/docs/`. Repository scripts keep generated API metadata,
social images, and executable documentation claims reproducible.

## Open Graph image generator

`docs/scripts/generate-og.ts` owns SVG rendering, font loading, lane artwork,
and PNG output. Static page copy and image metadata live separately in
`docs/scripts/og-covers.ts`; adding or changing a cover does not expand the
rendering pipeline or require an unsafe payload cast.

Run the generator from `docs/scripts/` so its relative font and output paths
resolve as documented:

```bash
bun generate-og.ts
```

It writes generated PNGs below `docs/scripts/out/`. Generated output is copied
to `docs/public/` only when the corresponding public asset is intentionally
being refreshed. Both source modules must remain below the repository's
300-line limit and pass Oxlint and Oxfmt together.

## Static analysis and formatting

The root project and TypeScript SDK use pinned Oxlint and Oxfmt releases.
Oxlint reads `.oxlintrc.json`, enables type-aware rules through the pinned
`oxlint-tsgolint` companion, and keeps intentional queue-specific exceptions in
scoped overrides or `oxlint-disable-next-line` directives. Oxfmt reads
`.oxfmtrc.json` and preserves the existing two-space, single-quote, semicolon,
100-column style.

The migration keeps the former prototype-call, switch-fallthrough, and array
constructor checks through Oxlint's `no-prototype-builtins`, `no-fallthrough`,
and `no-array-constructor` rules. Oxc has no exact equivalents for Biome's
`noArguments`, `useIterableCallbackReturn`, `noGlobalIsNan`, or
`useConsistentEnumValueType`; the nearest applicable checks remain enabled as
`prefer-rest-params`, `array-callback-return`, `use-isnan` through the
correctness category, and `typescript/no-mixed-enums`. These are intentional
semantic approximations rather than a claim that the two rule sets are
identical. The SDK similarly uses `typescript/no-empty-object-type` as the
supported replacement for Biome's broader `noBannedTypes` rule.

Run `bun run check:oxc` at the repository root. The TypeScript SDK exposes the
same check as `bun run check` from `sdk/typescript/` and owns nested Oxc configs
so it also works as a standalone checkout. CI, the pre-commit hook, and both
isolated validation images invoke these same scripts.

## Generated API reference

`bun run docs:api` generates the current minor's TypeDoc tree from every public
package entry point. TypeDoc reads `tsconfig.typedoc.json` so the optional MCP
export is included even though the production type-check configuration excludes
that subtree. Private and protected implementation members stay out of the
published surface.

Generation treats warnings as errors. A type reachable from a public signature
must either be documented through a real entry point or listed as a deliberate
internal structural helper in `typedoc.json`; an unmatched entry-point pattern or
unresolved type link blocks the release. The generated current tree is indexable,
while older minor trees receive `noindex, follow`. The complete contract and
versioning rationale live in [Generated API Reference](../generated-api-reference.md).

## LLM discovery outputs

`docs/public/llms.txt` is the curated, low-token documentation index.
`/llms-full.txt` is generated from the content collection and includes every
non-blog documentation page except the 404 route, with a title, description,
canonical URL, and source body. When an MDX page renders a tracked `?raw`
import through Starlight's `Code` component, the generator replaces the MDX
variable with the actual source in a fenced block. It fails the build if the
import has no code destination or cannot be read, preventing an apparently
complete dump from silently omitting executable examples.

`docs/public/robots.txt` advertises the curated and full-text endpoints and the
sitemap index. Astro's sitemap integration emits only canonical indexable
routes, derives per-page `lastmod` values from Git history when available, and
omits the 404, Markdown mirrors, Open Graph images, and text endpoints.

## Progressive examples and interactive explainers

`docs/src/content/docs/examples.mdx` is ordered as a learning path. It begins
with one embedded queue and worker, adds lifecycle and reliability controls,
introduces process boundaries, and ends with workflows and the PostgreSQL
multi-broker project. Existing section headings retain their anchors when the
reading order changes, so inbound links remain valid.

The page's small visual system lives under `docs/src/components/examples/`.
`ExamplesLearningPath.astro` provides direct anchor navigation, while
`JobJourney.astro` and `TopologyExplorer.astro` use guarded custom elements for
progressive enhancement. Their first server-rendered state is meaningful
without JavaScript. Native buttons, `aria-pressed`, `aria-current`, live status
text, visible keyboard focus, and reduced-motion styling are required parts of
the component contract.

`explainerModels.ts` is the shared source for lifecycle routes and deployment
topologies. `test/docs-examples-page.test.ts` verifies the page order, anchor
targets, model boundaries, topology progression, accessibility hooks, and the
300-line file limit. The unit validation image copies only this explicit
component subtree in addition to the documentation content already required by
the test suite.

## Release checks

- `bun run check:docs-data` verifies generated documentation metadata and
  resolves local module imports after removing Vite query or fragment suffixes
  such as `?raw`, so executable source imports remain tracked-file checked.
- `bun run docs:api` rebuilds the versioned TypeDoc reference.
- `bun run build` from `docs/` builds the complete public site and search
  index, then runs `scripts/validate-discovery.ts`. The validator compares the
  full-text and sitemap URL sets with the content tree, detects duplicates and
  stale curated links, proves every raw executable source is inlined, checks
  multi-broker reading order, and verifies the robots discovery pointers.
- `Dockerfile.test` copies the full-text transformer and the executable
  PostgreSQL multi-broker example into the sanitized unit image. This keeps the
  discovery regressions plus CLI, timeout, HTTP-bound, multi-phase cleanup, and
  verifier failure-path tests inside the same isolated gate as the repository.
  Its Dockerfile-specific ignore allowlist admits only that example subtree,
  not unrelated examples or host files.
- The tracked root and `docs/bun.lock` files are the frozen dependency inputs
  for CI, the documentation build, and both disposable validation images; a
  release snapshot must never rely on an ignored local lockfile.
- The executable guide contract is mapped in
  [Documented Feature Verification](./documented-feature-verification.md).
