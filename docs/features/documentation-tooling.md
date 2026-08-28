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

## Release checks

- `bun run check:docs-data` verifies generated documentation metadata.
- `bun run docs:api` rebuilds the versioned TypeDoc reference.
- `bun run build` from `docs/` builds the complete public site and search
  index.
- The tracked root and `docs/bun.lock` files are the frozen dependency inputs
  for CI, the documentation build, and both disposable validation images; a
  release snapshot must never rely on an ignored local lockfile.
- The executable guide contract is mapped in
  [Documented Feature Verification](./documented-feature-verification.md).
