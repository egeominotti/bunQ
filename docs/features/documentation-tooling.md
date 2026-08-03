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
300-line limit and pass Biome together.

## Release checks

- `bun run check:docs-data` verifies generated documentation metadata.
- `bun run docs:api` rebuilds the versioned TypeDoc reference.
- `bun run build` from `docs/` builds the complete public site and search
  index.
- The executable guide contract is mapped in
  [Documented Feature Verification](./documented-feature-verification.md).
