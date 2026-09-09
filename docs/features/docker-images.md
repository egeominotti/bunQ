# Docker images

> **Source:** `Dockerfile`, `Dockerfile.dockerignore`, `.github/workflows/ci.yml`, `scripts/test-docker-image.ts`

## Variants and build inputs

The production Dockerfile selects `VARIANT=alpine` by default. Supported values
are `alpine`, `debian`, `slim`, and `distroless`; an invalid value fails the build.
Alpine 3.22 uses musl. Debian 13, Debian 13 slim, and distroless
`cc-debian13:nonroot` use glibc. The cc base supplies the C++ runtime required by
the executable. All bases include CA certificates and required runtime libraries.

The builder uses Bun 1.4.2 on the build host's architecture, installs the frozen
lockfile with lifecycle scripts disabled, runs typecheck, and cross-compiles for
Docker's target architecture using `--compile --minify`. Only amd64 and arm64
are accepted. The musl target is selected only for Alpine. The final stage copies
the compiled executable and an empty owned data directory. It never copies the
builder's Bun runtime, source, manifests, or node_modules. The Dockerfile-specific
ignore allowlist admits only the Dockerfile, package manifest, lockfile,
TypeScript config, and source tree.

All images run as numeric UID/GID `1001:1001`, expose TCP 6789 and HTTP 6790,
and persist SQLite at `/app/data/bunqueue.db`. Named volumes inherit the data
directory ownership. Operators must set matching ownership on bind mounts.
The JSON-form probe invokes `/app/bunqueue healthcheck`; see [CLI](./cli.md).
Distroless contains neither a shell nor a package manager.

## Validation and publication

The CI Docker test matrix builds all four variants on native `ubuntu-latest`
amd64 and `ubuntu-24.04-arm` arm64 runners, after the quality and binary gates.
Each candidate is loaded into Docker and exercised with `test-docker-image.ts`:
offline network, non-root user, custom HTTP port, authenticated TCP, rejected
unauthenticated access, PUSH/PULL/ACK, two container replacements sharing only a
fresh named volume, persisted completion result, failing health endpoint, and
absence of build dependencies. Logs and image/container metadata are written
under ignored `artifacts/docker-images/`. Cleanup removes the container and volume.

Successful candidates are exported as image archives. Only after all eight
native checks pass does the publication matrix load those exact archives,
push commit/variant/architecture tags, and assemble two-platform indexes for
Docker Hub and GHCR. No rebuild occurs between the smoke test and publication.
Version/variant, moving variant, SHA/variant, and timestamp/variant tags exist for
every variant. Unsuffixed version/latest/SHA/timestamp aliases select Alpine only.
Full-SHA/variant/architecture tags retain the underlying platform images.

Normal pushes publish only new package versions. A manual CI run on main with
`rebuild_docker=true` repeats all quality, binary, and image gates and republishes
Docker images for an existing version without changing its GitHub release or tag.
Use digests when image immutability across base-image refreshes is required.
Docker publication does not publish npm packages.
An additional explicit `npm_version` manual input requests the separately gated
root npm package publication after every Docker variant succeeds. It must match
the current package version and be absent from npm. Git tag lookup failures stop
the version gate; only Git's exit status 2 means the release tag does not exist.
