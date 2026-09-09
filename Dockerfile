# syntax=docker/dockerfile:1
ARG VARIANT=alpine
FROM --platform=$BUILDPLATFORM oven/bun:1.4.2 AS builder
ARG TARGETARCH
ARG VARIANT
WORKDIR /app
COPY package.json bun.lock ./
# Development tools exist only in this build stage.
RUN bun install --frozen-lockfile --ignore-scripts
COPY src/ ./src/
COPY tsconfig.json ./
RUN bun run typecheck
RUN case "$TARGETARCH" in amd64) arch=x64 ;; arm64) arch=arm64 ;; *) exit 1 ;; esac; \
    case "$VARIANT" in alpine) libc=-musl ;; debian|slim|distroless) libc= ;; *) exit 1 ;; esac; \
    bun build --compile --minify --target="bun-linux-${arch}${libc}" src/main.ts --outfile bunqueue
RUN mkdir /app/data

FROM alpine:3.22 AS alpine-base
RUN apk add --no-cache ca-certificates libgcc libstdc++

FROM debian:trixie AS debian-base
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

FROM debian:trixie-slim AS slim-base
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

FROM gcr.io/distroless/cc-debian13:nonroot AS distroless-base

FROM ${VARIANT}-base AS production
WORKDIR /app
COPY --from=builder --chown=1001:1001 /app/bunqueue /app/bunqueue
COPY --from=builder --chown=1001:1001 /app/data /app/data
USER 1001:1001
ENV TCP_PORT=6789 HTTP_PORT=6790 DATA_PATH=/app/data/bunqueue.db NODE_ENV=production
EXPOSE 6789 6790
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD ["/app/bunqueue", "healthcheck"]
VOLUME ["/app/data"]
ENTRYPOINT ["/app/bunqueue"]
