# ============================================
# bunQ Dockerfile
# Multi-stage build for optimal image size
# ============================================

# Stage 1: Build
FROM oven/bun:1-alpine AS builder

WORKDIR /app

# Copy package files
COPY package.json bun.lock* ./

# Install dependencies (ignore prepare script - no git in container)
RUN bun install --frozen-lockfile --ignore-scripts

# Copy source code
COPY src/ ./src/
COPY tsconfig.json ./

# Type check
RUN bun run typecheck

# Build single executable
RUN bun build --compile --minify src/main.ts --outfile bunqueue

# ============================================
# Stage 2: Production
FROM oven/bun:1-alpine AS production

WORKDIR /app

# Create non-root user for security
RUN addgroup -g 1001 bunqueue && \
    adduser -D -u 1001 -G bunqueue bunqueue

# Create data directory
RUN mkdir -p /app/data && chown -R bunqueue:bunqueue /app

# Copy built executable from builder
COPY --from=builder --chown=bunqueue:bunqueue /app/bunqueue ./bunqueue

# Switch to non-root user
USER bunqueue

# Environment variables
ENV TCP_PORT=6789
ENV HTTP_PORT=6790
ENV DATA_PATH=/app/data/bunqueue.db
ENV NODE_ENV=production

# Expose ports
EXPOSE 6789 6790

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://127.0.0.1:6790/health || exit 1

# Volume for persistent data
VOLUME ["/app/data"]

# Run the server
ENTRYPOINT ["./bunqueue"]
