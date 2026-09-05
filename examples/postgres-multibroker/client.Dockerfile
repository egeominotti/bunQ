FROM oven/bun:1.4.2-alpine

WORKDIR /app

COPY package.json bun.lock tsconfig.json tsconfig.build.json ./
RUN bun install --frozen-lockfile --ignore-scripts

COPY src ./src
RUN bun run build:lib

COPY examples/postgres-multibroker ./examples/postgres-multibroker

ENTRYPOINT ["bun", "run", "examples/postgres-multibroker/run.ts"]
CMD ["all"]
