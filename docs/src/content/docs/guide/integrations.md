---
title: "Bunqueue Framework Integrations: Hono & Elysia for Bun"
description: Integrate bunqueue with Hono and Elysia web frameworks. Embedded mode setup, project structure, and graceful shutdown patterns for Bun apps.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/integrations.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · integrations</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Plays well with your <em>stack.</em></h1>
  <p class="bq-hero-sub">Integrate bunqueue with AI agents over MCP and with modern Bun-native web frameworks. Setup tables, project structure, and graceful shutdown patterns.</p>

  <div class="bq-proof">
    <span><b>73</b> MCP tools</span>
    <span><b>5</b> MCP resources</span>
    <span><b>3</b> MCP prompts</span>
    <span><b>2</b> framework guides, Hono and Elysia</span>
  </div>
</div>

## AI Agent Integrations (MCP)

bunqueue ships with a native MCP server, AI agents get full queue control out of the box.

| Client | Setup | Guide |
|--------|-------|-------|
| Claude Code | `claude mcp add bunqueue -- bunx --package=bunqueue bunqueue-mcp` | [MCP Server](/guide/mcp/) |
| Claude Desktop | Add to `claude_desktop_config.json` | [MCP Server](/guide/mcp/) |
| Cursor | Add to MCP settings | [MCP Server](/guide/mcp/) |
| Windsurf | Add to MCP settings | [MCP Server](/guide/mcp/) |
| Any MCP client | `bunx --package=bunqueue bunqueue-mcp` (stdio transport) | [MCP Server](/guide/mcp/) |

> `bunqueue-mcp` is a binary bundled inside the `bunqueue` package (not a standalone npm package). `--package=bunqueue` tells `bunx` which package provides it; alternatively run `bun add -g bunqueue` once and use `bunx bunqueue-mcp` directly.
>
> Since v2.8.1, `@modelcontextprotocol/sdk` is an **optional peer dependency**, queue-only installs skip it (7 packages and 5.5 MB instead of 117 and 93 MB, a 94% smaller install). To run the MCP server, install it once with `bun add @modelcontextprotocol/sdk`; `bunx --package=bunqueue` won't pull it in automatically.

73 tools, 5 resources, 3 prompts. Agents can add jobs, manage crons, retry failures, set rate limits, register HTTP handlers, and monitor everything.

## Web Frameworks

:::caution[Embedded Mode Required]
All framework integrations use `embedded: true` for in-process queues. Without it, bunqueue tries to connect to a TCP server.
:::

| Framework | Description | Guide |
|-----------|-------------|-------|
| [Hono](https://hono.dev) | Ultrafast web framework for the Edge | [Hono Integration](/guide/hono/) |
| [Elysia](https://elysiajs.com) | Ergonomic framework with end-to-end type safety | [Elysia Integration](/guide/elysia/) |

## Quick Comparison

| Feature | Hono | Elysia |
|---------|------|--------|
| Type Safety | Manual typing | Built-in with `t` schema |
| Middleware | Function-based | Plugin-based |
| Validation | External libraries | Native with `t.Object()` |
| WebSocket | Via adapters | Built-in |
| Performance | Excellent | Excellent |

## Best Practices

### Project Structure

```
src/
├── api/
│   ├── routes/
│   │   ├── emails.ts
│   │   └── reports.ts
│   └── index.ts
├── queues/
│   ├── definitions.ts    # Queue instances
│   └── index.ts
├── workers/
│   ├── email.worker.ts
│   ├── report.worker.ts
│   └── index.ts
└── index.ts              # Entry point
```

### Queue Definitions

```typescript
// queues/definitions.ts
import { Queue } from 'bunqueue/client';

export const queues = {
  emails: new Queue('emails', {
    embedded: true,
    defaultJobOptions: {
      attempts: 3,
      backoff: 5000,
      removeOnComplete: true,
    },
  }),
  reports: new Queue('reports', {
    embedded: true,
    defaultJobOptions: {
      timeout: 300000,
    },
  }),
  notifications: new Queue('notifications', {
    embedded: true,
    defaultJobOptions: {
      attempts: 5,
      backoff: 1000,
    },
  }),
} as const;

export type QueueName = keyof typeof queues;
```

### Graceful Shutdown

```typescript
import { shutdownManager } from 'bunqueue/client';
import { queues } from './queues';
import { workers } from './workers';

async function shutdown() {
  console.log('Shutting down...');

  // Stop accepting new jobs
  for (const worker of Object.values(workers)) {
    worker.pause();
  }

  // Wait for active jobs to complete
  await Promise.all(
    Object.values(workers).map((w) => w.close())
  );

  // Close queue connections
  await Promise.all(
    Object.values(queues).map((q) => q.close())
  );

  // Shutdown the embedded manager
  shutdownManager();

  console.log('Shutdown complete');
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
```

## Next Steps

- [MCP Server](/guide/mcp/) - Full AI agent integration guide
- [Hono Integration](/guide/hono/) - Complete guide with examples
- [Elysia Integration](/guide/elysia/) - Production-ready REST API example with tests
