---
title: "Bunqueue Framework Integrations: Hono & Elysia for Bun"
description: Integrate bunqueue with Hono and Elysia web frameworks. Embedded mode setup, project structure, and graceful shutdown patterns for Bun apps.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/integrations.png
---

Integrate bunqueue seamlessly with modern Bun-native frameworks.

:::caution[Embedded Mode Required]
All framework integrations use `embedded: true` for in-process queues. Without it, bunqueue tries to connect to a TCP server.
:::

## Supported Frameworks

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

- [Hono Integration](/guide/hono/) - Complete guide with examples
- [Elysia Integration](/guide/elysia/) - Production-ready REST API example with tests
