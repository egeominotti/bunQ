---
title: "Rate Limiting & Concurrency Control for Bun Job Queues"
description: Control job processing rates in bunqueue with per-queue rate limits and concurrency caps. Protect downstream services via CLI or HTTP API.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/advanced.png
---


Control the rate at which jobs are processed.

:::tip[Using AI agents?]
AI agents connected via MCP can set and clear rate limits and concurrency caps via natural language using `bunqueue_set_rate_limit`, `bunqueue_clear_rate_limit`, `bunqueue_set_concurrency`, and `bunqueue_clear_concurrency` tools. See [MCP Server](/guide/mcp/).
:::

## Rate Limit

Limit jobs per time window:

```bash
# CLI
bunqueue rate-limit set emails 100  # 100 jobs/second
bunqueue rate-limit clear emails
```

## Concurrency Limit

Limit concurrent active jobs:

```bash
# CLI
bunqueue concurrency set emails 5  # Max 5 concurrent
bunqueue concurrency clear emails
```

## Embedded Mode

:::note[Works in embedded mode]
Rate limiting (`setGlobalRateLimit`) and concurrency limiting (`setGlobalConcurrency`) work in **both embedded and TCP modes** — in embedded mode they call the in-process manager directly. A per-worker `limiter: { max, duration }` (in `WorkerOptions`) also works embedded.
:::

In embedded mode you can call `queue.setGlobalRateLimit(max, duration?)` / `queue.setGlobalConcurrency(n)` directly, use a per-worker `limiter`, or control throughput with worker concurrency:

```typescript
const queue = new Queue('emails', { embedded: true });

// Control processing rate with worker concurrency
const worker = new Worker('emails', processor, {
  embedded: true,
  concurrency: 5, // Max 5 parallel jobs
});
```

For custom time-based rate limiting beyond the built-in options, you can also implement it in your processor:

```typescript
import { Ratelimit } from '@upstash/ratelimit'; // or similar

const ratelimit = new Ratelimit({ ... });

const worker = new Worker('emails', async (job) => {
  await ratelimit.limit('email-send'); // External rate limiter
  await sendEmail(job.data);
}, { embedded: true });
```

:::tip[Related Guides]
- [Queue API](/guide/queue/) - Queue configuration options
- [Worker API](/guide/worker/) - Worker concurrency settings
- [Environment Variables](/guide/env-vars/) - Server-side rate limit defaults
:::
