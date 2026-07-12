---
title: "Rate Limiting & Concurrency Control for Bun Job Queues"
description: Control job processing rates in bunqueue with per-queue rate limits and concurrency caps. Protect downstream services via CLI or HTTP API.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/advanced.png
---


<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · rate limiting</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Rate limits and concurrency, under <em>control.</em></h1>
  <p class="bq-hero-sub">Cap jobs per time window or concurrent active jobs per queue to protect downstream services. Works from the CLI, over TCP, and in embedded mode.</p>
</div>

:::tip[Using AI agents?]
AI agents connected via MCP can set and clear rate limits and concurrency caps via natural language using `bunqueue_set_rate_limit`, `bunqueue_clear_rate_limit`, `bunqueue_set_concurrency`, and `bunqueue_clear_concurrency` tools. See [MCP Server](/guide/mcp/).
:::

## Rate Limit

Limit jobs per second (token bucket, refilled continuously):

```bash
# CLI
bunqueue rate-limit set emails 100  # 100 jobs/second
bunqueue rate-limit clear emails
```

The server-side rate limit window is fixed at **1 second**. No rate limit is set by default (unlimited).

## Concurrency Limit

Limit concurrent active jobs:

```bash
# CLI
bunqueue concurrency set emails 5  # Max 5 concurrent
bunqueue concurrency clear emails
```

## Embedded Mode

:::note[Works in embedded mode]
Rate limiting (`setGlobalRateLimit`) and concurrency limiting (`setGlobalConcurrency`) work in **both embedded and TCP modes**, in embedded mode they call the in-process manager directly. A per-worker `limiter: { max, duration }` (in `WorkerOptions`, sliding window enforced client-side) also works embedded.
:::

:::caution[Server rate limits are per second]
`queue.setGlobalRateLimit(max, duration?)` accepts a `duration` argument for BullMQ API compatibility, but the server ignores it: the queue-level limit is always `max` jobs per second. For a custom window (e.g. 100 jobs per minute), use the per-worker `limiter: { max, duration }` option instead.
:::

In embedded mode you can call `queue.setGlobalRateLimit(max)` / `queue.setGlobalConcurrency(n)` directly, use a per-worker `limiter`, or control throughput with worker concurrency:

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
