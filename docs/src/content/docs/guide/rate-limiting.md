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

:::note[Server-Side Feature]
Rate limiting (`setRateLimit`) and concurrency limiting (`setConcurrency`) are **server-side features** available only via CLI or HTTP API. They are not available in embedded mode.
:::

In embedded mode, control throughput using worker concurrency:

```typescript
const queue = new Queue('emails', { embedded: true });

// Control processing rate with worker concurrency
const worker = new Worker('emails', processor, {
  embedded: true,
  concurrency: 5, // Max 5 parallel jobs
});
```

For time-based rate limiting in embedded mode, implement it in your processor:

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
