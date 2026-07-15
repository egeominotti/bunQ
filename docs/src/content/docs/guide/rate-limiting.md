---
title: "Rate Limiting & Concurrency for Bun Job Queues"
description: Control job processing rates in bunqueue with per-queue rate limits and concurrency caps. Protect downstream services via CLI, SDK or MCP.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/rate-limiting.png
---


<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · rate limiting</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Rate limits and concurrency, under <em>control.</em></h1>
  <p class="bq-hero-sub">Cap how many jobs start per second, or how many run at the same time, so a busy queue never overwhelms the API or database behind it.</p>
</div>

bunqueue gives you two independent knobs per queue:

- **Rate limit**: how many jobs may *start* per second (throughput cap).
- **Concurrency limit**: how many jobs may be *active at once* (parallelism cap).

Neither is set by default, so queues run unlimited until you say otherwise.

## Set a rate limit

Cap a queue at 100 jobs per second:

```bash
bunqueue rate-limit set emails 100   # max 100 jobs/second
bunqueue rate-limit clear emails     # back to unlimited
```

Or from the SDK (works in both embedded and TCP mode):

```typescript
queue.setGlobalRateLimit(100);  // 100 jobs/second
queue.removeGlobalRateLimit();
```

The server-side window is fixed at **1 second**, implemented as a token bucket that refills continuously.

## Set a concurrency limit

Cap a queue at 5 jobs running at the same time, across all workers:

```bash
bunqueue concurrency set emails 5
bunqueue concurrency clear emails
```

```typescript
queue.setGlobalConcurrency(5);
queue.removeGlobalConcurrency();
```

This is a *queue-level* cap. Each worker also has its own `concurrency` option that limits how many jobs that one worker runs in parallel:

```typescript
const worker = new Worker('emails', processor, {
  concurrency: 5, // this worker runs at most 5 jobs at once
});
```

## Custom time windows (per worker)

Need "100 jobs per minute" instead of per second? The queue-level limit only supports a 1-second window, but each worker can enforce a sliding window of any duration with the `limiter` option:

```typescript
const worker = new Worker('emails', processor, {
  limiter: { max: 100, duration: 60_000 }, // 100 jobs per minute, per worker
});
```

The limit is enforced client-side by each worker, so with 3 identical workers the effective rate is 3x.

## Using AI agents?

Agents connected via [MCP](/guide/mcp/) can set and clear both limits in natural language ("rate limit emails to 50 per second") through the `bunqueue_set_rate_limit`, `bunqueue_clear_rate_limit`, `bunqueue_set_concurrency`, and `bunqueue_clear_concurrency` tools.

## Reference

| Control | Scope | Window | How |
|---------|-------|--------|-----|
| Rate limit | Queue (all workers) | Fixed 1 second | `bunqueue rate-limit set`, `queue.setGlobalRateLimit(max)` |
| Concurrency limit | Queue (all workers) | n/a | `bunqueue concurrency set`, `queue.setGlobalConcurrency(n)` |
| Worker concurrency | One worker | n/a | `new Worker(..., { concurrency })` |
| Worker limiter | One worker | Any duration | `new Worker(..., { limiter: { max, duration } })` |

## Gotchas

:::caution[The queue-level duration argument is ignored]
`queue.setGlobalRateLimit(max, duration?)` accepts a `duration` argument for BullMQ API compatibility, but the server ignores it: the queue-level limit is always `max` jobs **per second**. For a custom window, use the per-worker `limiter` option above.
:::

For rate limiting that must be shared with code outside the queue (for example, an API budget also consumed by web requests), use an external limiter inside your processor:

```typescript
const worker = new Worker('emails', async (job) => {
  await ratelimit.limit('email-send'); // external limiter, e.g. Upstash
  await sendEmail(job.data);
});
```

:::tip[Related Guides]
- [Queue API](/guide/queue/) - Queue configuration options
- [Worker API](/guide/worker/) - Worker concurrency settings
- [Environment Variables](/guide/env-vars/) - Server-side rate limit defaults
:::
