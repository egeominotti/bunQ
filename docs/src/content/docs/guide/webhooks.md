---
title: "Webhooks: Get Notified When Jobs Complete or Fail"
description: bunqueue sends HTTP callbacks on job events, signed with HMAC-SHA256 and retried automatically. No polling needed.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/webhooks.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · webhooks</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Job events, delivered as <em>webhooks.</em></h1>
  <p class="bq-hero-sub">A webhook is an HTTP POST that bunqueue sends to your URL when something happens to a job. Instead of polling for status, your service gets told, with a signed payload and automatic retries.</p>
</div>

## Quick Start

Register a URL and pick the events you care about (`--events` is required):

```bash
bunqueue webhook add https://api.example.com/hooks/bunqueue \
  --events job.completed,job.failed --secret my-webhook-secret
```

Then receive the POSTs. A minimal Bun server:

```typescript
Bun.serve({
  port: 3000,
  async fetch(req) {
    const payload = await req.json();
    console.log(`${payload.event} for job ${payload.jobId} on ${payload.queue}`);
    return Response.json({ received: true });
  },
});
```

That is enough to see events flowing. In production, always verify the signature first (see below).

## Common Tasks

### Scope a webhook to one queue

```bash
bunqueue webhook add https://api.example.com/hooks/emails \
  --events job.completed,job.failed --queue emails
```

### List and remove webhooks

```bash
bunqueue webhook list
# 01920b5e-7c4a-...: https://api.example.com/hooks/bunqueue
#   Events: job.completed, job.failed
#   Delivered: 42 ok / 0 failed

bunqueue webhook remove 01920b5e-7c4a-7000-8a3e-2f9d1c4b6e10
```

Each entry starts with the webhook ID; that ID is what `remove` takes. Disabled webhooks are marked `[disabled]`.

### Temporarily disable a webhook

Toggling is not a CLI command. Use the TCP command `SetWebhookEnabled`, the HTTP API, or the MCP tool:

```text
bunqueue_set_webhook_enabled({ id: "01920b5e-...", enabled: false })
```

Disabling stops delivery but keeps the configuration, useful during maintenance.

## Event Types

These five events are the only valid ones; anything else is rejected at registration time:

| Event | When it fires |
|-------|---------------|
| `job.pushed` | Job added to a queue |
| `job.started` | A worker picked the job up |
| `job.completed` | The worker finished successfully |
| `job.failed` | The worker threw an error |
| `job.progress` | The processor called `job.updateProgress()` |

## Payload

Every delivery is a JSON POST with three headers: `X-Webhook-Event`, `X-Webhook-Timestamp`, and `X-Webhook-Signature` (only when a secret is set).

```json
{
  "event": "job.completed",
  "timestamp": 1704067200000,
  "jobId": "1001",
  "queue": "emails",
  "data": { "sent": true }
}
```

All payloads carry `event`, `timestamp`, `jobId`, and `queue`. The rest depends on the event:

- `job.completed`: `data` is the **result** returned by the worker
- `job.failed`: `data` is the job's input data, plus an `error` message
- `job.progress`: carries a `progress` number instead of `data`
- `job.pushed` and `job.started`: base fields only

## Verifying Signatures

When a webhook is registered with `--secret`, bunqueue signs each payload with HMAC-SHA256 (a keyed hash: only someone who knows the secret can produce a valid signature). Verify it before trusting the request:

```typescript
import { createHmac, timingSafeEqual } from 'crypto';

function verifySignature(rawBody: string, signature: string, secret: string): boolean {
  const expected = createHmac('sha256', secret).update(rawBody).digest('hex');
  const a = Buffer.from(expected);
  const b = Buffer.from(signature);
  return a.length === b.length && timingSafeEqual(a, b);
}

Bun.serve({
  port: 3000,
  async fetch(req) {
    const signature = req.headers.get('x-webhook-signature');
    const rawBody = await req.text();

    if (!signature || !verifySignature(rawBody, signature, process.env.WEBHOOK_SECRET!)) {
      return Response.json({ error: 'Invalid signature' }, { status: 401 });
    }

    const payload = JSON.parse(rawBody);
    // process payload ...
    return Response.json({ received: true });
  },
});
```

Two things matter in any language: compute the HMAC over the **raw request body** (not a re-serialized object), and compare with a constant-time function (`timingSafeEqual` in Node/Bun, `hmac.compare_digest` in Python, `hmac.Equal` in Go).

:::caution[Unsigned webhooks]
Without `--secret`, payloads are sent unsigned and anyone can forge requests to your endpoint. Always set a secret in production.
:::

## Delivery and Retries

A delivery succeeds when your endpoint returns a 2xx status within 10 seconds. Failed deliveries are retried with linear backoff:

| Attempt | Delay |
|---------|-------|
| 1 | Immediate |
| 2 | 1 second |
| 3 | 2 seconds |

The totals are configurable with `WEBHOOK_MAX_RETRIES` (default 3 attempts) and `WEBHOOK_RETRY_DELAY_MS` (default 1000). After all attempts, the delivery is abandoned and logged. Webhook failures never affect job processing.

Because of retries, the same event can arrive twice. Deduplicate on `jobId` + `event` if that matters to your handler. Also return 2xx quickly and do heavy work asynchronously, so slow processing does not get counted as a failed delivery.

## Gotchas

- **SSRF protection:** URLs pointing to localhost, private or link-local IP ranges, or cloud metadata endpoints are rejected at registration time. Only `http:`/`https:` URLs up to 2048 characters are accepted. This means you cannot register a webhook to a local dev server from the same machine.
- **Missing events:** check the `--events` and `--queue` filters, and remember `job.progress` only fires when a processor calls `job.updateProgress()`.
- **Nothing delivered:** run `bunqueue webhook list` and look at the `Delivered: N ok / N failed` counters and the `[disabled]` marker; delivery failures are also logged by the server with the target URL.
- **Invalid signature errors:** verify against the raw body, make sure no proxy rewrites the payload, and confirm both sides use the same secret.

:::tip[Related Guides]
- [Queue API](/guide/queue/) - In-process events without HTTP
- [Environment Variables](/guide/env-vars/) - Webhook retry configuration
- [Server Mode](/guide/server/) - Webhooks require server mode
:::
