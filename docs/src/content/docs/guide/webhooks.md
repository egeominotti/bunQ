---
title: "Bunqueue Webhooks & Job Event Notifications"
description: Configure webhooks with HMAC-SHA256 signatures for bunqueue job events. HTTP callbacks on push, start, completion, failure, and progress with auto-retry.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/advanced.png
---


<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">guide · webhooks</span>
  <h1 class="bq-hero-h1 bq-bench-h1">The queue <em>calls you back.</em></h1>
  <p class="bq-hero-sub">Receive HTTP callbacks when job events occur, real-time notifications to external systems without polling. HMAC-SHA256 signed payloads with automatic retry.</p>

  <div class="bq-proof">
    <span><b>5</b> event types</span>
    <span><b>HMAC-SHA256</b> signatures</span>
    <span><b>3</b> delivery attempts with backoff</span>
    <span><b>10 s</b> delivery timeout</span>
  </div>
</div>

## Overview

When you register a webhook URL, bunqueue sends HTTP POST requests to that URL whenever specified events occur. This is useful for:

- Notifying external services when jobs complete
- Triggering downstream workflows
- Logging job events to external systems
- Alerting on failures

## CLI Commands

### Add Webhook

```bash
# Add webhook for specific events (--events is required)
bunqueue webhook add https://api.example.com/webhooks/bunqueue \
  --events job.completed,job.failed

# Add webhook for a specific queue
bunqueue webhook add https://api.example.com/webhooks/emails \
  --events job.completed,job.failed,job.progress --queue emails

# Add webhook with a signing secret
bunqueue webhook add https://api.example.com/webhooks/failures \
  --events job.failed --secret my-webhook-secret
```

:::note[SSRF protection]
Webhook URLs are validated at registration time: URLs pointing to localhost, private or link-local IP ranges (IPv4 and IPv6, including IPv4-mapped IPv6), or cloud metadata endpoints are **rejected**. Only `http:` and `https:` URLs up to 2048 characters are accepted.
:::

### List Webhooks

```bash
bunqueue webhook list
```

**Output:**
```
  01920b5e-7c4a-7000-8a3e-2f9d1c4b6e10: https://api.example.com/webhooks/bunqueue
    Events: job.completed, job.failed
    Delivered: 42 ok / 0 failed

  01920b5e-9d2b-7000-b1c7-8e5f3a2d9c44: https://api.example.com/webhooks/emails
    Events: job.completed, job.failed, job.progress
    Queue: emails
    Delivered: 8 ok / 1 failed
```

Each entry starts with the webhook ID, which is what `webhook remove` and `SetWebhookEnabled` take. Disabled webhooks are marked `[disabled]`.

### Enable/Disable Webhook

Toggling a webhook's enabled state is not a CLI command (the CLI supports `webhook list`, `add`, `remove`). Use the TCP protocol command `SetWebhookEnabled`, the HTTP API, or the MCP tool:

```text
# MCP tool
bunqueue_set_webhook_enabled({ id: "01920b5e-7c4a-7000-8a3e-2f9d1c4b6e10", enabled: false })
```

Disabling a webhook stops event delivery but preserves the configuration. This is useful for temporarily pausing webhook notifications during maintenance.

### Remove Webhook

```bash
# Remove webhook by its ID (shown in webhook list output)
bunqueue webhook remove 01920b5e-7c4a-7000-8a3e-2f9d1c4b6e10
```

## Event Types

These are the only valid webhook events, the canonical set
(`job.pushed`, `job.started`, `job.completed`, `job.failed`, `job.progress`)
shared by the CLI, TCP, HTTP, and MCP. Subscribing to any other event (e.g.
`job.active`, `job.waiting`, `job.delayed`, `job.stalled`) is **rejected** at
registration time with `Invalid webhook event(s): … Valid: job.pushed,
job.started, job.completed, job.failed, job.progress`.

| Event | Description | When Triggered |
|-------|-------------|----------------|
| `job.pushed` | Job added to queue | After `queue.add()` / `push` |
| `job.started` | Job picked up for processing | Worker leases the job |
| `job.completed` | Job finished successfully | Worker completes job |
| `job.failed` | Job failed | Worker throws error |
| `job.progress` | Progress updated | `job.updateProgress()` |

## Webhook Payload

### Request Format

bunqueue sends POST requests with JSON body:

```http
POST /webhooks/bunqueue HTTP/1.1
Host: api.example.com
Content-Type: application/json
X-Webhook-Event: job.completed
X-Webhook-Timestamp: 1704067200000
X-Webhook-Signature: a1b2c3d4e5f6...

{
  "event": "job.completed",
  "timestamp": 1704067200000,
  "jobId": "1001",
  "queue": "emails",
  "data": { "sent": true }
}
```

All payloads carry `event`, `timestamp`, `jobId`, and `queue`. The optional `data`, `error`, and `progress` fields depend on the event.

### Event-Specific Payloads

**job.completed**, `data` is the job **result** returned by the worker:
```json
{
  "event": "job.completed",
  "timestamp": 1704067200000,
  "jobId": "1001",
  "queue": "emails",
  "data": { "sent": true }
}
```

**job.failed**, `data` is the job's input data, plus the error message:
```json
{
  "event": "job.failed",
  "timestamp": 1704067200000,
  "jobId": "1001",
  "queue": "emails",
  "data": { "to": "user@example.com", "subject": "Welcome" },
  "error": "SMTP connection timeout"
}
```

**job.progress**, carries the progress percentage (no `data` field):
```json
{
  "event": "job.progress",
  "timestamp": 1704067200000,
  "jobId": "1001",
  "queue": "emails",
  "progress": 75
}
```

**job.pushed** and **job.started** carry the base fields only.

## Signature Verification

When a webhook is registered with a `--secret` flag, bunqueue signs all payloads using HMAC-SHA256. Always verify signatures to ensure requests are authentic.

:::caution[Security Warning]
If a webhook is registered without `--secret`, payloads are sent **without signatures**. This means anyone could forge webhook requests to your endpoint. Always set a per-webhook secret in production.
:::

### Setting the Secret

Secrets are configured per-webhook when adding them via the `--secret` flag:

```bash
bunqueue webhook add https://api.example.com/webhooks/bunqueue \
  --events job.completed,job.failed --secret your-secret-key
```

### Verifying Signatures

**TypeScript/JavaScript:**
```typescript
import { createHmac, timingSafeEqual } from 'crypto';

function verifyWebhookSignature(
  payload: string,
  signature: string,
  secret: string
): boolean {
  const expected = createHmac('sha256', secret)
    .update(payload)
    .digest('hex');

  const expectedBuf = Buffer.from(expected);
  const receivedBuf = Buffer.from(signature);

  if (expectedBuf.length !== receivedBuf.length) {
    return false;
  }

  return timingSafeEqual(expectedBuf, receivedBuf);
}

// Usage in webhook handler
app.post('/webhooks/bunqueue', (req, res) => {
  const signature = req.headers['x-webhook-signature'] as string;
  const payload = JSON.stringify(req.body);

  if (!verifyWebhookSignature(payload, signature, MY_WEBHOOK_SECRET)) {
    return res.status(401).json({ error: 'Invalid signature' });
  }

  // Process webhook...
  res.status(200).json({ received: true });
});
```

**Python:**
```python
import hmac
import hashlib

def verify_webhook_signature(payload: bytes, signature: str, secret: str) -> bool:
    expected = hmac.new(
        secret.encode(),
        payload,
        hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(expected, signature)

# Usage in Flask
@app.route('/webhooks/bunqueue', methods=['POST'])
def handle_webhook():
    signature = request.headers.get('X-Webhook-Signature')
    payload = request.get_data()

    if not verify_webhook_signature(payload, signature, MY_WEBHOOK_SECRET):
        return jsonify({'error': 'Invalid signature'}), 401

    # Process webhook...
    return jsonify({'received': True})
```

**Go:**
```go
import (
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
)

func verifyWebhookSignature(payload []byte, signature, secret string) bool {
    mac := hmac.New(sha256.New, []byte(secret))
    mac.Write(payload)
    expected := hex.EncodeToString(mac.Sum(nil))

    return hmac.Equal([]byte(expected), []byte(signature))
}
```

## Retry Behavior

bunqueue automatically retries failed webhook deliveries with linear backoff. The total number of delivery attempts is controlled by the `WEBHOOK_MAX_RETRIES` environment variable (default: 3, one immediate attempt plus two retries) and the base delay by `WEBHOOK_RETRY_DELAY_MS` (default: 1000ms):

| Attempt | Delay |
|---------|-------|
| 1 | Immediate |
| 2 | 1 second (`WEBHOOK_RETRY_DELAY_MS * 1`) |
| 3 | 2 seconds (`WEBHOOK_RETRY_DELAY_MS * 2`) |

After all attempts are exhausted, the webhook delivery is abandoned and logged.

### Success Criteria

A webhook delivery is considered successful if:
- HTTP status code is 2xx (200-299)
- Response is received within 10 seconds

### Failure Handling

Webhook failures are logged but don't affect job processing. Use `bunqueue webhook list` to review registered webhooks.

## Example Webhook Server

Complete example using Hono (works with Bun):

```typescript
import { Hono } from 'hono';
import { createHmac, timingSafeEqual } from 'crypto';

const app = new Hono();

// The secret you used when registering the webhook with --secret
const MY_WEBHOOK_SECRET = process.env.MY_WEBHOOK_SECRET || 'your-secret';

// Signature verification middleware
async function verifySignature(c: any, next: any) {
  const signature = c.req.header('x-webhook-signature');
  const body = await c.req.text();

  if (!signature) {
    return c.json({ error: 'Missing signature' }, 401);
  }

  const expected = createHmac('sha256', MY_WEBHOOK_SECRET)
    .update(body)
    .digest('hex');

  const expectedBuf = Buffer.from(expected);
  const receivedBuf = Buffer.from(signature);

  if (expectedBuf.length !== receivedBuf.length ||
      !timingSafeEqual(expectedBuf, receivedBuf)) {
    return c.json({ error: 'Invalid signature' }, 401);
  }

  // Store parsed body for handler
  c.set('webhookPayload', JSON.parse(body));
  await next();
}

// Webhook handler
app.post('/webhooks/bunqueue', verifySignature, async (c) => {
  const payload = c.get('webhookPayload');

  console.log(`Received ${payload.event} event for job ${payload.jobId}`);

  switch (payload.event) {
    case 'job.completed':
      console.log('Job completed:', payload.jobId);
      // Notify downstream service
      await notifyCompletion(payload);
      break;

    case 'job.failed':
      console.error('Job failed:', payload.error);
      // Alert on failure
      await sendAlert({
        type: 'job_failed',
        jobId: payload.jobId,
        queue: payload.queue,
        error: payload.error,
      });
      break;

    case 'job.progress':
      console.log(`Job ${payload.jobId}: ${payload.progress}%`);
      // Update UI or notify
      await broadcastProgress(payload);
      break;
  }

  return c.json({ received: true });
});

// Health check endpoint
app.get('/health', (c) => c.json({ status: 'ok' }));

export default {
  port: 3000,
  fetch: app.fetch,
};
```

## Express.js Example

```typescript
import express from 'express';
import crypto from 'crypto';

const app = express();
app.use(express.json({
  verify: (req: any, res, buf) => {
    req.rawBody = buf;
  }
}));

// The secret you used when registering the webhook with --secret
const MY_WEBHOOK_SECRET = process.env.MY_WEBHOOK_SECRET!;

function verifySignature(req: any, res: any, next: any) {
  const signature = req.headers['x-webhook-signature'];

  if (!signature) {
    return res.status(401).json({ error: 'Missing signature' });
  }

  const expected = crypto
    .createHmac('sha256', MY_WEBHOOK_SECRET)
    .update(req.rawBody)
    .digest('hex');

  if (!crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(signature))) {
    return res.status(401).json({ error: 'Invalid signature' });
  }

  next();
}

app.post('/webhooks/bunqueue', verifySignature, async (req, res) => {
  const { event, jobId, queue, data, error } = req.body;

  console.log(`[${queue}] ${event}: Job ${jobId}`);

  if (event === 'job.completed') {
    // Handle completion
    console.log('Completed job:', jobId);
  } else if (event === 'job.failed') {
    // Handle failure
    console.error('Error:', error);
  }

  res.json({ received: true });
});

app.listen(3000, () => {
  console.log('Webhook server listening on port 3000');
});
```

## Best Practices

### 1. Always Verify Signatures

Never trust webhook payloads without signature verification. Attackers could forge requests.

### 2. Respond Quickly

Return a 200 response as soon as possible. Process webhooks asynchronously if needed:

```typescript
app.post('/webhooks/bunqueue', verifySignature, async (c) => {
  const payload = c.get('webhookPayload');

  // Queue for async processing
  await processQueue.add('webhook', payload);

  // Return immediately
  return c.json({ received: true });
});
```

### 3. Handle Duplicates

Webhooks may be delivered multiple times due to retries. Use the `jobId` and `event` combination to deduplicate:

```typescript
const processedWebhooks = new Set<string>();

app.post('/webhooks/bunqueue', verifySignature, async (c) => {
  const payload = c.get('webhookPayload');
  const dedupeKey = `${payload.jobId}:${payload.event}`;

  if (processedWebhooks.has(dedupeKey)) {
    return c.json({ received: true, duplicate: true });
  }

  processedWebhooks.add(dedupeKey);
  // Process webhook...
});
```

### 4. Log Everything

Log all webhook events for debugging:

```typescript
app.post('/webhooks/bunqueue', verifySignature, async (c) => {
  const payload = c.get('webhookPayload');

  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    event: payload.event,
    queue: payload.queue,
    jobId: payload.jobId,
  }));

  // Process...
});
```

### 5. Monitor Delivery

Monitor webhook registrations and server logs for delivery issues:

```bash
# Review registered webhooks
bunqueue webhook list
```

## Troubleshooting

### Webhooks Not Delivered

1. Check webhook is registered and not `[disabled]`: `bunqueue webhook list`
2. Check the `Delivered: N ok / N failed` counters in `bunqueue webhook list`
3. Verify URL is accessible from the server (localhost and private IPs are rejected at registration)
4. Check firewall rules
5. Review the bunqueue server logs (webhook delivery failures are logged with the target URL)

### Invalid Signature Errors

1. Verify secret matches on both ends
2. Check payload isn't modified by proxies
3. Ensure raw body is used for signature verification
4. Check for encoding issues (UTF-8)

### Slow Webhook Processing

1. Return 200 immediately, process async
2. Add more webhook endpoints
3. Implement request queuing

### Missing Events

1. Check event filter: `--events` flag
2. Verify queue filter: `--queue` flag
3. Remember `job.progress` only fires when a processor calls `job.updateProgress()`

:::tip[Related Guides]
- [Queue API](/guide/queue/) - Queue events and configuration
- [Environment Variables](/guide/env-vars/) - Webhook retry configuration
- [Server Mode](/guide/server/) - Webhooks require server mode
:::
