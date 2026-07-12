---
title: "bunqueue Security: Authentication, TLS, Hardening & Disclosure"
description: "The bunqueue security model: token authentication, native TLS, network isolation, abuse protection and how to report a vulnerability."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/security.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">reference · security</span>
  <h1 class="bq-hero-h1 bq-bench-h1">Security, hardened by <em>default.</em></h1>
  <p class="bq-hero-sub">The bunqueue security model: the defaults you get out of the box, the controls available to harden a deployment, and how to report vulnerabilities. Every statement on this page reflects the current codebase.</p>
</div>

## Reporting vulnerabilities

**Do not open a public issue for security vulnerabilities.**

Report privately through either channel:

- Email: **security@bunqueue.dev**
- GitHub private vulnerability reporting: open the
  [Security tab](https://github.com/egeominotti/bunqueue/security) and select
  "Report a vulnerability"

You will receive an acknowledgement within 48 hours. Fixes ship as patch
releases and are announced through GitHub Security Advisories and the npm
advisory database.

## Security model

A bunqueue server exposes two listeners: the TCP protocol on port 6789, used
by every client SDK, and the HTTP API on port 6790, used for health,
metrics, dashboards and the REST surface. Both listeners share the same
authentication and TLS configuration. The server is a single process and a
single trust domain: any authenticated client can operate on any queue.
Multi tenant isolation, when required, is achieved by running one instance
per tenant, or by namespacing queues with `prefixKey` where the boundary is
organizational rather than adversarial.

## Authentication

Authentication is token based and disabled until you configure it. When
`AUTH_TOKENS` is set, every TCP connection must authenticate as its first
command and every HTTP request must carry the token; this includes the
debug endpoints `/gc` and `/heapstats`, which reject unauthenticated calls.

```bash
AUTH_TOKENS=$(openssl rand -hex 32) bunqueue start
```

Or through the [configuration file](/guide/configuration/):

```typescript
// bunqueue.config.ts
import { defineConfig } from 'bunqueue';

export default defineConfig({
  auth: { tokens: [process.env.AUTH_TOKEN!] },
});
```

Clients pass the token in their connection options, identically across
languages:

```typescript
// TypeScript SDK (Node.js, Deno, Bun, Cloudflare Workers)
const queue = new Queue('emails', { host: 'q.internal', token: process.env.BUNQUEUE_TOKEN });
```

```python
# Python SDK
queue = Queue("emails", host="q.internal", token=os.environ["BUNQUEUE_TOKEN"])
```

Multiple tokens are supported (`AUTH_TOKENS=token1,token2`), which enables
zero downtime rotation: add the new token, roll clients over, remove the old
one. `/metrics` can additionally be gated with `METRICS_AUTH=true`.

## Transport security

Three options, in order of preference for typical deployments:

1. **Native TLS on both listeners.** Provide a certificate and key and both
   the TCP protocol and the HTTP API serve TLS directly, no proxy required.
   Partial configuration (one variable without the other) is a startup
   error, not a silent downgrade.

   ```bash
   TLS_CERT_FILE=./cert.pem TLS_KEY_FILE=./key.pem bunqueue start
   ```

   Clients verify against system certificate authorities by default and
   accept a custom CA bundle (`tls: { caFile: './ca.pem' }`). Disabling
   verification is possible for development only. See the
   [TLS guide](/guide/tls/).

2. **A Unix domain socket for the HTTP API** on same host deployments, where
   access control reduces to filesystem permissions. The TCP protocol has no
   Unix-socket support today (`TCP_SOCKET_PATH` is reserved but not applied),
   so bind it to loopback:

   ```bash
   HTTP_SOCKET_PATH=/run/bunqueue/http.sock HOST=127.0.0.1 bunqueue start
   ```

3. **A reverse proxy** (nginx, Caddy) terminating TLS in front of the HTTP
   API, with the server bound to localhost.

## Network exposure and defaults

Defaults favor a working local setup. Review this table before exposing an
instance beyond a trusted network:

| Setting | Default | Production recommendation |
|---|---|---|
| `AUTH_TOKENS` | unset, no authentication | Always set; rotate with multiple tokens |
| `HOST` | `0.0.0.0`, all interfaces | Bind to `127.0.0.1` or a private interface unless remote clients need direct access |
| TLS | disabled | Enable native TLS or terminate at a proxy |
| `CORS_ALLOW_ORIGIN` | unset, no cross origin access is granted | Set explicitly, and only to your dashboard origins, when a browser client needs the HTTP API |
| `METRICS_AUTH` | `false`, `/metrics` is public | Set `true` if metrics may leak operational detail |
| Protocol rate limit | 10,000 requests per 60 s per client | Tune with `RATE_LIMIT_MAX_REQUESTS` / `RATE_LIMIT_WINDOW_MS` |

## Abuse protection

- **Protocol rate limiting.** A sliding window limiter caps requests per
  client on the wire, 10,000 per 60 seconds by default, configurable via
  environment variables.
- **Frame size cap.** TCP frames are limited to 64 MB; oversized frames are
  rejected before allocation, preventing memory exhaustion.
- **Per queue controls.** Rate limits and global concurrency caps can be set
  per queue at runtime (`RateLimit`, `SetConcurrency`).
- **Webhook SSRF protection.** Webhook URLs are validated before
  registration: only `http`/`https`, no localhost or loopback, no private
  IPv4 ranges, no IPv6 unique local, link local or IPv4 mapped bypasses, and
  no cloud metadata endpoints. Invalid targets are rejected at
  `AddWebhook` time.
- **Input validation.** Queue names are restricted to a safe character set,
  job payloads are capped at 10 MB, and numeric options are bounds checked
  server side.

## Data protection

- **At rest.** Persistence is a single SQLite file in WAL mode. Restrict it
  to the service user (`chmod 600`) and place it on an encrypted volume
  where the platform provides one. The `-wal` and `-shm` sidecar files live
  in the same directory and inherit the same handling.
- **Backups.** S3 backups support server side encryption; scope the IAM
  credentials to the single backup bucket.
- **Job payloads.** Do not place secrets in job data. Store a reference and
  resolve it inside the worker:

  ```typescript
  // Avoid
  await queue.add('task', { apiKey: 'secret123' });

  // Prefer
  await queue.add('task', { secretRef: 'vault:api-key' });
  ```

- **Cloud telemetry.** The optional bunqueue.io integration sends metrics
  only: job payloads are excluded by default
  (`BUNQUEUE_CLOUD_INCLUDE_JOB_DATA=false`), specific fields can be redacted
  (`BUNQUEUE_CLOUD_REDACT_FIELDS`), and outgoing events can be signed with
  HMAC (`BUNQUEUE_CLOUD_SIGNING_SECRET`). Remote commands are disabled by
  default.

## Hardening checklist

```bash
AUTH_TOKENS=$(openssl rand -hex 32) \
TLS_CERT_FILE=/etc/bunqueue/cert.pem \
TLS_KEY_FILE=/etc/bunqueue/key.pem \
HOST=10.0.0.5 \
CORS_ALLOW_ORIGIN=https://dashboard.example.com \
METRICS_AUTH=true \
BUNQUEUE_DATA_PATH=/data/bunq.db \
bunqueue start
```

1. Set `AUTH_TOKENS`; never run an exposed instance unauthenticated.
2. Enable TLS, natively or at a proxy; use Unix sockets when everything is
   on one host.
3. Bind `HOST` to the narrowest interface that still reaches your clients.
4. Leave `CORS_ALLOW_ORIGIN` unset unless a browser client needs the HTTP
   API; when it does, list the exact origins.
5. Gate `/metrics` with `METRICS_AUTH=true` where metrics are sensitive.
6. Run as an unprivileged user, `chmod 600` the data file, encrypt the
   volume.
7. Keep the data directory on a persistent volume and enable S3 backups
   with server side encryption.
8. Monitor `/health`, watch authentication failures in the logs, and alert
   on unusual job patterns.

## Supported versions

Security fixes are released as patch versions on the current 2.x line.
There are no long term support branches: keep the server and the client
SDKs (`bunqueue`, `bunqueue-client`) on the latest release. Updates are
announced through GitHub Security Advisories and npm advisories.

:::tip[Related]
- [Native TLS](/guide/tls/), certificates, custom CAs, client options
- [Environment Variables](/guide/env-vars/), the full configuration reference
- [Deployment Guide](/guide/deployment/), Docker, systemd, PM2
- [Server Mode](/guide/server/), running and operating the server
:::
