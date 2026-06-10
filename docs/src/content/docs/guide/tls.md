---
title: "Native TLS"
description: Encrypt bunqueue TCP and HTTP traffic with native TLS — no reverse proxy needed. Server cert/key setup, client options, CLI flags, self-signed certs.
---

bunqueue terminates TLS natively on both the TCP (msgpack) and HTTP servers.
No reverse proxy required. TLS is opt-in: without cert/key configuration the
server behaves exactly as before (plaintext).

## Server

```bash
# CLI flags
bunqueue start --tls-cert ./cert.pem --tls-key ./key.pem

# Or environment variables
TLS_CERT_FILE=./cert.pem TLS_KEY_FILE=./key.pem bunqueue start
```

Or in `bunqueue.config.ts`:

```typescript
import { defineConfig } from 'bunqueue';

export default defineConfig({
  server: {
    tlsCertFile: './cert.pem',
    tlsKeyFile: './key.pem',
  },
});
```

One cert pair covers both servers: TCP (`:6789`) and HTTP/WebSocket/SSE
(`:6790`, becomes `https://` / `wss://`).

The server fails fast at startup if the cert or key file is missing, or if
only one of the two is set — it never silently falls back to plaintext.

## Client SDK

```typescript
import { Queue, Worker } from 'bunqueue/client';

// Public CA (Let's Encrypt etc.) — verify with system CAs
const queue = new Queue('jobs', {
  connection: { host: 'queue.example.com', port: 6789, tls: true },
});

// Private CA or self-signed: trust a specific CA file
const queue2 = new Queue('jobs', {
  connection: { host: '10.0.0.5', port: 6789, tls: { caFile: './ca.pem' } },
});

// Dev only: skip verification
const queue3 = new Queue('jobs', {
  connection: { host: 'localhost', port: 6789, tls: { rejectUnauthorized: false } },
});
```

`Worker` accepts the same `connection.tls` options. The msgpack protocol is
unchanged — TLS only wraps the transport.

## CLI client

```bash
bunqueue stats --host queue.example.com --tls            # system CAs
bunqueue stats --tls-ca ./ca.pem                          # custom CA
bunqueue stats --tls-no-verify                            # self-signed, dev only
```

## Self-signed certificate (dev / internal networks)

```bash
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout key.pem -out cert.pem \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
```

Clients then connect with `tls: { caFile: './cert.pem' }` (the self-signed
cert acts as its own CA) — full verification, no `rejectUnauthorized: false`
needed.

## Notes

- TLS + auth tokens compose: use both for servers exposed beyond localhost.
- A TLS-enabled server only accepts TLS clients; plaintext clients fail the
  handshake (they do not hang).
- HTTP endpoints (`/health`, dashboards, `/ws`, `/events`) are served over
  `https://`/`wss://` when TLS is enabled.
