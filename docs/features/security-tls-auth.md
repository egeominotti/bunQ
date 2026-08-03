# Security: TLS, Auth, CORS

> **Category:** Infrastructure · **Source:** `src/infrastructure/server/tls.ts`, `src/client/resolveToken.ts`, `src/shared/webhookValidation.ts`, `src/config/resolve.ts`, `src/infrastructure/server/http.ts`, `src/infrastructure/server/tcp.ts`, `src/infrastructure/server/handler.ts`, `src/client/tcp/connection.ts`, `src/shared/hash.ts`, `src/application/webhookManager.ts`, `src/infrastructure/cloud/httpSender.ts`

## Purpose

This module groups the transport- and application-layer security primitives that protect a bunqueue server and its clients: native TLS termination on both the TCP and HTTP listeners, bearer-token authentication on both transports, CORS for browser dashboards, and outbound HMAC signing/SSRF protection for webhooks and the Cloud uplink. There is no auth framework or session store — security is a set of small, explicit checks wired into the server entrypoints and the client transport. Everything uses Bun-native crypto/file APIs; there are zero external security dependencies.

## Responsibilities & Scope

Owns:

- **Server TLS option building** — validating PEM cert/key paths and producing the `tls` object passed to `Bun.listen`/`Bun.serve` (`tls.ts`); partial-config fail-fast (`resolve.ts`).
- **Client TLS option mapping** — translating `tls: true | { caFile } | { rejectUnauthorized }` into Bun's `tls` connect option (`connection.ts`).
- **Token resolution precedence** for the client SDK and CLI (`resolveToken.ts`).
- **Bearer-token auth gates** on the HTTP fetch handler and the TCP command router, including the constant-time token compare (`hash.ts`).
- **CORS** header/preflight construction for HTTP responses.
- **SSRF prevention** for webhook URLs (`webhookValidation.ts`).
- **HMAC signing** of outbound webhook deliveries (`webhookManager.ts`) and Cloud snapshot uploads (`httpSender.ts`).

Does NOT own:

- Rate limiting per client IP — see `getRateLimiter()` and [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md).
- The HTTP route table / SSE / WebSocket plumbing — see [HTTP / REST / SSE / WebSocket API](./http-api.md).
- TCP framing and the wire protocol — see [TCP Wire Protocol & Framing](./tcp-protocol.md) and [TCP Server Command Handlers](./tcp-server-handlers.md).
- Webhook subscription storage and event matching — see [Webhooks, Events & Job Logs](./webhooks-and-events.md). This module only owns the URL validation and signing helpers.
- Cloud transport/circuit-breaker semantics — see [bunqueue Cloud Dashboard Integration](./cloud-integration.md).

## Dependencies

Internal:

- `src/shared/hash.ts` — `constantTimeEqual(a, b)` for token comparison; `uuid()` for client IDs.
- `src/config/resolve.ts` — env/file → `ResolvedConfig`, including `resolveTlsServerOptions`.
- `src/shared/webhookValidation.ts` — re-exported through `src/infrastructure/server/protocol.ts` and consumed by both the TCP `AddWebhook` handler and `WebhookManager`.

External / runtime (all Bun built-ins):

- `Bun.listen` / `Bun.serve` `tls` option (server termination).
- `Bun.connect` `tls` option (client).
- `Bun.file(path)` — lazily reads PEM cert/key/CA files.
- `Bun.CryptoHasher('sha256', secret)` — HMAC-SHA256 for webhooks.
- `crypto.subtle` (WebCrypto) — HMAC-SHA256 for Cloud uploads.
- `node:fs` `existsSync` — cert/key path existence check.

## Public Interface

Exported functions / types:

```typescript
// src/infrastructure/server/tls.ts
interface TlsServerOptions { certFile: string; keyFile: string; }
function loadTlsOptions(tls: TlsServerOptions): { cert: BunFile; key: BunFile };

// src/config/resolve.ts
function resolveTlsServerOptions(config: { tlsCertFile?: string; tlsKeyFile?: string })
  : { certFile: string; keyFile: string } | null;

// src/client/resolveToken.ts
function resolveToken(explicitToken: string | undefined): string | undefined;

// src/client/tcp/types.ts
interface ClientTlsOptions { rejectUnauthorized?: boolean; caFile?: string; }

// src/client/tcp/connection.ts
function buildClientTls(tls: boolean | ClientTlsOptions | undefined)
  : true | Record<string, unknown> | undefined;

// src/shared/webhookValidation.ts
function validateWebhookUrl(url: string): string | null; // null = valid

// src/shared/hash.ts
function constantTimeEqual(a: string, b: string): boolean;
```

TCP command handled:

- `Auth` (`{ cmd: 'Auth'; token: string }`) — authenticates the connection. Always permitted even when unauthenticated (`handler.ts:53`).

HTTP endpoints (auth behavior, not new routes):

- `OPTIONS *` → 204 CORS preflight (`corsResponse`).
- `GET /health`, `GET /healthz`, `GET /live`, `GET /ready` — **never** auth-gated.
- `POST /gc`, `GET /heapstats` — **always** auth-gated (debug).
- `GET /prometheus` — auth-gated **only** when `requireAuthForMetrics` is true;
  if true with an empty token set, returns 503 (fail closed).
- All other REST routes, `GET /ws*`, `GET /events*` — auth-gated when tokens are configured.

Events emitted (via `queueManager.emitDashboardEvent`):

- `auth:failed` — `{ transport: 'http' }` on HTTP gate failure (`http.ts:197-203`); `{ clientId }` on TCP `Auth` failure (`handler.ts:41`).

## Data Models

- `TlsServerOptions { certFile, keyFile }` — PEM file paths for the server.
- `ClientTlsOptions { rejectUnauthorized?, caFile? }` — client trust config; `caFile` is read via `Bun.file` and mapped to Bun's `ca`.
- `HandlerContext { queueManager, authTokens: Set<string>, authenticated: boolean, clientId? }` — carried through both transports; `authenticated` is the per-connection (TCP/WS) auth flag. See [data-model](../data-model.md).
- Webhook delivery headers: `X-Webhook-Event`, `X-Webhook-Timestamp`, and (when a secret is set) `X-Webhook-Signature` = hex HMAC-SHA256 of the JSON body.
- Cloud upload headers: `Authorization: Bearer <apiKey>`, `X-Timestamp`, and (when `signingSecret` is set) `X-Signature` = hex HMAC-SHA256 of the compressed body.

## Business Logic / Control Flow

### Server TLS

1. `resolveServerConfig` pulls `tlsCertFile`/`tlsKeyFile` from `TLS_CERT_FILE`/`TLS_KEY_FILE` (config file wins) — `resolve.ts:42-43`.
2. `resolveTlsServerOptions` returns `null` when neither is set, and **throws** when exactly one is set (`resolve.ts:70-92`). The bootstrap catches this and `process.exit(1)` before any listener binds (`bootstrap.ts:114-121`) — TLS is all-or-nothing.
3. When TLS is configured, the resolved `{ certFile, keyFile }` is passed identically to both `createTcpServer` and `createHttpServer` (`bootstrap.ts:131-151`).
4. Each server calls `loadTlsOptions` **before** binding (`http.ts:254-268`, `tcp.ts:120-125`). `loadTlsOptions` does an `existsSync` check on each path and throws a descriptive error on a missing file (`tls.ts:22-29`), then returns `{ cert: Bun.file(...), key: Bun.file(...) }` which is spread into the listener's `tls` option. Validating before bind avoids a half-started listener on a bad path.
5. **Per-socket init is lazy and idempotent** (`TcpConnectionRegistry.init`, `tcp/connections.ts:27-49`). Under native TLS, Bun can deliver a `data` event **before** `open` runs; the handler previously destructured a null `socket.data`, and the resulting `TypeError` escalated to the process-level unhandledRejection handler — a single unauthenticated connection to the TLS port could shut the whole server down (pre-auth remote DoS, #108). `registry.init` runs from both `open` and lazily from `data` (`tcp.ts:29-36`, guarded by `if (socket.data) return`), preserving that first frame; `close`/`drain` early-return when `socket.data` is absent.

### Client TLS

`buildClientTls` (`client/tcp/transport.ts:26-35`): `undefined`/`false` → `undefined` (plaintext, the default); `true` → `true` (TLS with system CAs); object form → `{ rejectUnauthorized?, ca: readFileSync(caFile)? }` (only the provided keys are set; the CA is read into **bytes**, not a `Bun.file` handle, so Bun computes the peer `authorizationError` against it). The result is spread into `Bun.connect`. The connection-pool dedup key incorporates a serialized form of the TLS option (`client/tcpPool.ts:229-246`), so callers with different TLS configs never share a socket.

**Server-certificate verification (#109).** `Bun.connect` never rejects an unauthorized peer client-side, so verification is enforced in the `handshake` handler: `tlsRequiresVerification` makes verification the default for any TLS connection (only explicit `rejectUnauthorized: false` opts out to encryption-only), and a non-null `authorizationError` closes the socket and rejects with `TLS verification failed`. Because registering a `handshake` handler makes Bun fire `open` before the handshake completes, every TLS connection resolves on `handshake` rather than `open`. Full control-flow in [Client Transport](./client-transport.md).

### Token resolution (client/CLI)

`resolveToken` (`resolveToken.ts:10-20`) returns the first truthy of: explicit `connection.token` → `BQ_TOKEN` → `BUNQUEUE_TOKEN` → `undefined`. Empty strings are falsy and treated as unset. Queue construction (`queue/runtime/state.ts:39-68`), Worker pool creation (`worker/runtime/options.ts:30-45`), and the CLI (`cli/globalOptions.ts:217`) all resolve through this. After connecting, the TCP client sends `Auth` **only if** a token was resolved; `authenticate()` issues `{ cmd: 'Auth', token }` and throws `Authentication failed` on a non-ok reply (`client/tcp/runtime/connectivity.ts:73-90`).

### HTTP auth gate

`authTokens` is a `Set` built from config (`http.ts:65-68`). `checkAuth` (`http.ts:38-46`): if the set is empty it returns `null` (open server); otherwise it strips the `Bearer ` prefix from `Authorization` and runs `validateAuthToken`, which loops the valid tokens comparing each with `constantTimeEqual` (`http.ts:26-35`). On mismatch it returns a `401 { ok:false, error:'Unauthorized' }`. The gate is applied per-endpoint as described in Public Interface; the catch-all gate at `http.ts:197-204` also emits `auth:failed`.

### TCP auth gate

On `open`, `authenticated` is initialized to `authTokens.size === 0` — i.e. auto-authenticated when no tokens are configured (`TcpConnectionRegistry.init`, `tcp/connections.ts:27-48`). In `handleCommand` (`handler.ts:48-60`): `Auth` is always routed to `handleAuth`; any other command is rejected with `error('Not authenticated')` when tokens exist and the connection is not yet authenticated. `handleAuth` (`handler.ts:30-43`) compares the supplied token against each configured token with `constantTimeEqual`, sets `ctx.authenticated = true` on the first match, and emits `auth:failed` otherwise.

### CORS

`corsOrigins` is a `Set` (`http.ts:65-68`). `getCorsOrigin()` returns `'*'` if the set contains `'*'`, else the comma-joined origins (`http.ts:87-88`). `OPTIONS` preflights return 204 from `corsResponse` with `Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS`, `Access-Control-Allow-Headers: Content-Type, Authorization`, and `Access-Control-Max-Age: 86400` (`httpEndpoints.ts:32-45`). `withCors` attaches `Access-Control-Allow-Origin` to out-of-pipeline responses (health/prometheus/debug) without overwriting an origin the endpoint already set (`http.ts:89-99`).

### Webhook SSRF validation

`validateWebhookUrl` (`webhookValidation.ts:93-131`) rejects, returning an error string (else `null`): empty/`>2048` chars; unparseable URL; non-`http(s)` protocol; localhost variants (`localhost`, `127.0.0.1`, `::1`, `[::1]`, `*.localhost`); private/loopback/link-local/unspecified IPv4 (10/8, 172.16–31/12, 192.168/16, 169.254, 127, 0; `checkPrivateIpv4`); IPv4-mapped/IPv4-compatible IPv6 literals (`[::ffff:a.b.c.d]` and the WHATWG hex-normalized `[::ffff:XXXX:YYYY]` form are unwrapped to the embedded IPv4 and run through the same octet check, `extractMappedIpv4`); IPv6 ULA `fc00::/7`, link-local `fe80::/10`, and `::` unspecified (`checkBlockedIpv6`); and cloud-metadata hosts (`169.254.169.254`, `metadata.google.internal`, `*.internal`). `WebhookManager.add` calls it unless `validateUrls` is disabled (`webhookManager.ts:51-65`); the TCP `AddWebhook` handler calls it directly (`handlers/monitoring/webhooks.ts:13-29`).

### Webhook & Cloud HMAC signing

- Webhooks: `signPayload` builds an HMAC-SHA256 via `new Bun.CryptoHasher('sha256', secret)` over the JSON body and emits hex in `X-Webhook-Signature`, only when the webhook has a `secret` (`webhookManager.ts:22-27`, `137-139`).
- Cloud: when `signingSecret` is set, `HttpSender` imports the key once (`crypto.subtle.importKey`, cached in `hmacKey`), signs the **compressed** (`zstd(msgpack)`) body, and sets hex in `X-Signature` (`httpSender.ts:99-109`). The secret comes from `BUNQUEUE_CLOUD_SIGNING_SECRET` (`resolve.ts:115`).

## Concurrency & Locking

No queue locks are taken in this module. The only per-connection mutable security state is `HandlerContext.authenticated`, owned by a single TCP socket / WebSocket and mutated only by that connection's command handler, so there is no cross-connection race. `constantTimeEqual` is pure. `HttpSender.hmacKey` is lazily memoized; a benign double-import race is possible if two snapshots race the first send, but both produce an equivalent key.

## Edge Cases & Failure Modes

- **TLS all-or-nothing:** exactly one of cert/key set → startup throw + `process.exit(1)` (`resolve.ts:75-91`, `bootstrap.ts:114-121`). Missing file at a configured path → throw at server creation before bind (`tls.ts:22-29`).
- **Open server:** with no `AUTH_TOKENS`, HTTP `checkAuth` returns null for everything and TCP connections start pre-authenticated. `/gc` and `/heapstats` then have **no** auth (known gotcha — debug endpoints lack auth unless tokens are configured).
- **CORS default discrepancy:** `createHttpServer` defaults to `['*']` only when `corsOrigins` is `undefined` (`http.ts:65-68`). The server bootstrap always passes an array, and an unset `CORS_ALLOW_ORIGIN` resolves to `[]` (`resolve.ts:51`) → empty Set → `getCorsOrigin()` returns the empty string, so `Access-Control-Allow-Origin` is `""` rather than `*`. The permissive `*` default applies only to direct `createHttpServer` calls.
- **constantTimeEqual length leak:** it folds `a.length ^ b.length` into the result and iterates over `minLen`, so it compares content in constant time for equal-length inputs but does reveal a length mismatch via early divergence — acceptable for fixed-format tokens.
- **Token compare is O(validTokens):** every check loops all configured tokens; intended for small token lists.
- **Webhook delivery:** fire-and-forget with up to `WEBHOOK_MAX_RETRIES` attempts (default 3), linear backoff `WEBHOOK_RETRY_DELAY_MS * (attempt+1)` (default 1000ms), and a 10s per-request `AbortSignal.timeout` (`webhookManager.ts:142-171`). Signature is computed once and reused across retries.
- **Cloud signing:** signs the post-compression body; the receiver must verify over the same compressed bytes. On `401`/`403` the error is logged loudly rather than silently buffered (`httpSender.ts:122-128`).
- **SSRF validator is hostname-string based:** it does not resolve DNS, so a public hostname that resolves to a private IP (DNS rebinding) is not caught; only literal private/loopback/metadata hosts (IPv4 and the IPv6 literal forms listed above) and the listed protocols are blocked.

## Configuration

| Variable | Default | Effect |
| --- | --- | --- |
| `TLS_CERT_FILE` | unset | PEM cert/chain path. Both or neither with key, else startup error. |
| `TLS_KEY_FILE` | unset | PEM private-key path. |
| `AUTH_TOKENS` | unset → `[]` | Comma-separated bearer tokens; empty = open server. |
| `METRICS_AUTH` | `false` | When `true`, `/prometheus` requires a valid token; an empty `AUTH_TOKENS` set returns 503 instead of exposing metrics. |
| `CORS_ALLOW_ORIGIN` | unset → `[]` | Comma-separated allowed origins; via bootstrap an unset value yields empty `Access-Control-Allow-Origin`. |
| `BQ_TOKEN` | unset | Client/CLI token fallback (after explicit `connection.token`). |
| `BUNQUEUE_TOKEN` | unset | Client/CLI token fallback (after `BQ_TOKEN`). |
| `WEBHOOK_MAX_RETRIES` | `3` | Webhook delivery attempts. |
| `WEBHOOK_RETRY_DELAY_MS` | `1000` | Base linear-backoff delay between webhook retries. |
| `BUNQUEUE_CLOUD_SIGNING_SECRET` | unset | HMAC-SHA256 key for Cloud uploads (`X-Signature`). |

Client SDK options: `connection.token`, `connection.tls = true | { caFile } | { rejectUnauthorized }`. Webhook `secret` (per-webhook) enables `X-Webhook-Signature`. CLI: `--auth-tokens <list>`, `--token`, plus `--tls`/CA flags.

## Related Docs

- [HTTP / REST / SSE / WebSocket API](./http-api.md)
- [TCP Wire Protocol & Framing](./tcp-protocol.md)
- [TCP Server Command Handlers](./tcp-server-handlers.md)
- [Client Transport (TCP pool, reconnect, batching)](./client-transport.md)
- [Webhooks, Events & Job Logs](./webhooks-and-events.md)
- [bunqueue Cloud Dashboard Integration](./cloud-integration.md)
- [Rate Limiting & Concurrency Control](./rate-limiting-and-concurrency.md)
- [Configuration & Entrypoint](./configuration.md)
- [architecture](../architecture.md)
- [data-model](../data-model.md)
