/**
 * Server TLS options
 * Shared by the TCP (Bun.listen) and HTTP (Bun.serve) servers.
 */

import { existsSync } from 'node:fs';
import type { BunFile } from 'bun';

/** TLS configuration for a server (paths to PEM files) */
export interface TlsServerOptions {
  /** Path to the PEM certificate (or full chain) file */
  certFile: string;
  /** Path to the PEM private key file */
  keyFile: string;
}

/**
 * Validate cert/key paths and build the `tls` option object for
 * Bun.listen/Bun.serve. Fails fast with a descriptive error so a typo in a
 * path surfaces at startup instead of as an opaque handshake failure.
 */
export function loadTlsOptions(tls: TlsServerOptions): { cert: BunFile; key: BunFile } {
  if (!existsSync(tls.certFile)) {
    throw new Error(`TLS cert file not found: ${tls.certFile}`);
  }
  if (!existsSync(tls.keyFile)) {
    throw new Error(`TLS key file not found: ${tls.keyFile}`);
  }
  return { cert: Bun.file(tls.certFile), key: Bun.file(tls.keyFile) };
}
