/**
 * Shared TCP Client Instances
 * One shared client per distinct connection target. Keyed by
 * host/port/token/tls so callers with different configs (notably TLS vs
 * plaintext to the same server) never receive each other's connection.
 */

import type { ConnectionOptions } from './types';
import { TcpClient } from './client';

/** Shared clients keyed by connection target */
const sharedClients = new Map<string, TcpClient>();

/** Build the sharing key from the connection-identity options */
function getClientKey(options?: Partial<ConnectionOptions>): string {
  const host = options?.host ?? 'localhost';
  const port = options?.port ?? 6789;
  const token = options?.token ?? '';
  const tokenHash = token ? String(Number(Bun.hash(token)) & 0xffff) : '0';
  const tlsKey = options?.tls ? JSON.stringify(options.tls) : '0';
  return `${host}:${port}:${tokenHash}:${tlsKey}`;
}

/** Get shared TCP client for the given connection target */
export function getSharedTcpClient(options?: Partial<ConnectionOptions>): TcpClient {
  const key = getClientKey(options);
  let client = sharedClients.get(key);
  if (!client) {
    client = new TcpClient(options);
    sharedClients.set(key, client);
  }
  return client;
}

/** Close all shared clients */
export function closeSharedTcpClient(): void {
  for (const client of sharedClients.values()) {
    client.close();
  }
  sharedClients.clear();
}
