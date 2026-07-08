/** Socket factory: opens a plain or TLS socket with a connect timeout. */

import { readFileSync } from 'node:fs';
import { connect as netConnect, type Socket } from 'node:net';
import { type ConnectionOptions as TlsConnectOptions, connect as tlsConnect } from 'node:tls';
import type { TlsOption } from './connection-types.js';
import { ConnectionClosedError } from './errors.js';

export function openSocket(
  host: string,
  port: number,
  tls: TlsOption,
  connectTimeoutMs: number
): Promise<Socket> {
  return new Promise((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      socket.destroy();
      reject(new ConnectionClosedError(`connect timeout to ${host}:${port}`));
    }, connectTimeoutMs);

    const onError = (err: Error) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      reject(new ConnectionClosedError(`connect failed: ${err.message}`));
    };
    const onReady = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.off('error', onError);
      resolve(socket);
    };

    let socket: Socket;
    if (tls) {
      const tlsOpts: TlsConnectOptions = { host: host, port: port };
      if (typeof tls === 'object') {
        if (tls.caFile) tlsOpts.ca = readFileSync(tls.caFile);
        if (tls.rejectUnauthorized === false) tlsOpts.rejectUnauthorized = false;
      }
      socket = tlsConnect(tlsOpts, onReady);
    } else {
      socket = netConnect({ host: host, port: port }, onReady);
    }
    socket.on('error', onError);
  });
}
