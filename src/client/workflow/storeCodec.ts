/**
 * msgpack codec shared by the workflow store and its signal coordinator.
 * Kept in its own module so both can encode blobs identically without either
 * owning the packer.
 */

import { Packr, Unpackr } from 'msgpackr';

const packr = new Packr({ structuredClone: true });
const unpackr = new Unpackr({ structuredClone: true });

export function pack(data: unknown): Uint8Array {
  return packr.pack(data);
}

export function unpack(buf: Uint8Array | null | undefined): unknown {
  if (!buf) return null;
  return unpackr.unpack(buf);
}
