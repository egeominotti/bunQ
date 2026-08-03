/**
 * Wire framing for the bunqueue TCP protocol: 4-byte big-endian length
 * prefix + msgpack payload. Runtime-neutral (plain Buffer operations).
 */

import { ConnectionClosedError, SerializationError } from './errors.js';

export const PROTOCOL_VERSION = 3;
export const MAX_FRAME_SIZE = 64 * 1024 * 1024; // mirror server-side limit

/** Drop undefined-valued keys so the msgpack frame stays minimal. */
export function compact<T extends Record<string, unknown>>(obj: T): T {
  const out: Record<string, unknown> = {};
  for (const key in obj) {
    if (obj[key] !== undefined) out[key] = obj[key];
  }
  return out as T;
}

/** Frame a msgpack payload with the 4-byte big-endian length prefix. */
export function frame(payload: Uint8Array): Buffer {
  if (payload.length > MAX_FRAME_SIZE) {
    throw new SerializationError(`frame size ${payload.length} exceeds maximum ${MAX_FRAME_SIZE}`);
  }
  const framed = Buffer.allocUnsafe(4 + payload.length);
  framed.writeUInt32BE(payload.length, 0);
  framed.set(payload, 4);
  return framed;
}

/** Incremental length-prefixed frame parser (O(total bytes) via offset cursor). */
export class FrameParser {
  private buffer: Buffer = Buffer.alloc(0);

  addData(data: Buffer): Buffer[] {
    const buffer = this.buffer.length === 0 ? data : Buffer.concat([this.buffer, data]);
    const frames: Buffer[] = [];
    let offset = 0;

    while (buffer.length - offset >= 4) {
      const length = buffer.readUInt32BE(offset);
      if (length > MAX_FRAME_SIZE) {
        this.buffer = Buffer.alloc(0);
        throw new ConnectionClosedError(`frame size ${length} exceeds maximum`);
      }
      if (buffer.length - offset < 4 + length) break;
      // explicit copy so frames never alias the retained tail
      frames.push(Buffer.from(buffer.subarray(offset + 4, offset + 4 + length)));
      offset += 4 + length;
    }

    if (offset >= buffer.length) {
      this.buffer = Buffer.alloc(0);
    } else if (offset === 0) {
      this.buffer = buffer;
    } else {
      this.buffer = Buffer.from(buffer.subarray(offset));
    }
    return frames;
  }

  clear(): void {
    this.buffer = Buffer.alloc(0);
  }
}
