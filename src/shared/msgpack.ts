import { pack, unpack, Unpackr } from 'msgpackr';

const unpackr = new Unpackr({ mapsAsObjects: false, useRecords: false });
const PROTO_KEY_BYTES = new TextEncoder().encode('__proto__');

function containsProtoKey(buffer: Uint8Array): boolean {
  for (let index = 0; index <= buffer.length - PROTO_KEY_BYTES.length; index++) {
    if (buffer[index] !== PROTO_KEY_BYTES[0]) continue;
    let matches = true;
    for (let offset = 1; offset < PROTO_KEY_BYTES.length; offset++) {
      if (buffer[index + offset] !== PROTO_KEY_BYTES[offset]) {
        matches = false;
        break;
      }
    }
    if (matches) return true;
  }
  return false;
}

function defineSafe(target: Record<PropertyKey, unknown>, key: PropertyKey, value: unknown): void {
  Object.defineProperty(target, key, {
    value,
    enumerable: true,
    configurable: true,
    writable: true,
  });
}

function materialize(value: unknown, seen: WeakMap<object, unknown>): unknown {
  if (value === null || typeof value !== 'object') return value;
  const cached = seen.get(value);
  if (cached !== undefined) return cached;

  if (Array.isArray(value)) {
    const result: unknown[] = [];
    seen.set(value, result);
    for (const item of value) result.push(materialize(item, seen));
    return result;
  }

  if (value instanceof Map) {
    const result: Record<PropertyKey, unknown> = {};
    seen.set(value, result);
    for (const [key, item] of value) {
      defineSafe(result, typeof key === 'symbol' ? key : String(key), materialize(item, seen));
    }
    return result;
  }

  if (
    value instanceof Date ||
    value instanceof RegExp ||
    value instanceof ArrayBuffer ||
    ArrayBuffer.isView(value)
  ) {
    return value;
  }

  const result: Record<PropertyKey, unknown> = {};
  seen.set(value, result);
  for (const key of Reflect.ownKeys(value)) {
    defineSafe(result, key, materialize(Reflect.get(value, key), seen));
  }
  return result;
}

export function encodeMessagePack(value: unknown): Uint8Array {
  return pack(value);
}

/** Decode maps through safe own properties, preserving keys such as `__proto__`. */
export function decodeMessagePack<T = unknown>(buffer: Uint8Array): T {
  if (!containsProtoKey(buffer)) return unpack(buffer) as T;
  return materialize(unpackr.unpack(buffer), new WeakMap()) as T;
}
