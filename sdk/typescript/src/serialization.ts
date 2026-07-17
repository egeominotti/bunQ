/** Safe command serialization for the bunqueue wire protocol. */

import { pack } from 'msgpackr';
import type { Command } from './connection-types.js';
import { SerializationError } from './errors.js';
import { frame } from './frame.js';

const invalid = (reason: string): never => {
  throw new SerializationError(`cannot serialize command as MessagePack: ${reason}`);
};

function normalizeWireValue(value: unknown, active: WeakSet<object>): unknown {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) invalid('numbers must be finite');
    return value;
  }
  if (typeof value === 'bigint') invalid('BigInt is not JavaScript-safe on the broker');
  if (typeof value === 'undefined') invalid('nested undefined values are not portable');
  if (typeof value === 'symbol' || typeof value === 'function') {
    invalid(`${typeof value} values are unsupported`);
  }
  if (typeof value !== 'object') invalid('unsupported value');
  const objectValue = value as object;

  if (value instanceof Date) {
    if (!Number.isFinite(value.getTime())) invalid('invalid Date');
    return new Date(value.getTime());
  }
  if (value instanceof Uint8Array || value instanceof ArrayBuffer) return value;
  if (active.has(objectValue)) invalid('cyclic structures are unsupported');
  active.add(objectValue);
  try {
    if (Array.isArray(value)) {
      return Array.from(value, (entry) => normalizeWireValue(entry, active));
    }
    if (value instanceof Map) {
      const normalized = new Map<string, unknown>();
      for (const [key, entry] of value) {
        if (typeof key !== 'string') invalid('map keys must be strings');
        normalized.set(key as string, normalizeWireValue(entry, active));
      }
      return normalized;
    }
    const prototype = Object.getPrototypeOf(objectValue);
    if (prototype !== Object.prototype && prototype !== null) {
      invalid('only plain objects, arrays, maps, dates, and binary values are supported');
    }
    const normalized = Object.create(null) as Record<string, unknown>;
    for (const key of Reflect.ownKeys(objectValue)) {
      if (typeof key !== 'string') invalid('symbol object keys are unsupported');
      const stringKey = key as string;
      const descriptor = Object.getOwnPropertyDescriptor(objectValue, stringKey);
      if (!descriptor?.enumerable) continue;
      if (!('value' in descriptor)) invalid('accessor properties are unsupported');
      normalized[stringKey] = normalizeWireValue(descriptor.value, active);
    }
    return normalized;
  } finally {
    active.delete(objectValue);
  }
}

function prepareCommand(command: Command, reqId: string): Record<string, unknown> {
  const envelope = Object.create(null) as Record<string, unknown>;
  for (const key of Reflect.ownKeys(command)) {
    if (typeof key !== 'string') invalid('symbol command keys are unsupported');
    const stringKey = key as string;
    const descriptor = Object.getOwnPropertyDescriptor(command, stringKey);
    if (!descriptor?.enumerable) continue;
    if (!('value' in descriptor)) invalid('accessor command properties are unsupported');
    if (descriptor.value === undefined) continue;
    envelope[stringKey] = descriptor.value;
  }
  envelope.reqId = reqId;
  return normalizeWireValue(envelope, new WeakSet()) as Record<string, unknown>;
}

/**
 * Encode and frame a command before it occupies an in-flight request slot.
 *
 * MessagePack may reject values such as cyclic objects. Normalize every such
 * encoder failure to the public SDK error hierarchy; `frame()` already uses
 * the same typed error for an oversized encoded body.
 */
export function serializeCommand(command: Command, reqId: string): Buffer {
  try {
    return frame(pack(prepareCommand(command, reqId)));
  } catch (error) {
    if (error instanceof SerializationError) throw error;
    throw new SerializationError('cannot serialize command as MessagePack');
  }
}
