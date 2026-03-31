/**
 * FNV-1a Hash Implementation
 * Same algorithm used in bunqueue for consistent sharding
 */

const FNV_PRIME = 0x01000193;
const FNV_OFFSET = 0x811c9dc5;

export function fnv1a(str: string): number {
  let hash = FNV_OFFSET;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = Math.imul(hash, FNV_PRIME);
  }
  return hash >>> 0;
}

export function calculateShardCount(cores: number = navigator.hardwareConcurrency || 4): number {
  const clamped = Math.min(Math.max(cores, 1), 64);
  let shards = 1;
  while (shards < clamped && shards < 64) {
    shards *= 2;
  }
  return shards;
}

export function shardIndex(key: string, shardMask: number): number {
  return fnv1a(key) & shardMask;
}

export function generateId(): string {
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).substring(2, 10);
  return `${timestamp}-${random}`;
}
