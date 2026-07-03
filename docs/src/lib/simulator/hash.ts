// FNV-1a — the same hash bunqueue uses to map a queue name to a shard.
// shardIndex = fnv1aHash(queueName) & SHARD_MASK

export const SHARD_COUNT = 8;
export const SHARD_MASK = SHARD_COUNT - 1;

export function fnv1aHash(str: string): number {
  let hash = 0x811c9dc5;
  for (let i = 0; i < str.length; i++) {
    hash ^= str.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return hash >>> 0;
}

export function shardIndexFor(queueName: string): number {
  return fnv1aHash(queueName) & SHARD_MASK;
}
