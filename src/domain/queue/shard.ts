/** Public Shard façade. */
import { ShardLifecycle } from './shard/lifecycle';

export type { ShardStats } from './shardCounters';
export type { ShardOptions } from '../types/shard';

/** Shard behavior is composed from focused manager-delegation layers. */
export class Shard extends ShardLifecycle {}
