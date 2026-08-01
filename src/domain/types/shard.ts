import type { DlqEntry } from './dlq';

export interface ShardOptions {
  onDlqEvicted?: (entry: DlqEntry) => void;
}
