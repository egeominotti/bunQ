export interface LockGuard {
  release(): void;
}

export interface LockQueueEntry {
  resolve: () => void;
  cancelled: boolean;
}
