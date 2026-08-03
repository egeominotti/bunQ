export type PendingTransitionSettlement =
  | { status: 'applied' }
  | { status: 'ignored' }
  | { status: 'failed'; error: Error };

export interface ManualMove {
  result:
    | { type: 'completed'; value?: unknown }
    | { type: 'failed'; error?: Error }
    | { type: 'ignored' | 'transitioned' }
    | {
        type: 'pending-transition';
        context: 'discard';
        pending: Promise<PendingTransitionSettlement>;
      }
    | null;
}
