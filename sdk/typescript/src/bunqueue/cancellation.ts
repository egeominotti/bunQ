/**
 * Bunqueue Simple Mode — graceful job cancellation.
 * 1:1 port of src/client/bunqueue/cancellation.ts: AbortController-based
 * cancellation with optional grace period.
 */

export class CancellationManager {
  private readonly controllers = new Map<string, AbortController>();

  /** Register a new AbortController for a job. */
  register(jobId: string): AbortController {
    const ac = new AbortController();
    this.controllers.set(jobId, ac);
    return ac;
  }

  /** Remove a job's controller (on completion). */
  unregister(jobId: string): void {
    this.controllers.delete(jobId);
  }

  /** Cancel a job with optional grace period. */
  cancel(jobId: string, gracePeriodMs = 0): void {
    const ac = this.controllers.get(jobId);
    if (!ac) return;

    if (gracePeriodMs > 0) {
      setTimeout(() => {
        ac.abort();
      }, gracePeriodMs);
    } else {
      ac.abort();
    }
  }

  /** Check if a job is cancelled. */
  isCancelled(jobId: string): boolean {
    const ac = this.controllers.get(jobId);
    return ac ? ac.signal.aborted : false;
  }

  /** Get the AbortSignal for a job. */
  getSignal(jobId: string): AbortSignal | null {
    return this.controllers.get(jobId)?.signal ?? null;
  }

  /** Cancel all and clear. */
  destroyAll(): void {
    for (const ac of this.controllers.values()) {
      ac.abort();
    }
    this.controllers.clear();
  }
}
