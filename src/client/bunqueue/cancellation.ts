/**
 * Bunqueue — Graceful Job Cancellation
 * AbortController-based cancellation with optional grace period.
 */

interface CancellationRegistration {
  readonly jobId: string;
  readonly controller: AbortController;
  timer: ReturnType<typeof setTimeout> | null;
  deadline: number | null;
}

export class CancellationManager {
  private readonly currentByJob = new Map<string, CancellationRegistration>();
  private readonly registrations = new Map<AbortController, CancellationRegistration>();

  /** Register a new AbortController for a job */
  register(jobId: string): AbortController {
    const controller = new AbortController();
    const registration = { jobId, controller, timer: null, deadline: null };
    this.currentByJob.set(jobId, registration);
    this.registrations.set(controller, registration);
    return controller;
  }

  /** Remove a job's controller (on completion) */
  unregister(jobId: string, controller?: AbortController): void {
    const registration = controller
      ? this.registrations.get(controller)
      : this.currentByJob.get(jobId);
    if (!registration || registration.jobId !== jobId) return;
    this.clearTimer(registration);
    this.registrations.delete(registration.controller);
    if (this.currentByJob.get(jobId) === registration) this.currentByJob.delete(jobId);
  }

  /** Cancel a job with optional grace period */
  cancel(jobId: string, gracePeriodMs = 0): void {
    const registration = this.currentByJob.get(jobId);
    if (!registration) return;

    if (gracePeriodMs <= 0) {
      this.clearTimer(registration);
      registration.controller.abort();
      return;
    }

    if (registration.controller.signal.aborted) return;
    const deadline = Date.now() + gracePeriodMs;
    if (registration.deadline !== null && registration.deadline <= deadline) return;
    this.clearTimer(registration);

    const timer = setTimeout(() => {
      if (registration.timer !== timer) return;
      registration.timer = null;
      registration.deadline = null;
      registration.controller.abort();
    }, gracePeriodMs);
    registration.timer = timer;
    registration.deadline = deadline;
  }

  /** Check if a job is cancelled */
  isCancelled(jobId: string): boolean {
    return this.currentByJob.get(jobId)?.controller.signal.aborted ?? false;
  }

  /** Get the AbortSignal for a job */
  getSignal(jobId: string): AbortSignal | null {
    return this.currentByJob.get(jobId)?.controller.signal ?? null;
  }

  /** Cancel all and clear */
  destroyAll(): void {
    const registrations = [...this.registrations.values()];
    this.currentByJob.clear();
    this.registrations.clear();
    for (const registration of registrations) this.clearTimer(registration);
    for (const registration of registrations) registration.controller.abort();
  }

  private clearTimer(registration: CancellationRegistration): void {
    const timer = registration.timer;
    registration.timer = null;
    registration.deadline = null;
    if (timer !== null) clearTimeout(timer);
  }
}
