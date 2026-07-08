/**
 * Backpressure gate: bounds the number of in-flight commands on a connection.
 *
 * When the in-flight count reaches `max`, `acquire()` parks the caller until a
 * slot frees (a pending command settles). This turns unbounded memory growth
 * under a fast producer / slow server into cooperative flow control. `max <= 0`
 * disables the gate (unbounded, the default).
 */
export class Backpressure {
  private readonly waiters: Array<() => void> = [];

  constructor(
    private readonly max: number,
    private readonly onWait?: () => void
  ) {}

  /** Resolve immediately if under the limit, else park until a slot frees. */
  acquire(inFlight: number): Promise<void> | void {
    if (this.max <= 0 || inFlight < this.max) return;
    this.onWait?.();
    return new Promise<void>((resolve) => {
      this.waiters.push(resolve);
    });
  }

  /** Release one parked caller (call when a pending command settles). */
  release(): void {
    this.waiters.shift()?.();
  }

  /** Release every parked caller (call on teardown so none hang forever). */
  clear(): void {
    while (this.waiters.length > 0) this.waiters.shift()?.();
  }

  /** Number of callers currently parked. */
  get waiting(): number {
    return this.waiters.length;
  }
}
