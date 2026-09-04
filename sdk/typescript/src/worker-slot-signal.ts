/** One-shot wake-up used by the pull loop while every worker slot is busy. */
export class SlotSignal {
  private waiter: (() => void) | null = null;

  wait(timeoutMs: number): Promise<void> {
    return new Promise((resolve) => {
      let settled = false;
      const finish = () => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (this.waiter === finish) this.waiter = null;
        resolve();
      };
      const timer = setTimeout(finish, timeoutMs);
      this.waiter = finish;
    });
  }

  notify(): void {
    this.waiter?.();
  }
}
