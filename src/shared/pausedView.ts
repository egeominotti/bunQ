/**
 * BullMQ paused-view counts (#92).
 *
 * When a queue is paused, none of its ready jobs (waiting + prioritized) can be
 * pulled, so they are reported under `paused` — and NOT in their own buckets, to
 * avoid double-counting a single job. This mirrors the per-state lists, where a
 * paused queue lists those jobs under `paused` and returns nothing for `waiting`
 * / `prioritized`. Delayed and active jobs keep their own state.
 *
 * Single source of truth shared by every count surface that exposes the BullMQ
 * `getJobCounts` shape (client SDK, TCP handler, dashboard detail), so they can
 * never drift apart.
 */
export interface PausedDerivedCounts {
  waiting: number;
  prioritized: number;
  paused: number;
}

export function pausedView(
  waiting: number,
  prioritized: number,
  isPaused: boolean
): PausedDerivedCounts {
  return {
    waiting: isPaused ? 0 : waiting,
    prioritized: isPaused ? 0 : prioritized,
    paused: isPaused ? waiting + prioritized : 0,
  };
}
