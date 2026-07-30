/**
 * The engine's only source of time and randomness.
 *
 * WHY THIS EXISTS. Every defect this engine has shipped in a crash or concurrency
 * window took a property-based campaign roughly one run in eleven to surface, and the
 * seed that produced it did NOT reproduce it: the seed drove the sequence of commands,
 * while the interleaving came from real timers and real process death. A failing seed
 * you cannot replay is a bug report you cannot act on.
 *
 * With every `Date.now()`, `Math.random()` and `setTimeout` routed through here, a
 * simulated clock makes a seed drive the interleaving too, so a failure replays
 * exactly, every time, and a fix can be proven rather than hoped for.
 *
 * WHAT THIS IS NOT. It does not make the engine deterministic on its own: SQLite, the
 * queue's own worker loop and the OS scheduler are still outside. It removes the
 * engine's own contribution, which is the part the engine can be held responsible for.
 *
 * The default is the real clock, and it is installed by default, so nothing changes
 * for anyone who does not ask for a simulated one.
 */

export interface Clock {
  /** Milliseconds since the epoch. */
  now(): number;
  /** Schedule `fn` after `ms`. The handle is whatever `clear` accepts. */
  setTimeout(fn: () => void, ms: number): TimerHandle;
  clearTimeout(handle: TimerHandle): void;
  /** A number in [0, 1). Used for retry jitter. */
  random(): number;
  /** Replayable entropy; cryptographically strong on the real clock. */
  randomBytes(length: number): Uint8Array;
}

export interface TimerHandle {
  /** Real timers can be unref'd so a pending one does not hold the process open. */
  unref?(): void;
}

const realClock: Clock = {
  now: () => Date.now(),
  setTimeout: (fn, ms) => globalThis.setTimeout(fn, ms) as unknown as TimerHandle,
  clearTimeout: (h) => globalThis.clearTimeout(h as unknown as ReturnType<typeof setTimeout>),
  random: () => Math.random(),
  randomBytes: (length) => crypto.getRandomValues(new Uint8Array(length)),
};

let current: Clock = realClock;

/** The clock the engine reads. Real unless a simulation installed its own. */
export function clock(): Clock {
  return current;
}

/**
 * Install a clock, returning the previous one so a test can restore it.
 *
 * Deliberately process-global rather than threaded through every call site: the
 * alternative is a `Clock` parameter on roughly forty internal functions, which is a
 * large diff through code whose correctness was hard-won, for no gain in a runtime
 * that already keeps one engine per process.
 */
export function setClock(next: Clock): Clock {
  const previous = current;
  current = next;
  return previous;
}

/** Restore the real clock. */
export function resetClock(): void {
  current = realClock;
}

/**
 * A clock where time only moves when the simulation says so.
 *
 * `advance` fires every timer due at or before the new instant, in due order, and
 * timers scheduled BY those callbacks are picked up in the same advance, so a chain of
 * re-armed timeouts settles the way it would in real time rather than needing one
 * `advance` per link. Ties break by insertion order, so a seed replays identically.
 */
export function simulatedClock(
  seed: number,
  startAt = 1_700_000_000_000
): Clock & {
  advance(ms: number): number;
  pending(): number;
} {
  let time = startAt;
  let sequence = 0;
  let state = seed >>> 0 || 0x9e3779b9;
  let entropyState = (seed ^ 0xa5a5a5a5) >>> 0 || 0x6d2b79f5;
  const timers = new Map<number, { at: number; order: number; fn: () => void }>();

  const random = (): number => {
    // xorshift32: small, seeded, and identical across runs and platforms.
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return ((state >>> 0) % 1_000_000) / 1_000_000;
  };

  return {
    now: () => time,
    random,
    randomBytes(length) {
      const bytes = new Uint8Array(length);
      for (let i = 0; i < length; i++) {
        entropyState ^= entropyState << 13;
        entropyState ^= entropyState >>> 17;
        entropyState ^= entropyState << 5;
        bytes[i] = entropyState >>> 24;
      }
      return bytes;
    },
    setTimeout(fn, ms) {
      const id = sequence++;
      timers.set(id, { at: time + Math.max(0, ms), order: id, fn });
      return {
        unref() {
          // Simulated timers own no process handle.
        },
        __id: id,
      } as TimerHandle;
    },
    clearTimeout(handle) {
      const id = (handle as { __id?: number } | undefined)?.__id;
      if (id !== undefined) timers.delete(id);
    },
    pending: () => timers.size,
    advance(ms: number): number {
      const target = time + ms;
      let fired = 0;
      for (;;) {
        const due = [...timers.entries()]
          .filter(([, t]) => t.at <= target)
          .sort((a, b) => a[1].at - b[1].at || a[1].order - b[1].order);
        const next = due[0];
        if (!next) break;
        timers.delete(next[0]);
        time = Math.max(time, next[1].at);
        next[1].fn();
        fired++;
        if (fired > 100_000) throw new Error('simulatedClock: timer storm, over 100000 firings');
      }
      time = target;
      return fired;
    },
  };
}
