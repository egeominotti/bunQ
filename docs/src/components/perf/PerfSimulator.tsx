import { useEffect, useRef, useState } from 'react';
import './perf.css';

/**
 * Homepage benchmark simulator: bunqueue-on-Bun vs BullMQ-on-Redis.
 *
 * There is no Redis in the browser, so this is not a live benchmark: it
 * replays the published, dated throughput figures (median of 3 runs,
 * bench/comparison/run.ts) as an animated race. Every counter is derived
 * from a fixed, measured ops/sec, so the ratio on screen is the ratio we
 * measured, never a random flourish. Single push is shown at honest parity;
 * bulk push is where the wire protocol earns its 3.5x.
 *
 * SSR-safe: the initial render is the idle state (no window / rAF / clock at
 * module load or first paint); the animation lives entirely inside effects.
 */

type Engine = 'bq' | 'mq';

interface Workload {
  id: 'bulk' | 'single';
  label: string;
  sub: string;
  ops: Record<Engine, number>;
}

// Measured 2026-07-08, Apple M1 Max, Bun 1.3.14, BullMQ 5.79.3, Redis 8.8.0,
// TCP mode, identical 111-byte payloads, median of 3 runs. Same figures the
// hero card and /guide/benchmarks/ publish.
const WORKLOADS: Workload[] = [
  { id: 'bulk', label: 'Bulk push', sub: '100-job batches', ops: { bq: 85700, mq: 24800 } },
  { id: 'single', label: 'Single push', sub: 'one awaited add at a time', ops: { bq: 52756, mq: 56736 } },
];

const SCALES = [
  { jobs: 10000, label: '10K' },
  { jobs: 100000, label: '100K' },
  { jobs: 1000000, label: '1M' },
];

// p99 push latency on the same run, lower is better (unchanged across workloads).
const P99 = { bq: 6.3, mq: 11.1 };

// Wall-clock budget the slower lane animates over; both lanes are scaled by the
// same factor so the on-screen ratio equals the measured ratio at any job count.
const WALL_MS = 3400;

const ENGINE_META: Record<Engine, { name: string; runtime: string }> = {
  bq: { name: 'bunqueue', runtime: 'on Bun' },
  mq: { name: 'BullMQ', runtime: 'on Redis' },
};

const nf = new Intl.NumberFormat('en-US');
const secs = (s: number) => (s >= 10 ? s.toFixed(1) : s.toFixed(2));
const ratio = (a: number, b: number) => (a / b).toFixed(1);

interface LaneState {
  progress: number; // 0..1
  processed: number;
  elapsed: number; // real seconds represented
  done: boolean;
}

const idleLane = (): LaneState => ({ progress: 0, processed: 0, elapsed: 0, done: false });

interface Frame {
  bq: LaneState;
  mq: LaneState;
}

const idleFrame = (): Frame => ({ bq: idleLane(), mq: idleLane() });

export default function PerfSimulator() {
  const [workloadId, setWorkloadId] = useState<Workload['id']>('bulk');
  const [scale, setScale] = useState(SCALES[1].jobs);
  const [phase, setPhase] = useState<'idle' | 'running' | 'done'>('idle');
  const [frame, setFrame] = useState<Frame>(idleFrame);

  const rafRef = useRef<number | null>(null);
  const startRef = useRef<number | null>(null);

  const workload = WORKLOADS.find((w) => w.id === workloadId) ?? WORKLOADS[0];
  const realElapsed: Record<Engine, number> = {
    bq: scale / workload.ops.bq,
    mq: scale / workload.ops.mq,
  };
  const slowest = Math.max(realElapsed.bq, realElapsed.mq);

  const stop = () => {
    if (rafRef.current !== null && typeof cancelAnimationFrame !== 'undefined') {
      cancelAnimationFrame(rafRef.current);
    }
    rafRef.current = null;
    startRef.current = null;
  };

  useEffect(() => stop, []);

  const finalFrame = (): Frame => ({
    bq: { progress: 1, processed: scale, elapsed: realElapsed.bq, done: true },
    mq: { progress: 1, processed: scale, elapsed: realElapsed.mq, done: true },
  });

  const run = () => {
    stop();
    setPhase('running');
    setFrame(idleFrame());

    const reduced =
      typeof window !== 'undefined' &&
      typeof window.matchMedia === 'function' &&
      window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    if (reduced || typeof requestAnimationFrame === 'undefined') {
      setFrame(finalFrame());
      setPhase('done');
      return;
    }

    const timeScale = WALL_MS / (slowest * 1000);

    const laneAt = (engine: Engine, tMs: number): LaneState => {
      const durMs = realElapsed[engine] * 1000 * timeScale;
      const progress = durMs > 0 ? Math.min(tMs / durMs, 1) : 1;
      return {
        progress,
        processed: Math.round(progress * scale),
        elapsed: progress * realElapsed[engine],
        done: progress >= 1,
      };
    };

    const step = (now: number) => {
      if (startRef.current === null) startRef.current = now;
      const t = now - startRef.current;
      setFrame({ bq: laneAt('bq', t), mq: laneAt('mq', t) });
      if (t >= WALL_MS) {
        setFrame(finalFrame());
        setPhase('done');
        rafRef.current = null;
        startRef.current = null;
        return;
      }
      rafRef.current = requestAnimationFrame(step);
    };

    rafRef.current = requestAnimationFrame(step);
  };

  const reset = () => {
    stop();
    setPhase('idle');
    setFrame(idleFrame());
  };

  const pick = <T,>(setter: (v: T) => void) => (v: T) => {
    stop();
    setPhase('idle');
    setFrame(idleFrame());
    setter(v);
  };

  // Verdict, computed from the measured ops, not the animation.
  const winner: Engine = workload.ops.bq >= workload.ops.mq ? 'bq' : 'mq';
  const loser: Engine = winner === 'bq' ? 'mq' : 'bq';
  const speedup = workload.ops[winner] / workload.ops[loser];
  const crossed = Math.round(workload.ops[loser] * realElapsed[winner]);

  return (
    <div className="bq-perf">
      <div className="bq-perf-controls" role="group" aria-label="Benchmark settings">
        <div className="bq-perf-group">
          <span className="bq-perf-glabel">Operation</span>
          <div className="bq-perf-toggle">
            {WORKLOADS.map((w) => (
              <button
                key={w.id}
                type="button"
                className={`bq-perf-chip${w.id === workloadId ? ' is-on' : ''}`}
                aria-pressed={w.id === workloadId}
                onClick={() => pick(setWorkloadId)(w.id)}
              >
                {w.label}
                <i>{w.sub}</i>
              </button>
            ))}
          </div>
        </div>

        <div className="bq-perf-group">
          <span className="bq-perf-glabel">Job count</span>
          <div className="bq-perf-toggle">
            {SCALES.map((s) => (
              <button
                key={s.jobs}
                type="button"
                className={`bq-perf-chip bq-perf-chip-sm${s.jobs === scale ? ' is-on' : ''}`}
                aria-pressed={s.jobs === scale}
                onClick={() => pick(setScale)(s.jobs)}
              >
                {s.label}
              </button>
            ))}
          </div>
        </div>

        <div className="bq-perf-actions">
          <button type="button" className="bq-btn bq-perf-run" onClick={run}>
            {phase === 'idle' ? 'Run benchmark' : phase === 'running' ? 'Running…' : 'Run again'}
          </button>
          {phase !== 'idle' && (
            <button type="button" className="bq-perf-reset" onClick={reset}>
              Reset
            </button>
          )}
        </div>
      </div>

      <div className="bq-perf-lanes">
        {(['bq', 'mq'] as Engine[]).map((engine) => {
          const lane = frame[engine];
          const meta = ENGINE_META[engine];
          const isWinner = phase === 'done' && engine === winner && speedup >= 1.15;
          return (
            <div
              key={engine}
              className={`bq-perf-lane bq-perf-lane-${engine}${lane.done ? ' is-done' : ''}${
                isWinner ? ' is-winner' : ''
              }`}
            >
              <div className="bq-perf-lane-head">
                <span className="bq-perf-engine">
                  <b>{meta.name}</b>
                  <i>{meta.runtime}</i>
                </span>
                <span className="bq-perf-ops">
                  {nf.format(workload.ops[engine])} <span>ops/sec</span>
                </span>
              </div>

              <div className="bq-perf-track" aria-hidden="true">
                <div className="bq-perf-fill" style={{ width: `${(lane.progress * 100).toFixed(2)}%` }}>
                  <span className={`bq-perf-flow${phase === 'running' && !lane.done ? ' is-live' : ''}`} />
                </div>
              </div>

              <div className="bq-perf-readout" aria-live="off">
                <span className="bq-perf-count">{nf.format(lane.processed)}</span>
                <span className="bq-perf-of">/ {nf.format(scale)} jobs</span>
                <span className="bq-perf-time">{secs(lane.elapsed)} s</span>
              </div>
            </div>
          );
        })}
      </div>

      <div className="bq-perf-foot">
        <div className="bq-perf-verdict" role="status" aria-live="polite">
          {phase !== 'done' ? (
            <p className="bq-perf-hint">
              No Redis in your browser: both lanes replay their measured throughput. Same payload, same
              machine, {nf.format(scale)} jobs each.
            </p>
          ) : speedup >= 1.15 ? (
            <p>
              <b className="bq-perf-win">{ratio(workload.ops[winner], workload.ops[loser])}x faster.</b>{' '}
              {ENGINE_META[winner].name} cleared {nf.format(scale)} jobs in {secs(realElapsed[winner])} s while{' '}
              {ENGINE_META[loser].name} took {secs(realElapsed[loser])} s. At {ENGINE_META[winner].name}'s finish
              line, {ENGINE_META[loser].name} had processed only {nf.format(crossed)}.
            </p>
          ) : (
            <p>
              <b className="bq-perf-tie">Dead heat.</b> {ENGINE_META[winner].name} edged ahead by{' '}
              {ratio(workload.ops[winner], workload.ops[loser])}x ({nf.format(workload.ops.bq)} vs{' '}
              {nf.format(workload.ops.mq)} ops/sec). Raw single push is a tie, batching is where bunqueue
              pulls away. Switch to <b>Bulk push</b> to see the 3.5x.
            </p>
          )}
        </div>

        <div className="bq-perf-lat">
          <span className="bq-perf-lat-label">p99 push latency</span>
          <span className="bq-perf-lat-val bq-perf-lat-bq">{P99.bq} ms</span>
          <span className="bq-perf-lat-vs">vs</span>
          <span className="bq-perf-lat-val">{P99.mq} ms</span>
          <b>{ratio(P99.mq, P99.bq)}x lower</b>
        </div>
      </div>

      <p className="bq-perf-method">
        Apple M1 Max · Bun 1.3.14 · BullMQ 5.79.3 · Redis 8.8.0 · median of 3 runs ·{' '}
        <a href="https://github.com/egeominotti/bunqueue/blob/main/bench/comparison/run.ts">methodology</a> ·{' '}
        <a href="/guide/benchmarks/">all benchmarks →</a>
      </p>
    </div>
  );
}
