import { useEffect, useRef, useState } from 'react';
import './perf.css';

/**
 * Homepage throughput replay: bunqueue vs BullMQ, from measured benchmarks.
 *
 * There is no Redis in the browser, so nothing is benchmarked live. Every bar
 * and counter is derived from fixed, published ops/sec, so the on-screen ratio
 * is the measured ratio. The matrix is honest end to end: the one cell bunqueue
 * loses (single push over TCP) stays on the board.
 *
 * The Mode switch changes bunqueue's transport only. BullMQ has no in-process
 * mode, it always crosses the network to Redis, so its numbers repeat across
 * modes and its lane carries that caveat as real text.
 *
 * Numbers (Apple M1 Max, 32 GB, Bun 1.3.14, 111-byte payload, median of 3):
 *  - in-process (bunqueue embedded) figures: benchmarks.mdx scaling bench,
 *    bulk peak at 10K jobs (630,568), single peak at 50K (241,825).
 *  - TCP + all BullMQ + p99: bench/comparison/run.ts, 2026-07-08
 *    (BullMQ 5.79.3, Redis 8.8.0).
 * Counts/times shown are projected at the measured throughput; the ratio and
 * the frozen sliver are the measured rate ratio and are scale-independent.
 *
 * SSR-safe: the initial render is the static frozen pose of the default cell
 * (meaningful without JS, no layout shift). window / rAF are touched only in
 * effects and handlers.
 */

type Engine = 'bq' | 'mq';
type Mode = 'embedded' | 'tcp';
type Op = 'bulk' | 'single';

const MATRIX: Record<Mode, Record<Op, Record<Engine, number>>> = {
  embedded: {
    bulk: { bq: 630568, mq: 24800 },
    single: { bq: 241825, mq: 56736 },
  },
  tcp: {
    bulk: { bq: 85700, mq: 24800 },
    single: { bq: 52756, mq: 56736 },
  },
};

const MODES: { id: Mode; label: string; why: string }[] = [
  { id: 'embedded', label: 'In-process', why: 'same address space, no network' },
  { id: 'tcp', label: 'Client / server', why: 'over the wire, like Redis' },
];

const OPERATIONS: { id: Op; label: string; sub: string }[] = [
  { id: 'bulk', label: 'Bulk push', sub: '100-job batches' },
  { id: 'single', label: 'Single push', sub: '32 concurrent add()' },
];

const COUNTS = [
  { jobs: 10000, label: '10K' },
  { jobs: 100000, label: '100K' },
  { jobs: 1000000, label: '1M' },
];

// p99 push latency, TCP head-to-head only (lower is better).
const P99 = { bq: 6.3, mq: 11.1 };

const WALL_MS = 2200; // the winner lane crosses the finish line in this wall time
const WIN_GAP = 1.15; // below this multiple the cell is called a tie, not a win

const ENGINE_NAME: Record<Engine, string> = { bq: 'bunqueue', mq: 'BullMQ' };

const nf = new Intl.NumberFormat('en-US');
// One decimal everywhere, matching the published figures (3.5x, 4.3x, 25.4x, 1.8x).
const mult = (n: number) => n.toFixed(1);
const secs = (s: number) => (s >= 10 ? s.toFixed(1) : s >= 1 ? s.toFixed(2) : s.toFixed(3));

interface Pose {
  ops: Record<Engine, number>;
  winner: Engine;
  loser: Engine;
  ratio: number; // winnerOps / loserOps, >= 1
  loserFrac: number; // loserOps / winnerOps, the frozen sliver
  winnerTime: number; // seconds to drain `count` at winner rate (projected)
  loserTimeFull: number; // seconds to drain `count` at loser rate (projected)
  crossedCount: number; // loser jobs done when winner finishes
}

function poseFor(mode: Mode, op: Op, count: number): Pose {
  const ops = MATRIX[mode][op];
  const winner: Engine = ops.bq >= ops.mq ? 'bq' : 'mq';
  const loser: Engine = winner === 'bq' ? 'mq' : 'bq';
  const ratio = ops[winner] / ops[loser];
  const winnerTime = count / ops[winner];
  return {
    ops,
    winner,
    loser,
    ratio,
    loserFrac: ops[loser] / ops[winner],
    winnerTime,
    loserTimeFull: count / ops[loser],
    crossedCount: Math.round(ops[loser] * winnerTime),
  };
}

/** Accessible single-select segmented control (radiogroup + roving tabindex). */
function Segmented<T extends string>({
  label,
  options,
  value,
  onChange,
  render,
}: {
  label: string;
  options: { id: T }[];
  value: T;
  onChange: (v: T) => void;
  render: (o: { id: T }) => unknown;
}) {
  const refs = useRef<(HTMLButtonElement | null)[]>([]);
  const idx = options.findIndex((o) => o.id === value);
  const onKeyDown = (e: React.KeyboardEvent) => {
    let ni = idx;
    if (e.key === 'ArrowRight' || e.key === 'ArrowDown') ni = (idx + 1) % options.length;
    else if (e.key === 'ArrowLeft' || e.key === 'ArrowUp') ni = (idx - 1 + options.length) % options.length;
    else return;
    e.preventDefault();
    onChange(options[ni].id);
    refs.current[ni]?.focus();
  };
  return (
    <div className="bq-perf-seg" role="radiogroup" aria-label={label} onKeyDown={onKeyDown}>
      {options.map((o, i) => (
        <button
          key={o.id}
          ref={(el) => {
            refs.current[i] = el;
          }}
          type="button"
          role="radio"
          aria-checked={o.id === value}
          tabIndex={o.id === value ? 0 : -1}
          className={`bq-perf-chip${o.id === value ? ' is-on' : ''}`}
          onClick={() => onChange(o.id)}
        >
          {render(o) as React.ReactNode}
        </button>
      ))}
    </div>
  );
}

export default function PerfSimulator() {
  const [mode, setMode] = useState<Mode>('embedded');
  const [op, setOp] = useState<Op>('bulk');
  const [count, setCount] = useState(1000000);
  const [running, setRunning] = useState(false);
  const [t, setT] = useState(1); // animation progress 0..1; starts settled at the final pose

  const rafRef = useRef<number | null>(null);
  const startRef = useRef<number | null>(null);
  const genRef = useRef(0); // bumped on every stop/replay so stale frames self-cancel

  const pose = poseFor(mode, op, count);
  const sameTransport = poseFor('tcp', op, count); // honest apples-to-apples reference

  const stop = () => {
    genRef.current++;
    if (rafRef.current !== null && typeof cancelAnimationFrame !== 'undefined') {
      cancelAnimationFrame(rafRef.current);
    }
    rafRef.current = null;
    startRef.current = null;
  };
  useEffect(() => stop, []);

  // Any control change re-poses live to the settled outcome (no Run needed).
  const repose = <T,>(setter: (v: T) => void) => (v: T) => {
    stop();
    setRunning(false);
    setT(1);
    setter(v);
  };

  const replay = () => {
    stop();
    const reduced =
      typeof window !== 'undefined' &&
      typeof window.matchMedia === 'function' &&
      window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    if (reduced || typeof requestAnimationFrame === 'undefined') {
      setRunning(false);
      setT(1);
      return;
    }
    setRunning(true);
    setT(0);
    const gen = genRef.current;
    const step = (now: number) => {
      if (genRef.current !== gen) return; // a newer stop/replay superseded this run
      if (startRef.current === null) startRef.current = now;
      const p = Math.min((now - startRef.current) / WALL_MS, 1);
      setT(p);
      if (p >= 1) {
        setRunning(false);
        rafRef.current = null;
        startRef.current = null;
        return;
      }
      rafRef.current = requestAnimationFrame(step);
    };
    rafRef.current = requestAnimationFrame(step);
  };

  // Per-lane animated view. Winner reaches its target at t=1; loser freezes at
  // t * loserFrac of the track, and its counter reaches crossedCount.
  const laneView = (engine: Engine) => {
    const isWinner = engine === pose.winner;
    const width = isWinner ? t * 100 : t * pose.loserFrac * 100;
    const finalCount = isWinner ? count : pose.crossedCount;
    return {
      isWinner,
      width,
      shownCount: Math.round(t * finalCount),
      finalCount,
      elapsed: t * pose.winnerTime, // both lanes share the wall clock
    };
  };

  const isTie = pose.ratio < WIN_GAP;
  const bqAhead = pose.ops.bq >= pose.ops.mq;

  return (
    <div className={`bq-perf${running ? ' is-running' : ''}`}>
      {/* headline ratio, always qualified by mode + operation */}
      <div className="bq-perf-head">
        <div className="bq-perf-badge">
          <b className={isTie ? 'is-tie' : bqAhead ? 'is-win' : 'is-them'}>
            {isTie ? 'tie' : `${mult(pose.ratio)}x`}
          </b>
          <span>
            {isTie ? 'neck and neck' : bqAhead ? 'bunqueue faster' : 'BullMQ faster'}
            <i>
              {mode === 'embedded' ? 'in-process' : 'over TCP'} · {op === 'bulk' ? 'bulk push' : 'single push'}
            </i>
          </span>
        </div>
        {mode === 'embedded' && (
          <p className="bq-perf-qual">
            Same transport, bunqueue over TCP vs Redis:{' '}
            {sameTransport.ratio < WIN_GAP ? 'a tie' : `${mult(sameTransport.ratio)}x`} on{' '}
            {op === 'bulk' ? 'bulk' : 'single'} push. In-process removes the network entirely, which BullMQ
            cannot.
          </p>
        )}
      </div>

      {/* the whole honest picture, one click per cell */}
      <div className="bq-perf-matrix" role="group" aria-label="Throughput ratios by mode and operation">
        {MODES.flatMap((m) =>
          OPERATIONS.map((o) => {
            const p = poseFor(m.id, o.id, count);
            const on = m.id === mode && o.id === op;
            const win = p.ops.bq >= p.ops.mq;
            const tie = p.ratio < WIN_GAP;
            return (
              <button
                key={`${m.id}-${o.id}`}
                type="button"
                className={`bq-perf-cell${on ? ' is-on' : ''}${win && !tie ? ' is-win' : ''}`}
                aria-pressed={on}
                onClick={() => {
                  stop();
                  setRunning(false);
                  setT(1);
                  setMode(m.id);
                  setOp(o.id);
                }}
              >
                <b>{tie ? 'tie' : `${mult(p.ratio)}x`}</b>
                <i>
                  {m.label.toLowerCase()} · {o.id}
                </i>
              </button>
            );
          }),
        )}
      </div>

      {/* controls */}
      <div className="bq-perf-controls">
        <div className="bq-perf-group">
          <span className="bq-perf-glabel">Mode</span>
          <Segmented
            label="Mode"
            options={MODES}
            value={mode}
            onChange={repose(setMode)}
            render={(o) => {
              const m = MODES.find((x) => x.id === o.id)!;
              return (
                <>
                  {m.label}
                  <i>{m.why}</i>
                </>
              );
            }}
          />
        </div>
        <div className="bq-perf-group">
          <span className="bq-perf-glabel">Operation</span>
          <Segmented
            label="Operation"
            options={OPERATIONS}
            value={op}
            onChange={repose(setOp)}
            render={(o) => {
              const x = OPERATIONS.find((y) => y.id === o.id)!;
              // The embedded single-push figure is sequential (awaited one at a time);
              // only the TCP single-push figure is 32 concurrent add() calls.
              const sub =
                x.id === 'single' ? (mode === 'embedded' ? 'awaited, one at a time' : '32 concurrent add()') : x.sub;
              return (
                <>
                  {x.label}
                  <i>{sub}</i>
                </>
              );
            }}
          />
        </div>
        <div className="bq-perf-group">
          <span className="bq-perf-glabel">Jobs to drain</span>
          <Segmented
            label="Jobs to drain"
            options={COUNTS.map((c) => ({ id: String(c.jobs) }))}
            value={String(count)}
            onChange={repose((v: string) => setCount(Number(v)))}
            render={(o) => COUNTS.find((c) => String(c.jobs) === o.id)!.label}
          />
        </div>
        <div className="bq-perf-actions">
          <button
            type="button"
            className="bq-btn bq-perf-run"
            onClick={replay}
            aria-busy={running}
          >
            {running ? 'Replaying…' : 'Replay'}
          </button>
        </div>
      </div>

      {/* lanes */}
      <div className="bq-perf-lanes">
        {(['bq', 'mq'] as Engine[]).map((engine) => {
          const v = laneView(engine);
          const runtime =
            engine === 'bq'
              ? mode === 'embedded'
                ? 'in-process on Bun'
                : 'on Bun, over TCP'
              : 'on Redis, over network';
          return (
            <div
              key={engine}
              className={`bq-perf-lane bq-perf-lane-${engine}${v.isWinner ? ' crossed-first' : ''}`}
            >
              <div className="bq-perf-lane-head">
                <span className="bq-perf-engine">
                  <b>{ENGINE_NAME[engine]}</b>
                  <i>{runtime}</i>
                  {engine === 'mq' && <span className="bq-perf-tag">no in-process mode</span>}
                </span>
                <span className="bq-perf-ops">
                  {nf.format(pose.ops[engine])} <span>ops/sec</span>
                </span>
              </div>

              <div
                className="bq-perf-track"
                role="progressbar"
                aria-valuemin={0}
                aria-valuemax={count}
                aria-valuenow={v.finalCount}
                aria-valuetext={
                  v.isWinner
                    ? `${ENGINE_NAME[engine]} cleared ${nf.format(count)} jobs in ${secs(pose.winnerTime)} seconds`
                    : `${ENGINE_NAME[engine]} reached ${nf.format(pose.crossedCount)} of ${nf.format(count)} jobs, ${(pose.loserFrac * 100).toFixed(1)} percent, projected ${secs(pose.loserTimeFull)} seconds for the full run`
                }
              >
                <div className="bq-perf-fill" style={{ width: `${v.width.toFixed(2)}%` }} />
              </div>

              <div className="bq-perf-readout">
                <span className="bq-perf-count">{nf.format(v.shownCount)}</span>
                <span className="bq-perf-of">
                  {v.isWinner ? `/ ${nf.format(count)} jobs` : `of ${nf.format(count)} · frozen`}
                </span>
                <span className="bq-perf-time">
                  {v.isWinner ? `${secs(pose.winnerTime)} s` : `${secs(pose.loserTimeFull)} s for all`}
                </span>
              </div>
            </div>
          );
        })}
      </div>

      {/* verdict + latency */}
      <div className="bq-perf-foot">
        <div className="bq-perf-verdict" role="status" aria-live="polite">
          {mode === 'embedded' && (
            <p className="bq-perf-caveat">
              BullMQ has no in-process mode. It always crosses the network to Redis, so this is its best
              number and it does not change when you switch modes.
            </p>
          )}
          {isTie ? (
            <p>
              <b className="bq-perf-tie">Dead heat.</b> Over TCP, 32-concurrent single push is a tie,{' '}
              {bqAhead ? 'bunqueue' : 'BullMQ'} by a nose ({nf.format(pose.ops.bq)} vs {nf.format(pose.ops.mq)}{' '}
              ops/sec). Batch it for 3.5x, or run bunqueue in-process for 25x. The cell you lose stays on the
              board.
            </p>
          ) : (
            <p>
              <b className="bq-perf-win">{mult(pose.ratio)}x the throughput.</b> {ENGINE_NAME[pose.winner]}{' '}
              pushes {nf.format(pose.ops[pose.winner])}/sec{mode === 'embedded' ? ' with no Redis to run' : ''};{' '}
              {ENGINE_NAME[pose.loser]} tops out at {nf.format(pose.ops[pose.loser])}/sec. Draining{' '}
              {nf.format(count)} jobs: {ENGINE_NAME[pose.winner]} ~{secs(pose.winnerTime)} s, while{' '}
              {ENGINE_NAME[pose.loser]} is still at {nf.format(pose.crossedCount)} (
              {(pose.loserFrac * 100).toFixed(1)}%), ~{secs(pose.loserTimeFull)} s for the full run.
            </p>
          )}
        </div>

        {mode === 'tcp' && (
          <div className="bq-perf-lat">
            <span className="bq-perf-lat-label">p99 push latency</span>
            <span className="bq-perf-lat-val bq-perf-lat-bq">{P99.bq} ms</span>
            <span className="bq-perf-lat-vs">vs</span>
            <span className="bq-perf-lat-val">{P99.mq} ms</span>
            <b>{mult(P99.mq / P99.bq)}x lower</b>
          </div>
        )}
      </div>

      <p className="bq-perf-method">
        Counts and times are projected at the measured throughput; the ratio is the measured rate ratio.{' '}
        {mode === 'embedded'
          ? 'bunqueue from the embedded scaling bench (benchmarks.mdx, bulk peak at 10K jobs, single at 50K); BullMQ from the comparison bench, no embedded equivalent exists.'
          : 'bunqueue TCP, BullMQ and p99 from bench/comparison/run.ts, 2026-07-08, BullMQ 5.79.3, Redis 8.8.0.'}{' '}
        Apple M1 Max · Bun 1.3.14 · 111-byte payload · median of 3 ·{' '}
        <a href="https://github.com/egeominotti/bunqueue/blob/main/bench/comparison/run.ts">methodology</a> ·{' '}
        <a href="/guide/benchmarks/">all benchmarks →</a>
      </p>
    </div>
  );
}
