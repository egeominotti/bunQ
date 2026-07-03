import type { SimulatorEngine } from '../../lib/simulator';
import type { Snapshot } from '../../lib/simulator';

const SPEEDS = [0.5, 1, 2, 4];

function fmtClock(ms: number): string {
  const total = Math.floor(ms / 1000);
  const mm = String(Math.floor(total / 60)).padStart(2, '0');
  const ss = String(total % 60).padStart(2, '0');
  return `${mm}:${ss}`;
}

// The simulation's transport control: clock, pause, speed — plus the
// live counters that summarize everything below.
export default function TransportBar({
  snap,
  engine,
}: {
  snap: Snapshot;
  engine: SimulatorEngine;
}) {
  const { totals } = snap;
  return (
    <div className="transport">
      <div className="transport-clock">
        <button
          type="button"
          className="btn-icon"
          onClick={() => engine.setRunning(!snap.running)}
          aria-label={snap.running ? 'Pause simulation' : 'Resume simulation'}
          title={snap.running ? 'Pause simulation' : 'Resume simulation'}
        >
          {snap.running ? '⏸' : '▶'}
        </button>
        <span className={`clock-time ${snap.running ? '' : 'clock-paused'}`}>
          {fmtClock(snap.simTime)}
        </span>
        <div className="speed-group" role="group" aria-label="Simulation speed">
          {SPEEDS.map((s) => (
            <button
              key={s}
              type="button"
              className={`speed-btn ${snap.speed === s ? 'is-on' : ''}`}
              onClick={() => engine.setSpeed(s)}
              aria-pressed={snap.speed === s}
            >
              ×{s}
            </button>
          ))}
        </div>
      </div>

      <div className="transport-stats">
        <Stat label="pushed" value={totals.pushed} />
        <Stat label="active" value={totals.active} tone="active" />
        <Stat label="completed" value={totals.completed} />
        <Stat label="retries" value={totals.retried} tone={totals.retried > 0 ? 'delayed' : undefined} />
        <Stat label="dead" value={totals.dead} tone={totals.dead > 0 ? 'dlq' : undefined} />
        <Stat label="jobs/s" value={totals.jobsPerSec.toFixed(1)} tone="accent" />
      </div>

      <button
        type="button"
        className="btn btn-ghost"
        onClick={() => engine.reset()}
        title="Clear all queues, workers and history"
      >
        Reset
      </button>
    </div>
  );
}

function Stat({
  label,
  value,
  tone,
}: {
  label: string;
  value: number | string;
  tone?: string;
}) {
  return (
    <div className={`t-stat ${tone ? `tone-${tone}` : ''}`}>
      <span className="t-stat-value">{value}</span>
      <span className="t-stat-label">{label}</span>
    </div>
  );
}
