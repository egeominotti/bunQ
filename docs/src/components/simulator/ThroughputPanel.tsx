import { useState } from 'react';
import type { Snapshot } from '../../lib/simulator';

const W = 240;
const H = 72;
const PAD_TOP = 6;

// Completions per second over the last 60s of sim-time: a hero number
// with its sparkline, crosshair on hover.
export default function ThroughputPanel({ snap }: { snap: Snapshot }) {
  const [hover, setHover] = useState<number | null>(null);
  const { series } = snap;
  const max = Math.max(1, ...series);
  const stepX = W / (series.length - 1);
  const y = (v: number) => H - (v / max) * (H - PAD_TOP);

  const linePath = series.map((v, i) => `${i === 0 ? 'M' : 'L'}${i * stepX},${y(v)}`).join(' ');
  const areaPath = `${linePath} L${W},${H} L0,${H} Z`;

  const onMove = (e: React.MouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect();
    const i = Math.round(((e.clientX - rect.left) / rect.width) * (series.length - 1));
    setHover(Math.min(series.length - 1, Math.max(0, i)));
  };

  return (
    <div className="panel spark-panel">
      <h2 className="panel-title">
        Throughput <span className="panel-hint">last 60s</span>
      </h2>
      <div className="spark-hero">
        <span className="spark-value">{snap.totals.jobsPerSec.toFixed(1)}</span>
        <span className="spark-unit">completed / sec</span>
      </div>
      <div className="spark-chart">
        <svg
          viewBox={`0 0 ${W} ${H}`}
          preserveAspectRatio="none"
          role="img"
          aria-label={`Throughput sparkline, current ${snap.totals.jobsPerSec.toFixed(1)} jobs per second, peak ${max === 1 && Math.max(...series) === 0 ? 0 : Math.max(...series)} in the last minute`}
          onMouseMove={onMove}
          onMouseLeave={() => setHover(null)}
        >
          <path className="spark-area" d={areaPath} />
          <path className="spark-line" d={linePath} />
          {hover !== null && (
            <g className="spark-cursor">
              <line x1={hover * stepX} y1={0} x2={hover * stepX} y2={H} />
              <circle cx={hover * stepX} cy={y(series[hover])} r={3} />
            </g>
          )}
        </svg>
        {hover !== null && (
          <div className="spark-tip" style={{ left: `${(hover / (series.length - 1)) * 100}%` }}>
            {series[hover]}/s · {series.length - 1 - hover}s ago
          </div>
        )}
      </div>
    </div>
  );
}
