import type { SimulatorEngine } from '../../lib/simulator';
import { SCENARIOS } from '../../lib/simulator';

// One-click demos — the fastest way to see each mechanic move.
export default function ScenarioBar({ engine }: { engine: SimulatorEngine }) {
  return (
    <div className="scenario-bar" role="group" aria-label="Demo scenarios">
      <span className="scenario-eyebrow">Scenarios</span>
      {SCENARIOS.map((s) => (
        <button
          key={s.id}
          type="button"
          className="scenario-chip"
          title={s.hint}
          onClick={() => s.apply(engine)}
        >
          {s.label}
        </button>
      ))}
    </div>
  );
}
