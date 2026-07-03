import { useEffect, useState } from 'react';
import { SimulatorEngine, warmup } from '../../lib/simulator';
import ControlPanel from './ControlPanel';
import EventLog from './EventLog';
import PipelineBoard from './PipelineBoard';
import ScenarioBar from './ScenarioBar';
import ShardGrid from './ShardGrid';
import ThroughputPanel from './ThroughputPanel';
import TransportBar from './TransportBar';
import WorkerRail from './WorkerRail';
import './simulator.css';
import './pipeline.css';

const POLL_MS = 120;

// Root island for /simulator/. The engine lives outside React; the UI
// polls an immutable snapshot a few times per second and every control
// calls straight into the engine.
export default function Simulator() {
  const [engine] = useState(() => new SimulatorEngine());
  const [snap, setSnap] = useState(() => engine.snapshot());

  useEffect(() => {
    engine.start();
    warmup(engine);
    const interval = setInterval(() => {
      setSnap(engine.snapshot());
    }, POLL_MS);
    return () => {
      clearInterval(interval);
      engine.destroy();
    };
  }, [engine]);

  return (
    <div className="simulator not-content">
      <TransportBar snap={snap} engine={engine} />
      <ScenarioBar engine={engine} />
      <div className="sim-grid">
        <ControlPanel engine={engine} snap={snap} />
        <div className="sim-main">
          <PipelineBoard snap={snap} />
          <div className="sim-row">
            <ShardGrid shards={snap.shards} />
            <ThroughputPanel snap={snap} />
          </div>
          <div className="sim-row">
            <WorkerRail workers={snap.workers} onStop={(id) => engine.stopWorker(id)} />
            <EventLog events={snap.events} />
          </div>
        </div>
      </div>
    </div>
  );
}
