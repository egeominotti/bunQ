import { useState } from 'react';
import type { SimulatorEngine, Snapshot } from '../../lib/simulator';

// Manual controls: push jobs, start workers, pause/drain queues, and
// dial in chaos (failure rate, per-queue rate limit).
export default function ControlPanel({
  engine,
  snap,
}: {
  engine: SimulatorEngine;
  snap: Snapshot;
}) {
  const [queueName, setQueueName] = useState('emails');
  const [jobName, setJobName] = useState('send-welcome');
  const [priority, setPriority] = useState(0);
  const [delay, setDelay] = useState(0);
  const [bulkCount, setBulkCount] = useState(25);
  const [concurrency, setConcurrency] = useState(3);

  const target = queueName.trim();
  const queue = snap.queues.find((q) => q.name === target);
  const failurePct = Math.round(snap.failureRate * 100);
  const rateLimit = queue?.rateLimit ?? 0;

  const pushOne = () => {
    if (!target) return;
    engine.push(target, jobName.trim() || 'job', { priority, delay });
  };

  const pushMany = () => {
    if (!target) return;
    const base = jobName.trim() || 'job';
    engine.pushBulk(target, bulkCount, () => ({
      name: base,
      priority: Math.floor(Math.random() * 10),
      delay,
    }));
  };

  return (
    <aside className="panel controls">
      <section className="ctl-section">
        <h3 className="ctl-title">Push jobs</h3>
        <label className="ctl-field">
          <span>Queue</span>
          <input
            type="text"
            value={queueName}
            onChange={(e) => setQueueName(e.target.value)}
            list="sim-queues"
            placeholder="emails"
          />
          <datalist id="sim-queues">
            {snap.queues.map((q) => (
              <option key={q.name} value={q.name} />
            ))}
          </datalist>
        </label>
        <label className="ctl-field">
          <span>Job name</span>
          <input
            type="text"
            value={jobName}
            onChange={(e) => setJobName(e.target.value)}
            placeholder="send-welcome"
          />
        </label>
        <div className="ctl-pair">
          <label className="ctl-field">
            <span>Priority {priority}</span>
            <input
              type="range"
              min={0}
              max={9}
              value={priority}
              onChange={(e) => setPriority(Number(e.target.value))}
            />
          </label>
          <label className="ctl-field">
            <span>Delay</span>
            <select value={delay} onChange={(e) => setDelay(Number(e.target.value))}>
              <option value={0}>none</option>
              <option value={2000}>2s</option>
              <option value={5000}>5s</option>
              <option value={10000}>10s</option>
            </select>
          </label>
        </div>
        <div className="ctl-buttons">
          <button type="button" className="btn btn-primary" onClick={pushOne}>
            Push job
          </button>
          <button type="button" className="btn" onClick={pushMany}>
            Push ×{bulkCount}
          </button>
        </div>
        <label className="ctl-field">
          <span>Bulk size {bulkCount}</span>
          <input
            type="range"
            min={5}
            max={100}
            step={5}
            value={bulkCount}
            onChange={(e) => setBulkCount(Number(e.target.value))}
          />
        </label>
      </section>

      <section className="ctl-section">
        <h3 className="ctl-title">Workers</h3>
        <label className="ctl-field">
          <span>Concurrency {concurrency}</span>
          <input
            type="range"
            min={1}
            max={8}
            value={concurrency}
            onChange={(e) => setConcurrency(Number(e.target.value))}
          />
        </label>
        <button
          type="button"
          className="btn btn-primary"
          onClick={() => target && engine.createWorker(target, concurrency)}
        >
          Start worker on “{target || '…'}”
        </button>
      </section>

      <section className="ctl-section">
        <h3 className="ctl-title">
          Queue controls
          {queue?.paused && <span className="badge badge-paused">paused</span>}
        </h3>
        <div className="ctl-buttons">
          {queue?.paused ? (
            <button type="button" className="btn" onClick={() => engine.resume(target)}>
              Resume
            </button>
          ) : (
            <button type="button" className="btn" onClick={() => engine.pause(target)}>
              Pause
            </button>
          )}
          <button type="button" className="btn" onClick={() => engine.drain(target)}>
            Drain
          </button>
          <button type="button" className="btn" onClick={() => engine.retryDlq(target)}>
            Retry DLQ
          </button>
        </div>
        {snap.queues.length > 0 && (
          <ul className="queue-list">
            {snap.queues.map((q) => (
              <li key={q.name}>
                <button
                  type="button"
                  className={`queue-row ${q.name === queueName ? 'is-selected' : ''}`}
                  onClick={() => setQueueName(q.name)}
                >
                  <span className="queue-row-name">
                    {q.name}
                    <span className="queue-row-shard">S{q.shardIndex}</span>
                    {q.paused && <span className="badge badge-paused">paused</span>}
                    {q.rateLimit > 0 && <span className="badge badge-rate">≤{q.rateLimit}/s</span>}
                  </span>
                  <span className="queue-row-counts">
                    <b className="c-waiting">{q.waiting}</b>
                    <b className="c-delayed">{q.delayed}</b>
                    <b className="c-active">{q.active}</b>
                    <b className="c-dlq">{q.dlq}</b>
                  </span>
                </button>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="ctl-section">
        <h3 className="ctl-title">Chaos</h3>
        <label className="ctl-field">
          <span>Failure rate {failurePct}%</span>
          <input
            type="range"
            min={0}
            max={80}
            step={5}
            value={failurePct}
            onChange={(e) => engine.setFailureRate(Number(e.target.value) / 100)}
          />
        </label>
        <label className="ctl-field">
          <span>
            Rate limit {rateLimit === 0 ? 'off' : `${rateLimit}/s`} — {target || '…'}
          </span>
          <input
            type="range"
            min={0}
            max={20}
            value={rateLimit}
            onChange={(e) => {
              if (target) engine.setRateLimit(target, Number(e.target.value));
            }}
          />
        </label>
      </section>
    </aside>
  );
}
