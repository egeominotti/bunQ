import { useState, useEffect, useCallback, useRef } from 'react';
import { QueueSimulator, type Job, type SimulatorEvent, type QueueStats, type WorkerInfo } from '../../lib/simulator';
import './simulator.css';

interface ShardDisplay {
  index: number;
  jobs: Job[];
  stats: {
    queuedJobs: number;
    delayedJobs: number;
    activeJobs: number;
    completedJobs: number;
    failedJobs: number;
    dlqJobs: number;
  };
}

export default function Simulator() {
  const simulatorRef = useRef<QueueSimulator | null>(null);
  const [shards, setShards] = useState<ShardDisplay[]>([]);
  const [queues, setQueues] = useState<QueueStats[]>([]);
  const [workers, setWorkers] = useState<WorkerInfo[]>([]);
  const [events, setEvents] = useState<SimulatorEvent[]>([]);
  const [globalStats, setGlobalStats] = useState({
    shardCount: 8,
    totalPushed: 0,
    totalProcessed: 0,
    totalFailed: 0,
    activeWorkers: 0,
    queues: 0,
  });

  // Form state
  const [queueName, setQueueName] = useState('emails');
  const [jobName, setJobName] = useState('send-email');
  const [jobData, setJobData] = useState('{"to": "user@example.com"}');
  const [priority, setPriority] = useState(0);
  const [delay, setDelay] = useState(0);
  const [bulkCount, setBulkCount] = useState(10);
  const [workerConcurrency, setWorkerConcurrency] = useState(3);
  const [failureRate, setFailureRate] = useState(10);
  const [shardCount] = useState(8);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);
  const failureRateRef = useRef(failureRate / 100);

  // Update failure rate ref when slider changes (no re-init)
  useEffect(() => {
    failureRateRef.current = failureRate / 100;
  }, [failureRate]);

  // Initialize simulator (only on mount/shardCount change)
  useEffect(() => {
    const sim = new QueueSimulator({
      shardCount,
      failureRate: failureRateRef.current,
    });
    simulatorRef.current = sim;

    // Subscribe to events
    const unsubscribe = sim.on((event) => {
      setEvents((prev) => [event, ...prev].slice(0, 50));
    });

    // Update loop (500ms is sufficient for demo)
    const interval = setInterval(() => {
      if (simulatorRef.current) {
        updateState();
      }
    }, 500);

    return () => {
      unsubscribe();
      clearInterval(interval);
      sim.destroy();
    };
  }, [shardCount]);

  const updateState = useCallback(() => {
    const sim = simulatorRef.current;
    if (!sim) return;

    setShards(sim.getShardStats());
    setWorkers(sim.getWorkers());
    setGlobalStats(sim.getGlobalStats());

    const queueNames = sim.getAllQueues();
    setQueues(queueNames.map((name) => sim.getQueueStats(name)));
  }, []);

  const handlePush = () => {
    const sim = simulatorRef.current;
    if (!sim) return;

    try {
      const data = JSON.parse(jobData);
      sim.push(queueName, jobName, data, { priority, delay });
      updateState();
    } catch (_e) {
      setErrorMsg('Invalid JSON data. Check syntax.');
      setTimeout(() => setErrorMsg(null), 3000);
    }
  };

  const handleBulkPush = () => {
    const sim = simulatorRef.current;
    if (!sim) return;

    try {
      const data = JSON.parse(jobData);
      const jobs = Array.from({ length: bulkCount }, (_, i) => ({
        name: `${jobName}-${i + 1}`,
        data: { ...data, index: i },
        options: { priority: Math.floor(Math.random() * 10) },
      }));
      sim.pushBulk(queueName, jobs);
      updateState();
    } catch (_e) {
      setErrorMsg('Invalid JSON data. Check syntax.');
      setTimeout(() => setErrorMsg(null), 3000);
    }
  };

  const handleStartWorker = () => {
    const sim = simulatorRef.current;
    if (!sim) return;

    sim.createWorker(
      queueName,
      (job) => {
        return Promise.resolve({ processed: true, jobId: job.id });
      },
      { concurrency: workerConcurrency }
    );
    updateState();
  };

  const handleStopWorker = (workerId: string) => {
    const sim = simulatorRef.current;
    if (!sim) return;
    sim.stopWorker(workerId);
    updateState();
  };

  const handlePauseQueue = () => {
    const sim = simulatorRef.current;
    if (!sim) return;
    sim.pause(queueName);
    updateState();
  };

  const handleResumeQueue = () => {
    const sim = simulatorRef.current;
    if (!sim) return;
    sim.resume(queueName);
    updateState();
  };

  const handleDrainQueue = () => {
    const sim = simulatorRef.current;
    if (!sim) return;
    sim.drain(queueName);
    updateState();
  };

  const handleRetryDlq = () => {
    const sim = simulatorRef.current;
    if (!sim) return;
    sim.retryDlq(queueName);
    updateState();
  };

  const handleReset = () => {
    const sim = simulatorRef.current;
    if (!sim) return;
    sim.reset();
    setEvents([]);
    updateState();
  };

  const getStateColor = (state: Job['state']) => {
    switch (state) {
      case 'waiting': return '#3b82f6';
      case 'delayed': return '#f59e0b';
      case 'active': return '#22c55e';
      case 'completed': return '#6b7280';
      case 'failed': return '#ef4444';
      case 'dlq': return '#dc2626';
      default: return '#9ca3af';
    }
  };

  return (
    <div className="simulator">
      <div className="simulator-header">
        <h2>bunqueue Interactive Simulator</h2>
        <p>Real-time visualization of queue operations, sharding, and job lifecycle</p>
      </div>

      {/* Global Stats */}
      <div className="stats-bar">
        <div className="stat">
          <span className="stat-value">{globalStats.shardCount}</span>
          <span className="stat-label">Shards</span>
        </div>
        <div className="stat">
          <span className="stat-value">{globalStats.totalPushed}</span>
          <span className="stat-label">Pushed</span>
        </div>
        <div className="stat">
          <span className="stat-value">{globalStats.totalProcessed}</span>
          <span className="stat-label">Processed</span>
        </div>
        <div className="stat">
          <span className="stat-value">{globalStats.totalFailed}</span>
          <span className="stat-label">Failed</span>
        </div>
        <div className="stat">
          <span className="stat-value">{globalStats.activeWorkers}</span>
          <span className="stat-label">Workers</span>
        </div>
        <div className="stat">
          <span className="stat-value">{globalStats.queues}</span>
          <span className="stat-label">Queues</span>
        </div>
      </div>

      <div className="simulator-grid">
        {/* Controls Panel */}
        <div className="panel controls-panel">
          <h3>Controls</h3>

          <div className="control-group">
            <label htmlFor="queue-name">Queue Name</label>
            <input
              id="queue-name"
              type="text"
              value={queueName}
              onChange={(e) => { setQueueName(e.target.value); }}
              placeholder="queue name"
              aria-label="Queue name"
            />
          </div>

          <div className="control-group">
            <label htmlFor="job-name">Job Name</label>
            <input
              id="job-name"
              type="text"
              value={jobName}
              onChange={(e) => { setJobName(e.target.value); }}
              placeholder="job name"
              aria-label="Job name"
            />
          </div>

          <div className="control-group">
            <label htmlFor="job-data">Job Data (JSON)</label>
            <textarea
              id="job-data"
              value={jobData}
              onChange={(e) => { setJobData(e.target.value); }}
              rows={2}
              aria-label="Job data in JSON format"
            />
          </div>

          <div className="control-row">
            <div className="control-group half">
              <label htmlFor="priority">Priority</label>
              <input
                id="priority"
                type="number"
                value={priority}
                onChange={(e) => { setPriority(Number(e.target.value)); }}
                aria-label="Job priority (higher = processed sooner)"
              />
            </div>
            <div className="control-group half">
              <label htmlFor="delay">Delay (ms)</label>
              <input
                id="delay"
                type="number"
                value={delay}
                onChange={(e) => { setDelay(Number(e.target.value)); }}
                aria-label="Delay in milliseconds before job is processed"
              />
            </div>
          </div>

          <div className="button-group">
            <button onClick={handlePush} className="btn btn-primary">
              Push Job
            </button>
            <button onClick={handleBulkPush} className="btn btn-secondary">
              Push {bulkCount} Jobs
            </button>
          </div>

          <div className="control-group">
            <label htmlFor="bulk-count">Bulk Count</label>
            <input
              id="bulk-count"
              type="range"
              min="1"
              max="100"
              value={bulkCount}
              onChange={(e) => { setBulkCount(Number(e.target.value)); }}
              aria-label={`Bulk count: ${bulkCount} jobs`}
              aria-valuemin={1}
              aria-valuemax={100}
              aria-valuenow={bulkCount}
            />
            <span>{bulkCount}</span>
          </div>

          <hr />

          <div className="control-group">
            <label htmlFor="worker-concurrency">Worker Concurrency</label>
            <input
              id="worker-concurrency"
              type="range"
              min="1"
              max="10"
              value={workerConcurrency}
              onChange={(e) => { setWorkerConcurrency(Number(e.target.value)); }}
              aria-label={`Worker concurrency: ${workerConcurrency}`}
              aria-valuemin={1}
              aria-valuemax={10}
              aria-valuenow={workerConcurrency}
            />
            <span>{workerConcurrency}</span>
          </div>

          <button onClick={handleStartWorker} className="btn btn-success">
            Start Worker
          </button>

          <hr />

          <div className="button-group">
            <button onClick={handlePauseQueue} className="btn btn-warning">
              Pause
            </button>
            <button onClick={handleResumeQueue} className="btn btn-success">
              Resume
            </button>
            <button onClick={handleDrainQueue} className="btn btn-danger">
              Drain
            </button>
          </div>

          <button onClick={handleRetryDlq} className="btn btn-secondary">
            Retry DLQ
          </button>

          <hr />

          <div className="control-group">
            <label htmlFor="failure-rate">Failure Rate: {failureRate}%</label>
            <input
              id="failure-rate"
              type="range"
              min="0"
              max="50"
              value={failureRate}
              onChange={(e) => { setFailureRate(Number(e.target.value)); }}
              aria-label={`Failure rate: ${failureRate} percent`}
              aria-valuemin={0}
              aria-valuemax={50}
              aria-valuenow={failureRate}
            />
          </div>

          <button onClick={handleReset} className="btn btn-danger">
            Reset Simulator
          </button>

          {errorMsg && (
            <div className="error-message" role="alert">
              {errorMsg}
            </div>
          )}
        </div>

        {/* Shards Visualization */}
        <div className="panel shards-panel">
          <h3>Shards ({shards.length})</h3>
          <div className="shards-grid">
            {shards.map((shard) => (
              <div key={shard.index} className="shard">
                <div className="shard-header">
                  <span className="shard-index">S{shard.index}</span>
                  <span className="shard-count">{shard.jobs.length}</span>
                </div>
                <div className="shard-jobs">
                  {shard.jobs.slice(0, 20).map((job) => (
                    <div
                      key={job.id}
                      className="job-dot"
                      style={{ backgroundColor: getStateColor(job.state) }}
                      title={`${job.name} (${job.state}) P:${job.priority}`}
                    />
                  ))}
                  {shard.jobs.length > 20 && (
                    <span className="more-jobs">+{shard.jobs.length - 20}</span>
                  )}
                </div>
                <div className="shard-stats">
                  <span className="stat-mini" title="Waiting">W:{shard.stats.queuedJobs}</span>
                  <span className="stat-mini" title="Active">A:{shard.stats.activeJobs}</span>
                  <span className="stat-mini" title="DLQ">D:{shard.stats.dlqJobs}</span>
                </div>
              </div>
            ))}
          </div>

          <div className="legend">
            <span><span className="dot" style={{ backgroundColor: '#3b82f6' }} /> Waiting</span>
            <span><span className="dot" style={{ backgroundColor: '#f59e0b' }} /> Delayed</span>
            <span><span className="dot" style={{ backgroundColor: '#22c55e' }} /> Active</span>
            <span><span className="dot" style={{ backgroundColor: '#6b7280' }} /> Completed</span>
            <span><span className="dot" style={{ backgroundColor: '#ef4444' }} /> Failed</span>
          </div>
        </div>

        {/* Queues Panel */}
        <div className="panel queues-panel">
          <h3>Queues</h3>
          {queues.length === 0 ? (
            <p className="empty">No queues yet. Push some jobs!</p>
          ) : (
            <div className="queue-list">
              {queues.map((q) => (
                <div key={q.name} className={`queue-item ${q.paused ? 'paused' : ''}`}>
                  <div className="queue-header">
                    <span className="queue-name">{q.name}</span>
                    <span className="queue-shard">Shard {q.shardIndex}</span>
                    {q.paused && <span className="badge paused">PAUSED</span>}
                  </div>
                  <div className="queue-stats">
                    <span className="qs waiting" title="Waiting">W:{q.waiting}</span>
                    <span className="qs delayed" title="Delayed">D:{q.delayed}</span>
                    <span className="qs active" title="Active">A:{q.active}</span>
                    <span className="qs completed" title="Completed">C:{q.completed}</span>
                    <span className="qs dlq" title="Dead Letter Queue">X:{q.dlq}</span>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Workers Panel */}
        <div className="panel workers-panel">
          <h3>Workers</h3>
          {workers.length === 0 ? (
            <p className="empty">No workers running. Start one!</p>
          ) : (
            <div className="worker-list">
              {workers.map((w) => (
                <div key={w.id} className={`worker-item ${w.status}`}>
                  <div className="worker-header">
                    <span className="worker-id">{w.id.slice(0, 12)}...</span>
                    <span className={`badge ${w.status}`}>{w.status}</span>
                  </div>
                  <div className="worker-info">
                    <span>Queue: {w.queue}</span>
                    <span>Concurrency: {w.concurrency}</span>
                  </div>
                  <div className="worker-stats">
                    <span>Active: {w.activeJobs}</span>
                    <span>Processed: {w.processedCount}</span>
                    <span>Failed: {w.failedCount}</span>
                  </div>
                  {w.status === 'running' && (
                    <button
                      onClick={() => { handleStopWorker(w.id); }}
                      className="btn btn-small btn-danger"
                    >
                      Stop
                    </button>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>

        {/* Events Panel */}
        <div className="panel events-panel">
          <h3>Events (Last 50)</h3>
          <div className="events-list">
            {events.map((event, i) => (
              <div key={i} className={`event-item ${event.type.split(':')[1]}`}>
                <span className="event-time">
                  {new Date(event.timestamp).toLocaleTimeString()}
                </span>
                <span className="event-type">{event.type}</span>
                <span className="event-data">
                  {JSON.stringify(event.data).slice(0, 60)}...
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
