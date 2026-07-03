import { fmtMs } from '../../lib/simulator';
import type { LaneId, SimJob, Snapshot } from '../../lib/simulator';

const LANES: { id: LaneId; label: string; empty: string }[] = [
  { id: 'waiting', label: 'Waiting', empty: 'Push a job' },
  { id: 'delayed', label: 'Delayed', empty: 'No scheduled jobs' },
  { id: 'active', label: 'Active', empty: 'Start a worker' },
  { id: 'completed', label: 'Completed', empty: 'Nothing done yet' },
  { id: 'dlq', label: 'Dead letter', empty: 'No dead letters' },
];

// The signature element: the job lifecycle as five live lanes.
// Chips move waiting → active → completed; failures bounce back through
// delayed with a backoff countdown until they land in the dead-letter lane.
export default function PipelineBoard({ snap }: { snap: Snapshot }) {
  const showQueueTag = snap.queues.length > 1;
  return (
    <div className="panel board">
      <div className="board-lanes">
        {LANES.map(({ id, label, empty }) => {
          const lane = snap.lanes[id];
          const overflow = lane.total - lane.jobs.length;
          return (
            <section key={id} className={`lane lane-${id}`} aria-label={`${label} jobs`}>
              <header className="lane-head">
                <span className="lane-dot" aria-hidden="true" />
                <h3 className="lane-title">{label}</h3>
                <span className="lane-count">{lane.total}</span>
              </header>
              <ul className="lane-body">
                {lane.jobs.map((job) => (
                  <Chip key={job.id} job={job} lane={id} simTime={snap.simTime} showQueue={showQueueTag} />
                ))}
                {overflow > 0 && <li className="lane-overflow">+{overflow} more</li>}
                {lane.total === 0 && <li className="lane-empty">{empty}</li>}
              </ul>
            </section>
          );
        })}
      </div>
    </div>
  );
}

function Chip({
  job,
  lane,
  simTime,
  showQueue,
}: {
  job: SimJob;
  lane: LaneId;
  simTime: number;
  showQueue: boolean;
}) {
  return (
    <li className={`chip chip-${lane}`} title={chipTitle(job)}>
      <span className="chip-line">
        <span className="chip-id">#{job.id}</span>
        <span className="chip-name">{job.name}</span>
        <span className="chip-meta">{chipMeta(job, lane, simTime)}</span>
      </span>
      {showQueue && <span className="chip-queue">{job.queue}</span>}
      {lane === 'active' && (
        <span className="chip-progress" aria-hidden="true">
          <i style={{ width: `${Math.round(job.progress * 100)}%` }} />
        </span>
      )}
      {lane === 'dlq' && job.failedReason && (
        <span className="chip-reason">{job.failedReason}</span>
      )}
    </li>
  );
}

function chipMeta(job: SimJob, lane: LaneId, simTime: number): string {
  switch (lane) {
    case 'waiting':
      return `P${job.priority}`;
    case 'delayed': {
      const eta = fmtMs(Math.max(0, job.runAt - simTime));
      return job.attemptsMade > 0 ? `↻ ${job.attemptsMade}/${job.maxAttempts} · ${eta}` : `⏱ ${eta}`;
    }
    case 'active':
      return `${Math.round(job.progress * 100)}%`;
    case 'completed':
      return fmtMs((job.finishedAt ?? 0) - (job.startedAt ?? 0));
    case 'dlq':
      return `${job.attemptsMade}/${job.maxAttempts}`;
  }
}

function chipTitle(job: SimJob): string {
  const parts = [`#${job.id} ${job.name}`, `queue ${job.queue}`, `priority ${job.priority}`];
  if (job.attemptsMade > 0) parts.push(`attempts ${job.attemptsMade}/${job.maxAttempts}`);
  if (job.failedReason) parts.push(job.failedReason);
  return parts.join(' · ');
}
