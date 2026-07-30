import type { SimEvent } from '../../lib/simulator';

function fmtT(ms: number): string {
  const total = Math.floor(ms / 1000);
  const mm = String(Math.floor(total / 60)).padStart(2, '0');
  const ss = String(total % 60).padStart(2, '0');
  return `${mm}:${ss}`;
}

// The server log tail: every push, completion, retry, dead letter and
// control action, newest first, stamped with sim-time.
export default function EventLog({ events }: { events: SimEvent[] }) {
  return (
    <div className="panel event-panel">
      <h2 className="panel-title">Events</h2>
      {events.length === 0 ? (
        <p className="panel-empty">Quiet so far. Push a job or run a scenario.</p>
      ) : (
        <ul className="event-list" aria-live="off">
          {events.map((ev) => (
            <li key={ev.id} className={`event ev-${ev.type}`}>
              <span className="event-t">{fmtT(ev.t)}</span>
              <span className="event-msg">{ev.msg}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
