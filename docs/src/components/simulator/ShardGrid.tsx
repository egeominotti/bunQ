import type { ShardView } from '../../lib/simulator';

// Where each queue physically lives: fnv1aHash(queueName) & 7.
// A cell flashes when a push lands on it.
export default function ShardGrid({ shards }: { shards: ShardView[] }) {
  const maxLoad = Math.max(1, ...shards.map((s) => s.load));
  return (
    <div className="panel shard-panel">
      <h2 className="panel-title">
        Shards <span className="panel-hint">fnv1a(queue) &amp; {shards.length - 1}</span>
      </h2>
      <div className="shard-grid">
        {shards.map((shard) => (
          <div
            key={shard.index}
            className={`shard-cell ${shard.flash ? 'is-flash' : ''} ${shard.load > 0 ? 'has-load' : ''}`}
            title={
              shard.queues.length > 0
                ? `S${shard.index}: ${shard.queues.join(', ')} — ${shard.load} pending`
                : `S${shard.index}: empty`
            }
          >
            <span className="shard-name">S{shard.index}</span>
            <span className="shard-load">{shard.load}</span>
            <span className="shard-bar" aria-hidden="true">
              <i style={{ width: `${Math.round((shard.load / maxLoad) * 100)}%` }} />
            </span>
            <span className="shard-queues">{shard.queues.join(' ') || '—'}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
