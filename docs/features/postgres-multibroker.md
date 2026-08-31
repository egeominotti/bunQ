# PostgreSQL 15–18 Multi-Broker Persistence

> **Category:** Persistence · **Source:** `src/infrastructure/persistence/postgres.ts`, `src/infrastructure/persistence/postgres/`, `src/application/postgresQueueManager.ts`, `src/application/postgres-queue-manager/`, `src/infrastructure/server/storageAdapter.ts`, `src/infrastructure/server/storageManager.ts`, `src/infrastructure/cloud/queueAdapter/`, `docker-compose.postgres.yml`

## Purpose

The PostgreSQL backend is an optional, database-authoritative server runtime for
deployments that need more than one bunqueue broker. Every broker connects to the
same PostgreSQL database and namespace. CI validates majors 15, 16, 17, and the
pinned/recommended 18.6 release. PostgreSQL transactions, row locks,
advisory locks, lease fencing, and `FOR UPDATE SKIP LOCKED` coordinate job
admission and delivery so one job generation is owned by at most one live lease.

This backend does not replace the existing SQLite engine. Memory and SQLite
remain the defaults, keep their synchronous hot path, and continue to power
embedded mode. PostgreSQL is selected only by explicit server configuration or a
PostgreSQL URL. An explicit `memory` driver also ignores an inherited SQLite
data path when its `QueueManager` is constructed, while the SQLite Strategy
continues to receive that path unchanged. MySQL is not implemented.

## Runtime selection

`resolveServerConfig()` resolves the backend with config-file values taking
precedence over environment variables:

1. `storage.driver` / `BUNQUEUE_STORAGE_DRIVER`, when explicitly set.
2. A configured `storage.url` / `BUNQUEUE_POSTGRES_URL` implies `postgres`.
3. A configured SQLite data path implies `sqlite`.
4. With neither URL nor data path, the server remains in-memory.

`storageAdapter.ts` is the engine extension boundary. Each backend is a Strategy
registered once in an immutable `StorageAdapterRegistry`; `ServerStorageManager`
is the lifecycle Facade used by bootstrap. Its owner map makes shutdown
idempotent, coalesces concurrent calls, and retains ownership after a transient
adapter error so shutdown can be retried. The PostgreSQL Strategy waits for
schema and event-stream initialization before TCP or HTTP listeners open. The
registry rejects duplicate or unsupported drivers, while the strategies reject:

- PostgreSQL without a URL;
- PostgreSQL combined with a SQLite data path;
- SQLite without a data path when `driver: 'sqlite'` is explicit; and
- S3 snapshot backup for memory or PostgreSQL, because that facility snapshots a
  local SQLite file.

The supported server settings are:

| Config path                        | Environment variable                            | Default                                      |
| ---------------------------------- | ----------------------------------------------- | -------------------------------------------- |
| `storage.driver`                   | `BUNQUEUE_STORAGE_DRIVER`                       | inferred (`memory`, `sqlite`, or `postgres`) |
| `storage.url`                      | `BUNQUEUE_POSTGRES_URL`                         | none                                         |
| `storage.namespace`                | `BUNQUEUE_POSTGRES_NAMESPACE`                   | `default`                                    |
| `storage.brokerId`                 | `BUNQUEUE_BROKER_ID`                            | host/PID/random ID                           |
| `storage.poolSize`                 | `BUNQUEUE_POSTGRES_POOL_SIZE`                   | `4` (runtime minimum `2`)                    |
| `storage.leaseDurationMs`          | `BUNQUEUE_POSTGRES_LEASE_DURATION_MS`           | `30000` (runtime minimum `1000`)             |
| `storage.pollIntervalMs`           | `BUNQUEUE_POSTGRES_POLL_INTERVAL_MS`            | `250` (runtime minimum `25`)                 |
| `storage.statementTimeoutMs`       | `BUNQUEUE_POSTGRES_STATEMENT_TIMEOUT_MS`        | `30000`                                      |
| `storage.lockTimeoutMs`            | `BUNQUEUE_POSTGRES_LOCK_TIMEOUT_MS`             | `5000`                                       |
| `storage.idleTransactionTimeoutMs` | `BUNQUEUE_POSTGRES_IDLE_TRANSACTION_TIMEOUT_MS` | `30000`                                      |
| `storage.maxConcurrentOperations`  | `BUNQUEUE_POSTGRES_MAX_CONCURRENT_OPERATIONS`   | `16`                                         |
| `storage.maxQueuedOperations`      | `BUNQUEUE_POSTGRES_MAX_QUEUED_OPERATIONS`       | `128`                                        |
| `storage.maxSnapshotJobs`          | `BUNQUEUE_POSTGRES_MAX_SNAPSHOT_JOBS`           | `100000`                                     |
| `storage.maxSnapshotPayloadBytes`  | `BUNQUEUE_POSTGRES_MAX_SNAPSHOT_PAYLOAD_BYTES`  | `268435456`                                  |

## Components

### Store and runtime

`PostgresQueueStore` is the database operation facade. Its focused modules own
admission, claims, outcomes, recovery, mutations, queue control, queries,
dependencies, flow-failure policies, repeat successors, cron state, worker
registrations, job logs, metrics, DLQ maintenance, and lease renewal/release.
`admissionStore.ts` isolates single/batch/flow admission from the facade;
`admissionResult.ts` is its typed decision contract, while
`serialAdmission.ts` reconciles final dependency state and the original
transactional-outbox payload after feature-bearing batch decisions;
`completionOutcome.ts`, `completionLifecycle.ts`, and `completionQueries.ts`
separate terminal writes, generation retention, and reads; and
`dependencyDestruction.ts`, `destructiveMutations.ts`, and
`queueDestruction.ts` share the dependency-safe removal protocol.
The high-volume path is split further: `batchAdmission.ts`, `claimSelection.ts`,
`claimBatch.ts`, `batchCompletion.ts`, `batchEvents.ts`, and
`completionEvents.ts` keep candidate selection, bulk SQL, event retention, and
metric aggregation out of the single-job implementations. `metricWrites.ts`
owns canonical-order exact bucket and lifetime-total updates for both single
and batch terminal paths. Its upserts keep `prevTS` monotonic when an older
transaction reaches the shared row after a newer one, and retain the matching
latest-minute `prevCount` even when bucket history is disabled.

`rateLimit.ts` is the single PostgreSQL normalization policy for both the
manager snapshot and durable queue state: a non-positive or non-finite duration
uses the same effective one-second window as SQLite, while a non-positive or
non-finite TTL means a permanent limit. `logs.ts` locks the owning job row before
insertion, retention, or clearing. That row is the serialization point shared
with job deletion, so concurrent writers retain the exact configured window and
cannot commit an orphan log.

`groupClaims.ts` owns durable round-robin cursor assignment and fixed-window
budget consumption, while `groups.ts` owns database-authoritative per-group
depth/configuration/TTL queries. `admission.ts` and `batchAdmission.ts` assign
the immutable `group_order` sequence before insert chunking.
`groupStateRetention.ts` reclaims bounded inactive policyless state without
deleting explicit overrides or live effective windows. Their state lives in
`bunqueue_jobs.group_order` and `bunqueue_group_state`; no broker-local cache
decides a multi-broker group claim.

This is a deliberate combination of small patterns rather than one generic SQL
abstraction:

- **Strategy + Registry** select memory, SQLite, or PostgreSQL at the server
  composition root and allow a fake adapter to test lifecycle behavior without a
  database;
- **Facade** keeps transports dependent on one stable manager/store surface while
  feature modules remain independently replaceable;
- **transaction scripts / Unit of Work** receive an explicit `PostgresContext`
  and, where needed, one `TransactionSQL`, so related rows, metrics, and events
  commit or roll back together;
- **Lock Plan + Command** acquires dependency-evidence and parent locks before a
  completion locks child rows, then exposes one immutable promotion command for
  the remainder of that transaction;
- **transactional outbox + deferred commit sequencer + Observer** use
  `bunqueue_events` as the durable record, assign namespace-local order at
  pre-commit, and use LISTEN/NOTIFY only as a wake-up;
- **Snapshot read model + projection reconciler** isolate synchronous
  compatibility reads from the database-authoritative write model. Manager and
  queue snapshots use `REPEATABLE READ READ ONLY`; journal events request
  generation-fenced authoritative projections instead of mutating cached job
  state from an unversioned payload. The bounded startup accumulator, projection
  scheduler, and deferred-write checkpoint remain separately testable; and
- **reentrant lifecycle gate** gives every database-backed manager operation one
  shutdown admission boundary. Nested calls reuse their admitted scope, while
  deferred synchronous writes and late cleanup have explicit acceptance rules.

New PostgreSQL features should be added as focused transaction/query modules and
exposed through `PostgresQueueStore`, without adding backend conditionals to TCP
handlers or changing the SQLite critical path. SQL remains explicit in hot
modules so query plans, locks, and rollback boundaries stay visible in tests and
reviews.

`PostgresQueueStoreRuntime` owns the Bun `SQL` pool, schema initialization,
broker registration, LISTEN/NOTIFY event stream, health state, and periodic
maintenance. Health failures are keyed by lifecycle, event stream, queue
refresh, heartbeat, recovery, DLQ/worker/event pruning, and cron subsystem. A
success clears only its own key, so an unrelated timer cannot make a persistently
failed subsystem appear healthy. Schema creation is serialized with the
transaction-scoped 64-bit advisory identity `bunqueue:schema`. While holding
that lock, a broker first checks `bunqueue_schema_migrations` and the semantic
catalog fingerprint. It skips all DDL only when its own
`POSTGRES_SCHEMA_VERSION` is already present (19 in this release) and every
guarded object matches, so a broker joining a healthy live cluster does not
request locks on active job tables.
The v17 migration upgrades the shared commit sequencer to the same
domain-separated lock encoding as runtime writers. The additive v16 migration
adds broker-session fencing to broker, lease, and
worker rows. The v15 migration backfills durable queue identity; the prior v14
migration adds queue/recent completion indexes, and the v13 journal migration
still upgrades v12 in place. Initialization rejects a
newer recorded version and verifies every correctness-critical journal table,
column, index, function, and enabled trigger before taking the fast path;
detected drift is repaired under the same advisory lock. Verification compares
sequence type and settings, column types/defaults/nullability, ordered index
keys and predicates, trigger transition-table bindings, and normalized function
bodies. The guard also verifies that `bunqueue_jobs_live_unique_key_idx` is a
valid, ready, non-expression, three-column `btree` with the exact live-state
predicate and unique enforcement. Same-name semantic drift is repaired by a
transactional drop/recreate at the end of the migration. If live duplicate keys
already exist, unique-index creation fails closed: the transaction restores the
previous index and data, records no new migration, and startup remains blocked
until an operator resolves the inconsistency. An unchanged schema keeps the
no-DDL path and preserves existing index object IDs. The v19 group fingerprint
adds the exact `BIGINT CACHE 1` admission sequence, `group_order`, every group-
state column/default/nullability rule, the semantic three-column primary key,
and both group indexes to this fast-path decision. Same-name but wrong-type,
wrong-order, wrong-predicate, or wrong-cache objects therefore cannot pass
because the recorded version is already current.

Schema migration is an automatic one-way compatibility boundary, not permission
to keep mixed bunqueue binary versions running. The safe production procedure is
to verify backup/PITR, stop every old broker, start and verify one new broker,
then start the remaining brokers at the same version. Because initialization
rejects a recorded schema version newer than the binary supports, an old binary
may not restart after migration. Rollback therefore means roll-forward or a
coordinated restore of the pre-upgrade database and old binaries. Any
zero-downtime mixed-version rollout needs explicit compatibility evidence for
the exact source/target pair.

The primary client/runtime pair is Bun 1.4.0 with its built-in `SQL` API and
PostgreSQL 18.6; the same integration suite also validates PostgreSQL 15, 16,
and 17. There is no additional JavaScript PostgreSQL SDK: tagged
templates, binary protocol, prepared statements, connection pooling,
transaction-reserved connections, and the dedicated reconnecting LISTEN
connection come from Bun itself. Queue mutations remain set-based when one SQL
statement can express them; dependent read-modify-write steps stay inside
`SQL.begin()` so automatic rollback preserves atomicity. PostgreSQL 18 AIO is a
server setting mainly relevant to scans and maintenance, so bunqueue does not
override it or other cluster-wide durability settings.
Each pooled connection receives PostgreSQL-native `application_name`,
`statement_timeout`, `lock_timeout`, and
`idle_in_transaction_session_timeout` startup parameters through Bun's
`connection` option. A blocked query therefore fails within an operator-visible
deadline and the same pool remains usable after the blocker ends.
Core admission, claim, ACK, ACKB, and FAIL transactions replay the complete
callback at most once, with jitter, only for PostgreSQL SQLSTATE `40001`,
`40P01`, or `55P03`, which guarantee rollback. Connection failures, constraint
errors, statement cancellation, and message-only matches are never replayed
because their commit outcome or repeatability is not safe to infer. Admission
uses a fresh job/timeline copy for every attempt.
The Bun SQL pool uses a 10-second connection timeout, a 30-second idle timeout,
and a 3,600-second maximum connection lifetime. These fixed lifecycle guards are
not public configuration fields.

The storage choice is entirely server-side. TypeScript, Python, PHP, Go, Rust,
and Elixir clients continue to use the same TCP protocol and require no database
driver or PostgreSQL credentials. CI and the isolated SDK gate run all 18 shared
conformance checks for every official SDK against both the unchanged SQLite
backend and PostgreSQL 18.6. Each PostgreSQL run receives an isolated namespace,
and the harness strips storage/auth variables from each driver, proves broker
exit, and only then removes its rows.

### Manager adapter

`PostgresQueueManager` subclasses the normal manager surface so the unchanged TCP
and HTTP transports can use it. Database mutations are asynchronous and
authoritative. Its constructor contract is isolated in
[`postgres-queue-manager/config.ts`](../../src/application/postgres-queue-manager/config.ts)
so lifecycle collaborators can be assembled with fakes without growing the
mutable manager state. A local snapshot serves synchronous compatibility reads
and is refreshed from durable events; point reads and handler paths that require
current distributed state use the durable methods exposed by the PostgreSQL
manager.
That includes `IsPaused`: after `Pause` or `Resume` returns, the TCP handler
reads `queue_state` from PostgreSQL instead of consulting the eventually
consistent compatibility snapshot. This provides read-your-write behavior to
all network SDKs even when the local broker is concurrently replaying its own
notification. Memory and SQLite retain the original synchronous lookup.
TCP `GetResult` and both branches of `WaitJob` use an asynchronous result port.
The PostgreSQL implementation reads the indexed completion record directly,
while memory and SQLite delegate to their existing synchronous result lookup.
For a live PostgreSQL wait, the manager registers its cancellable event waiter
before rechecking the completion table. This prevents both a result-cache race
after a remote broker completes the job and the check-before-subscribe gap; the
waiter is cancelled immediately when the durable recheck already finds the
completion.
Queue refresh, terminal retry, removal, and custom-ID generation reuse clear an
obsolete cached completion result before a non-completed generation becomes
visible.
Events observed during initial snapshot loading are captured by a dedicated
256-event accumulator and replayed after hydration. Manager hydration reads
jobs, completion results, crons, queue policies, and lifetime metrics from one
`REPEATABLE READ READ ONLY` snapshot. If the accumulator overflows, the partial
capture is discarded and hydration retries once from durable state; a second
overflow cannot starve readiness because every affected queue retains one dirty
marker for authoritative projection repair. Terminal lifetime counters are
finalized under the journal's namespace commit lock. The finalizer records the
durable `commit_seq` baseline before detaching the startup buffer, so a
pre-fence event delivered late is ignored and a post-fence commit is counted
exactly once. Queue refreshes likewise read jobs, results, existence, and policy
from one MVCC snapshot.

Before decoding a manager or queue read model, `snapshotBudget.ts` weighs the
exact rows visible to that same `REPEATABLE READ` transaction. It includes job,
completion, cron, policy, result, DLQ, and retry payloads. Exceeding
`maxSnapshotJobs` or `maxSnapshotPayloadBytes` rejects startup/refresh with an
explicit health error; no partial projection is installed and no data is
silently truncated. This is a fail-safe for the synchronous compatibility view,
not a retention policy for authoritative PostgreSQL rows.

After readiness, durable journal events never directly overwrite local job or
lease state. A coalescing projection scheduler reloads the current job and, for
removed rows, its completion tombstone. Per-job generations discard an older
in-flight read when a direct local claim wins. An authoritative queue refresh
also supersedes every older per-job projection for that queue immediately before
installing its MVCC snapshot, so a read started before `obliterate` cannot
restore a deleted completion result afterward. Completion projections obtain
their owning queue from the durable completion row rather than trusting an
optional request hint. Generation identities are unique for each flight and
their map entries are reclaimed as soon as refresh or local supersession
settles, so fencing metadata does not grow with historical job IDs.
A failed projection is reported through storage health and retried, while the
already committed public operation keeps its database-defined success result.
Deferred compatibility writes
use a dedicated serial queue that retains failures until an observed flush.
Concurrent flush callers at the same sequence share one checkpoint and observe
the same failure set; shutdown drains that queue and remains coalesced but
retryable. A failed authoritative queue refresh keeps its invalidation dirty and
retries with bounded exponential backoff. This prevents a transient read error
after journal pruning from leaving only the retained event subset in the local
snapshot. Per-queue refresh failures remain visible through storage health until
that queue succeeds; shutdown cancels pending retries before closing the pool.
Periodic maintenance is single-flight per subsystem, while unrelated subsystems
may proceed concurrently. Store shutdown closes timer admission and awaits every
admitted maintenance flight before releasing broker leases or the SQL pool.
Keyed post-commit maintenance serializes each subsystem and coalesces only work
that has not started. A late success or failure can neither overlap nor report
for a newer entry; current failures therefore remain visible and retryable
without an ABA generation race. Unrelated subsystems may still run in parallel.
Shutdown closes maintenance admission before the SQL pool, skips work submitted
after that boundary, and drains stale in-flight outcomes without allowing them
to replace a newer generation. A dedicated
reentrant lifecycle gate rejects every new database-backed manager operation
once shutdown starts and drains operations admitted earlier through their final
database transition. Projection retries stop before this drain and any
already-running projection is awaited before the store closes, so a committed
admission, claim, mutation, or query cannot be
surfaced as a connection failure. An async scope can call another gated manager
method without deadlocking, but descendants that escape the admitted scope are
rejected after it settles. Claims hold admission only for each database attempt,
not for the surrounding long-poll wait, so an empty 60-second pull never delays
shutdown. Synchronous compatibility mutations acquire admission together with
their deferred SQL enqueue; after shutdown, disconnect cleanup may clear only
its local tracking state. The same gate permits 16 active and 128 queued
PostgreSQL operations by default. Saturation fails fast instead of growing an
unbounded waiter list while the database is slow or unavailable; nested work
retains its admitted scope and cannot deadlock behind its own capacity slot.
Startup hydration and every accepted deferred write
are drained before the pool closes. Completion retention is
reconciled at startup and periodically. DLQ maintenance likewise performs a
set-based overage scan, re-reads each current policy under a queue-state share
lock, and safely repairs interrupted retention across concurrent brokers.
DLQ auto-retry separately discovers the bounded failed-consumer set before it
takes locks, then acquires queue-policy rows, the sorted union of consumer and
dependency identities, and failed rows in that order. It revalidates current
edges, database time, policy, and retry eligibility after all waits. A custom-ID
dependency reused concurrently therefore either remains protected by its old
completion proof or makes the consumer `waiting-children`; stale evidence can
never make the consumer runnable. Four concurrent maintenance brokers converge
to one retry and one event.
The base memory/SQLite maintenance timers are stopped immediately; only the
PostgreSQL runtime owns lease recovery, DLQ, cron, broker, and worker maintenance
for this manager.

Client disconnect release is a retryable generation session. It snapshots each
`(jobId, token)` once, removes an item only after PostgreSQL has returned a
definitive result, and preserves both pending entries and the cumulative release
count across transport retries. Concurrent callers share the same in-flight
session. A transient failure therefore cannot turn the next retry into a false
zero-success response or orphan the remaining active jobs.

Cloud commands and snapshots use a second Strategy/Registry boundary under
`infrastructure/cloud/queueAdapter/`. The PostgreSQL Strategy never falls back
to synchronous compatibility reads for shared data. Its snapshot read model
loads bounded jobs, queue configuration/counts, lifetime terminal totals,
results, logs, workers, crons, and active leases inside one
`REPEATABLE READ READ ONLY` transaction. Webhooks and process telemetry remain
broker-local by definition.
Cloud job pages use the local manager's half-open `[start, end)` contract for
both engines; the command boundary normalizes finite non-negative `offset` and
`limit` values without changing SQLite query semantics. PostgreSQL storage and
endpoint diagnostics remain in local logs, while both normal command failures
and `snapshot:get` responses apply the shared server sanitizer before leaving
the process.

Process shutdown is coordinated outside the storage engine. The first signal
owns a memoized cleanup task; later signals share it. Timers and listeners stop
before the active-job drain, the shutdown event precedes the bounded final Cloud
flush, and storage closes last. A transient storage rejection is retried once;
permanent failure exits non-zero instead of recursively entering the
`unhandledRejection` handler and stranding the broker.

The handler routers accept `Response | Promise<Response>` where a PostgreSQL
operation needs I/O. SQLite handler calls remain synchronous, preserving the
existing behavior and latency boundary. Worker/cron views used by
`DashboardOverview`, the HTTP dashboard, `GET /queues/:queue/workers`, and the
periodic WebSocket/SSE stats snapshot use the durable registries in PostgreSQL
mode; a broker therefore reports registrations created through another broker
instead of its process-local compatibility maps.

Progress mutation has the same omission contract as SQLite: the PostgreSQL row
is locked, a supplied string replaces `progressMessage`, and an omitted
`message` preserves the prior value. Progress, the effective message, heartbeat,
payload, and outbox event commit in the same transaction. The SQLite mutation
path is unchanged.

## Transaction and ordering model

### Admission

Admission runs in one transaction. It uses namespace-scoped advisory transaction
locks for custom IDs and unique keys, row-locks an existing generation, verifies
dependency and parent ownership, retires a replaceable terminal generation, and
inserts the job, dependencies, and durable event atomically. TTL-based unique-key
expiry uses the PostgreSQL clock. Dependency-completion locks are acquired in
canonical ID order before evidence is read or an edge is inserted. The server
performs a set-based authoritative dependency preflight so event-stream lag on
the receiving broker cannot reject a parent committed elsewhere. Existence is
asserted again inside the admission transaction under those locks; if a parent
was removed after preflight, the transaction rolls back instead of publishing
an orphan `waiting-children` job. Bulk and flow admission pre-lock the complete
union with one set-based statement and defer the final existence assertion until
all planned rows are visible in the transaction, preserving reverse-order
same-batch parents while rejecting a planned parent that deduplicates to another
ID. A child completion and a concurrent new consumer therefore have only two
legal outcomes: the edge is visible to that completion, or admission observes
its committed proof.
Single and bulk admission also acquire the complete union of custom-ID and
deduplication advisory keys in one canonical set-based order. A reversed batch
therefore cannot form a primary-key or unique-index deadlock with another batch
or a simultaneous single admission.
Before those identity locks, single, serial-batch, set-based batch, flow, and
cron admission acquire shared transaction-scoped queue-lifecycle advisory locks
in canonical queue order. Ordinary producers remain mutually concurrent. A
queue registry row is inserted only after a generation was actually inserted,
so a duplicate custom ID cannot create a phantom queue. Schema v15 backfills the
registry from existing job rows.
Single and serial-batch admission resolve live-ID and unique-key outcomes before
retiring the requested generation. Returning another deduplication owner leaves
the candidate's completion-only proof or retained terminal row byte-for-byte
intact. Once the exact candidate is committed to insertion, retirement may
exclude only consumers actually inserted earlier by the same transaction;
pre-existing consumers still reject reuse. After every serial decision, one
set reconciliation derives each surviving inserted row from final completion
evidence. It can promote or demote reverse-order consumers, replaces only their
initial timeline state, and patches the payload of the already-recorded
`pushed` event in place. Event IDs and causal `pushed`/`removed` order never
change. Validated flows still retire their all-or-nothing planned set before
insertion because flow inputs do not deduplicate and any rejection rolls the
transaction back. A deduplication replacement that discovers a different owner
uses a non-blocking dependency-identity probe; if another admission owns that
identity, replacement returns the existing generation as a duplicate.

Bulk admission uses one transaction and therefore publishes all accepted jobs or
none. Independent jobs without dependencies or parents use chunked multi-row
inserts plus one set-based event write. This fast path includes new custom IDs
and deduplication keys, including their TTL metadata. Any primary-key or live
unique-key conflict makes the inserted-row count incomplete before completion
retirement, rolls the complete attempt back, and retries through the normal
serial admission rules. The retry
preserves custom-ID generation reuse and deduplication reject/extend/replace
semantics without partial rows, duplicate timeline entries, or duplicate events.
Duplicate input IDs, dependencies, and parent links use the serial path directly.
Flow admission validates the complete graph before writing it. After acquiring
the complete queue, dependency, parent, and admission lock plan, it samples one
post-lock database timestamp and does not reacquire those per job. Completion
generations are retired once for the full planned set, queue identities are
registered once, and all ordered `pushed` records use the set-based event writer
with retention and wakeups amortized per queue. The graph remains one
all-or-nothing transaction; a retry recreates every job and timeline before the
batched durable writes.

After a batch commits, the manager fetches only the distinct affected job IDs in
one set query and merges those authoritative rows into its compatibility
snapshot. It does not reload, sort, and decode every earlier job in the queue.
An in-flight per-ID watch skips a query row when a newer event for that exact job
has already updated or removed it; unrelated or already-committed local events do
not restart the database read. This keeps repeated fixed-size pushes linear in
total admitted jobs while remote events remain the source of cross-broker
snapshot updates.

### Claims

A claim first ensures that the queue has a durable `bunqueue_queue_state` row.
The transaction then takes `FOR SHARE` when pause/rate/concurrency policy is at
its default, allowing independent claimers to run concurrently while still
blocking a policy update until their transactions finish. A configured rate or
concurrency limit retries under `FOR UPDATE`, preserving exact shared capacity
and rate-window accounting. Paused queues can be rejected under the shared lock.

Within that lock, a claim:

1. refreshes normalized rate-limit state using database time (invalid or
   non-positive duration means the default one-second window; invalid or
   non-positive TTL means no automatic expiry);
2. expires pending TTL jobs and promotes resolved dependencies;
3. calculates rate/concurrency capacity only when the corresponding policy is
   configured;
4. serves ungrouped candidates first, then uses the grouped query path to take
   FIFO positions round-robin across groups;
5. locks a narrow ID/order tuple with `FOR UPDATE SKIP LOCKED` before applying
   the batch limit, then fetches payloads only for locked IDs; and
6. writes payloads, opaque tokens, owner, broker ID, lease deadlines, and pulled
   events with set-based statements before commit.

Ungrouped candidate ordering remains deterministic: higher priority first, then
the LIFO partition, then FIFO by ready time and ID. Grouped jobs are FIFO by
durable `(run_at, group_order, id)` within each group and round-robin by durable
`last_served`; ungrouped work has
precedence. Group concurrency is unlimited unless the Worker supplies a
`group.concurrency` default, and fixed-window group limiting is disabled unless
it supplies `group.limit`. Stored group-specific values override only those
enabled defaults. A grouped claim takes the exclusive queue-state row,
calculates active leases and remaining fixed-window budget, fences candidates
with `FOR UPDATE SKIP LOCKED`, then advances `group_sequence`, every selected
group's `last_served`, and rate counts in the same transaction as the leases and
events. Rechecking eligibility on the locked row prevents double delivery.

Every destructive command uses one shared protocol: discover candidate IDs
without row locks, acquire their dependency-completion advisory locks in sorted
order, lock candidate and live-consumer rows in sorted order, revalidate the
command predicate, and delete only producers whose consumers are also deleted
atomically. Single-job cancel/remove/DLQ removal returns `false` for a protected
producer; clean, TTL, drain, DLQ limit, and DLQ expiry skip protected rows;
`obliterate` rejects the transaction if another queue still has a live consumer.
DLQ limit/expiry deletion runs as its own canonical transaction after the
terminal transition commits, avoiding a row-lock-to-advisory-lock inversion.
`obliterate` first acquires the matching exclusive queue-lifecycle advisory
lock, then ensures and locks the queue-state sentinel, discovers candidates
inside that same transaction, and only then takes dependency identities and job
rows. Admissions committed before the exclusive lock are removed; admissions
that begin afterward wait and survive. This order also prevents the inverse
queue-state/job wait cycle.

`removeUnprocessedChildren` has a focused transaction script in
[`removeUnprocessedChildren.ts`](../../src/infrastructure/persistence/postgres/removeUnprocessedChildren.ts).
It discovers direct waiting, prioritized, delayed, and waiting-children
candidates, then locks their sorted dependency identities, the flow parent, all
candidate rows, and every live consumer before revalidation. A fixed-point pass
protects a candidate required by any surviving external consumer and propagates
that protection through candidate-to-candidate dependencies. The same
transaction deletes only the safe pending generations, detaches protected shared
children, removes the current parent's dependency edges, updates the parent once,
and emits the exact durable event batch. Active and terminal children retain
their state, lease token, result, completion proof, and DLQ data. Repeating the
command is therefore idempotent, and another broker cannot observe an orphaned
consumer between detach and deletion.

### Completion, failure, and fencing

ACK, FAIL, lease renewal, delay changes, and active-job removal lock the job row
and validate the exact lease token. ACK/FAIL is rejected after lease expiry or
when another broker has recovered the row. Completion results, retry/DLQ state,
flow failure propagation, repeat successor admission, metrics, and the event are
committed with the state transition. `dependencyPromotion.ts` first builds an
immutable completion-scoped lock plan. After completion evidence is persisted,
it promotes every newly ready parent with set-based payload, timeline, state,
version, and durable event writes in the same transaction. This applies to ACK,
ACKB, `removeOnComplete`, and multi-parent fan-in; pull-time promotion remains an
idempotent repair path rather than the normal visibility boundary.

Dynamic parent attachment, detach, terminal failure, and expired-lease recovery
share the child's dependency-completion advisory key. Once that key is held, the
transaction reads the current `parent_id`, acquires all flow-parent keys in
canonical order, and only then locks job rows. Attachment either commits before
that parent read and is included in failure propagation, or waits until the
terminal transition finishes; there is no unlocked child-to-parent TOCTOU path.

A unique-ID `ACKB` locks rows in deterministic ID order, validates every lease
before the first write, and then persists completions, retained/deleted rows,
events, and queue metric additions with set-based statements. One invalid token
rolls back the complete batch. Duplicate IDs retain the public positional
semantics through the serial compatibility path. Repeat successors remain a
rare per-job step inside the same transaction. The manager applies returned
completion rows directly to its local snapshot; it does not reload the complete
queue after every ACK batch.

If the deferred event sequencer exceeds `lock_timeout`, PostgreSQL aborts the
transaction with `55P03`; the bounded core replay uses the same IDs and lease
tokens. Exhaustion returns the error with the job still active and no completion,
completed event, or metric increment. Local ACKB logs retain bounded `name`,
`code`, SQLSTATE, `where`, `routine`, request ID, and batch size diagnostics while
the protocol response remains redacted.

Repeat ACKs discover the successor queue only after they already own completion
locks. They therefore use a sorted non-blocking shared lifecycle try-lock rather
than waiting behind `obliterate`. A conflict rolls back the entire ACK for safe
retry: the original transition and repeat successor can never split, and the
late lock cannot deadlock the exclusive deletion transaction.

Completion evidence is generation-scoped. Reusing a terminal custom ID first
locks that identity and retires its old completion row; reuse is rejected while
a live consumer still owns that proof. `removeOnComplete` tombstones without a
live consumer are retained newest-first up to `maxCompletedJobs`, while live
dependency proofs are pinned outside that cap. The compatibility snapshot keeps
at most `maxCompletedJobs` completed job rows and an independent
`maxJobResults` LRU result cache; durable child-result reads still query
PostgreSQL when that cache is zero or an entry was evicted.

Tombstone pruning commits at most 1,000 removals per transaction and repeats
until the configured bound is reached. The broker performs this convergent sweep
before startup readiness, after removed completions, and every 60 seconds as a
repair path. Post-commit failures retain a coalesced retry and degraded storage
health; a later broker startup also repairs durable backlog left by a shutdown or
configuration reduction. Every batch rechecks live dependency consumers after
its ordered identity locks, so pinned proofs remain outside the bound.

Lease renewal increments `lease_renewals`, updates the payload heartbeat, and
transfers both `lease_broker_id` and `lease_broker_session_id` to the broker
session that received the heartbeat. This
matters when a pooled worker pulls through one broker and renews through another.
Disconnect cleanup releases only a never-renewed lease owned by that exact client;
a remotely renewed lease remains fenced. Both awaited and deferred disconnect
paths snapshot every `(jobId, leaseToken)` before their first await or queued
write. A callback for an old custom-ID generation can therefore never discover
and release the token of a reused generation; local token cleanup also uses an
exact-token comparison. Graceful broker shutdown releases only leases owned by
its exact internal session, so it cannot release a successor process's work.

### Recovery

Expired active rows are recovered under row locks. Retryable jobs return to their
ready/delayed state; terminal attempts enter the DLQ exactly once. Cron-generated
jobs with overlap protection are discarded instead of requeued when their broker
dies, preserving `preventOverlap`. Recovery first locks candidate child
relationship keys, re-reads their current parents, locks the sorted parent set,
and then revalidates the active rows with `FOR UPDATE SKIP LOCKED`. All scheduling
and lease comparisons use the PostgreSQL clock so broker clock skew cannot cause
early cleanup or stale ACKs. PostgreSQL DLQ entry, attempt, retry, and expiry
timestamps are also derived from the transition's database timestamp. Age-based
maintenance therefore compares values from one clock even when brokers and the
database have different wall-clock offsets; the memory and SQLite constructors
retain their existing host-clock behavior.

## Distributed coordination

- Every runtime advisory identity is length-prefixed and domain-separated before
  `hashtextextended(..., 0)`. Batch admission, dependency completion, flow parent,
  and queue lifecycle plans deduplicate and sort the resulting physical `BIGINT`
  keys before acquiring blocking, try, shared, or exclusive locks. Distinct
  client-controlled IDs, deduplication keys, and queue names therefore cannot
  alias merely because their legacy 32-bit `hashtext` values collide.
- `bunqueue_events` is the transactional outbox. Statement triggers register one
  `(namespace, transaction_id)` commit envelope, and a deferred constraint trigger
  takes a namespace transaction advisory lock and allocates from the global
  `CACHE 1` event sequence immediately before commit. It stamps the compact
  envelope and any watermark, while the event rows remain immutable and replay
  joins through `transaction_id`. The lock orders same-namespace commits without
  a hot event-row rewrite; a rollback removes the envelope and may leave only a
  harmless sequence gap. Startup and periodic maintenance collect envelopes
  after their final event and watermark references disappear. GC deletes up to
  eight 10,000-row batches per database turn and reschedules after 25 ms while
  every batch remains full, instead of waiting another minute with a growing
  commit-envelope backlog.
- `bunqueue_event_prune_watermarks` records, in the pruning transaction, the
  highest removed physical event ID and a cumulative, monotonic pruned-commit
  frontier per affected queue. Brokers compare that frontier with a per-queue
  applied commit cursor and refresh the queue unless the cursor is strictly
  ahead of the pruned frontier. Equality is not proof of a complete view: one
  transaction that writes more queue events than the retained window prunes its
  own older events, so a reader that applied exactly that commit may hold only
  its retained tail. Such a commit is unobservable from events by
  construction, so the commit trigger also records it in the cumulative,
  per-queue `self_pruned_commit_seq`. Every later watermark inherits the
  per-queue maximum on insert and on conflict, and an uncommitted
  `prunes_current_transaction` marker is inherited the same way, so neither a
  superseding checkpoint nor a second prune inside the same transaction drops
  the evidence. `events.ts` delegates the reader side to
  `eventCatchupCursors.ts`, which remembers the newest watermark and self-prune
  frontier already accounted for per queue, so each such commit refreshes that
  queue once instead of on every scan, and an already-current broker with no
  unhandled self-prune does not reload it. The memo keeps three numbers per
  queue name observed by the process, alongside that queue's applied-commit
  cursor, and like the cursor it is not evicted when a queue is obliterated. It
  is seeded at startup because `PostgresQueueManagerState` loads a full snapshot
  of every queue immediately afterwards. Because a reader can also miss history
  that a newer commit pruned, a drain that loaded journal entries always
  re-scans watermarks against its pre-batch position before applying them
  instead of relying on a notification having armed the scan. That trades
  latency for correctness: the watermark query costs roughly one row per queue
  in the namespace, so a busy namespace with thousands of queues pays it on
  every non-empty drain instead of once per poll interval. Idle brokers are
  unaffected, since an empty journal batch does not arm the scan.

  This bookkeeping depends on the commit-sequencer trigger, so it was introduced
  in PostgreSQL schema version 18. A broker built against version 17 refuses to
  start against an upgraded database rather than reinstalling its own trigger
  and disabling the guarantee for every broker sharing that database; upgrade
  all brokers in a cluster together. Manual trim derives the frontier from the deleted event envelopes instead
  of treating its own transaction as pruned history.

- PostgreSQL schema version 19 adds `bunqueue_group_order_seq`,
  `bunqueue_jobs.group_order`, `bunqueue_queue_state.group_sequence`,
  `bunqueue_group_state`, and exact ready/rotation indexes. A v18 binary refuses
  to start after this migration; upgrade every broker in a namespace together.
  Admission allocates grouped order in input order before 1,000-row insert
  chunks. Bounded cleanup resets inactive rotation positions and deletes only
  rows with no job, override, or live effective rate window; queue obliteration
  removes all group state transactionally.

- Event retention finds the first row beyond the configured per-queue window
  through the `(namespace, queue, id DESC)` index and deletes through that cutoff.
  Inline pruning takes a non-blocking per-queue advisory lock, materializes the
  candidate IDs, and row-locks them in ascending order before deletion. A
  transaction that already owns another queue's event tuples never waits for a
  second retention lock, preventing both same-queue tuple cycles and inverse
  multi-queue lock orders. A skipped prune requests, after commit, a coalesced
  single-queue sweep with a fresh snapshot. Notification-driven sweeps also use
  a non-blocking advisory-lock attempt: a busy queue remains in the bounded
  pending set and arms one retry no later than 250 ms, so multiple brokers do
  not occupy pool connections for the full lock timeout. The retry timer gates
  only retention; durable journal replay continues while it is armed, and
  shutdown cancels it before awaiting an in-flight drain. Expected contention
  does not degrade storage health, while a real SQL failure remains unhealthy
  until a complete retry succeeds. Manual trim, startup recovery, and periodic
  crash repair retain their blocking exact sweeps. The retained window therefore
  converges exactly after concurrent commits without placing a namespace lock on
  the lifecycle hot path. A batch larger than the window publishes a durable
  watermark instead of storing a synthetic public event.
- Queue controls record their invalidation in the same transaction as pause,
  rate/concurrency limits, policy updates, drain, and obliterate. Catch-up
  therefore removes an obliterated queue even when its live notification was
  missed.
- `PostgresEventStream` LISTENs on `bunqueue_jobs_changed` and also polls the
  durable journal in `(commit_seq, id)` order. Notifications contain only wake-up
  hints; correctness does not depend on their payload or delivery. Drain requests
  coalesce into bounded state instead of an unbounded range queue. A retention
  miss is coalesced separately from journal work, while a complete failed or
  recovered durable scan updates runtime health independently of periodic
  maintenance. Job events carry the
  authoritative job/state payload and update the snapshot directly. Terminal
  and retry events also carry the complete DLQ entry and retry state, including
  explicit `null` clearing. Queue-control or payload-less events schedule a full
  queue refresh. A per-queue event version retries stale full-queue refreshes,
  and transient refresh failures retain a dirty marker for bounded-backoff retry,
  while an affected-ID refresh leaves any ID changed by an in-flight event
  untouched. PostgreSQL requires `maxQueueEvents >= 1`; zero is rejected
  explicitly because multi-broker convergence requires a durable journal row.
  Memory and SQLite keep their existing zero-retention behavior.
- DLQ `maxEntries` eviction and `maxAge` expiry publish one transactional queue
  invalidation per affected queue, regardless of the number of rows removed.
  Every broker then replaces its compatibility snapshot from PostgreSQL instead
  of retaining locally stale failed jobs. DLQ creation and expiry use the same
  database clock, so a broker whose host clock is ahead cannot postpone purge or
  auto-retry decisions for the other brokers.
- `bunqueue_brokers` heartbeats identify active broker sessions in a namespace.
  A user-facing `brokerId` is stable, while every process owns a random internal
  session UUID. A second live process with the same ID fails startup. After the
  heartbeat is stale, takeover atomically installs a new session; the old
  process can no longer claim, renew, register workers, resurrect its heartbeat,
  delete the successor row, or release successor leases. Session locks serialize
  an in-flight old operation before takeover. Under a namespace advisory lock,
  startup cron reconciliation deterministically elects the oldest live
  `(started_at, broker_id, session_id)` session. Simultaneous broker startup
  therefore cannot make every process skip missed-schedule reconciliation.
  Heartbeats run every `max(1000, floor(leaseDurationMs / 3))` milliseconds;
  stale takeover uses
  `max(leaseDurationMs, 3 × heartbeatInterval)` (10 seconds and 30 seconds at
  defaults). Expired processing leases are scanned every
  `max(500, floor(leaseDurationMs / 2))` milliseconds (15 seconds by default),
  in addition to the worker generation's own `lockDuration` expiry.
- `bunqueue_workers` makes worker registration and heartbeat state visible to
  every broker. `skipIfNoWorker` therefore evaluates the shared registry rather
  than one process's memory.
- queue pause, rate limit, concurrency, stall and DLQ policies are stored in one
  row per namespace/queue and serialized by row lock.
- job-log writers and retention operations lock the owning job row, serializing
  them with each other and with every job-removal transaction.
- all tenant-visible tables include `namespace`; independent installations may
  share a database without sharing jobs.

## PostgreSQL schema

The normalized schema is defined in `postgres/schema.ts` and summarized in
[Data Model](../data-model.md). The authoritative tables are:

- `bunqueue_jobs`: job payload/state, scheduling fields, deduplication,
  parent/group links, lease fencing, terminal/DLQ state, and row version;
- `bunqueue_dependencies`, `bunqueue_completions`, `bunqueue_flow_failures`, and
  `bunqueue_repeat_links`: dependency and flow/repeat ownership. Completion
  rows have `(namespace, queue, job_id)` and
  `(namespace, completed_at DESC, job_id DESC)` indexes for queue invalidation
  and bounded newest-first retention;
- `bunqueue_queue_state`: shared pause, rate, concurrency, stall and DLQ policy;
- `bunqueue_crons`, `bunqueue_workers`, and `bunqueue_brokers`: distributed
  runtime coordination;
- `bunqueue_job_logs`, `bunqueue_metric_buckets`, and
  `bunqueue_metric_totals`: durable observability; and
- `bunqueue_events`: ordered invalidation and event replay; and
- `bunqueue_event_prune_watermarks`: per-queue proof that retained history has a
  gap requiring an authoritative refresh for brokers behind that watermark, plus
  the carried-forward frontier of commits that pruned their own events; and
- `bunqueue_event_commit_seq` and `bunqueue_event_commits`: the deferred commit
  sequence and immutable commit envelopes used by replay. The sequence is fixed
  at `CACHE 1`; a namespace advisory lock prevents connection-local allocation
  order from diverging from same-namespace commit order.

Job/options/results/policies are MessagePack `BYTEA` values. Indexed scalar
columns are duplicated beside the payload for locking, filtering, and ordering.

## Feature parity and intentional boundaries

The PostgreSQL server path supports the public TCP/HTTP queue lifecycle,
priority/FIFO/LIFO ordering, delayed jobs, retries and backoff, TTL, custom IDs,
unique-key reject/extend/replace, groups, dependencies and flow failure policies,
repeat successors, pause, rate/concurrency limits, stall recovery, DLQ policies,
cron reconciliation, workers, logs, queue metrics, and event trimming.

Intentional boundaries:

- embedded `Queue`/`Worker` still use memory or SQLite; PostgreSQL is a server
  storage driver;
- S3 snapshot/restore is SQLite-only; PostgreSQL backup is an operator/database
  concern;
- MySQL is not supported;
- local snapshot reads may briefly lag a remote commit, while durable point and
  command reads query PostgreSQL. Dependency admission, worker/cron lists,
  dashboard overviews, per-queue worker HTTP reads, and streamed stats snapshots
  are among the explicitly durable server surfaces; and
- PostgreSQL performance is not represented by the existing native SQLite
  benchmarks. Container functional results must not be published as benchmarks.

## Failure modes

- Schema or connection initialization failure prevents network listeners from
  binding and closes the partially created pool.
- Lifecycle, event-stream, queue-refresh, heartbeat, recovery, DLQ, and cron
  health are tracked independently. Only a success from the same subsystem
  clears its prior failure; only a complete successful journal scan clears a
  prior journal failure. Any stored runtime error marks HTTP health/readiness
  and the WebSocket health snapshot degraded and sets
  `bunqueue_storage_degraded=1`, even when `diskFull` is false. Client-facing
  status, dashboard, MCP, and Cloud payloads redact the internal PostgreSQL
  diagnostic while the runtime retains it for local logs and debugging.
- Lost or malformed NOTIFY messages only delay a wake-up. Durable
  `(commit_seq, id)` polling catches every committed transaction, including a
  lower physical ID that commits after a higher one.
- Bun reconnects the SQL pool, LISTEN subscription, and durable polling after
  backend termination. A command already using a terminated connection may
  still fail because its commit outcome is not safe to guess; the store never
  blindly replays such a write. Bounded internal replay is restricted to
  PostgreSQL's rollback-certain transaction SQLSTATEs. Callers retry ambiguous
  failures with the same custom ID or
  deduplication identity. A transaction interrupted after its job row write but
  before its event/commit rolls back every row and can then be retried exactly
  once.
- Shutdown attempts close broker rows, workers, leases, the event subscription,
  and the SQL pool independently. Concurrent callers share one attempt; a
  transient failure retains ownership so the next attempt can finish cleanup.
- A dead broker is not trusted to release anything; lease expiry plus fencing is
  the correctness boundary.
- Broker IDs must be unique within a namespace. A live duplicate fails fast;
  stale takeover is session-fenced and automatic after the liveness window.
- PostgreSQL URL and credentials are secrets and must be injected, not committed.

## Validation

Dedicated tests require `BUNQUEUE_TEST_POSTGRES_URL` and create a unique namespace
per case:

```bash
BUNQUEUE_TEST_POSTGRES_URL=postgres://... bun run test:postgres
BUNQUEUE_TEST_POSTGRES_URL=postgres://... bun run test:postgres:smoke
BUNQUEUE_TEST_POSTGRES_URL=postgres://... bun run test:postgres:destruction
BUNQUEUE_TEST_POSTGRES_URL=postgres://... bun run test:postgres:pressure
BUNQUEUE_TEST_POSTGRES_URL=postgres://... bun run test:postgres:battle
BUNQUEUE_TEST_POSTGRES_URL=postgres://... bun run test:postgres:fast-check
BUNQUEUE_TEST_POSTGRES_URL=postgres://... bun run test:postgres:ten-broker
```

The dedicated commands fail before launching Bun Test when the URL is absent or
blank, so they cannot report a false pass made entirely of skipped integration
cases. The ordinary repository-wide `bun test` command still skips PostgreSQL
cases when no database was requested. `smoke` exercises startup, schema,
lifecycle, multi-broker, and public Queue/Worker/Flow paths. `destruction`
combines connection/transaction termination, destructive dependency and queue
races, bounded core-transition rollback/replay, shutdown boundaries, schema
corruption, generated destructive histories, and broker `SIGKILL`. `pressure`
enables the ten-broker soak and combines it with batch, contention, 32-bit
collision fixtures, four-process, event, metric, and extreme public-API load.
`battle` runs the complete PostgreSQL suite with the ten-broker soak enabled and
raises every Fast Check campaign to 100 runs. Pressure/soak timings remain
functional diagnostics, never publishable benchmarks. Process-backed pressure
fixtures drain both broker output streams, prefix every line, retain bounded
head/tail diagnostics, and classify human or JSON log records incrementally.
The ten-broker campaign stops the processes and waits for stream EOF before it
reports bounded transaction retries or treats any ACKB failure as a failed
invariant, so the assertion cannot race the final log chunk or ignore JSON logs
written to stdout. A stream read error or missing EOF makes the diagnostic gate
fail closed after best-effort pipe cancellation; an incomplete capture can never
be reported as an authoritative zero-failure result.

The primary tests assert PostgreSQL reports version `18.6`; the same complete
suite also runs against the current PostgreSQL 17, 16, and 15 Alpine images, with the
expected major supplied through `BUNQUEUE_TEST_POSTGRES_VERSION_PREFIX`.
`postgres-public-api-queue-worker.test.ts` and
`postgres-public-api-flow.test.ts`, and
`postgres-public-api-extreme.test.ts` launch four independent bunqueue processes
on one namespace and deliberately connect each public surface to a different
broker. They prove `Queue` admission, pagination, state and counts; `Worker`
execution, progress, results and logs; `QueueEvents`; cross-broker pause,
custom-ID idempotency, DLQ inspection/retry; and durable cross-queue
`FlowProducer` graphs with exact-once execution, dependency ordering, child
results, tree reads, and parent-result reads. The extreme campaign additionally
linearizes 256 simultaneous admissions for one custom ID, resolves 256 late
remote waiters after one `removeOnComplete` transaction, executes 32 concurrent
eight-way flows (288 jobs), and kills the broker owning an active public Worker
job before proving survivor recovery. A remote wait plus `GetResult` retains
the result when `removeOnComplete` deletes the job row, and the
completion-retention suite repeats the authoritative lookup with a zero-sized
local result cache. These files are part of the
normal `test:postgres` command and therefore run in every PostgreSQL 15–18 CI
matrix entry; they are not an opt-in soak.
Concurrent brokers claim each job once under both ordinary and 16-consumer
high-contention campaigns, stale owners are fenced, broker/client shutdown is safe, remotely
renewed leases survive disconnect, shared limits and workers work, missed cron
slots reconcile once, protected cron leases do not resurrect, and durable TCP
maintenance commands retain lifecycle state. Batch tests additionally assert
atomic invalid-token rollback, primary-key conflict fallback, mixed FIFO/LIFO
ordering, custom-ID/deduplication bounded latency, no whole-queue reload after a
push batch, exact event retention, and deduplication-conflict fallback without a
partial fast-path commit. Opposite-order, same-ID batches from two brokers are
repeated to reject transaction deadlocks. Event-retention races hold two
transactions after their first prune, then request `Q1 -> Q2` and `Q2 -> Q1`;
both must commit and converge to the exact window. A deterministic blocker
also proves that notification-driven retention defers without degrading
health, coalesces its retry, reaches the exact window after release, and
cancels the timer during close. The unchanged 5,000-job, two-broker campaign
remains the black-box guard against retention lock convoys. A repeated
manual-trim versus obliterate race covers the
lifecycle/retention/event/watermark order. Public TCP tests compare zero and
negative rate-limit duration/TTL behavior with SQLite, while concurrent log
tests assert exact retention and no orphan on removal. Runtime tests keep a
failed maintenance key unhealthy across unrelated successes; configuration and
handler tests cover URL-safe Compose credentials and PostgreSQL diagnostic
redaction across thrown errors and non-throwing storage-health payloads. They
also cover durable failed-job `MoveToWait`, immediate broker-A-to-broker-B
dependency admission with a paused event stream, the preflight/removal TOCTOU,
reverse-order same-batch dependencies, and shared worker/cron data in HTTP,
dashboard, WebSocket, and SSE surfaces. The disposable unit-test image includes the PostgreSQL Compose
manifest, so that credential regression also runs inside the mandatory
network-isolated sandbox. Two controlled active-child races hold the parent
advisory key while explicit failure or expired-lease recovery runs, proving
neither terminal path can bypass a concurrently committed parent attachment. A
queue-state lock regression proves default-policy claims remain compatible with
an existing shared lock rather than requesting an exclusive one. Additional
deterministic regressions prove the destructive lock
order, result-cache invalidation after retry/clean/custom-ID reuse, and DLQ
snapshot convergence after bounded eviction or expiry, including a one-event
journal window. The multi-process topology harness retries broker startup when it loses the
race for its probed port pair, and only when the broker actually exited; see
`docs/testing.md`. `postgres-event-partial-commit-retention.test.ts` drains
journal entries before any watermark scan runs and proves three cases: a commit
that pruned its own older events still refreshes the reader, a later
superseding watermark cannot hide such a commit, and history pruned by a newer
commit is still repaired. It also asserts that a stable frontier stops
refreshing the queue after the first repair. Dependency regressions also prove same-transaction parent
promotion for single and batch completion, removed completion evidence,
priority/delay selection, concurrent fan-in, and both commit orders of the
completion-versus-admission race. Startup tests force more than 256 captured
events and prove a bounded authoritative retry; adapter tests prove explicit
memory ignores an inherited data path. `test/postgres-config.test.ts` separately proves SQLite remains
the inferred default and rejects ambiguous storage/backup configuration.
`test/postgres-connection-recovery.test.ts` terminates every pooled backend for
two live brokers, retries stable custom IDs through reconnect, and requires both
event projections to converge. Its transaction-reset case blocks event insert
after the job row has been written, terminates that exact backend, proves the
entire admission rolled back, and then admits the same generation once.
`test/postgres-schema-dedup-guard.test.ts` replaces the live-key index with
same-name weaker definitions and proves exact repair; duplicate live rows must
instead preserve data and fail every retry without partially applying DDL.
`test/postgres-core-transaction-retry.test.ts` holds the exact deferred commit
sequencer beyond one `lock_timeout` and proves single admission, batch claim,
single ACK, and terminal FAIL replay once with exact durable state. The companion
ACKB regression proves retry exhaustion is bounded and leaves no partial job,
completion, event, or metric transition. `test/postgres-advisory-lock-collisions.test.ts`
uses dependency-ID and queue-name pairs whose legacy hashes collide on every
supported PostgreSQL major; same logical identities still coordinate while the
distinct colliding identities remain independent.

Six dedicated fast-check files add 24 property campaigns over arbitrary JSON,
batch admission, custom-ID and deduplication races, ordering, dependency fan-in,
conflict fallback, competing claims, global concurrency/rate/group ownership,
generated lifecycle histories, retries/DLQ, lease fencing, TTL, progress-message
preservation, completion-proof retention, reverse-order generation reuse, logs,
destructive-adapter safety, and live or missed-LISTEN convergence when
admission/claim/completion batches, sequential commits, or generated
physical-ID/commit orders cross the event window. Failures print a shrinkable seed;
`BUNQUEUE_POSTGRES_FC_SEED` replays it and `BUNQUEUE_POSTGRES_FC_RUNS` deepens a
campaign. `postgres-storage-adapter-pattern.test.ts` tests Strategy resolution,
duplicate rejection, delegated lifecycle, concurrent shutdown coalescing, and
retry after an adapter failure with in-memory fakes.

Deterministic shutdown regressions pause PostgreSQL immediately after an
admission, claim, relationship mutation, or startup hydration commits and prove
that shutdown waits through the corresponding refresh. They also prove that an
empty long-poll does not own lifecycle admission, late synchronous writes fail
before reaching a closed pool, and disconnect cleanup remains harmless. Separate
multi-broker relationship regressions cover direct pending-child removal,
terminal/active retention, idempotent replay, exact events, and fixed-point
protection of shared dependency graphs.

`test/postgres-four-processes.test.ts` launches four independent bunqueue OS
processes with separate TCP/HTTP ports and SQL pools against one PostgreSQL 18.6
database and namespace. It proves exact delivery for 1,000 jobs produced and
consumed through all four endpoints, cross-broker ACK, global concurrency/rate
limits, pause/resume visibility, and authoritative counts on every broker. It
then sends `SIGKILL` to a broker holding 24 leases: the three survivors must
recover and complete all 120 jobs exactly once, while PostgreSQL fencing rejects
the crashed generation's stale batch ACK. The dedicated GitHub Actions matrix
runs the complete `test/postgres-*.test.ts` set against
`postgres:18.6-alpine`, `postgres:17-alpine`, `postgres:16-alpine`, and
`postgres:15-alpine`.

A separate manual Kubernetes campaign on 2026-08-27 used a fresh kind v0.33.0
cluster with Kubernetes 1.33.1, four broker Pods built from the sanitized Git
snapshot, and one disposable `postgres:18.6-alpine` StatefulSet. The functional
phase admitted, claimed, and cross-broker-acknowledged 1,000 jobs exactly once,
then executed a four-job public `FlowProducer` graph with leaf Workers connected
to different Pods. The failure phase held 24 leases on one Pod, deleted it with
zero grace, and completed all 120 jobs through the three survivors after lease
recovery. PostgreSQL rejected every stale token from the deleted session, and
the Deployment restored four Ready brokers with distinct current sessions.
Final database evidence was 1,124 completed jobs, no other job states, zero
deadlocks, and zero temporary bytes. This validates Kubernetes orchestration and
the shared-database failure path; it is not a benchmark or a database-HA test.
The public deployment guide records the tested `initContainer`, Downward API
broker identity, probes, shutdown budget, and coordinated upgrade constraints.

`test/postgres-ten-processes.test.ts` is the opt-in scale and failure campaign.
It launches ten independent broker OS processes with private TCP/HTTP ports and
four SQL pool slots each, then connects 40 TCP consumers to one PostgreSQL
database. The steady phase streams 20,000 jobs with 512-byte payloads from ten
producers while every consumer drains concurrently. The failure phase holds 200
leases across two brokers, pauses the queue globally, sends `SIGKILL` to both
owners, waits for the eight survivors to recover every lease, proves both stale
token sets are fenced, resumes through another broker, and completes all 5,000
jobs. The explicit `BUNQUEUE_POSTGRES_TEN_BROKER_SOAK=1` gate keeps this
minute-long production-timing campaign out of the normal version-matrix job;
the package command above sets the gate.

### Native diagnostic benchmark

The 2026-08-25 native macOS engineering campaign used PostgreSQL 18.6 with
`fsync=on`, `synchronous_commit=on`, and `full_page_writes=on`. Each sample used
a fresh database/file, broker process, ports, namespace, and queue. The deep
matrix ran 180 measured samples across 56 scenarios: 500-25,000 job scale,
1-128 worker concurrency, push batches of 1-500, payloads of 0-16 KiB, one/two/
four PostgreSQL brokers, plain/custom-ID/deduplicated/prioritized admission, and
streaming latency distributions. Streaming scenarios used five repetitions;
the rest used three, plus per-topology warm-ups.

The optimized and baseline campaigns each submitted 696,000 jobs. Both had
exact accepted/invoked ID conservation, zero duplicate invocations, and zero
deadlocks. PostgreSQL scenario throughput improved by a 3.01x geometric mean;
the SQLite control geometric mean was 1.00x. PostgreSQL temporary spill fell
from 6,176 files/54.97 GiB to zero. Selected post-change medians:

| Scenario                                  | Baseline | Optimized | Change |
| ----------------------------------------- | -------: | --------: | -----: |
| PG1 lifecycle, 10,000 jobs                |      577 |     1,542 |  2.67x |
| PG2 lifecycle, 10,000 jobs                |      618 |     1,867 |  3.02x |
| PG1 lifecycle, 25,000 jobs                |      309 |     1,108 |  3.59x |
| PG1 enqueue, batch size 1                 |       24 |       881 | 37.49x |
| PG1 custom-ID enqueue, batch size 100     |       31 |     8,414 |   268x |
| PG1 deduplication enqueue, batch size 100 |       31 |     8,244 |   267x |
| PG2 streaming lifecycle, batch size 100   |    1,110 |     2,672 |  2.41x |

Rates are jobs/s. The optimized scale matrix measured PostgreSQL two-broker
medians of 2,281 jobs/s at 2,000 jobs and 1,867 jobs/s at 10,000 jobs. At fixed
64 total worker slots and 10,000 jobs, one/two/four brokers measured
1,524/1,858/1,136 jobs/s: two brokers were optimal on this host, while four
added database contention. A native `pgbench` 18.6 prepared TPC-B-like baseline
with eight clients and four threads measured 8,243 TPS, 0.971 ms average latency,
and zero failed transactions.

The host was not globally quiesced (observed one-minute load 3.51-12.01), so
these are local diagnostics rather than publishable cross-project claims.

An additional `pg_stat_statements` profile used two managers, 16 concurrent
claim loops, 100-job batches, and 20,000 jobs. After the claim-lock and index
work, admission measured 10,205 jobs/s, processing 9,278 jobs/s, and the complete
lifecycle 4,860 jobs/s before the final per-ID snapshot guard. The final profile
measured 11,749 admission, 9,782 processing, and 5,338 lifecycle jobs/s with
exact delivery, zero deadlocks, and zero temporary files. Relative to the same
profile before claim optimization, processing rose from 3,200 jobs/s (3.06x)
and lifecycle throughput rose from 2,378 jobs/s (2.24x). Queue-state lock SQL
fell from 88,426 ms of aggregate execution/wait time to 3.5 ms; affected-ID
reads fell from roughly 380 to exactly 200 for 200 push batches; and narrow
indexed candidate selection eliminated the 2.12 GiB temporary spill observed
during the intermediate wide-row plan.

A separate safe-settings matrix ran 21 fresh native PostgreSQL 18.6 instances
(three repetitions across seven configurations, 420,000 jobs total) without
disabling `fsync`, `synchronous_commit`, or `full_page_writes`. Median lifecycle
throughput ranged from 4,744 jobs/s at defaults to 5,041 jobs/s with
`shared_buffers=512MB` (+6.3% on this host). Eight AIO workers measured only
+0.9%; `io_method=sync`, `jit=off`, and `wal_compression=lz4` remained within a
4% band. Those small differences are host/workload-specific and do not justify
hard-coded server settings. PostgreSQL should be tuned from production working
set and I/O evidence; the SQL/locking improvements are the material result.

The final end-to-end subset repeated 60 measured samples across 18 scale,
concurrency, broker, streaming, payload, feature, and SQLite-control scenarios
(423,000 jobs per compared campaign). Against the already optimized first pass,
PostgreSQL gained another 1.16x geometric-mean throughput. PG1/PG2 at 25,000
jobs measured 1,569/2,078 jobs/s (1.42x/1.97x), four brokers at fixed 64 total
slots measured 2,428 jobs/s (2.14x), and PG2 streaming measured 2,761 jobs/s.
All samples preserved exact delivery with zero duplicates and zero deadlocks.
The two SQLite controls measured 0.96x geometric mean on the non-quiesced host;
no SQLite source or execution path changed. A final five-sample focused rerun
measured 8,635 custom-ID and 8,212 deduplicated admissions/s, removing the
redundant-refresh regression visible before the per-ID guard.

After adding durable prune watermarks, a fresh native five-sample run at 10,000
jobs per sample rechecked the complete lifecycle with one warm-up and a new
database, broker set, namespace, ports, and queue for every sample. Median
enqueue/process/lifecycle rates were 11,548/2,396/1,986 jobs/s for one PostgreSQL
broker and 11,114/3,505/2,638 jobs/s for two. Lifecycle coefficients of variation
were 0.5% and 0.6%; every sample accepted, invoked, uniquely invoked, and
completed all 10,000 IDs with zero duplicates. The unchanged SQLite control
measured 4,439/926/767 jobs/s with 1.5% lifecycle variation. This is native local
diagnostic evidence, not a cross-project benchmark.

The final commit-envelope implementation repeated that exact one-warm-up,
five-sample protocol after fixing physical-ID/commit-order inversions. Median
enqueue/process/lifecycle rates were 10,841/2,260/1,868 jobs/s for one PostgreSQL
broker and 10,503/3,233/2,474 jobs/s for two; lifecycle variation was 0.4%/1.4%.
Compared with the first correct hot-row sequencer, lifecycle throughput improved
16.8%/26.0%. The remaining roughly 6% versus the earlier unsafe-ID journal is the
measured cost of commit-order correctness. SQLite remained on its existing path
at 4,431/921/763 jobs/s. Every sample still completed 10,000 unique IDs with zero
duplicates and balanced two-broker work (approximately 50/50).

The compatibility-final candidate repeated the protocol after bounded queue
refresh retry and per-queue health reporting were complete. Median
enqueue/process/lifecycle rates were 9,006/1,876/1,552 jobs/s for one PostgreSQL
broker and 8,914/2,953/2,218 jobs/s for two; lifecycle variation was 1.5% and
3.9%. SQLite measured 4,154/866/717 jobs/s with 1.7% variation on its unchanged
path. Every sample again accepted, invoked, uniquely invoked, and completed all
10,000 IDs with zero duplicates. Two-broker samples split work 4,992/5,008 or
5,008/4,992. The host remained non-quiesced, so these are local diagnostics.

After canonical admission locking was complete, a focused native regression
campaign ran two stores submitting the same 10,000 custom-ID jobs in opposite
order, in 20 concurrent pairs of 500-job batches. One warm-up and five measured
samples each used a fresh PostgreSQL 18.6 cluster/process, Bun process, port,
namespace, and queue set. Median time was 2,271.9 ms: 4,402 unique durable jobs/s
or 8,803 attempted admissions/s, with a 4,257-4,443 unique-jobs/s range and 1.7%
variation. Every sample persisted exactly 10,000 rows with zero errors and zero
deadlocks. Durability remained fully enabled; the non-quiesced host makes this
local engineering evidence rather than a publishable cross-project claim.

The post-review candidate repeated the native lifecycle protocol after the
authoritative dependency and server-surface fixes. SQLite/PG1/PG2 median
lifecycle rates were 773/1,840/2,413 jobs/s with 0.4%/0.4%/0.9% variation;
PostgreSQL admission medians were 10,330 and 9,865 jobs/s. All 15 measured
10,000-job samples preserved exact accepted/invoked/unique ID sets, zero
duplicates, exact terminal counts, and balanced two-broker work. Every sample
again used a fresh native PostgreSQL 18.6 cluster, broker set, ports, namespace,
and queue with durability enabled. These are local non-quiesced diagnostics.

The final dependency-safe deletion and generation-retention candidate repeated
that full protocol. SQLite/PG1/PG2 median lifecycle rates were
767/1,785/2,362 jobs/s with 0.7%/0.5%/1.3% variation; PostgreSQL admission
medians were 9,735 and 9,507 jobs/s. All 15 measured samples again completed the
exact 10,000 accepted IDs once, with zero duplicates, exact terminal counts, and
work on both PostgreSQL brokers. Every measured sample used fresh native storage,
processes, ports, namespaces, and queues with PostgreSQL durability enabled; the
non-quiesced host still makes these local engineering diagnostics only.

After the final generation-lifecycle review fixes, the same protocol measured
SQLite/PG1/PG2 median lifecycle rates of 759/1,792/2,335 jobs/s with
0.5%/0.6%/1.0% variation. PostgreSQL admission medians were 9,922 and 9,465
jobs/s. All 15 measured samples preserved the exact 10,000 accepted, invoked,
unique, and terminal IDs with zero duplicates; the two PostgreSQL brokers split
work 5,024/4,976 or 5,040/4,960. Every sample again used fresh native storage,
processes, ports, namespaces, and queues with durability enabled. These remain
local, non-quiesced engineering diagnostics rather than publishable claims.

The final queue-lifecycle, durable Cloud read-model, and post-commit maintenance
candidate repeated the campaign after all correctness fixes. SQLite/PG1/PG2
median lifecycle rates were 766/1,733/2,276 jobs/s with 1.1%/0.9%/1.9%
variation; PostgreSQL admission medians were 9,622 and 9,372 jobs/s. Every one
of the 15 measured 10,000-job samples retained exact accepted, invoked, unique,
and terminal ID sets with zero duplicates. Both PostgreSQL brokers participated
in every two-broker sample, with distributions between 5,152/4,848 and
5,024/4,976. Each sample again used fresh native storage, processes, ports,
namespaces, and queues with PostgreSQL 18.6 durability enabled. These results
remain local engineering diagnostics from a non-quiesced host.

The event-retention race fix repeated the same native protocol on the final
worktree. SQLite/PG1/PG2 lifecycle medians were 750/1,525/2,492 jobs/s with
0.1%/0.4%/1.8% variation; enqueue medians were 4,321/9,121/8,492 jobs/s. All
150,000 measured jobs again preserved exact ID conservation, zero duplicates,
exact terminal counts, and participation by both PostgreSQL brokers.
PostgreSQL durability remained fully enabled, and every sample used fresh
processes, databases, ports, namespaces, and queues.

The shutdown-drain and DLQ-repair candidate repeated the full native campaign
after the final lifecycle changes. SQLite/PG1/PG2 lifecycle medians were
722/1,366/2,357 jobs/s with 1.4%/1.0%/2.1% variation; enqueue medians were
4,207/7,708/7,960 jobs/s, while PostgreSQL processing medians were 1,641 and
3,376 jobs/s. All 150,000 measured jobs retained exact accepted, invoked,
unique, and terminal ID sets with zero duplicates. Both PostgreSQL brokers
participated in every two-broker sample, with fully durable PostgreSQL 18.6 and
fresh storage, processes, ports, namespaces, and queues. The non-quiesced host
makes these local engineering diagnostics rather than publishable claims.

After extending shutdown admission to every PostgreSQL manager surface and
making direct-child removal atomic, the candidate repeated that protocol once
more. SQLite/PG1/PG2 lifecycle medians were 738/1,462/2,530 jobs/s with
0.6%/2.3%/1.6% variation; enqueue medians were 4,309/8,639/8,425 jobs/s, while
PostgreSQL processing medians were 1,756 and 3,617 jobs/s. Relative to the
immediately preceding candidate, PostgreSQL lifecycle medians increased by
7.0% with one broker and 7.3% with two. All 150,000 measured jobs again retained
exact accepted, invoked, unique, and terminal ID sets with zero duplicates, and
both PostgreSQL brokers participated in every two-broker sample. Each sample
used a fresh native PostgreSQL 18.6 cluster, broker process set, ports,
namespace, and queue with full durability. These remain local non-quiesced
engineering diagnostics, not publishable cross-project claims.

### Native PostgreSQL 15–18 compatibility benchmark

The 2026-08-26 compatibility campaign ran PostgreSQL 15.19, 16.15, 17.11,
and 18.6 natively on the same host. It used fresh clusters and independent
one/two/four-process broker topologies for every sample, one discarded warm-up
and seven measured 10,000-job samples per cell. All 840,000 measured IDs were
accepted, invoked, and completed exactly once with zero duplicate invocations,
deadlocks, or PostgreSQL temporary spill.

Lifecycle medians ranged from 6,550–6,945 jobs/s with one broker,
8,004–8,494 with two, and 7,168–7,788 with four. PostgreSQL 18.6 led the
one- and four-broker medians; PostgreSQL 15.19 led the two-broker median by
0.7%, within overlapping confidence intervals. The topology effect was larger
than the version spread: two brokers improved every major by 18.2–23.9%, while
four brokers were 7.6–12.8% slower than two at a fixed 16 consumers.

See the complete
[Native PostgreSQL 15–18 Engineering Benchmark](../benchmarks/postgres-versions-2026-08-26.md)
for methodology, CV and Student-t CI95, command-tail latency, WAL per job,
broker fairness, integrity totals, limits, reproduction controls, and the raw
artifact digest.

### PostgreSQL 18 bottleneck tuning

The follow-up PostgreSQL 18 analysis retained four narrow hot-path changes.
Journal catch-up reads scale from 1,000 to a bounded maximum of 4,096 events;
authoritative projection repair issues at most 1,000 IDs per query; and exact
completion metrics use one canonical queue order and one CTE for bucket plus
lifetime-total updates. Claim no longer performs an autonomous queue-state
sentinel insert because the locked claim transaction already creates a missing
row. TTL expiry remains autonomous to preserve destruction/job versus
queue-state lock order.

At four brokers and batch 100, the code candidate did not establish a
throughput win: lifecycle medians were 6,740 and 6,776 jobs/s with overlapping
CI95 intervals. It did reduce median-of-sample `ACKB` p95 from 165.8 to
108.9 ms and transaction count by 4.7%, while `PULLB` p95 and WAL/job increased.
The strongest throughput result came from configuration: at pool 4 and 250 ms
polling, batch 250 improved the four-broker lifecycle median from 7,478 to
8,362 jobs/s (+11.8%) versus batch 100, with non-overlapping CI95 intervals and
41.7% fewer commits. Larger commands increased p95 latency, WAL/job, and
temporary spill, so batch 100 remains a latency-oriented option.

See
[PostgreSQL 18 Multi-Broker Performance Analysis](../benchmarks/postgres-performance-analysis-2026-08-26.md)
for the `pg_stat_activity` bottleneck evidence, 100,000-job diagnostics, batch
and pool sweeps, `work_mem` comparison, rejected changes, exact integrity
totals, caveats, and raw artifact hashes.

### PostgreSQL 18 ten-broker functional diagnostic

The opt-in native test extends the functional topology to ten bunqueue
processes sharing one PostgreSQL 18.6 database. Its checked workload uses four
consumer connections per broker, 20,000 steady-state jobs, 5,000 recovery jobs,
250-job batches, a four-connection broker pool, 250 ms polling, and the
production 30-second lease. It kills two owners holding 200 active leases and
asserts exact ID conservation, exclusive delivery, stale-token rejection,
survivor health, eventual database completion, one retry per recovered lease,
and zero new PostgreSQL deadlocks.

The test emits a `POSTGRES_TEN_BROKER_SOAK` JSON record containing timings,
per-broker claims, and database-stat deltas. Exploratory 2026-08-27 samples
informed the test shape, but their raw native-host records were not retained;
their numerical throughput, RTO, WAL, temporary-I/O, and statement-attribution
values are therefore deliberately excluded from published evidence. A future
performance publication must retain the raw JSON, environment manifest,
PostgreSQL settings/stat snapshots, sample order, integrity totals, and hashes
before quoting measurements. Until then this suite is functional/fault
evidence, not a capacity benchmark.

## Related docs

- [Executable PostgreSQL multi-broker example](../../examples/postgres-multibroker/README.md)
  — disposable PostgreSQL 18.6, three active brokers, four asserted public-SDK
  scenarios, bounded HTTP/polling/scenario waits, behaviorally enforced shared
  concurrency and fixed-window rate limits, priority/delay scheduling, and
  multi-phase application plus container/network/volume/image teardown. A
  forced-timeout campaign and focused fake-Docker regressions prove the failure
  path. The user-facing walkthrough and validation report start at
  `/examples/postgres-multibroker/`.
- [Persistence](./persistence.md) — unchanged SQLite engine.
- [Configuration & Entrypoint](./configuration.md) — driver resolution and
  bootstrap selection.
- [TCP Server Command Handlers](./tcp-server-handlers.md) — async durable handler
  adapters.
- [Data Model](../data-model.md) — PostgreSQL tables, indexes, and lease fields.
- [Architecture](../architecture.md) — single-broker SQLite and multi-broker
  PostgreSQL deployment topologies.
