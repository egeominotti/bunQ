# PostgreSQL 15–18 Hot-Path Optimization Verification

Date: 2026-09-03
Runtime: Bun 1.4.0
Database: PostgreSQL 18.6, native Apple silicon
Baseline: `320639a5a1fa03aba00ffc4f2ac84cac93408175`

## Scope

This campaign changes only the PostgreSQL server engine. Memory and SQLite do
not use the new lease transaction, retention schema, or snapshot aggregation.
The shared `PUSHB` handler takes its new branch only when the manager exposes
the PostgreSQL durable dependency lookup.

The implementation follows an earlier CPU, heap, and `pg_stat_statements`
profile. Profiled timings were used for attribution only. The focused comparison
below used unprofiled native processes and one fresh PostgreSQL cluster,
namespace, queue, and port for each code candidate. Durable defaults (`fsync`, `synchronous_commit`, and
`full_page_writes` all enabled), 3,000 jobs, concurrency 10, batch size 10, a
200-byte payload, and periodic worker heartbeats disabled. Pool size 8 still
exercised the immediate post-pull ownership heartbeat.

## Results

| PostgreSQL path                            |               Baseline evidence |                   Optimized evidence |               Observed change |
| ------------------------------------------ | ------------------------------: | -----------------------------------: | ----------------------------: |
| 3,000-job drain, client pool 8             |                        3,860 ms |                             2,540 ms |       34.2% less elapsed time |
| Immediate lease-renewal writes, pool 8     |            2,993 scalar updates | 300 set-based updates for 3,000 rows | 90.0% fewer update statements |
| Under-cap retention accounting, 600 cycles |                 913.46 ms total |                       22.26 ms total |     97.6% less execution time |
| Dependency-free `PUSHB` snapshot views     | 3 complete views per validation |                     0 complete views |        Removed from this path |
| Stats + queue-summary scans, four queues   |         8 individual full scans |                   2 aggregate passes |   One pass per requested view |

These are workload-specific engineering measurements, not universal throughput
guarantees. The final retention value includes the trigger delta append,
state-row existence check, and delta consolidation. The drain result includes
both set-based lease renewal and exact-count retention, so the SQL reductions
must not be added to it.

## Implementation

### Set-based fenced heartbeats

`renewPostgresLeases()` deduplicates `(jobId, token)` pairs, locks the broker
session once, reads database time once, locks matching live lease rows in
deterministic ID order, and applies one `UPDATE ... FROM unnest(...)`. The
returned versioned rows update the accepting broker's projection directly.
Invalid or expired fences remain failures and share one authoritative repair
query. A generation ticket is reserved before the write: a later terminal
mutation, event, or projection request invalidates the ticket, so a delayed
renewal response cannot overwrite completed, removed, retried, or re-leased
state. Conflicts use the same bounded repair path while uncontended renewals
retain the zero-reload fast path. The ownership-transfer heartbeat itself remains in place because it
protects pooled workers when pull and follow-up commands use different brokers.

### Exact event-retention accounting

Schema version 21 adds `bunqueue_event_retention_state` and transaction-private
delta rows. Statement-level transition triggers append insert/delete deltas
under the current transaction ID without locking state shared by concurrent
writers. A retention pass consolidates visible deltas under the existing
per-queue lock, returns immediately while the exact count is within the
configured cap, or deletes exactly the oldest excess rows. This also prevents
inverse multi-queue transactions from creating a counter-row deadlock.

An earlier candidate used a physical event-ID high-water mark. The full
multi-broker suite disproved it: under concurrent commits, the retained window
stalled at 1,437 rows instead of 32 because physical IDs can commit out of
allocation order. Transactional insert/delete accounting replaced that design;
the same 5,000-job high-contention test then retained exactly 32 events.

### Bounded snapshot work

PostgreSQL `PUSHB` validation first determines whether external dependencies
exist. Dependency-free and entirely intra-batch graphs avoid all local
job/completion membership views; external IDs use the database-authoritative
set query. Queue-wide dashboard views aggregate all job states in one pass and
preserve the existing sorted queue order.

## Correctness boundaries

- Lease rows remain fenced by state, opaque token, unexpired database deadline,
  broker ID, and broker session ID.
- Batch renewal locks jobs in deterministic order and rolls back as a unit on an
  internal row-count mismatch.
- Returned lease and batch-completion projections apply only through a
  pre-write generation ticket; any newer local or journal transition forces an
  authoritative reload before the command returns.
- Retention still publishes prune watermarks atomically and retains the exact
  configured hard cap after concurrent writers converge.
- Schema repair treats retention counters and deltas as derived state: it locks
  event writes, reconstructs exact counts, repairs canonical immediate primary
  keys, and restores triggers in one transaction.
- Physical event ID remains a storage identity, never a commit-order proof.
- No wire command, public option, SQLite schema, or memory-engine transition is
  changed.

## Verification

Focused PostgreSQL regression coverage exercises successful and mixed-fence
heartbeat batches, exact retention down to zero, schema drift repair,
dependency-free `PUSHB`, and one-pass dashboard aggregation. The normal
PostgreSQL suite additionally covers multi-broker claims, event gaps, manual
trim, queue obliteration, recovery, shutdown, and commit-order races.

The final compatibility gate used a sanitized worktree image with Bun 1.4.0,
fresh disposable PostgreSQL and test-runner containers, an internal Docker
network, no host mounts, and the repository's complete PostgreSQL suite. The
ten-process soak remains opt-in and was the only skipped test.

| Official image         | Server version | Tests            | Deadlocks |
| ---------------------- | -------------- | ---------------- | --------: |
| `postgres:18.6-alpine` | 18.6           | 331 pass, 1 skip |         0 |
| `postgres:17-alpine`   | 17.11          | 331 pass, 1 skip |         0 |
| `postgres:16-alpine`   | 16.15          | 331 pass, 1 skip |         0 |
| `postgres:15-alpine`   | 15.19          | 331 pass, 1 skip |         0 |

PostgreSQL 18.6 also passed the same 331-test suite natively. The inverse
multi-queue retention race was repeated ten additional times (30 tests) with no
increase in the server deadlock count.

Raw CPU profiles, heap snapshots, samplers, SQL statistics, and the original
diagnostic report remain under the ignored
`artifacts/profiles/2026-09-03-postgres-2.9.4/` directory.
Complete per-version runner/server logs, checksums, and container/image
manifests are under the ignored
`artifacts/postgres-matrix/2026-09-03-final-331/` directory.
