# Job Group Scheduling Benchmark — 2026-08-31

This report compares `c39facb9` with the complete candidate worktree. It tests
both sides of the scheduler change: the pathological blocked-tenant lookup it
is intended to remove and ordinary queue paths that must not hide a regression.

## Method

- Host: Apple M1 Max, 10 cores, 32 GiB RAM; macOS 26.6.2 (25G83).
- Runtime: Bun 1.4.0, native arm64.
- Baseline: a clean `git archive` of `c39facb9` with the frozen lockfile.
- Candidate: the complete current worktree. Both revisions used the same
  current benchmark harness.
- Order: baseline A1 -> candidate B1 -> candidate B2 -> baseline A2. Every run
  used a fresh Bun process and every sample used a unique queue.
- Aggregation: the two seven-sample runs for each revision were pooled, giving
  14 samples per revision. Tables report pooled median and nearest-rank p95.
- Correctness was checked immediately after every timed sample and outside the
  measured interval. The final oracle verifies exact plain FIFO order followed
  by the first FIFO head from every group in first-seen round-robin order.
- Machine-readable `comparable` follows that exact oracle: an incorrect sample
  set remains useful diagnostic timing but is explicitly non-comparable.
- The eight raw reports are archived beside this report: queue paths
  [A1](./raw/job-groups-2026-08-31/baseline-a1.json),
  [B1](./raw/job-groups-2026-08-31/candidate-b1.json),
  [B2](./raw/job-groups-2026-08-31/candidate-b2.json), and
  [A2](./raw/job-groups-2026-08-31/baseline-a2.json); head-of-line
  [A1](./raw/job-groups-2026-08-31/baseline-hol-a1.json),
  [B1](./raw/job-groups-2026-08-31/candidate-hol-b1.json),
  [B2](./raw/job-groups-2026-08-31/candidate-hol-b2.json), and
  [A2](./raw/job-groups-2026-08-31/baseline-hol-a2.json).

The benchmark ran natively on the host. Container and VM timings are not used
as performance evidence.

## Saturated-group head of line

Each sample claims `A1`, leaves 5,000 more jobs from group A queued while that
group is at concurrency one, and times the next pull of independently eligible
`B1`. Admission and the first claim are outside the timed region.

```bash
bun bench/fix-impact.ts \
  --only=pull-group-head-of-line --profile=full \
  --source-root=/path/to/revision --label=candidate --revision=worktree \
  --output=/tmp/bunqueue-group-candidate.json
```

Times are milliseconds. `x` is baseline divided by candidate.

| Revision    | Samples |     Median |        p95 | Correct result |
| ----------- | ------: | ---------: | ---------: | -------------- |
| `c39facb9`  |      14 |   1.427042 |   2.762375 | `B1` in 14/14  |
| Candidate   |      14 |   0.019542 |   0.083417 | `B1` in 14/14  |
| Improvement |       — | **73.02x** | **33.12x** | Preserved      |

The candidate reduces median scheduler time by 98.63% and p95 by 96.98% in
this saturated-tenant workload. The old path walks and temporarily parks the
blocked group-A prefix. The candidate checks one FIFO lane head per group via
the circular rotation, so those 5,000 blocked jobs are not revisited.

## Ordinary and mixed queue paths

The companion campaign times complete in-memory batch admission and claim
paths:

- **Ungrouped:** admit and then claim 20,000 ordinary jobs.
- **Mixed:** admit 20,000 jobs (10,000 plain and 10,000 spread over 100 groups),
  then claim all 10,000 plain jobs plus one serial turn from each group.

```bash
bun bench/fix-impact.ts \
  --only=queue-paths --profile=full \
  --source-root=/path/to/revision --label=candidate --revision=worktree \
  --output=/tmp/bunqueue-paths-candidate.json
```

Positive deltas mean the candidate took longer; negative deltas mean it was
faster.

| Workload / operation | Baseline median | Candidate median | Median delta | Baseline p95 | Candidate p95 |   p95 delta | Exact contract, baseline → candidate |
| -------------------- | --------------: | ---------------: | -----------: | -----------: | ------------: | ----------: | -----------------------------------: |
| Ungrouped push batch |       39.251500 |        38.027417 |       -3.12% |   100.254083 |     54.730250 |     -45.41% |                        14/14 → 14/14 |
| Ungrouped pull batch |       34.802646 |        35.107834 |       +0.88% |    58.418833 |     43.067792 |     -26.28% |                        14/14 → 14/14 |
| Mixed push batch     |       35.532167 |        50.875833 |  **+43.18%** |    42.975625 |     65.272375 | **+51.88%** |                         0/14 → 14/14 |
| Mixed pull batch     |       22.150167 |        23.400084 |       +5.64% |    25.044958 |     25.208291 |      +0.65% |                         0/14 → 14/14 |

The ungrouped path remains close to neutral in its medians: push and pull
changed -3.12% and +0.88%. Its p95 readings changed -45.41% and -26.28%, but
with 14 samples nearest-rank p95 is the maximum observation; those tail values
are reported rather than promoted as a general speedup. The mixed admission
result is a real disclosed cost: once the first grouped job appears, the
candidate builds and maintains the secondary plain, wake-up, and FIFO-lane
indexes.

The strengthened oracle also found a behavioral difference hidden by the
earlier count-only check. `c39facb9` returned the requested mixed cardinality
but satisfied the new exact plain-first/per-group-head sequence in 0/14 samples;
the candidate satisfied it in 14/14. The raw mixed results therefore record
`comparable: false` for the baseline and `true` for the candidate. Their timing
deltas are useful directional cost evidence, not a same-semantics speed
comparison. The candidate adds exact BullMQ Pro-compatible order as well as the
secondary-index work.

The demonstrated win is therefore specific and substantial rather than
universal: it removes latency that grows with a blocked tenant's queued prefix,
while buying that bound with extra grouped-admission work. Applications with
many grouped writes but no blocked or rate-limited tenants should account for
that trade-off.

## Correctness and regression evidence

- All 28 saturated-group pulls returned the independently eligible `B1` job.
- Both revisions preserved exact ungrouped FIFO in 14/14 samples. The candidate
  preserved the exact mixed contract in 14/14 samples; the baseline did so in
  0/14, despite returning the expected count and distinct groups.
- `test/fix-impact-benchmark.test.ts` injects a deliberately misordered mixed
  result with valid counts and group cardinality and proves the oracle rejects it.
- `test/repro-group-head-of-line.test.ts` preserves the deterministic blocked-
  prefix reproduction.
- `test/repro-job-groups-skeptic.test.ts` covers durable FIFO order, exact TTL
  boundaries, lazy scheduler activation, long-poll wake-up, cleanup, and input
  validation.
- `test/groups-bullmq-pro-e2e.test.ts` and the shared
  `scripts/shared/job-groups-contract.ts` exercise the public Queue/Worker
  contract through real TCP and embedded brokers.
- `test/postgres-group-order-retention.test.ts` covers cross-broker/restart FIFO,
  batch-boundary order, and safe group-state retention in PostgreSQL.

These are revision-specific native engineering measurements, not a claim about
absolute end-to-end application latency. Serialization, network transport,
persistence, and user processing are outside these timed regions.
