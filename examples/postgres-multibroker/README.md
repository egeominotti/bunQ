# PostgreSQL multi-broker example

This executable example runs three active bunqueue brokers against one
PostgreSQL 18.6 database, then verifies realistic SDK behavior from a separate
client container.

## What it proves

- all three brokers pass liveness and PostgreSQL-aware readiness checks;
- Prometheus metrics require the configured bearer token;
- producers, workers, and `QueueEvents` can use different brokers while sharing
  jobs, progress, logs, results, worker registrations, and queue counts;
- a paused single-slot worker proves priority ordering, delayed ineligibility,
  and explicit delayed-job promotion before final completion;
- custom job IDs remain idempotent across concurrent producers;
- pause/resume, an occupied global concurrency slot, a fixed-window rate budget,
  retries, and the DLQ are shared and enforced across brokers;
- a durable three-level `FlowProducer` graph executes children before parents,
  exactly once, across three queues and three brokers.

## Run every scenario and remove everything

From the repository root:

```bash
./examples/postgres-multibroker/verify.sh
```

The script assigns a unique Compose project name. Its bounded startup and
scenario deadlines prevent silent hangs. Its exit trap makes independent
attempts to remove containers, the internal network, PostgreSQL volume, and
locally built images on success, failure, timeout, or interruption.

Set `BUNQUEUE_EXAMPLE_SCENARIO_TIMEOUT_MS` to override the 60-second per-scenario
deadline. `BUNQUEUE_EXAMPLE_PROJECT` is accepted only with the dedicated
`bunqueue-pg-example-` prefix and lowercase Compose-safe characters; cleanup is
destructive for that explicitly selected project.

Run an individual scenario while the infrastructure is up:

```bash
docker compose -p bunqueue-example \
  -f examples/postgres-multibroker/compose.yaml \
  up -d --wait postgres broker-a broker-b broker-c

docker compose -p bunqueue-example \
  -f examples/postgres-multibroker/compose.yaml \
  --profile tools run --rm sdk-example reliability

docker compose -p bunqueue-example \
  -f examples/postgres-multibroker/compose.yaml \
  down --volumes --remove-orphans --rmi local
```

Valid scenario names are `topology`, `multi-queue`, `reliability`, `flow`, and
`all`.

## Connect from the host

The Compose file binds broker ports only to loopback:

| Broker |               TCP |              HTTP |
| ------ | ----------------: | ----------------: |
| A      | `127.0.0.1:16789` | `127.0.0.1:16790` |
| B      | `127.0.0.1:17789` | `127.0.0.1:17790` |
| C      | `127.0.0.1:18789` | `127.0.0.1:18790` |

Use `demo-token` for TCP or authenticated HTTP calls. These credentials and
ports are for this local example only.

## Scale beyond three brokers

Every active broker must use the same PostgreSQL URL and namespace but a unique,
stable `BUNQUEUE_BROKER_ID`. Compose replicas cannot safely share the static ID
from one service definition, so this example declares each broker explicitly.
Add `broker-d`, `broker-e`, and so on with unique IDs, or use an orchestrator
identity such as a Kubernetes Pod name. Budget PostgreSQL connections as
`broker count × BUNQUEUE_POSTGRES_POOL_SIZE`, plus operational headroom.

Clients connect to TCP brokers, never directly to PostgreSQL. A production
service or TCP load balancer should route only to `/ready` brokers. A long-lived
TCP connection stays on the selected broker; automatic multi-host failover is
not a client-SDK feature.
