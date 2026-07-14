---
title: "CLI: Run and Manage bunqueue from the Terminal"
description: "The bunqueue CLI starts the server and talks to a running one: push and process jobs, manage the DLQ and cron, and script everything with JSON output."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/cli.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">server · cli</span>
  <h1 class="bq-hero-h1 bq-bench-h1">The queue, from the <em>CLI.</em></h1>
  <p class="bq-hero-sub">One binary, two roles: <code>bunqueue start</code> runs the server, every other command talks to a running one. Push, pull, ack, DLQ, cron, backups, and monitoring, all scriptable with JSON output.</p>
</div>

## Start the Server

```bash
bunqueue start                                      # defaults: TCP 6789, HTTP 6790
bunqueue start --tcp-port 7000 --http-port 7001    # custom ports
bunqueue start --host 127.0.0.1 -p 6789            # bind to a specific host
bunqueue start --data-path ./data/production.db    # persistent storage
AUTH_TOKENS=secret-token bunqueue start             # with authentication
bunqueue start --config ./bunqueue.config.ts       # with a config file
```

On startup the server prints its ports, data path, and enabled features (TLS, auth, S3 backup, cloud, shard count).

:::tip[Configuration File]
Instead of CLI flags and env vars, you can centralize all settings in a typed `bunqueue.config.ts`. See [Configuration File](/guide/configuration/).
:::

## Connect to a Server

Client commands default to `localhost:6789`:

```bash
bunqueue stats                                     # local server
bunqueue stats --host 192.168.1.100 --port 6789   # remote server
bunqueue stats --token secret-token               # with authentication
```

Set the token once via environment variable instead of repeating the flag. Priority: `--token` flag > `BQ_TOKEN` > `BUNQUEUE_TOKEN`.

```bash
export BQ_TOKEN=my-secret-token
```

## Push, Pull, Ack, Fail

The core loop: add a job, take it, and report the outcome.

```bash
bunqueue push emails '{"to":"user@example.com","subject":"Welcome"}'
# Job created: 019ce9d7-6983-7000-946f-48737be2b0f9
```

Job IDs are UUID v7 strings (time-ordered). Push accepts options for priority, retries, deduplication, and more:

```bash
bunqueue push emails '{"to":"vip@example.com"}' --priority 10          # higher = sooner
bunqueue push notifications '{"msg":"hi"}' --delay 5000                # run in 5s
bunqueue push orders '{"orderId":"ORD-123"}' --job-id order-ORD-123    # idempotent ID
bunqueue push emails '{"to":"a@b.c"}' --max-attempts 5 --backoff 2000  # retry config
bunqueue push notifications '{"userId":"1"}' -u user-1-notify          # unique key (dedup)
bunqueue push aggregate '{"type":"sum"}' --depends-on job-1,job-2      # wait for other jobs
```

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--priority` | `-P` | `0` | Higher = processed first |
| `--delay` | `-d` | `0` | Delay in ms before processing |
| `--job-id` | - | - | Custom ID for deduplication |
| `--max-attempts` | - | `3` | Max retry attempts |
| `--backoff` | - | `1000` | Delay between retries (ms) |
| `--ttl` | - | - | Time-to-live in ms |
| `--timeout` | - | - | Processing timeout in ms |
| `--unique-key` | `-u` | - | Deduplication key |
| `--depends-on` | - | - | Comma-separated job IDs to wait for |
| `--tags` | - | - | Comma-separated tags |
| `--group-id` | `-g` | - | Group identifier |
| `--lifo` | - | `false` | Last in, first out ordering |
| `--remove-on-complete` | - | `false` | Auto-delete on completion |
| `--remove-on-fail` | - | `false` | Auto-delete on failure |

Pull the next job (typically a worker's job, but handy for debugging):

```bash
bunqueue pull emails                  # prints the job, or "No job available"
bunqueue pull emails --timeout 5000   # wait up to 5s for a job
```

Then acknowledge (mark done) or fail it:

```bash
bunqueue ack 019ce9d7-... --result '{"delivered":true}'   # result retrievable later
bunqueue fail 019ce9d7-... --error "SMTP connection timeout"
```

A failed job is retried with backoff while attempts remain, then moved to the [DLQ](/guide/dlq/).

## Inspect and Control Jobs

```bash
bunqueue job get <id>        # full details (use --json for the raw object)
bunqueue job state <id>      # just the state
bunqueue job result <id>     # the stored result
bunqueue job logs <id>       # log entries attached to the job

bunqueue job cancel <id>     # cancel a waiting/delayed job
bunqueue job promote <id>    # run a delayed job now
bunqueue job discard <id>    # send a job to the DLQ

bunqueue job progress <id> 50 --message "Halfway"   # update progress (active jobs)
bunqueue job update <id> '{"to":"new@example.com"}' # replace job data
bunqueue job priority <id> 20                       # change priority
bunqueue job delay <id> 60000                       # move an active job back to delayed
bunqueue job wait <id> --timeout 30000              # block until completed, print result
bunqueue job log <id> "Checkpoint reached" --level info  # append a log entry
```

Commands print `OK` on success, or `Error: Job not found ...` with exit code 1. `job wait` exits 1 if the job does not complete within the timeout.

## Queue Control

```bash
bunqueue queue list                       # list all queues
bunqueue queue count emails               # total jobs in a queue
bunqueue queue pause emails               # workers stop picking new jobs
bunqueue queue resume emails
bunqueue queue paused emails              # prints "Queue is paused" or "Queue is active"

bunqueue queue jobs emails --state waiting --limit 10   # list jobs by state
# states: waiting, delayed, active, completed, failed (--offset for pagination)

bunqueue queue clean emails --grace 3600000 --state completed  # remove old jobs
# default state when omitted: waiting/delayed; --limit caps per call (default 1000)

bunqueue queue drain emails         # remove all waiting jobs (active ones keep running)
bunqueue queue obliterate emails    # remove EVERYTHING for this queue
```

## DLQ

Inspect and recover permanently failed jobs (see [Dead Letter Queue](/guide/dlq/)):

```bash
bunqueue dlq list emails                 # entries with error and timestamp (--count 10)
bunqueue dlq retry emails                # re-queue all, prints the count moved
bunqueue dlq retry emails --id <job-id>  # re-queue one
bunqueue dlq purge emails                # delete all entries, prints the count
```

## Cron

Schedule recurring jobs (see [Cron Jobs](/guide/cron/)):

```bash
# Cron expression: daily at 6 AM (optionally --timezone/-z Europe/Rome)
bunqueue cron add daily-report -q reports -d '{"type":"daily"}' -s "0 6 * * *"
# Cron scheduled: daily-report (next run: 2024-01-16T06:00:00.000Z)

# Plain interval: every 30 minutes
bunqueue cron add health-check -q health -d '{"check":"all"}' -e 1800000

bunqueue cron list      # name, queue, schedule, executions, next run
bunqueue cron delete daily-report
```

## Rate and Concurrency Limits

```bash
bunqueue rate-limit set emails 100    # max 100 jobs/second
bunqueue concurrency set emails 10    # max 10 concurrent jobs
bunqueue rate-limit clear emails
bunqueue concurrency clear emails
```

## Monitoring

```bash
bunqueue ping      # quickest TCP liveness check (works, though not listed in --help)
bunqueue stats     # waiting/active/delayed/completed/failed/DLQ counts, uptime, rates
bunqueue metrics   # Prometheus text format, same as GET /prometheus
bunqueue health    # alias of stats over TCP
bunqueue version   # client + server version, warns on mismatch
```

For a JSON health payload (status, version, memory, connections), use the HTTP endpoint: `curl http://localhost:6790/health`.

`bunqueue doctor` runs a full diagnostic: client and server version, reachability, health status, uptime, connections, queue counts, and memory. It prints a check-by-check report and `All checks passed.` when healthy. Use `--host`/`--port` to check a remote server.

## Workers and Webhooks

```bash
bunqueue worker list                                   # registered workers with status
bunqueue worker register email-worker -q emails,notifications
bunqueue worker unregister w-abc123
```

:::caution
CLI worker registrations are transient: the server unregisters a worker when its TCP connection closes, and the one-shot CLI process exits immediately (the CLI warns about this). For persistent workers, run a long-lived process with the SDK `Worker` class.
:::

```bash
bunqueue webhook list
bunqueue webhook add https://example.com/hooks -e job.completed,job.failed -q emails
# Webhook added: <id>   (keep the ID for webhook remove)
bunqueue webhook remove <id>
```

`--events` (`-e`) is required; valid events are `job.pushed`, `job.started`, `job.completed`, `job.failed`, `job.progress`. Optional: `--queue`/`-q` filter and `--secret`/`-s` HMAC secret. See [Webhooks](/guide/webhooks/).

## Backups

Backup commands run **locally**, not through the TCP server: they read the database path from `BUNQUEUE_DATA_PATH` and credentials from the `S3_*` environment variables (see [S3 Backup](/guide/backup/)).

```bash
bunqueue backup now              # create a backup, prints key/size/duration
bunqueue backup list             # list backups in the bucket
bunqueue backup status           # show configuration
bunqueue backup restore <key> -f # restore; requires --force, stop the server first
```

## Global Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--host` | `-H` | Server hostname | `localhost` |
| `--port` | `-p` | TCP port | `6789` |
| `--token` | `-t` | Authentication token (env: `BQ_TOKEN`, `BUNQUEUE_TOKEN`) | - |
| `--tls` | - | Connect with TLS (verify with system CAs) | `false` |
| `--tls-ca <file>` | - | Trust a custom CA cert (implies `--tls`) | - |
| `--tls-no-verify` | - | TLS without cert verification (self-signed, dev only) | `false` |
| `--json` | - | Output as JSON | `false` |
| `--help` | - | Show help | - |
| `--version` | - | Show version | - |

:::note
Two subcommands define their own short `-t` (`--timeout`): `pull` and `job wait`. There, use the long `--token` form.
:::

## Scripting with JSON

Every command supports `--json`. It prints the raw server response (`{ "ok": true, ... }`), so nest your `jq` path under the response field (`.stats`, `.jobs`, `.job`, `.counts`, ...):

```bash
bunqueue stats --json | jq '.stats.waiting'
# 234
```

Process a job manually:

```bash
JOB=$(bunqueue pull emails --json)          # { "ok": true, "job": { ... } }
JOB_ID=$(echo $JOB | jq -r '.job.id')
echo "Processing job $JOB_ID..."            # your logic here
bunqueue ack $JOB_ID --result '{"processed":true}'
```

Daily maintenance script:

```bash
#!/bin/bash
bunqueue queue clean emails --grace 86400000 --state completed
bunqueue dlq purge emails
bunqueue backup now
```
