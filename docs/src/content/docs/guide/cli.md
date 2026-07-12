---
title: "CLI Reference: Manage Bun Job Queues from the Command Line"
description: "Complete bunqueue CLI reference: push, pull, and ack jobs, manage DLQ, schedule cron tasks, and monitor server stats from your terminal."
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/guide/cli.png
---

<div class="bq-wrap bq-hero">
  <span class="bq-eyebrow">server · cli</span>
  <h1 class="bq-hero-h1 bq-bench-h1">The queue, from the <em>CLI.</em></h1>
  <p class="bq-hero-sub">The bunqueue CLI works in two modes: server mode starts the bunqueue server, client mode sends commands to a running one. Push, pull, ack, DLQ, cron, backups, and monitoring, all scriptable with JSON output.</p>
</div>

## Getting Started

### Start the Server

```bash
# Start with defaults (TCP: 6789, HTTP: 6790)
bunqueue start

# Custom ports
bunqueue start --tcp-port 7000 --http-port 7001

# Bind to specific host
bunqueue start --host 127.0.0.1 -p 6789

# With persistent storage
bunqueue start --data-path ./data/production.db

# With authentication
AUTH_TOKENS=secret-token bunqueue start

# With a config file
bunqueue start --config ./bunqueue.config.ts
```

:::tip[Configuration File]
Instead of CLI flags and env vars, you can centralize all settings in a typed `bunqueue.config.ts`. See [Configuration File](/guide/configuration/).
:::

**Output:**
```
        (\(\
        ( -.-)      bunqueue v2.8.30
        o_(")(")    High-performance job queue for Bun

  ● TCP    0.0.0.0:6789
  ● HTTP   0.0.0.0:6790
  ● Socket disabled
  ● Data   ./data/production.db
  ● TLS    disabled
  ● Auth   disabled
  ● S3 Backup disabled
  ● Cloud  disabled
  ● Shards 16 (10 CPU cores)
```

### Connect to Server

All client commands connect to a running server:

```bash
# Default connection (localhost:6789)
bunqueue stats

# Connect to remote server
bunqueue stats --host 192.168.1.100 --port 6789

# With authentication
bunqueue stats --token secret-token
```

---

## Core Operations

### Push Jobs

Add jobs to a queue for processing.

```bash
# Basic push
bunqueue push emails '{"to":"user@example.com","subject":"Welcome"}'
```

**Output:**
```
Job created: 019ce9d7-6983-7000-946f-48737be2b0f9
```

Job IDs are UUID v7 strings (time-ordered).

```bash
# With priority (higher = processed first)
bunqueue push emails '{"to":"vip@example.com"}' --priority 10

# Delayed job (process after 5 seconds)
bunqueue push notifications '{"message":"Reminder"}' --delay 5000
```

```bash
# With custom job ID
bunqueue push orders '{"orderId":"ORD-123"}' --job-id order-ORD-123
```

```bash
# With retry configuration
bunqueue push emails '{"to":"user@example.com"}' --max-attempts 5 --backoff 2000
```

```bash
# With TTL and timeout
bunqueue push reports '{"type":"monthly"}' --ttl 3600000 --timeout 30000
```

```bash
# With unique key for deduplication
bunqueue push notifications '{"userId":"123"}' --unique-key user-123-notify
# or short form
bunqueue push notifications '{"userId":"123"}' -u user-123-notify
```

```bash
# With dependencies (wait for other jobs)
bunqueue push aggregate '{"type":"sum"}' --depends-on job-1,job-2,job-3
```

```bash
# With tags for organization
bunqueue push emails '{"to":"user@example.com"}' --tags marketing,campaign-q1
```

```bash
# With group ID for correlation
bunqueue push tasks '{"action":"sync"}' --group-id batch-2024-01
# or short form
bunqueue push tasks '{"action":"sync"}' -g batch-2024-01
```

```bash
# LIFO ordering (last in, first out)
bunqueue push urgent '{"data":"latest"}' --lifo
```

```bash
# Auto-remove after completion or failure
bunqueue push temp-tasks '{"data":"test"}' --remove-on-complete --remove-on-fail
```

#### Push Options Reference

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--priority` | `-P` | number | `0` | Higher = processed first |
| `--delay` | `-d` | number | `0` | Delay in ms before processing |
| `--job-id` | - | string | - | Custom ID for deduplication |
| `--max-attempts` | - | number | `3` | Max retry attempts |
| `--backoff` | - | number | `1000` | Backoff between retries (ms) |
| `--ttl` | - | number | - | Time-to-live in ms |
| `--timeout` | - | number | - | Processing timeout in ms |
| `--unique-key` | `-u` | string | - | Deduplication key |
| `--depends-on` | - | string | - | Comma-separated job IDs |
| `--tags` | - | string | - | Comma-separated tags |
| `--group-id` | `-g` | string | - | Group identifier |
| `--lifo` | - | boolean | `false` | LIFO ordering |
| `--remove-on-complete` | - | boolean | `false` | Auto-delete on completion |
| `--remove-on-fail` | - | boolean | `false` | Auto-delete on failure |

### Pull Jobs

Retrieve jobs for processing (typically used by workers).

```bash
# Pull next job
bunqueue pull emails
```

**Output:**
```
Job: 019ce9d7-6983-7000-946f-48737be2b0f9
  Queue:      emails
  State:      active
  Priority:   0
  Attempts:   0/3
  Data:       {"to":"user@example.com","subject":"Welcome"}
  Created:    2024-01-15T10:30:00.000Z
  Started:    2024-01-15T10:30:01.000Z
```

```bash
# Pull with timeout (wait up to 5s for job)
bunqueue pull emails --timeout 5000
```

```bash
# Pull on an empty queue
bunqueue pull empty-queue
```

**Output:**
```
No job available
```

### Acknowledge Jobs

Mark jobs as completed after successful processing.

```bash
# Simple acknowledgment
bunqueue ack 019ce9d7-6983-7000-946f-48737be2b0f9
```

**Output:**
```
OK
```

```bash
# With result data (retrievable later via `bunqueue job result <id>`)
bunqueue ack 019ce9d7-6983-7000-946f-48737be2b0f9 --result '{"messageId":"msg-abc123","delivered":true}'
```

### Fail Jobs

Mark jobs as failed (will retry if attempts remaining).

```bash
# Mark as failed (retried with backoff while attempts remain,
# moved to the DLQ once max attempts are exhausted)
bunqueue fail 019ce9d7-6983-7000-946f-48737be2b0f9 --error "SMTP connection timeout"
```

**Output:**
```
OK
```

---

## Job Management

### Get Job Information

```bash
# Full job details
bunqueue job get 019ce9d7-6983-7000-946f-48737be2b0f9
```

**Output:**
```
Job: 019ce9d7-6983-7000-946f-48737be2b0f9
  Queue:      emails
  State:      completed
  Priority:   0
  Attempts:   1/3
  Data:       {"to":"user@example.com"}
  Progress:   100%
  Created:    2024-01-15T10:30:00.000Z
  Started:    2024-01-15T10:30:01.000Z
```

Use `--json` for the raw job object (`createdAt`, `startedAt`, `completedAt`, `finishedOn`, `processedOn`, and all options).

```bash
# Just the state
bunqueue job state 019ce9d7-6983-7000-946f-48737be2b0f9
```

**Output:**
```
State: completed
```

```bash
# Get the result
bunqueue job result 019ce9d7-6983-7000-946f-48737be2b0f9
```

**Output:**
```
Result: {
  "sent": true,
  "messageId": "msg-abc123"
}
```

### Control Jobs

```bash
# Cancel a waiting/delayed job
bunqueue job cancel 019ce9d7-6983-7000-946f-48737be2b0f9

# Promote delayed job to waiting (process immediately)
bunqueue job promote 019ce9d7-6983-7000-946f-48737be2b0f9

# Discard a job to the DLQ
bunqueue job discard 019ce9d7-6983-7000-946f-48737be2b0f9
```

Each prints `OK` on success, or `Error: Job not found ...` (exit code 1) on failure.

### Update Job Properties

```bash
# Update progress (0-100) on an active job
bunqueue job progress 019ce9d7-... 50 --message "Processing attachments"

# Update job data
bunqueue job update 019ce9d7-... '{"to":"new@example.com"}'

# Change priority
bunqueue job priority 019ce9d7-... 20

# Move an active job back to delayed
bunqueue job delay 019ce9d7-... 60000

# Wait for a job to complete (exit 1 if not completed within the timeout)
bunqueue job wait 019ce9d7-... --timeout 30000
```

Each prints `OK` on success. `job wait` prints the stored result when the job completes.

### Job Logs

```bash
# View job logs
bunqueue job logs 019ce9d7-6983-7000-946f-48737be2b0f9
```

**Output:**
```
  [2024-01-15T10:30:00.000Z] INFO: Starting email processing
  [2024-01-15T10:30:01.000Z] INFO: Template loaded: welcome
  [2024-01-15T10:30:02.000Z] INFO: Email sent successfully
```

```bash
# Add log entry (levels: info, warn, error)
bunqueue job log 019ce9d7-6983-7000-946f-48737be2b0f9 "Custom checkpoint reached" --level info
```

**Output:**
```
OK
```

---

## Queue Control

### List Queues

```bash
bunqueue queue list
```

**Output:**
```
  - emails
  - notifications
  - reports
```

For per-queue counts use `bunqueue queue count <queue>` (total jobs) or the HTTP API (`GET /queues/summary`).

### Pause and Resume

```bash
# Pause processing (workers won't pick new jobs; active jobs complete)
bunqueue queue pause emails

# Resume processing
bunqueue queue resume emails

# Check pause state
bunqueue queue paused emails
```

**Output:**
```
OK
OK
Queue is active
```

`queue paused` prints `Queue is paused` or `Queue is active`.

### View Queue Jobs

```bash
# List waiting jobs (--offset for pagination)
bunqueue queue jobs emails --state waiting --limit 10
```

**Output:**
```
ID                   Queue           State        Priority   Attempts
-------------------------------------------------------------------------
019ce9d7-6983-7000.. emails          waiting      10         0/3
019ce9d7-6a01-7000.. emails          waiting      5          0/3
019ce9d7-6a7f-7000.. emails          waiting      0          0/3
```

```bash
# List failed jobs (valid states: waiting, delayed, active, completed, failed)
bunqueue queue jobs emails --state failed
```

### Clean Old Jobs

```bash
# Remove completed jobs older than 1 hour
bunqueue queue clean emails --grace 3600000 --state completed
```

**Output:**
```
Cleaned 1523 jobs: 019ce9d7-..., 019ce9d8-..., ...
```

```bash
# Clean old waiting/delayed jobs (default when --state is omitted)
bunqueue queue clean emails --grace 86400000

# Optional cap per call (default: 1000)
bunqueue queue clean emails --grace 86400000 --limit 500
```

### Drain and Obliterate

```bash
# Remove all waiting jobs (keep active)
bunqueue queue drain emails
```

**Output:**
```
Count: 125
```

```bash
# Remove everything (dangerous!)
bunqueue queue obliterate emails
```

**Output:**
```
OK
```

---

## DLQ Operations

### View Dead Letter Queue

```bash
bunqueue dlq list emails            # optionally: --count 10
```

**Output:**
```
  019ce9d7-6983-7000-946f-48737be2b0f9
    Queue: emails
    Error: SMTP timeout
    Failed: 2024-01-15T10:30:00.000Z

  019ce9d7-7a01-7000-946f-48737be2b0f9
    Queue: emails
    Error: Invalid recipient
    Failed: 2024-01-15T10:31:00.000Z
```

### Retry DLQ Jobs

```bash
# Retry all DLQ jobs
bunqueue dlq retry emails
```

**Output:**
```
Count: 3
```

The count is the number of jobs moved back to the queue.

```bash
# Retry specific job
bunqueue dlq retry emails --id 019ce9d7-6983-7000-946f-48737be2b0f9
```

**Output:**
```
Count: 1
```

### Purge DLQ

```bash
bunqueue dlq purge emails
```

**Output:**
```
Count: 12
```

The count is the number of DLQ entries removed.

---

## Cron Jobs

### List Scheduled Jobs

```bash
bunqueue cron list
```

**Output:**
```
  daily-report
    Queue: reports
    Schedule: 0 6 * * *
    Executions: 45
    Next run: 2024-01-16T06:00:00.000Z

  health-check
    Queue: health
    Schedule: every 300000ms
    Executions: 8640
    Next run: 2024-01-15T10:35:00.000Z
```

### Add Cron Job

```bash
# Using cron expression (daily at 6 AM)
bunqueue cron add daily-report \
  -q reports \
  -d '{"type":"daily","format":"pdf"}' \
  -s "0 6 * * *"
```

**Output:**
```
Cron scheduled: daily-report (next run: 2024-01-16T06:00:00.000Z)
```

```bash
# Using interval (every 30 minutes)
bunqueue cron add health-check \
  -q health \
  -d '{"check":"all"}' \
  -e 1800000
```

**Output:**
```
Cron scheduled: health-check (next run: 2024-01-15T11:00:00.000Z)
```

### Delete Cron Job

```bash
bunqueue cron delete daily-report
```

**Output:**
```
OK
```

---

## Rate Limiting

### Set Rate Limit

```bash
# Limit to 100 jobs per second
bunqueue rate-limit set emails 100
```

### Set Concurrency Limit

```bash
# Max 10 concurrent jobs
bunqueue concurrency set emails 10
```

### Clear Limits

```bash
bunqueue rate-limit clear emails
bunqueue concurrency clear emails
```

All four commands print `OK` on success.

---

## Monitoring

### Ping

```bash
bunqueue ping
```

Sends the `Ping` wire command and prints the round trip result, the quickest liveness check over TCP (it is not listed in `--help` today, but it works).

### Server Stats

```bash
bunqueue stats
```

**Output:**
```
Server Statistics:

  Waiting:     234
  Active:      12
  Delayed:     8
  Completed:   155800
  Failed:      188
  DLQ:         3

  Uptime:      185400s
  Push/sec:    120
  Pull/sec:    118
```

### Prometheus Metrics

```bash
bunqueue metrics
```

Prints the same Prometheus text exposition the server serves on `GET /prometheus` (there is no `--format` flag):

**Output:**
```
# HELP bunqueue_jobs_waiting Number of jobs waiting in queue
# TYPE bunqueue_jobs_waiting gauge
bunqueue_jobs_waiting 234

# HELP bunqueue_jobs_pushed_total Total jobs pushed
# TYPE bunqueue_jobs_pushed_total counter
bunqueue_jobs_pushed_total 156234

# HELP bunqueue_queue_jobs_waiting Number of waiting jobs per queue
# TYPE bunqueue_queue_jobs_waiting gauge
bunqueue_queue_jobs_waiting{queue="emails"} 125
...
```

### Health Check

```bash
bunqueue health
```

`health` is an alias of `stats` over TCP: it prints the same `Server Statistics:` block. For a JSON health payload (status, version, memory, connections), query the HTTP endpoint instead:

```bash
curl http://localhost:6790/health
```

### Version

Show client and server version, with mismatch detection:

```bash
bunqueue version
```

**Output:**
```
Client: bunqueue v2.8.30
Server: bunqueue v2.8.30
```

If versions differ:
```
Client: bunqueue v2.8.30
Server: bunqueue v2.8.30

⚠ Version mismatch! Update server or client to match.
```

### Doctor

Run diagnostics to check connectivity, version, health, and queue state:

```bash
bunqueue doctor
```

**Output:**
```
bunqueue doctor

Client
  ✓ Version: 2.8.30

Server
  ✓ Reachable at localhost:6790
  ✓ Version: 2.8.30
  ✓ Status: healthy
  ✓ Uptime: 2h 15m 30s
  ✓ Connections: TCP=3, WS=0, SSE=0

Queues
  ✓ Waiting: 12
  ✓ Active: 3
  ✓ Delayed: 1
  ✓ Completed: 1542
  ✓ DLQ: 0

Memory
  ✓ Heap: 45MB
  ✓ RSS: 128MB

All checks passed.
```

Use `--port` to check a remote server:

```bash
bunqueue doctor --port 6789 --host 192.168.1.100
```

---

## Workers & Webhooks

### Workers

```bash
# List registered workers (with status: active/stale)
bunqueue worker list

# Register a worker (name + comma-separated queues)
bunqueue worker register email-worker -q emails,notifications

# Unregister a worker by ID
bunqueue worker unregister w-abc123
```

:::caution
CLI worker registrations are transient: the server unregisters a worker when its TCP connection closes, and the one-shot CLI process exits immediately. The CLI prints a warning about this. For persistent workers, run a long-lived process with the SDK `Worker` class.
:::

### Webhooks

```bash
# List webhooks
bunqueue webhook list

# Add a webhook (--events/-e is required, comma-separated;
# optional: --queue/-q filter, --secret/-s HMAC secret)
bunqueue webhook add https://example.com/hooks -e completed,failed -q emails

# Remove a webhook by ID
bunqueue webhook remove wh-abc123
```

`webhook add` prints `Webhook added: <id>`; keep the ID for `webhook remove`.

---

## Backup Operations

### Create Backup

Backup commands run locally against the database at `BUNQUEUE_DATA_PATH` (they do not go through the TCP server) and require the `S3_*` environment variables to be configured.

```bash
bunqueue backup now
```

**Output:**
```
Backup created successfully
{
  "key": "backups/bunq-2024-01-15T10-30-00.db",
  "size": "45.20 MB",
  "duration": "2300ms"
}
```

### List Backups

```bash
bunqueue backup list
```

**Output:**
```
Found 3 backup(s)
[
  { "key": "backups/bunq-2024-01-15T10-30-00.db", "size": "45.20 MB", "date": "2024-01-15T10:30:00.000Z" },
  { "key": "backups/bunq-2024-01-14T10-30-00.db", "size": "44.80 MB", "date": "2024-01-14T10:30:00.000Z" }
]
```

### Restore Backup

```bash
# Restore requires --force (-f) flag to confirm
# (stop the server first: restore overwrites the current database)
bunqueue backup restore backups/bunq-2024-01-14T10-30-00.db --force
```

**Output:**
```
Restore completed successfully
{
  "key": "backups/bunq-2024-01-14T10-30-00.db",
  "size": "44.80 MB",
  "duration": "1800ms"
}
```

### Backup Status

```bash
bunqueue backup status
```

**Output:**
```
Backup configuration
{
  "enabled": true,
  "bucket": "my-backups",
  "endpoint": null,
  "interval": "360 minutes",
  "retention": "7 backups"
}
```

---

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
Two subcommands define their own short `-t` (`--timeout`): `pull` and `job wait`. After those commands, `-t` means timeout; use the long `--token` form there.
:::

### Authentication

The `--token` flag can be set via environment variables to avoid repeating it:

```bash
# Set once, use everywhere
export BQ_TOKEN=my-secret-token
bunqueue stats
bunqueue push emails '{"to":"user@example.com"}'
bunqueue queue pause emails
```

Priority: `--token` flag > `BQ_TOKEN` > `BUNQUEUE_TOKEN`.

### JSON Output

All commands support JSON output for scripting:

```bash
bunqueue stats --json | jq '.stats.waiting'
```

**Output:**
```
234
```

The `--json` flag prints the raw server response (`{ "ok": true, ... }`), so nest your `jq` path under the response field (`.stats`, `.jobs`, `.counts`, ...).

```bash
# Use in scripts
WAITING=$(bunqueue queue jobs emails --state waiting --json | jq '.jobs | length')
if [ "$WAITING" -gt 1000 ]; then
  echo "Warning: Queue backlog detected"
fi
```

---

## Common Workflows

### Process Jobs Manually

```bash
# 1. Pull a job (response shape: { "ok": true, "job": { ... } })
JOB=$(bunqueue pull emails --json)
JOB_ID=$(echo $JOB | jq -r '.job.id')

# 2. Process it (your logic here)
echo "Processing job $JOB_ID..."

# 3. Acknowledge or fail
bunqueue ack $JOB_ID --result '{"processed":true}'
```

### Monitor Queue Health

```bash
#!/bin/bash
# monitor.sh - Alert if queue backlog grows

THRESHOLD=1000
WAITING=$(bunqueue queue jobs emails --state waiting --json | jq '.jobs | length')

if [ "$WAITING" -gt "$THRESHOLD" ]; then
  echo "ALERT: emails queue has $WAITING waiting jobs"
  # Send notification...
fi
```

### Scheduled Maintenance

```bash
#!/bin/bash
# maintenance.sh - Daily cleanup

# Clean old completed jobs (older than 24h)
bunqueue queue clean emails --grace 86400000 --state completed
bunqueue queue clean notifications --grace 86400000 --state completed

# Purge old DLQ entries
bunqueue dlq purge emails

# Create backup
bunqueue backup now
```
