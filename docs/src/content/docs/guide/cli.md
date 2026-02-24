---
title: CLI Commands Reference
description: Complete bunqueue CLI reference for Bun job queues. Push, pull, and ack jobs, manage DLQ, schedule cron tasks, and monitor server stats.
head:
  - tag: meta
    attrs:
      property: og:image
      content: https://bunqueue.dev/og/server-mode.png
---


bunqueue includes a powerful CLI for server management and job operations. The CLI works in two modes:

- **Server mode**: Start the bunqueue server
- **Client mode**: Send commands to a running server

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
```

**Output:**
```
bunqueue v2.1.8
TCP server listening on port 6789
HTTP server listening on port 6790
Database: ./data/bunq.db
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
Job pushed successfully
ID: 1001
Queue: emails
Priority: 0
```

```bash
# With priority (higher = processed first)
bunqueue push emails '{"to":"vip@example.com"}' --priority 10
```

**Output:**
```
Job pushed successfully
ID: 1002
Queue: emails
Priority: 10
```

```bash
# Delayed job (process after 5 seconds)
bunqueue push notifications '{"message":"Reminder"}' --delay 5000
```

**Output:**
```
Job pushed successfully
ID: 1003
Queue: notifications
State: delayed
Run at: 2024-01-15T10:30:05.000Z
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
```json
{
  "id": "1001",
  "name": "default",
  "data": {"to":"user@example.com","subject":"Welcome"},
  "priority": 0,
  "attempts": 0,
  "timestamp": 1704067200000
}
```

```bash
# Pull with timeout (wait up to 5s for job)
bunqueue pull emails --timeout 5000
```

```bash
# Pull returns null if queue is empty
bunqueue pull empty-queue
```

**Output:**
```
No jobs available
```

### Acknowledge Jobs

Mark jobs as completed after successful processing.

```bash
# Simple acknowledgment
bunqueue ack 1001
```

**Output:**
```
Job 1001 acknowledged
State: completed
```

```bash
# With result data
bunqueue ack 1001 --result '{"messageId":"msg-abc123","delivered":true}'
```

**Output:**
```
Job 1001 acknowledged
State: completed
Result: {"messageId":"msg-abc123","delivered":true}
```

### Fail Jobs

Mark jobs as failed (will retry if attempts remaining).

```bash
# Mark as failed
bunqueue fail 1001 --error "SMTP connection timeout"
```

**Output:**
```
Job 1001 marked as failed
Error: SMTP connection timeout
Attempts: 1/3
Next retry: 2024-01-15T10:31:00.000Z
```

```bash
# Job moved to DLQ after max attempts
bunqueue fail 1001 --error "Permanent failure"
```

**Output:**
```
Job 1001 marked as failed
Error: Permanent failure
Attempts: 3/3
Status: Moved to DLQ
```

---

## Job Management

### Get Job Information

```bash
# Full job details
bunqueue job get 1001
```

**Output:**
```json
{
  "id": "1001",
  "name": "send-email",
  "queue": "emails",
  "data": {"to":"user@example.com"},
  "state": "completed",
  "priority": 0,
  "attempts": 1,
  "progress": 100,
  "timestamp": 1704067200000,
  "processedOn": 1704067201000,
  "finishedOn": 1704067202000,
  "returnvalue": {"sent":true}
}
```

```bash
# Just the state
bunqueue job state 1001
```

**Output:**
```
completed
```

```bash
# Get the result
bunqueue job result 1001
```

**Output:**
```json
{"sent":true,"messageId":"msg-abc123"}
```

### Control Jobs

```bash
# Cancel a waiting/delayed job
bunqueue job cancel 1002
```

**Output:**
```
Job 1002 cancelled
Previous state: delayed
```

```bash
# Promote delayed job to waiting (process immediately)
bunqueue job promote 1003
```

**Output:**
```
Job 1003 promoted
Previous state: delayed
New state: waiting
```

```bash
# Discard a job completely
bunqueue job discard 1004
```

**Output:**
```
Job 1004 discarded
```

### Update Job Properties

```bash
# Update progress (0-100)
bunqueue job progress 1001 50 --message "Processing attachments"
```

**Output:**
```
Job 1001 progress updated
Progress: 50%
Message: Processing attachments
```

```bash
# Change priority
bunqueue job priority 1001 20
```

**Output:**
```
Job 1001 priority updated
New priority: 20
```

```bash
# Add delay to existing job
bunqueue job delay 1001 60000
```

**Output:**
```
Job 1001 delayed
Run at: 2024-01-15T10:31:00.000Z
```

### Job Logs

```bash
# View job logs
bunqueue job logs 1001
```

**Output:**
```
[2024-01-15 10:30:00] INFO  Starting email processing
[2024-01-15 10:30:01] INFO  Template loaded: welcome
[2024-01-15 10:30:02] INFO  Email sent successfully
```

```bash
# Add log entry
bunqueue job log 1001 "Custom checkpoint reached" --level info
```

**Output:**
```
Log entry added to job 1001
```

---

## Queue Control

### List Queues

```bash
bunqueue queue list
```

**Output:**
```
QUEUE          WAITING   ACTIVE   COMPLETED   FAILED   PAUSED
emails         125       5        10,234      23       no
notifications  50        2        5,102       5        no
reports        0         1        892         0        yes
```

### Pause and Resume

```bash
# Pause processing (workers won't pick new jobs)
bunqueue queue pause emails
```

**Output:**
```
Queue 'emails' paused
Waiting jobs: 125 (will not be processed)
Active jobs: 5 (will complete)
```

```bash
# Resume processing
bunqueue queue resume emails
```

**Output:**
```
Queue 'emails' resumed
Processing will continue
```

### View Queue Jobs

```bash
# List waiting jobs
bunqueue queue jobs emails --state waiting --limit 10
```

**Output:**
```
ID      NAME         PRIORITY   CREATED
1001    send-email   10         2024-01-15 10:30:00
1002    send-email   5          2024-01-15 10:30:01
1003    send-email   0          2024-01-15 10:30:02
...
Showing 10 of 125 jobs
```

```bash
# List failed jobs
bunqueue queue jobs emails --state failed
```

### Clean Old Jobs

```bash
# Remove completed jobs older than 1 hour
bunqueue queue clean emails --grace 3600000 --state completed
```

**Output:**
```
Cleaned 1,523 jobs from 'emails'
State: completed
Older than: 1 hour
```

```bash
# Clean all old jobs (completed and failed)
bunqueue queue clean emails --grace 86400000
```

### Drain and Obliterate

```bash
# Remove all waiting jobs (keep active)
bunqueue queue drain emails
```

**Output:**
```
Queue 'emails' drained
Removed: 125 waiting jobs
Active jobs: 5 (still processing)
```

```bash
# Remove everything (dangerous!)
bunqueue queue obliterate emails
```

**Output:**
```
⚠️  This will remove ALL data for queue 'emails'
Type 'emails' to confirm: emails
Queue 'emails' obliterated
Removed: 125 waiting, 5 active, 10,234 completed, 23 failed
```

---

## DLQ Operations

### View Dead Letter Queue

```bash
bunqueue dlq list emails
```

**Output:**
```
ID     JOB_ID   ERROR                        FAILED_AT            ATTEMPTS
1      1001     SMTP timeout                 2024-01-15 10:30:00  3
2      1005     Invalid recipient            2024-01-15 10:31:00  3
3      1008     Rate limit exceeded          2024-01-15 10:32:00  3

Total: 3 entries
```

### Retry DLQ Jobs

```bash
# Retry all DLQ jobs
bunqueue dlq retry emails
```

**Output:**
```
Retrying 3 jobs from DLQ
Jobs moved back to 'emails' queue
```

```bash
# Retry specific job
bunqueue dlq retry emails --id 1001
```

**Output:**
```
Job 1001 moved from DLQ to 'emails' queue
```

### Purge DLQ

```bash
bunqueue dlq purge emails
```

**Output:**
```
⚠️  This will permanently delete all DLQ entries for 'emails'
Type 'purge' to confirm: purge
Purged 3 entries from DLQ
```

---

## Cron Jobs

### List Scheduled Jobs

```bash
bunqueue cron list
```

**Output:**
```
NAME              QUEUE      SCHEDULE        NEXT RUN              EXECUTIONS
daily-report      reports    0 6 * * *       2024-01-16 06:00:00   45
hourly-cleanup    cleanup    0 * * * *       2024-01-15 11:00:00   1,082
health-check      health     */5 * * * *     2024-01-15 10:35:00   8,640
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
Cron job 'daily-report' created
Queue: reports
Schedule: 0 6 * * * (daily at 6:00 AM)
Next run: 2024-01-16 06:00:00
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
Cron job 'health-check' created
Queue: health
Interval: every 30 minutes
Next run: 2024-01-15 11:00:00
```

### Delete Cron Job

```bash
bunqueue cron delete daily-report
```

**Output:**
```
Cron job 'daily-report' deleted
```

---

## Rate Limiting

### Set Rate Limit

```bash
# Limit to 100 jobs per second
bunqueue rate-limit set emails 100
```

**Output:**
```
Rate limit set for 'emails'
Limit: 100 jobs/second
```

### Set Concurrency Limit

```bash
# Max 10 concurrent jobs
bunqueue concurrency set emails 10
```

**Output:**
```
Concurrency limit set for 'emails'
Limit: 10 concurrent jobs
```

### Clear Limits

```bash
bunqueue rate-limit clear emails
bunqueue concurrency clear emails
```

**Output:**
```
Rate limit cleared for 'emails'
Concurrency limit cleared for 'emails'
```

---

## Monitoring

### Server Stats

```bash
bunqueue stats
```

**Output:**
```
bunqueue Server Statistics
==========================
Uptime: 2d 5h 30m
Version: 2.1.8

Queues: 5
Total Jobs: 156,234
  - Waiting: 234
  - Active: 12
  - Completed: 155,800
  - Failed: 188

Database Size: 45.2 MB
WAL Size: 2.1 MB
```

### Prometheus Metrics

```bash
bunqueue metrics
```

**Output:**
```
# HELP bunqueue_jobs_total Total number of jobs
# TYPE bunqueue_jobs_total counter
bunqueue_jobs_total{queue="emails",state="completed"} 155800
bunqueue_jobs_total{queue="emails",state="failed"} 188

# HELP bunqueue_job_duration_seconds Job processing duration
# TYPE bunqueue_job_duration_seconds histogram
bunqueue_job_duration_seconds_bucket{queue="emails",le="0.1"} 145000
bunqueue_job_duration_seconds_bucket{queue="emails",le="1"} 155000
...
```

### Health Check

```bash
bunqueue health
```

**Output:**
```json
{
  "status": "healthy",
  "uptime": 185400,
  "version": "2.1.8",
  "database": {
    "status": "ok",
    "size": 47409152,
    "wal_size": 2202880
  },
  "queues": 5,
  "jobs": {
    "active": 12,
    "waiting": 234
  }
}
```

---

## Backup Operations

### Create Backup

```bash
bunqueue backup now
```

**Output:**
```
Backup started...
Uploading to S3: backups/2024-01-15/bunq-103000.db
Backup completed successfully
Size: 45.2 MB
Duration: 2.3s
```

### List Backups

```bash
bunqueue backup list
```

**Output:**
```
KEY                                    SIZE      CREATED
backups/2024-01-15/bunq-103000.db     45.2 MB   2024-01-15 10:30:00
backups/2024-01-14/bunq-103000.db     44.8 MB   2024-01-14 10:30:00
backups/2024-01-13/bunq-103000.db     43.2 MB   2024-01-13 10:30:00
```

### Restore Backup

```bash
bunqueue backup restore backups/2024-01-14/bunq-103000.db
```

**Output:**
```
⚠️  This will overwrite the current database
Type 'restore' to confirm: restore
Downloading backup...
Restoring database...
Restore completed successfully
```

### Backup Status

```bash
bunqueue backup status
```

**Output:**
```
S3 Backup Configuration
=======================
Enabled: true
Bucket: my-backups
Region: us-east-1
Interval: 6 hours
Retention: 7 backups
Last backup: 2024-01-15 10:30:00
Next backup: 2024-01-15 16:30:00
```

---

## Global Options

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--host` | `-H` | Server hostname | `localhost` |
| `--port` | `-p` | TCP port | `6789` |
| `--token` | `-t` | Authentication token | - |
| `--json` | - | Output as JSON | `false` |
| `--help` | `-h` | Show help | - |
| `--version` | `-v` | Show version | - |

### JSON Output

All commands support JSON output for scripting:

```bash
bunqueue stats --json | jq '.jobs.waiting'
```

**Output:**
```
234
```

```bash
# Use in scripts
WAITING=$(bunqueue queue jobs emails --state waiting --json | jq 'length')
if [ "$WAITING" -gt 1000 ]; then
  echo "Warning: Queue backlog detected"
fi
```

---

## Common Workflows

### Process Jobs Manually

```bash
# 1. Pull a job
JOB=$(bunqueue pull emails --json)
JOB_ID=$(echo $JOB | jq -r '.id')

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
WAITING=$(bunqueue queue jobs emails --state waiting --json | jq 'length')

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
