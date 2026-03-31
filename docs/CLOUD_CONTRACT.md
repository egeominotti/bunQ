# bunqueue Cloud — Contract

Complete reference for all data bunqueue sends to the dashboard.

## 1. Snapshot (POST /api/v1/ingest)

Sent every 5s. Heavy data (recentJobs, dlqEntries, topErrors, workerDetails, queueConfigs, webhooks, s3Backup) refreshed every 30s, cached between refreshes.

### Headers

```
Authorization: Bearer bq_xxx
Content-Type: application/json
X-Timestamp: 1773957266666
X-Signature: <hmac-sha256>  (optional, if signing secret configured)
```

### Payload

```json
{
  "instanceId": "ac2b687c-0666-4cbe-840f-53a3706739eb",
  "instanceName": "prod-eu-1",
  "version": "2.6.43",
  "hostname": "Mac-Studio-di-Egeo-3.local",
  "pid": 69400,
  "startedAt": 1773957203631,
  "timestamp": 1773957266666,
  "sequenceId": 7,
  "shutdown": false,

  "stats": {
    "waiting": 234,
    "prioritized": 45,
    "delayed": 12,
    "active": 50,
    "dlq": 5,
    "completed": 8420,
    "stalled": 0,
    "paused": 0,
    "totalPushed": "12500",
    "totalPulled": "12200",
    "totalCompleted": "11800",
    "totalFailed": "400",
    "uptime": 63035,
    "cronJobs": 3,
    "cronPending": 0
  },

  "throughput": {
    "pushPerSec": 125.5,
    "pullPerSec": 110.2,
    "completePerSec": 98.7,
    "failPerSec": 2.3
  },

  "latency": {
    "averages": { "pushMs": 0.09, "pullMs": 0.19, "ackMs": 0.27 },
    "percentiles": {
      "push": { "p50": 0.1, "p95": 0.5, "p99": 1.0 },
      "pull": { "p50": 0.5, "p95": 1.0, "p99": 2.0 },
      "ack": { "p50": 0.5, "p95": 1.0, "p99": 1.5 }
    }
  },

  "memory": {
    "heapUsed": 245.5,
    "heapTotal": 512.0,
    "rss": 380.2,
    "external": 10.1
  },

  "collections": {
    "jobIndex": 1500,
    "completedJobs": 8420,
    "jobResults": 5000,
    "jobLogs": 200,
    "customIdMap": 0,
    "jobLocks": 50,
    "processingTotal": 50,
    "queuedTotal": 234,
    "temporalIndexTotal": 12,
    "delayedHeapTotal": 12
  },

  "queues": [
    {
      "name": "emails",
      "waiting": 120,
      "prioritized": 25,
      "delayed": 5,
      "active": 25,
      "completed": 5200,
      "failed": 3,
      "paused": false,
      "totalCompleted": 5200,
      "totalFailed": 180
    },
    {
      "name": "orders",
      "waiting": 89,
      "prioritized": 15,
      "delayed": 4,
      "active": 20,
      "completed": 4100,
      "failed": 2,
      "paused": false,
      "totalCompleted": 4100,
      "totalFailed": 120
    }
  ],

  "workers": {
    "total": 8,
    "active": 7,
    "totalProcessed": 11800,
    "totalFailed": 400,
    "activeJobs": 45
  },

  "crons": [
    {
      "name": "daily-cleanup",
      "queue": "maintenance",
      "schedule": "0 3 * * *",
      "nextRun": 1773972000000,
      "executions": 14,
      "maxLimit": null
    },
    {
      "name": "health-check",
      "queue": "monitoring",
      "schedule": "*/2 * * * *",
      "nextRun": 1773957400000,
      "executions": 320,
      "maxLimit": null
    }
  ],

  "storage": {
    "diskFull": false,
    "error": null
  },

  "taskErrors": {
    "cleanup": { "consecutiveFailures": 0 },
    "dependency": { "consecutiveFailures": 0 }
  },

  "connections": {
    "tcp": 12,
    "ws": 2,
    "sse": 0
  },

  "recentJobs": [
    {
      "id": "019d0589-ab12-7001-b234-1234567890ab",
      "name": "send-email",
      "queue": "emails",
      "state": "completed",
      "data": { "name": "send-email", "to": "user5@test.com", "subject": "Welcome!" },
      "priority": 5,
      "createdAt": 1773957261666,
      "startedAt": 1773957261866,
      "completedAt": 1773957261896,
      "attempts": 1,
      "maxAttempts": 3,
      "duration": 30,
      "progress": 0
    },
    {
      "id": "019d0589-cd34-7001-a567-abcdef123456",
      "name": "process-order",
      "queue": "orders",
      "state": "active",
      "data": { "name": "process-order", "orderId": "ORD-42", "amount": 89.99 },
      "priority": 10,
      "createdAt": 1773957260000,
      "startedAt": 1773957265000,
      "failedReason": "Retry 1/3",
      "attempts": 1,
      "maxAttempts": 3
    },
    {
      "id": "019d0589-ef56-7001-c890-fedcba654321",
      "name": "send-email",
      "queue": "emails",
      "state": "waiting",
      "data": { "name": "send-email", "to": "user12@test.com", "subject": "Welcome!" },
      "priority": 0,
      "createdAt": 1773957262000,
      "attempts": 0,
      "maxAttempts": 3
    }
  ],

  "dlqEntries": [
    {
      "jobId": "019d0500-1234-7001-aaaa-111111111111",
      "queue": "emails",
      "reason": "max_attempts",
      "error": "SMTP timeout",
      "enteredAt": 1773957200000,
      "retryCount": 0,
      "attempts": 3
    }
  ],

  "workerDetails": [
    {
      "id": "worker-emails-1773957100000-abc123",
      "name": "emails",
      "queues": ["emails"],
      "concurrency": 5,
      "hostname": "Mac-Studio-di-Egeo-3.local",
      "pid": 69500,
      "lastSeen": 1773957266000,
      "activeJobs": 5,
      "processedJobs": 5200,
      "failedJobs": 180,
      "currentJob": "019d0589-ab12-7001-b234-1234567890ab"
    },
    {
      "id": "worker-orders-1773957100000-def456",
      "name": "orders",
      "queues": ["orders"],
      "concurrency": 3,
      "hostname": "Mac-Studio-di-Egeo-3.local",
      "pid": 69501,
      "lastSeen": 1773957265000,
      "activeJobs": 3,
      "processedJobs": 4100,
      "failedJobs": 120,
      "currentJob": null
    }
  ],

  "queueConfigs": {
    "emails": {
      "paused": false,
      "rateLimit": null,
      "concurrencyLimit": null,
      "concurrencyActive": 25,
      "stallConfig": { "stallInterval": 30000, "maxStalls": 3 },
      "dlqConfig": { "maxRetries": 3, "maxAge": 604800000 }
    },
    "orders": {
      "paused": false,
      "rateLimit": null,
      "concurrencyLimit": null,
      "concurrencyActive": 20,
      "stallConfig": { "stallInterval": 30000, "maxStalls": 3 },
      "dlqConfig": { "maxRetries": 3, "maxAge": 604800000 }
    }
  },

  "webhooks": [
    {
      "id": "wh-001",
      "url": "https://api.acme.com/webhooks/bunqueue",
      "events": ["job.completed", "job.failed"],
      "queue": null,
      "enabled": true,
      "successCount": 1200,
      "failureCount": 3,
      "lastTriggered": 1773957265000
    }
  ],

  "topErrors": [
    { "message": "SMTP timeout", "count": 42, "queue": "emails", "lastSeen": 1773957260000 },
    { "message": "Database connection lost", "count": 12, "queue": "orders", "lastSeen": 1773957250000 },
    { "message": "Rate limit exceeded", "count": 5, "queue": "payments", "lastSeen": 1773957240000 }
  ],

  "s3Backup": null
}
```

## 2. WebSocket Events (WS /api/v1/stream)

### Handshake (bunqueue → dashboard)

```json
{
  "type": "handshake",
  "instanceId": "ac2b687c-0666-4cbe-840f-53a3706739eb",
  "apiKey": "bq_xxx",
  "remoteCommands": true
}
```

### Job Event (bunqueue → dashboard)

```json
{
  "instanceId": "ac2b687c-0666-4cbe-840f-53a3706739eb",
  "timestamp": 1773957266666,
  "jobEvent": {
    "eventType": "failed",
    "queue": "emails",
    "jobId": "019d0589-ab12-7001-b234-1234567890ab",
    "error": "SMTP timeout"
  }
}
```

Event types: `pushed`, `pulled`, `completed`, `failed`, `progress`, `stalled`, `removed`, `delayed`, `duplicated`, `retried`, `waiting-children`, `drained`, `paused`, `resumed`

## 3. Remote Commands (dashboard → bunqueue via WS)

Requires `BUNQUEUE_CLOUD_REMOTE_COMMANDS=true`.

### Request format

```json
{ "type": "command", "id": "cmd_42_1773884000000", "action": "queue:pause", "queue": "emails" }
```

### Response format

```json
{ "type": "command_result", "id": "cmd_42_1773884000000", "success": true, "data": { "queue": "emails", "paused": true } }
```

Error:
```json
{ "type": "command_result", "id": "cmd_42_1773884000000", "success": false, "error": "Queue not found" }
```

### Available Commands

| Action | Params | Response data |
|--------|--------|---------------|
| `queue:pause` | `queue` | `{ queue, paused: true }` |
| `queue:resume` | `queue` | `{ queue, paused: false }` |
| `queue:drain` | `queue` | `{ queue, drained: number }` |
| `queue:clean` | `queue`, `graceMs?`, `state?`, `limit?` | `{ queue, cleaned: number }` |
| `queue:detail` | `queue` | `{ queue, paused, counts, stallConfig, dlqConfig, dlqEntries, jobs }` |
| `job:cancel` | `jobId` | `{ cancelled: boolean }` |
| `job:promote` | `jobId` | `{ promoted: boolean }` |
| `job:retry` | `queue`, `jobId?` | `{ retried: number }` |
| `job:list` | `queue`, `state?`, `limit?`, `offset?` | `{ jobs: [...], total, offset, limit }` |
| `job:get` | `jobId` | `{ job: { ...full, logs, result } }` |
| `job:logs` | `jobId` | `{ logs: JobLogEntry[] }` |
| `job:result` | `jobId` | `{ result: unknown \| null }` |
| `dlq:retry` | `queue`, `jobId?` | `{ retried: number }` |
| `dlq:purge` | `queue` | `{ purged: number }` |
| `cron:upsert` | `name`, `queue`, `schedule`, `data?` | `{ name, nextRun }` |
| `cron:delete` | `name` | `{ deleted: boolean }` |
| `stats:refresh` | — | full stats object |

## 4. Batch Ingest (POST /api/v1/ingest/batch)

Used after reconnect to flush buffered snapshots. Body is an **array** of CloudSnapshot objects.

```json
[
  { "instanceId": "...", "sequenceId": 5, "timestamp": 1773957200000, ... },
  { "instanceId": "...", "sequenceId": 6, "timestamp": 1773957205000, ... },
  { "instanceId": "...", "sequenceId": 7, "timestamp": 1773957210000, ... }
]
```

## 5. Environment Variables

```bash
# Required
BUNQUEUE_CLOUD_URL=https://bunqueue.io
BUNQUEUE_CLOUD_API_KEY=bq_xxx

# Optional
BUNQUEUE_CLOUD_INSTANCE_NAME=prod-eu-1      # default: hostname
BUNQUEUE_CLOUD_INTERVAL_MS=5000             # snapshot interval
BUNQUEUE_CLOUD_REMOTE_COMMANDS=true         # enable remote commands (default: false)
BUNQUEUE_CLOUD_SIGNING_SECRET=whsec_xxx     # HMAC signing
BUNQUEUE_CLOUD_INCLUDE_JOB_DATA=true        # include job data in WS events (default: false)
BUNQUEUE_CLOUD_REDACT_FIELDS=email,password # redact fields from job data
BUNQUEUE_CLOUD_EVENTS=failed,stalled        # filter WS events (default: all)
BUNQUEUE_CLOUD_BUFFER_SIZE=720              # offline buffer size
BUNQUEUE_CLOUD_USE_WEBSOCKET=false          # disable WS channel
BUNQUEUE_CLOUD_USE_HTTP=false               # disable HTTP channel
```

## 6. Two-Tier Snapshot Schedule

| Tier | Frequency | Data | Cost |
|------|-----------|------|------|
| Light | Every 5s | stats, throughput, latency, memory, collections, queues, workers, crons, connections, storage, taskErrors | O(SHARD_COUNT) ~0.1ms |
| Heavy | Every 30s | + recentJobs, dlqEntries, topErrors, workerDetails, queueConfigs, webhooks, s3Backup | O(active_queues × shards) ~1-5ms |

Heavy data is cached between refreshes. Shutdown snapshot always includes heavy data.
