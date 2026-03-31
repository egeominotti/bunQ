# Human-in-the-Loop Gates — Design Document

## Overview

Allow AI agent jobs to pause mid-execution, request human approval, and resume based on the decision. The job stays in `active` state (processingShards) — no state machine changes.

## API Surface

### Worker (developer-facing)

```typescript
const worker = new Worker('ai-actions', async (job) => {
  const action = await ai.analyze(job.data);

  if (action.risk > 0.5) {
    const decision = await job.requestApproval({
      metadata: {
        action: action.description,
        customer: job.data.customerName,
        riskScore: action.risk,
      },
      timeout: 3600000,        // 1 hour
      timeoutAction: 'reject', // 'reject' | 'approve' | 'fail'
    });

    if (!decision.approved) {
      return { skipped: true, reason: decision.reason };
    }
  }

  return await executeAction(action);
}, { concurrency: 5 });
```

### Approval (human/system-facing)

```bash
# HTTP REST
curl -X POST http://bunqueue:6790/jobs/<id>/approve \
  -d '{"approved": true, "approvedBy": "marco@company.com"}'

curl -X POST http://bunqueue:6790/jobs/<id>/approve \
  -d '{"approved": false, "reason": "Amount too high"}'

# List pending
curl http://bunqueue:6790/approvals

# Get status
curl http://bunqueue:6790/jobs/<id>/approval
```

```typescript
// MCP Tool (AI agent approves another AI agent)
bunqueue_approve_job({ jobId: "job-abc", reason: "Verified" })
bunqueue_reject_job({ jobId: "job-abc", reason: "Budget exceeded" })
bunqueue_list_pending_approvals()

// TCP Command
await queue.approveJob('job-abc', { approvedBy: 'system' });
```

## Architecture Decision: No New Job State

Job state is DERIVED from location in bunqueue:

```
jobIndex: Map<JobId, JobLocation>

JobLocation =
  | { type: 'queue' }       → waiting/delayed
  | { type: 'processing' }  → active
  | { type: 'completed', queueName }   → completed
  | { type: 'dlq' }         → failed
```

Adding a new state would require changes to 15+ files. Instead, the job **stays in processingShards** (active state) and approval is tracked by a separate `ApprovalManager`. This means:

- JobState enum: unchanged (5 values)
- JobLocation type: unchanged (4 variants)
- JobStateType client: unchanged (6 values)
- BullMQ v5 compatibility: preserved
- getJobState(), getJobs(), getJobCounts(): unchanged
- Prometheus job metrics: unchanged
- SQLite jobs table schema: unchanged
- All existing tests: unaffected

## Internal Flow

```
Worker                              Server                           Human
  |                                   |                                |
  |  RequestApproval {metadata}       |                                |
  |---------------------------------->| 1. Create ApprovalRequest      |
  |  { ok, status: 'pending' }       |    status = 'pending'          |
  |<----------------------------------| 2. Save to SQLite              |
  |                                   | 3. Broadcast ApprovalRequested |
  |  WaitApproval {timeout: 25s}      | 4. Webhook -> Slack            |
  |---------------------------------->|    |                           |
  |  [waiter created, blocks 25s]     |    |                           |
  |  [heartbeat continues at 10s]     |    |                           |
  |  [other jobs process normally]    |    |                           |
  |                                   |    +-------------------------->|
  |                                   |                                |
  |  { status: 'pending' }           |    [25s elapsed, no decision]  |
  |<----------------------------------|                                |
  |                                   |                                |
  |  WaitApproval {timeout: 25s}      |                                |
  |---------------------------------->|                                |
  |  [waiter created again]           |                                |
  |                                   |    POST /jobs/:id/approve      |
  |                                   |<-------------------------------|
  |                                   | 1. status = 'approved'         |
  |                                   | 2. Resolve waiter              |
  |                                   | 3. Save to SQLite              |
  |                                   | 4. Broadcast ApprovalGranted   |
  |  { status: 'approved',           |                                |
  |    approvedBy: 'marco@...' }      |                                |
  |<----------------------------------|                                |
  |                                   |                                |
  |  requestApproval() returns        |                                |
  |  { approved: true, ... }          |                                |
  |  continues execution...           |                                |
  |  ACK -> completed                 |                                |
```

## Long-Polling Mechanism

TCP commandTimeout is 30s. Approvals can take hours. Solution: long-poll in 25s chunks.

```
requestApproval() internally:
  1. Send RequestApproval -> server responds immediately with 'pending'
  2. Loop:
     - Send WaitApproval with timeout=25s (under 30s TCP limit)
     - Server creates Promise (waiter), awaits up to 25s
     - If approval arrives within 25s -> responds immediately
     - If 25s expire -> responds 'pending' -> loop repeats
  3. Return result when status != 'pending'
```

In embedded mode, no TCP layer — direct Promise/waiter, no timeout issues.

## Worker Concurrency

```
Worker (concurrency: 5)
  Slot 1: job-abc  [awaiting approval]     <- paused on await
  Slot 2: job-def  [processing...]         <- active
  Slot 3: job-ghi  [processing...]         <- active
  Slot 4: job-jkl  [processing...]         <- active
  Slot 5: (empty)  [polling for jobs...]   <- free

  Heartbeat: ticks every 10s for ALL jobs (including job-abc)
```

requestApproval() is an async await. The JS thread is NOT blocked. Only that single job is paused. The heartbeat setInterval continues independently.

## Modifications to Existing Code

Only 2 conditions added to existing files:

### stallDetection.ts — Phase 1 (line ~28) and Phase 2 (line ~57)

```typescript
// Skip jobs awaiting human approval
if (ctx.approvalManager.isPending(jobId)) {
  ctx.stalledCandidates.delete(jobId);
  continue;
}
```

### cleanupTasks.ts — cleanOrphanedProcessingEntries (line ~51)

```typescript
for (const [jobId, job] of ctx.processingShards[i]) {
  if (ctx.approvalManager.isPending(jobId)) continue; // skip approval jobs
  if (job.startedAt && now - job.startedAt > stallTimeout) {
    orphaned.push(jobId);
  }
}
```

### types.ts (BackgroundContext)

```typescript
approvalManager: ApprovalManager; // add to context
```

Everything else is NEW additive code.

## New Domain Type

### src/domain/types/approval.ts

```typescript
export interface ApprovalRequest {
  readonly jobId: JobId;
  readonly queue: string;
  status: 'pending' | 'approved' | 'rejected' | 'timed_out';
  readonly metadata: Record<string, unknown>;
  readonly requestedAt: number;
  resolvedAt: number | null;
  resolvedBy: string | null;
  reason: string | null;
  readonly timeout: number;
  readonly timeoutAction: 'reject' | 'approve' | 'fail';
}
```

## ApprovalManager

### src/application/approvalManager.ts (~150 lines)

```typescript
class ApprovalManager {
  private readonly requests = new Map<JobId, ApprovalRequest>();
  private readonly waiters = new Map<string, ApprovalWaiter[]>();
  private readonly timers = new Map<JobId, Timer>();

  requestApproval(jobId, queue, metadata, timeout, timeoutAction): void
  approve(jobId, approvedBy?, reason?): boolean
  reject(jobId, reason?): boolean
  waitForApproval(jobId, timeoutMs): Promise<ApprovalResult>
  isPending(jobId): boolean              // O(1) Map lookup
  getPending(queue?): ApprovalRequest[]
  getApproval(jobId): ApprovalRequest | null
  clear(): void                          // shutdown cleanup
}
```

Waiter pattern identical to `EventsManager.waitForJobCompletion()` (eventsManager.ts:56-96).

## SQLite Persistence

### New table: job_approvals

```sql
CREATE TABLE IF NOT EXISTS job_approvals (
  job_id TEXT PRIMARY KEY,
  queue TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  metadata BLOB,
  requested_at INTEGER NOT NULL,
  resolved_at INTEGER,
  resolved_by TEXT,
  reason TEXT,
  timeout INTEGER NOT NULL,
  timeout_action TEXT NOT NULL DEFAULT 'reject'
);
CREATE INDEX IF NOT EXISTS idx_approvals_status ON job_approvals(status);
```

Writes are DURABLE (bypass write buffer) — approval state must survive crashes.

### New prepared statements (statements.ts)

- insertApproval
- updateApprovalStatus
- getApproval
- getPendingApprovals
- deleteApproval

### New sqlite.ts methods

- saveApproval(request)
- updateApproval(jobId, status, resolvedAt, resolvedBy, reason)
- loadPendingApprovals() — called on startup for crash recovery
- deleteApproval(jobId)

## TCP Commands

### New commands (command.ts)

```typescript
RequestApprovalCommand    { cmd: 'RequestApproval', id, metadata?, timeout?, timeoutAction? }
WaitApprovalCommand       { cmd: 'WaitApproval', id, timeout? }
ApproveJobCommand         { cmd: 'ApproveJob', id, approvedBy?, reason? }
RejectJobCommand          { cmd: 'RejectJob', id, reason? }
ListPendingApprovalsCmd   { cmd: 'ListPendingApprovals', queue? }
GetApprovalStatusCommand  { cmd: 'GetApprovalStatus', id }
```

### New handler file: handlers/approval.ts (~120 lines)

### New route group: routeApprovalCommand in handlerRoutes.ts

### New chain entry in handler.ts (line ~86)

## HTTP Endpoints

### New routes in http.ts routeRequest()

```
POST   /jobs/:id/approve    — approve or reject (body.approved: boolean)
GET    /jobs/:id/approval   — get approval status
GET    /approvals            — list all pending approvals
```

## Events & Webhooks

### New EventType values (queue.ts)

```typescript
ApprovalRequested = 'approval-requested'
ApprovalGranted   = 'approval-granted'
ApprovalDenied    = 'approval-denied'
ApprovalTimedOut  = 'approval-timed-out'
```

### New WebhookEvent values (webhook.ts)

```
'job.approval_requested'
'job.approved'
'job.rejected'
```

### Webhook mapping in eventsManager.ts mapEventToWebhook()

Webhook payload includes `approveUrl` for Slack/Teams button integration:

```json
{
  "event": "job.approval_requested",
  "jobId": "job-abc",
  "queue": "ai-actions",
  "data": {
    "action": "Refund $847",
    "approveUrl": "http://bunqueue:6790/jobs/job-abc/approve",
    "timeout": 3600000
  }
}
```

## Client SDK

### New types (client/types.ts)

```typescript
interface ApprovalOptions {
  metadata?: Record<string, unknown>;
  timeout?: number;                          // default 3600000 (1h)
  timeoutAction?: 'reject' | 'approve' | 'fail';
}

interface ApprovalResult {
  approved: boolean;
  status: 'approved' | 'rejected' | 'timed_out';
  reason?: string;
  resolvedBy?: string;
  resolvedAt?: number;
}
```

### New method on Job interface

```typescript
requestApproval(options: ApprovalOptions): Promise<ApprovalResult>;
```

### Implementation in processor.ts

Embedded mode: direct QueueManager.waitForApproval() call.
TCP mode: RequestApproval + WaitApproval long-poll loop (25s chunks).

## MCP Tools

### New file: mcp/tools/approvalTools.ts (~80 lines)

- `bunqueue_approve_job` — approve a job
- `bunqueue_reject_job` — reject a job
- `bunqueue_list_pending_approvals` — list pending approvals

## Prometheus Metrics (metricsExporter.ts)

```
bunqueue_approvals_pending{queue="..."} 3
bunqueue_approvals_total{queue="...",status="approved"} 42
bunqueue_approvals_total{queue="...",status="rejected"} 5
bunqueue_approval_duration_seconds{queue="..."} 120.5
```

## Failure & Recovery Scenarios

### Worker crashes during approval wait

1. Worker crashes, heartbeat stops
2. Stall detection: job has pending approval -> SKIP (not stalled)
3. Approval arrives via HTTP -> server sees no active worker
4. Server re-pushes job to queue as 'waiting'
5. New worker pulls job, calls requestApproval()
6. Server: approval already 'approved' -> returns IMMEDIATELY
7. Worker continues execution

### Server crashes

1. Job in SQLite: state='active'. Approval in job_approvals: status='pending'
2. Server restarts: loads jobs + pending approvals from SQLite
3. Reconstructs processingShards + ApprovalManager
4. Timeout timer restarted with remaining time
5. Approval flow continues normally

### Approval timeout

1. Server-side timer expires
2. ApprovalRequest.status = timeoutAction result ('timed_out')
3. Waiter resolved -> worker receives { status: 'timed_out' }
4. Worker decides how to proceed (fail, use default, skip)

### Duplicate approval attempts

1. First approve/reject sets status, resolves waiter
2. Subsequent calls return { ok: false, error: 'Already resolved' }

## File Inventory

| Action | File                                          | Lines |
|--------|-----------------------------------------------|-------|
| NEW    | src/domain/types/approval.ts                  | ~25   |
| NEW    | src/application/approvalManager.ts             | ~150  |
| NEW    | src/infrastructure/server/handlers/approval.ts | ~120  |
| NEW    | src/mcp/tools/approvalTools.ts                 | ~80   |
| NEW    | test/approval.test.ts                          | ~200  |
| NEW    | test/tcp/approval.test.ts                      | ~150  |
| MOD    | src/domain/types/command.ts                    | +40   |
| MOD    | src/domain/types/queue.ts                      | +4    |
| MOD    | src/domain/types/webhook.ts                    | +3    |
| MOD    | src/infrastructure/persistence/schema.ts       | +12   |
| MOD    | src/infrastructure/persistence/statements.ts   | +20   |
| MOD    | src/infrastructure/persistence/sqlite.ts       | +30   |
| MOD    | src/application/queueManager.ts                | +25   |
| MOD    | src/application/stallDetection.ts               | +4    |
| MOD    | src/application/cleanupTasks.ts                | +2    |
| MOD    | src/application/types.ts                       | +1    |
| MOD    | src/application/eventsManager.ts               | +6    |
| MOD    | src/application/metricsExporter.ts             | +15   |
| MOD    | src/infrastructure/server/handler.ts            | +3    |
| MOD    | src/infrastructure/server/handlerRoutes.ts     | +25   |
| MOD    | src/infrastructure/server/http.ts               | +30   |
| MOD    | src/client/types.ts                            | +20   |
| MOD    | src/client/worker/processor.ts                 | +50   |
| TOTAL  | 23 files (6 new, 17 modified)                  | ~1015 |

## Test Plan

1. requestApproval() -> approve -> job completes
2. requestApproval() -> reject -> job returns { approved: false }
3. requestApproval({ timeout: 500 }) -> timeout -> returns 'timed_out'
4. Job with pending approval -> stall detection skips it
5. Job with pending approval -> cleanup skips it
6. Server restart -> pending approvals restored from SQLite
7. Worker crash -> new worker re-processes -> approval already approved -> returns immediately
8. Webhook triggered on job.approval_requested
9. HTTP POST /jobs/:id/approve works
10. Concurrency: other jobs processed while one awaits approval
11. Embedded mode: direct waiter, no long-polling
12. TCP mode: long-poll 25s chunks within 30s commandTimeout
13. Duplicate approve/reject -> second call returns error
14. timeoutAction: 'approve' -> auto-approves on timeout
15. timeoutAction: 'fail' -> job fails on timeout
