/**
 * SECURITY AUDIT — BUG H1: Cloud snapshot path leaks PII/secrets
 *
 * Redaction (CloudAgent.redactData, applying BUNQUEUE_CLOUD_REDACT_FIELDS)
 * is ONLY wired into subscribeToEvents() — the per-event WebSocket/HTTP event
 * channel. The PERIODIC SNAPSHOT path (collectSnapshot → collectLiveJobs /
 * collectDlqEntries in snapshotHelpers.ts) copies job.data and DLQ jobData
 * VERBATIM:
 *   - snapshotHelpers.ts mapJobCore:   `data,`               (recentJobs[].data)
 *   - snapshotHelpers.ts collectDlqEntries: `jobData: e.job.data`
 * CollectSnapshotParams has no config/redactFields/includeJobData, so the
 * snapshot collectors have no way to redact even if they wanted to.
 *
 * Result: with BUNQUEUE_CLOUD_REDACT_FIELDS=password,apiKey configured, every
 * periodic snapshot still ships RAW secrets to the cloud dashboard.
 *
 * This test asserts the CORRECT (secure) behavior:
 *   recentJobs[].data and dlqEntries[].jobData have the configured fields
 *   replaced with '[REDACTED]' and the secret values DO NOT appear anywhere in
 *   the serialized snapshot.
 *
 * On the current (buggy) code the raw secrets are present → test is RED.
 */
import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { QueueManager } from '../src/application/queueManager';
import type { HandlerContext } from '../src/infrastructure/server/types';
import { handlePush, handlePull, handleFail } from '../src/infrastructure/server/handlers/core';
import { collectSnapshot } from '../src/infrastructure/cloud/snapshotCollector';

function createContext(qm: QueueManager): HandlerContext {
  return {
    queueManager: qm,
    authTokens: new Set<string>(),
    authenticated: false,
    clientId: 'audit-redaction-1',
  };
}

const SECRET_PASSWORD = 'super-secret-password-12345';
const SECRET_API_KEY = 'sk-live-DEADBEEFxyz-PII';
const REDACT_FIELDS = ['password', 'apiKey'];

describe('AUDIT H1 — Cloud snapshot redaction (BUNQUEUE_CLOUD_REDACT_FIELDS)', () => {
  let qm: QueueManager;
  let ctx: HandlerContext;

  beforeEach(() => {
    qm = new QueueManager();
    ctx = createContext(qm);
  });

  afterEach(() => {
    qm.shutdown();
  });

  test('periodic snapshot must redact configured fields in recentJobs[].data and dlqEntries[].jobData', async () => {
    // ── Populate a WAITING job whose data holds sensitive fields ──
    // A waiting job shows up in recentJobs via collectLiveJobs (state: waiting).
    await handlePush(
      {
        cmd: 'PUSH',
        queue: 'audit-live',
        data: { password: SECRET_PASSWORD, apiKey: SECRET_API_KEY, user: 'alice' },
      },
      ctx
    );

    // ── Populate a DLQ entry whose job.data holds sensitive fields ──
    // Push → pull → fail with maxAttempts=1 sends the job straight to the DLQ,
    // where its data is read verbatim by collectDlqEntries (jobData: e.job.data).
    await handlePush(
      {
        cmd: 'PUSH',
        queue: 'audit-dlq',
        data: { password: SECRET_PASSWORD, apiKey: SECRET_API_KEY, user: 'bob' },
        maxAttempts: 1,
      },
      ctx
    );
    const pulled = await handlePull({ cmd: 'PULL', queue: 'audit-dlq' }, ctx);
    const dlqJobId = (pulled as { job?: { id: string } }).job?.id;
    expect(dlqJobId).toBeTruthy();
    await handleFail({ cmd: 'FAIL', id: dlqJobId as string, error: 'boom' }, ctx);

    // Sanity: the DLQ entry exists and (un-redacted) still holds the raw data,
    // proving our fixture is wired correctly before we test the snapshot.
    const dlqEntriesRaw = qm.getDlqEntries('audit-dlq');
    expect(dlqEntriesRaw.length).toBe(1);

    // ── Collect a snapshot via the real production path the agent uses ──
    // CollectSnapshotParams currently has NO redactFields/config — the bug.
    // We pass the intended redaction config; once the fix threads it through,
    // these assertions pass. Until then, raw secrets leak → RED.
    const params = {
      queueManager: qm,
      instanceId: 'audit-instance',
      instanceName: 'audit',
      startedAt: Date.now(),
      sequenceId: 1,
      includeHeavy: true,
      // Intended secure config (mirrors CloudConfig.redactFields / includeJobData)
      redactFields: REDACT_FIELDS,
      includeJobData: true,
    } as unknown as Parameters<typeof collectSnapshot>[0];

    const snapshot = await collectSnapshot(params);

    // ── Assert recentJobs data is redacted ──
    const liveJob = snapshot.recentJobs.find((j) => j.queue === 'audit-live');
    expect(liveJob).toBeTruthy();
    const liveData = liveJob?.data as Record<string, unknown> | undefined;
    expect(liveData).toBeTruthy();
    expect(liveData?.user).toBe('alice'); // non-secret field preserved
    expect(liveData?.password).toBe('[REDACTED]');
    expect(liveData?.apiKey).toBe('[REDACTED]');

    // ── Assert DLQ jobData is redacted ──
    const dlqEntry = snapshot.dlqEntries.find((e) => e.queue === 'audit-dlq');
    expect(dlqEntry).toBeTruthy();
    const dlqData = dlqEntry?.jobData as Record<string, unknown> | undefined;
    expect(dlqData).toBeTruthy();
    expect(dlqData?.user).toBe('bob'); // non-secret field preserved
    expect(dlqData?.password).toBe('[REDACTED]');
    expect(dlqData?.apiKey).toBe('[REDACTED]');

    // ── Strongest check: secret values must be absent from the entire
    //    serialized snapshot that gets shipped to the cloud dashboard. ──
    const serialized = JSON.stringify(snapshot);
    expect(serialized).not.toContain(SECRET_PASSWORD);
    expect(serialized).not.toContain(SECRET_API_KEY);
  });
});
