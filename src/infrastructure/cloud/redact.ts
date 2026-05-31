/**
 * Shared redaction helper for the Cloud agent.
 *
 * Single implementation used by BOTH the event channel (subscribeToEvents)
 * and the periodic snapshot path (collectLiveJobs / collectDlqEntries) so
 * configured BUNQUEUE_CLOUD_REDACT_FIELDS are stripped consistently
 * everywhere job data leaves the process.
 */

/** Replace configured fields in an object with '[REDACTED]'. Non-objects pass through. */
export function redactData(data: unknown, redactFields: readonly string[]): unknown {
  if (!data || typeof data !== 'object' || redactFields.length === 0) {
    return data;
  }

  const redacted = { ...(data as Record<string, unknown>) };
  for (const field of redactFields) {
    if (field in redacted) {
      redacted[field] = '[REDACTED]';
    }
  }
  return redacted;
}
