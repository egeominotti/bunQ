import { assertValidCronTiming, type CronJob } from '../../../domain/types/cron';
import { expandCronShortcut, validateCronExpression } from '../cronParser';

const NATIVE_CRON_MIGRATION_VERSION = '2.9.0';

/** Reject persisted Croner-only syntax before any scheduler state is mutated. */
export function assertPersistedCronSupported(cron: CronJob): void {
  try {
    assertValidCronTiming(cron);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(
      `Persisted cron ${JSON.stringify(cron.name)} has invalid timing: ${message}. ` +
        `Update or remove it before upgrading to bunqueue ${NATIVE_CRON_MIGRATION_VERSION}.`
    );
  }
  if (!cron.schedule) return;
  const error = validateCronExpression(
    expandCronShortcut(cron.schedule),
    cron.timezone ?? undefined
  );
  if (!error) return;
  throw new Error(
    `Persisted cron ${JSON.stringify(cron.name)} uses unsupported schedule ${JSON.stringify(cron.schedule)}. ` +
      `Update or remove it before upgrading to bunqueue ${NATIVE_CRON_MIGRATION_VERSION}. ` +
      `Bun.cron validation failed: ${error}`
  );
}

/** Validate a persisted collection atomically before loading any definition. */
export function assertPersistedCronsSupported(crons: readonly CronJob[]): void {
  for (const cron of crons) assertPersistedCronSupported(cron);
}
