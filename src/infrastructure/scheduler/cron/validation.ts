import { assertValidCronTiming, type CronJobInput } from '../../../domain/types/cron';
import { expandCronShortcut, validateCronExpression } from '../cronParser';

/** Reject invalid timing, calendar syntax, and timezones before state mutation. */
export function assertValidCronInput(input: CronJobInput): void {
  assertValidCronTiming(input);
  if (!input.schedule) return;
  const error = validateCronExpression(expandCronShortcut(input.schedule), input.timezone);
  if (error) throw new Error(`Invalid cron expression: ${error}`);
}
