import type { Job } from '../../../domain/types/job';

export interface PostgresAdmissionResult {
  readonly job: Job;
  readonly inserted: boolean;
  readonly pushedEventId?: number;
  readonly timelineIndex?: number;
}
