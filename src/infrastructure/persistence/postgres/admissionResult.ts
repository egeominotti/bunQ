import type { Job } from '../../../domain/types/job';
import type { PostgresJobState } from './types';

export interface PostgresAdmissionResult {
  readonly job: Job;
  readonly inserted: boolean;
  readonly state: PostgresJobState;
  readonly pushedEventId?: number;
  readonly timelineIndex?: number;
}
