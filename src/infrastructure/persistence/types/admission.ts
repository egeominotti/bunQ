import type { JobId } from '../../../domain/types/job';

/** Extra persisted state carried by a two-phase job admission. */
export interface DurableAdmissionMetadata {
  /** Terminal generation replaced by the incoming deterministic job ID. */
  readonly retireGenerationId?: JobId;
  /** Payload-free completion records referenced by the incoming job. */
  readonly completionPins?: readonly JobId[];
}
