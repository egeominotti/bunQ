import type { JobId } from '../../../domain/types/job';
import { PostgresAdmissionStore } from './admissionStore';
import {
  renewPostgresLease,
  renewPostgresLeases,
  type PostgresLeaseRenewalInput,
  type PostgresLeaseRenewalResult,
} from './leaseRenewal';

/** Lease-renewal operations exposed by the PostgreSQL store facade. */
export class PostgresLeaseStore extends PostgresAdmissionStore {
  async renew(id: JobId, token: string, durationMs: number): Promise<number | null> {
    await this.initialize();
    return await renewPostgresLease(this.context, id, token, durationMs);
  }

  async renewMany(
    inputs: readonly PostgresLeaseRenewalInput[]
  ): Promise<PostgresLeaseRenewalResult[]> {
    await this.initialize();
    return await renewPostgresLeases(this.context, inputs);
  }
}
