import type { DurableAdmissionMetadata } from '../types/admission';
import { SqliteState } from './state';

/** SQL primitives shared by every durable job-admission transaction. */
export abstract class SqliteAdmission extends SqliteState {
  protected hasAdmissionMetadata(metadata?: DurableAdmissionMetadata): boolean {
    return (
      metadata?.retireGenerationId !== undefined || (metadata?.completionPins?.length ?? 0) > 0
    );
  }

  protected runAdmissionMetadata(metadata?: DurableAdmissionMetadata): void {
    const retiredId = metadata?.retireGenerationId;
    if (retiredId !== undefined) {
      this.statements.get('deleteJob')!.run(retiredId);
      this.statements.get('deleteJobResult')!.run(retiredId);
      this.statements.get('deleteDlqEntry')!.run(retiredId);
      this.statements.get('deleteDependencyCompletionForAdmission')!.run(retiredId);
      this.statements.get('deleteFlowFailuresForAdmission')!.run(retiredId, retiredId);
    }

    for (const jobId of new Set(metadata?.completionPins ?? [])) {
      this.statements.get('pinDependencyCompletionForAdmission')!.run(jobId);
    }
  }

  protected finalizeAdmissionMetadata(metadata?: DurableAdmissionMetadata): void {
    const retiredId = metadata?.retireGenerationId;
    if (retiredId !== undefined) this.writeBuffer.removePending(retiredId);
  }

  protected commitBufferedAdmissionMetadata(metadata?: DurableAdmissionMetadata): void {
    if (!this.hasAdmissionMetadata(metadata)) return;
    this.safeWrite(() => {
      const transaction = this.db.transaction(() => this.runAdmissionMetadata(metadata));
      transaction();
    });
    this.finalizeAdmissionMetadata(metadata);
  }
}
