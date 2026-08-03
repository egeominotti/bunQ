import type { JobId } from '../domain/types/job';

/**
 * Retains results while at least one live dependency consumer may still read
 * them. Ordinary completed-job results remain governed by the configured LRU.
 */
export class DependencyResultTracker {
  private readonly consumersByDependency = new Map<JobId, Set<JobId>>();
  private readonly dependenciesByConsumer = new Map<JobId, Set<JobId>>();
  private readonly protectedResults = new Map<JobId, unknown>();

  registerConsumer(consumerId: JobId, dependencyIds: readonly JobId[]): void {
    if (dependencyIds.length === 0) return;

    let dependencies = this.dependenciesByConsumer.get(consumerId);
    if (!dependencies) {
      dependencies = new Set();
      this.dependenciesByConsumer.set(consumerId, dependencies);
    }

    for (const dependencyId of dependencyIds) {
      dependencies.add(dependencyId);
      let consumers = this.consumersByDependency.get(dependencyId);
      if (!consumers) {
        consumers = new Set();
        this.consumersByDependency.set(dependencyId, consumers);
      }
      consumers.add(consumerId);
    }
  }

  retain(dependencyId: JobId, result: unknown): void {
    if (this.consumersByDependency.has(dependencyId)) {
      this.protectedResults.set(dependencyId, result);
    }
  }

  has(dependencyId: JobId): boolean {
    return this.protectedResults.has(dependencyId);
  }

  get(dependencyId: JobId): unknown {
    return this.protectedResults.get(dependencyId);
  }

  hasConsumers(dependencyId: JobId): boolean {
    return (this.consumersByDependency.get(dependencyId)?.size ?? 0) > 0;
  }

  releaseConsumer(consumerId: JobId): void {
    const dependencies = this.dependenciesByConsumer.get(consumerId);
    if (!dependencies) return;

    for (const dependencyId of dependencies) {
      const consumers = this.consumersByDependency.get(dependencyId);
      if (!consumers) continue;
      consumers.delete(consumerId);
      if (consumers.size === 0) {
        this.consumersByDependency.delete(dependencyId);
        this.protectedResults.delete(dependencyId);
      }
    }
    this.dependenciesByConsumer.delete(consumerId);
  }

  releaseDependency(consumerId: JobId, dependencyId: JobId): void {
    const dependencies = this.dependenciesByConsumer.get(consumerId);
    dependencies?.delete(dependencyId);
    if (dependencies?.size === 0) this.dependenciesByConsumer.delete(consumerId);

    const consumers = this.consumersByDependency.get(dependencyId);
    consumers?.delete(consumerId);
    if (consumers?.size === 0) {
      this.consumersByDependency.delete(dependencyId);
      this.protectedResults.delete(dependencyId);
    }
  }

  clear(): void {
    this.consumersByDependency.clear();
    this.dependenciesByConsumer.clear();
    this.protectedResults.clear();
  }
}
