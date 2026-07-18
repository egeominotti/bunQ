import { expect, test } from 'bun:test';
import { DependencyResultTracker } from '../src/application/dependencyResultTracker';
import { jobId } from '../src/domain/types/job';

test('protected dependency results survive until the last fan-out consumer exits', () => {
  const tracker = new DependencyResultTracker();
  const dependency = jobId('dependency');
  const consumerA = jobId('consumer-a');
  const consumerB = jobId('consumer-b');

  tracker.registerConsumer(consumerA, [dependency]);
  tracker.registerConsumer(consumerB, [dependency]);
  tracker.retain(dependency, null);

  expect(tracker.has(dependency)).toBe(true);
  expect(tracker.get(dependency)).toBeNull();

  tracker.releaseConsumer(consumerA);
  expect(tracker.has(dependency)).toBe(true);

  tracker.releaseConsumer(consumerB);
  expect(tracker.has(dependency)).toBe(false);
});

test('releasing one fan-in edge preserves the consumer other dependencies', () => {
  const tracker = new DependencyResultTracker();
  const consumer = jobId('consumer');
  const dependencyA = jobId('dependency-a');
  const dependencyB = jobId('dependency-b');

  tracker.registerConsumer(consumer, [dependencyA, dependencyB]);
  tracker.retain(dependencyA, 'a');
  tracker.retain(dependencyB, 'b');
  tracker.releaseDependency(consumer, dependencyA);

  expect(tracker.has(dependencyA)).toBe(false);
  expect(tracker.get(dependencyB)).toBe('b');
});
