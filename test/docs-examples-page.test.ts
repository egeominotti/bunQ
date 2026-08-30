import { describe, expect, test } from 'bun:test';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import {
  clampJourneyStep,
  JOB_JOURNEYS,
  resolveTopology,
  TOPOLOGIES,
} from '../docs/src/components/examples/explainerModels';

const REPO = join(import.meta.dir, '..');
const EXAMPLES = readFileSync(join(REPO, 'docs/src/content/docs/examples.mdx'), 'utf8');
const COMPONENT_ROOT = join(REPO, 'docs/src/components/examples');

function source(name: string): string {
  return readFileSync(join(COMPONENT_ROOT, name), 'utf8');
}

describe('examples page progression', () => {
  test('moves from the learning path through local jobs to the end-to-end deployment', () => {
    const headings = [
      '## Learning path',
      '## Minimal queue and worker',
      '## Understand the job lifecycle',
      '## Retries and the dead letter queue',
      '## Scheduled and repeating jobs',
      '## Deduplicate jobs with jobId',
      '## Choose a deployment topology',
      '## Distributed mode (server + TCP)',
      '## Watch job events',
      '## Graceful shutdown',
      '## Workflow: automatic rollback on failure',
      '## Workflow: wait for a human decision',
      '## End-to-end example projects',
    ];
    const positions = headings.map((heading) => EXAMPLES.indexOf(heading));

    expect(positions.every((position) => position >= 0)).toBe(true);
    expect(positions).toEqual([...positions].sort((left, right) => left - right));
    expect(EXAMPLES.match(/## End-to-end example projects/g)).toHaveLength(1);
  });

  test('keeps every learning-path destination local and explicit', () => {
    const learningPath = source('ExamplesLearningPath.astro');
    for (const anchor of [
      '#minimal-queue-and-worker',
      '#understand-the-job-lifecycle',
      '#retries-and-the-dead-letter-queue',
      '#scheduled-and-repeating-jobs',
      '#deduplicate-jobs-with-jobid',
      '#choose-a-deployment-topology',
      '#workflow-automatic-rollback-on-failure',
      '#end-to-end-example-projects',
    ]) {
      expect(learningPath).toContain(`href="${anchor}"`);
    }
  });
});

describe('interactive job lifecycle model', () => {
  test('covers success, retry recovery, and terminal DLQ routes', () => {
    expect(Object.keys(JOB_JOURNEYS)).toEqual(['success', 'retry', 'dlq']);
    expect(JOB_JOURNEYS.success.steps.map(({ label }) => label)).toEqual([
      'Producer',
      'Ready queue',
      'Worker',
      'Completed',
    ]);
    expect(JOB_JOURNEYS.retry.steps.map(({ label }) => label)).toEqual([
      'Producer',
      'Ready queue',
      'Attempt 1',
      'Retry delay',
      'Ready again',
      'Attempt 2',
      'Completed',
    ]);
    expect(JOB_JOURNEYS.dlq.steps.at(-1)?.label).toBe('Dead letter queue');
  });

  test('clamps every requested step to a legal route index', () => {
    expect(clampJourneyStep(-1, 4)).toBe(0);
    expect(clampJourneyStep(2.9, 4)).toBe(2);
    expect(clampJourneyStep(99, 4)).toBe(3);
    expect(clampJourneyStep(Number.NaN, 4)).toBe(0);
    expect(clampJourneyStep(1, 0)).toBe(0);
  });

  test('uses native controls and announces state changes', () => {
    const component = source('JobJourney.astro');
    expect(component).toContain('type="button"');
    expect(component).toContain('aria-pressed=');
    expect(component).toContain('aria-live="polite"');
    expect(component).toContain("customElements.get('bq-job-journey')");
    expect(component).toContain('clampJourneyStep(stepIndex, steps.length)');
    expect(component).toContain("matchMedia('(prefers-reduced-motion: reduce)')");
    expect(component).toContain('scrollIntoView({');
  });
});

describe('interactive deployment topology model', () => {
  test('progresses from embedded to one broker and then N PostgreSQL brokers', () => {
    expect(TOPOLOGIES.map(({ id }) => id)).toEqual(['embedded', 'single-broker', 'multi-broker']);
    expect(TOPOLOGIES[0].durability).toContain('SQLite');
    expect(TOPOLOGIES[1].layers.flatMap(({ nodes }) => nodes)).toContain('bunqueue broker');
    expect(TOPOLOGIES[2].layers.flatMap(({ nodes }) => nodes)).toContain('PostgreSQL');
    expect(
      TOPOLOGIES[2].layers.find(({ label }) => label === 'N active brokers')?.nodes
    ).toHaveLength(3);
  });

  test('falls back to the simplest topology for missing or invalid state', () => {
    expect(resolveTopology(undefined).id).toBe('embedded');
    expect(resolveTopology('not-a-topology').id).toBe('embedded');
    expect(resolveTopology('multi-broker').id).toBe('multi-broker');
  });

  test('connects each control to one labelled panel and exposes a live summary', () => {
    const component = source('TopologyExplorer.astro');
    expect(component).toContain('aria-controls={`topology-${topology.id}`}');
    expect(component).toContain('aria-pressed=');
    expect(component).toContain('role="img"');
    expect(component).toContain('aria-live="polite"');
    expect(component).toContain("customElements.get('bq-topology-explorer')");
  });

  test('keeps every new source below the project file-size limit', () => {
    for (const name of [
      'ExamplesLearningPath.astro',
      'JobJourney.astro',
      'TopologyExplorer.astro',
      'examples-explainers.css',
      'examples-learning.css',
      'explainerModels.ts',
    ]) {
      expect(source(name).split('\n').length, name).toBeLessThanOrEqual(300);
    }
    const styles = source('examples-explainers.css');
    expect(styles).toContain('@media (prefers-reduced-motion: reduce)');
    expect(styles).toContain('.ex-route[hidden],');
    expect(styles).toContain('.ex-topology-panel[hidden]');
  });
});
