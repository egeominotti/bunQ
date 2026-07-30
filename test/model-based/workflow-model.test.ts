/**
 * Property-based state-machine model for the Workflow Engine.
 *
 * Generates a random workflow graph (including timed gates, pivots, child workflows
 * and fallible compensations), starts it unconditionally, then applies a random
 * operator history. Every action checks the persisted state and real handler ledger.
 *
 * The point is coverage of interleavings no hand-written test enumerates: a signal
 * landing mid-step, a restart mid-compensation, an approval for an event the graph
 * never waits on, two signals for the same gate, a settle that straddles a retry
 * backoff. The lost-update bug that destroyed signal payloads
 * (test/repro-workflow-signal-lost-update.test.ts) is exactly an I1 violation, and
 * a waitFor silently dropped inside a branch path is an I4 violation.
 *
 * On failure fast-check shrinks and prints the seed, the spec and the command
 * history. Replay a specific campaign with:
 *
 *   BUNQUEUE_WF_MODEL_SEED=123 BUNQUEUE_WF_MODEL_RUNS=200 \
 *     bun test test/model-based/workflow-model.test.ts
 */

import { afterAll, describe, expect, test } from 'bun:test';
import fc from 'fast-check';
import type { Execution, ExecutionState } from '../../src/client/workflow';
import { Workflow } from '../../src/client/workflow';
import { WorkflowStore } from '../../src/client/workflow/store';
import { workflowCommandArbitraries } from './workflow-commands';
import { disposeCampaign, RealWorkflow, type WorkflowModel } from './workflow-model-harness';
import { workflowSpec } from './workflow-spec';

const DEFAULT_RUNS = 40;
const DEFAULT_COMMANDS = 12;

describe('Workflow engine state-machine model', () => {
  afterAll(() => {
    disposeCampaign();
  });

  test('execution pagination matches the total SQLite ordering for every filter', () => {
    const state = fc.constantFrom<ExecutionState>('completed', 'failed');
    fc.assert(
      fc.property(
        fc.array(
          fc.record({
            workflowName: fc.constantFrom('alpha', 'beta'),
            state,
            createdAt: fc.integer({ min: 1, max: 5 }),
          }),
          { maxLength: 50 }
        ),
        fc.option(fc.constantFrom('alpha', 'beta'), { nil: undefined }),
        fc.option(state, { nil: undefined }),
        fc.integer({ min: 1, max: 10 }),
        fc.integer({ min: 0, max: 55 }),
        (rows, workflowName, stateFilter, limit, offset) => {
          const store = new WorkflowStore();
          try {
            const executions = rows.map(
              (row, index): Execution => ({
                id: `wf_property_${index.toString().padStart(3, '0')}`,
                workflowName: row.workflowName,
                state: row.state,
                input: {},
                steps: {},
                currentNodeIndex: 0,
                signals: {},
                createdAt: row.createdAt,
                updatedAt: row.createdAt,
              })
            );
            for (const exec of executions) store.save(exec);

            const expected = executions
              .filter(
                (exec) =>
                  (workflowName === undefined || exec.workflowName === workflowName) &&
                  (stateFilter === undefined || exec.state === stateFilter)
              )
              .sort(
                (left, right) =>
                  right.createdAt - left.createdAt ||
                  (left.id < right.id ? 1 : left.id > right.id ? -1 : 0)
              )
              .slice(offset, offset + limit)
              .map((exec) => exec.id);
            expect(
              store.list(workflowName, stateFilter, { limit, offset }).map((exec) => exec.id)
            ).toEqual(expected);
          } finally {
            store.close();
          }
        }
      ),
      { numRuns: 80 }
    );
  });

  test('workflow builders reject every generated invalid numeric bound', () => {
    const invalidTimeout = fc.constantFrom(
      -1,
      -0.1,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      Number.NEGATIVE_INFINITY
    );
    const invalidIterations = fc.constantFrom(
      0,
      -1,
      1.5,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      Number.MAX_SAFE_INTEGER + 1
    );
    const invalidPositiveDuration = fc.oneof(invalidTimeout, fc.constant(0));
    fc.assert(
      fc.property(
        invalidTimeout,
        invalidIterations,
        invalidPositiveDuration,
        (timeout, maxIterations, positiveDuration) => {
          expect(() =>
            new Workflow('invalid-step').step('work', () => null, { timeout })
          ).toThrow();
          expect(() => new Workflow('invalid-wait').waitFor('event', { timeout })).toThrow();
          expect(() =>
            new Workflow('invalid-loop').doUntil(
              () => false,
              (loop) => loop.step('work', () => null),
              { maxIterations }
            )
          ).toThrow();
          expect(() =>
            new Workflow('invalid-each')
              // biome-ignore lint/suspicious/useIterableCallbackReturn: Workflow.forEach extracts items
              .forEach(
                () => [],
                'work',
                () => null,
                { maxIterations }
              )
          ).toThrow();
          expect(() =>
            new Workflow('invalid-child-timeout').subWorkflow('child', () => ({}), {
              timeout: positiveDuration,
            })
          ).toThrow();
          expect(() =>
            new Workflow('invalid-child-poll').subWorkflow('child', () => ({}), {
              pollInterval: positiveDuration,
            })
          ).toThrow();
        }
      )
    );
  });

  test('generated graphs and operator histories preserve every invariant', async () => {
    const seed = optionalInteger(Bun.env.BUNQUEUE_WF_MODEL_SEED);
    const numRuns = optionalInteger(Bun.env.BUNQUEUE_WF_MODEL_RUNS) ?? DEFAULT_RUNS;
    const maxCommands = optionalInteger(Bun.env.BUNQUEUE_WF_MODEL_COMMANDS) ?? DEFAULT_COMMANDS;
    let run = 0;

    await fc.assert(
      fc.asyncProperty(
        workflowSpec(),
        fc.commands(workflowCommandArbitraries(), { maxCommands }),
        async (spec, commands) => {
          const real = RealWorkflow.create(spec, `${seed ?? 'r'}-${run++}`);
          const model: WorkflowModel = { implicitStart: true };
          try {
            // Every generated case executes a workflow even when fast-check draws an
            // empty command history. Operator commands model interleavings after the
            // real start; start itself is not optional model input.
            await real.start();
            await fc.asyncModelRun(() => ({ model, real }), commands);
            await real.assertSettles();
          } finally {
            await real.dispose();
          }
        }
      ),
      {
        endOnFailure: true,
        interruptAfterTimeLimit: 150_000,
        markInterruptAsFailure: false,
        numRuns,
        seed,
        verbose: 2,
      }
    );
  }, 180_000);
});

function optionalInteger(value: string | undefined): number | undefined {
  if (value === undefined || value === '') return undefined;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`expected a signed safe integer, received ${value}`);
  }
  return parsed;
}
