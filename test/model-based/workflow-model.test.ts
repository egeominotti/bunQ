/**
 * Property-based state-machine model for the Workflow Engine.
 *
 * Generates a random workflow graph (steps, waitFor gates, parallel groups,
 * branches, forEach loops, maps — with scripted success/failure/flake behaviour)
 * and then a random history of operator actions against it: start, signal, settle,
 * restart. After every command it asserts the invariants in workflow-invariants.ts.
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

import { afterAll, describe, test } from 'bun:test';
import fc from 'fast-check';
import { workflowCommandArbitraries } from './workflow-commands';
import { disposeCampaign, RealWorkflow, type WorkflowModel } from './workflow-model-harness';
import { workflowSpec } from './workflow-spec';

const DEFAULT_RUNS = 40;
const DEFAULT_COMMANDS = 12;

describe('Workflow engine state-machine model', () => {
  afterAll(() => {
    disposeCampaign();
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
          const model: WorkflowModel = { started: false, delivered: new Set() };
          try {
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
