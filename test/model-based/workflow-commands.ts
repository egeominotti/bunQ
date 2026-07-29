/**
 * Generated command vocabulary for the workflow state-machine model.
 *
 * Commands are deliberately coarse — start, signal, settle, restart — because the
 * interesting interleavings are not between engine internals but between an
 * operator's actions and whatever the worker happens to be doing at that instant.
 * `SignalCommand` fired while a step is mid-flight is exactly the shape that used
 * to destroy the payload.
 */

import fc from 'fast-check';
import type { RealWorkflow, WorkflowModel } from './workflow-model-harness';
import { EVENTS } from './workflow-spec';

type Cmd = fc.AsyncCommand<WorkflowModel, RealWorkflow>;

class StartCommand implements Cmd {
  check(model: WorkflowModel): boolean {
    return !model.started;
  }
  async run(model: WorkflowModel, real: RealWorkflow): Promise<void> {
    model.started = true;
    await real.start();
    real.check();
  }
  toString(): string {
    return 'start()';
  }
}

class SignalCommand implements Cmd {
  constructor(
    private readonly event: string,
    private readonly payload: number
  ) {}
  check(model: WorkflowModel): boolean {
    return model.started;
  }
  async run(model: WorkflowModel, real: RealWorkflow): Promise<void> {
    // The harness owns whether a delivery counts: it withdraws the record when the
    // engine refuses a signal to a run that is no longer live. Keeping that decision
    // in one place is why this does not try to second-guess it.
    model.delivered.add(this.event);
    await real.signal(this.event, { v: this.payload });
    real.check();
  }
  toString(): string {
    return `signal(${this.event}, ${this.payload})`;
  }
}

class SettleCommand implements Cmd {
  constructor(private readonly ms: number) {}
  check(): boolean {
    return true;
  }
  async run(_model: WorkflowModel, real: RealWorkflow): Promise<void> {
    await real.settle(this.ms);
  }
  toString(): string {
    return `settle(${this.ms}ms)`;
  }
}

class RestartCommand implements Cmd {
  check(model: WorkflowModel): boolean {
    return model.started;
  }
  async run(_model: WorkflowModel, real: RealWorkflow): Promise<void> {
    await real.restart();
  }
  toString(): string {
    return 'restart()';
  }
}

/**
 * `recover()` while the engine is still running, which the docs allow. It used to
 * start a SECOND driver for the node the engine was already on, and a second unwind
 * for a sub-workflow already being rolled back.
 */
class RecoverCommand implements Cmd {
  check(model: WorkflowModel): boolean {
    return model.started;
  }
  async run(_model: WorkflowModel, real: RealWorkflow): Promise<void> {
    await real.recoverLive();
  }
  toString(): string {
    return 'recoverLive()';
  }
}

/** `recover()` timed to land inside an in-flight unwind. See the harness. */
class RecoverDuringUnwindCommand implements Cmd {
  check(model: WorkflowModel): boolean {
    return model.started;
  }
  async run(_model: WorkflowModel, real: RealWorkflow): Promise<void> {
    await real.recoverDuringUnwind();
  }
  toString(): string {
    return 'recoverDuringUnwind()';
  }
}

/** Operator retries the reversal that parked the run. No-op unless parked. */
class ResumeCompensationCommand implements Cmd {
  check(model: WorkflowModel): boolean {
    return model.started;
  }
  async run(_model: WorkflowModel, real: RealWorkflow): Promise<void> {
    await real.resumeCompensation();
  }
  toString(): string {
    return 'resumeCompensation()';
  }
}

/** Operator accepts a partial rollback. No-op unless parked. */
class AbandonCompensationCommand implements Cmd {
  check(model: WorkflowModel): boolean {
    return model.started;
  }
  async run(_model: WorkflowModel, real: RealWorkflow): Promise<void> {
    real.abandonCompensation();
  }
  toString(): string {
    return 'abandonCompensation()';
  }
}

export function workflowCommandArbitraries(): fc.Arbitrary<Cmd>[] {
  return [
    fc.constant(new StartCommand()),
    fc.constant(new RecoverCommand()),
    fc.constant(new RecoverDuringUnwindCommand()),
    fc.constant(new ResumeCompensationCommand()),
    fc.constant(new AbandonCompensationCommand()),
    fc
      .tuple(fc.constantFrom(...EVENTS), fc.integer({ min: 0, max: 99 }))
      .map(([event, payload]) => new SignalCommand(event, payload)),
    fc.integer({ min: 10, max: 220 }).map((ms) => new SettleCommand(ms)),
    fc.constant(new RestartCommand()),
  ];
}
