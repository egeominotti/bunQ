/**
 * Generated command vocabulary for the workflow state-machine model.
 *
 * Start is implicit and mandatory in the harness. Generated commands are the
 * operator actions that can race the worker after that point: signal, settle,
 * restart, live recovery and rollback control.
 */

import fc from 'fast-check';
import type { RealWorkflow, WorkflowModel } from './workflow-model-harness';
import { EVENTS } from './workflow-spec';

type Cmd = fc.AsyncCommand<WorkflowModel, RealWorkflow>;

class SignalCommand implements Cmd {
  constructor(
    private readonly event: string,
    private readonly payload: number
  ) {}
  check(): boolean {
    return true;
  }
  async run(_model: WorkflowModel, real: RealWorkflow): Promise<void> {
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
  check(): boolean {
    return true;
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
  check(): boolean {
    return true;
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
  check(): boolean {
    return true;
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
  check(): boolean {
    return true;
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
  check(): boolean {
    return true;
  }
  async run(_model: WorkflowModel, real: RealWorkflow): Promise<void> {
    await real.abandonCompensation();
  }
  toString(): string {
    return 'abandonCompensation()';
  }
}

export function workflowCommandArbitraries(): fc.Arbitrary<Cmd>[] {
  return [
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
