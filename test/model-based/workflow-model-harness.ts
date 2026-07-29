/**
 * Real-engine harness for the workflow state-machine model.
 *
 * All runs in a campaign share ONE SQLite file, and isolate through a unique queue
 * name plus a unique workflow name per run. That is not a stylistic choice: the
 * embedded QueueManager is a process-wide singleton that binds to the FIRST
 * dataPath it is given and silently ignores every later one, so a per-run database
 * would leave the manager writing to a directory the previous run already deleted
 * (SQLITE_IOERR_VNODE). Unique workflow names also keep restart() honest — recover()
 * skips executions whose workflow is not registered, so a run only ever recovers
 * its own.
 *
 * `restart()` is the model's stand-in for a crash: it closes the engine and opens
 * a fresh one over the same database, then calls recover(). That exercises the
 * same code path an operator hits after a pod restart, without paying for a real
 * SIGKILL on every generated command (test/workflow-e2e-crash-recovery.test.ts
 * covers the genuine SIGKILL path).
 */

import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Engine, type Execution } from '../../src/client/workflow';
import { checkInvariants, type InvariantState } from './workflow-invariants';
import {
  buildWorkflow,
  type Ledger,
  type WorkflowSpec,
  buildChildWorkflows,
} from './workflow-spec';

export interface WorkflowModel {
  started: boolean;
  delivered: Set<string>;
}

export class RealWorkflow {
  private engine: Engine;
  private readonly dataPath: string;
  private readonly queueName: string;
  readonly ledger: Ledger = { steps: [], compensations: [] };
  readonly invariants: InvariantState;
  runId: string | null = null;

  private constructor(
    readonly spec: WorkflowSpec,
    dir: string,
    queueName: string
  ) {
    this.dataPath = join(dir, 'wf.db');
    this.queueName = queueName;
    this.invariants = {
      spec,
      ledger: this.ledger,
      delivered: new Map(),
      maxNodeIndex: 0,
      sawTerminal: false,
      terminalState: null,
      compensable: compensableSteps(spec),
      retryBudget: retryBudgets(spec),
    };
    this.engine = this.openEngine();
  }

  static create(spec: WorkflowSpec, tag: string): RealWorkflow {
    // Unique workflow name so restart()/recover() never touches another run's rows.
    const scoped: WorkflowSpec = { ...spec, name: `gen_${tag}` };
    return new RealWorkflow(scoped, campaignDir(), `__wf:model:${tag}`);
  }

  private openEngine(): Engine {
    const engine = new Engine({
      embedded: true,
      dataPath: this.dataPath,
      queueName: this.queueName,
      concurrency: 2,
    });
    // Children first: a parent's subWorkflow node resolves its child by name at run
    // time, and recover() skips executions whose workflow is not registered.
    for (const child of buildChildWorkflows(this.spec, this.ledger)) engine.register(child);
    engine.register(buildWorkflow(this.spec, this.ledger));
    return engine;
  }

  async start(): Promise<void> {
    const run = await this.engine.start(this.spec.name, { seed: 1 });
    this.runId = run.id;
  }

  async signal(event: string, payload: unknown): Promise<void> {
    if (!this.runId) return;
    // Record BEFORE delivering: if the engine ACCEPTS a signal and then loses it, I1
    // must fire. That is the point of recording first and it is preserved.
    this.invariants.delivered.set(event, payload);
    try {
      await this.engine.signal(this.runId, event, payload);
    } catch (err) {
      // A run that is no longer running or waiting REFUSES the signal rather than
      // writing it into a closed record. A refusal is not a lost delivery, it is the
      // absence of one, so the provisional record is withdrawn. Anything else
      // rethrows: a signal that fails for another reason is still a defect.
      const message = err instanceof Error ? err.message : String(err);
      if (!/cannot receive the signal/.test(message)) throw err;
      this.invariants.delivered.delete(event);
    }
  }

  /** Let the engine make progress, then assert every invariant. */
  async settle(ms: number): Promise<void> {
    await Bun.sleep(ms);
    this.check();
  }

  /**
   * Close and reopen over the same database — the model's crash/restart.
   *
   * KNOWN LIMIT, and the reason a real defect escaped this model: this is a graceful
   * in-PROCESS restart, so module-level state survives it. `compensator.ts` keeps its
   * in-flight unwind claims in a module `Set`; after this "crash" the process still
   * holds them, which masks any defect whose trigger is a fresh process meeting rows
   * that a dead one left mid-unwind. `recover()` re-dispatching a sub-workflow's
   * compensate handlers is exactly that shape, and it needs a real `SIGKILL` plus a
   * new process to reproduce — see test/repro-workflow-recover-double-compensate.
   *
   * Making this a true process boundary would need the harness to spawn and kill a
   * child per restart, which is a different, much slower model. Until then: defects
   * that depend on losing process memory belong in a dedicated repro, not here.
   */
  async restart(): Promise<void> {
    await this.engine.close(true);
    this.engine = this.openEngine();
    await this.engine.recover();
    this.check();
  }

  /**
   * `recover()` on a LIVE engine, with nothing closed.
   *
   * Distinct from `restart()`, and the distinction is the point: `recover()` is
   * public, documented, and lists every `running`/`compensating` execution, so a
   * caller can legitimately invoke it while the engine is still driving one. That is
   * the shape that produced a node running twice and a sub-workflow compensating
   * twice, and it was unreachable through `restart()` alone because closing the
   * engine first removes the concurrent driver.
   */
  async recoverLive(): Promise<void> {
    await this.engine.recover();
    this.check();
  }

  /**
   * `recover()` aimed at the window where an unwind is actually in flight.
   *
   * The random `recoverLive()` almost never lands there: a rollback is over in
   * milliseconds while commands are seconds apart, so the concurrent-driver class of
   * defect stays out of reach no matter how many campaigns run. This waits briefly
   * for `compensating` and only then recovers, turning a lottery into a targeted
   * probe. It is a no-op when no unwind is running, so it costs a few ms otherwise.
   */
  async recoverDuringUnwind(): Promise<void> {
    const deadline = Date.now() + 250;
    while (Date.now() < deadline) {
      if (this.execution()?.state === 'compensating') {
        await this.engine.recover();
        this.check();
        return;
      }
      await Bun.sleep(2);
    }
  }

  /** Operator action on a parked unwind: retry the reversal that failed. */
  async resumeCompensation(): Promise<void> {
    if (!this.runId) return;
    if (this.execution()?.state !== 'compensation-stuck') return;
    try {
      await this.engine.resumeCompensation(this.runId);
    } catch {
      // A lost claim is reported as a rejection; the invariants still have to hold.
    }
    this.check();
  }

  /** Operator action on a parked unwind: accept a partial rollback. */
  abandonCompensation(): void {
    if (!this.runId) return;
    if (this.execution()?.state !== 'compensation-stuck') return;
    this.engine.abandonCompensation(this.runId);
    this.check();
  }

  execution(): Execution | null {
    return this.runId ? this.engine.getExecution(this.runId) : null;
  }

  check(): void {
    checkInvariants(this.invariants, this.execution());
  }

  /**
   * Final liveness gate: given enough time and every signal the spec asks for, a run
   * must come to rest. Resting states are terminal (completed/failed) or awaiting an
   * outside decision — `waiting` for a signal, `compensation-stuck` for an operator.
   * Being left in 'running' or 'compensating' means a node failed to enqueue its
   * successor: a wedge no amount of waiting will clear.
   */
  async assertSettles(): Promise<void> {
    if (!this.runId) return;
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline) {
      if (isAtRest(this.execution()?.state)) break;
      await Bun.sleep(25);
    }
    this.check();
    const state = this.execution()?.state;
    if (!isAtRest(state)) {
      throw new Error(
        `LIVENESS violated: execution wedged in "${state}"\n` +
          `spec=${JSON.stringify(this.spec)}\nledger=${JSON.stringify(this.ledger)}`
      );
    }
  }

  async dispose(): Promise<void> {
    await this.engine.close(true);
  }
}

const AT_REST = new Set(['completed', 'failed', 'waiting', 'compensation-stuck']);

function isAtRest(state: string | undefined): boolean {
  return state !== undefined && AT_REST.has(state);
}

let sharedDir: string | null = null;

/** One directory for the whole campaign — see the note at the top of this file. */
function campaignDir(): string {
  sharedDir ??= mkdtempSync(join(tmpdir(), 'bq-wf-model-'));
  return sharedDir;
}

/** Remove the campaign directory. Safe to call when no campaign ever ran. */
export function disposeCampaign(): void {
  if (sharedDir) rmSync(sharedDir, { recursive: true, force: true });
  sharedDir = null;
}

/**
 * Braces are load-bearing here. Written as bare `else if` chains with unbraced
 * loop bodies, the `else` after `for (...) if (s.compensate) out.add(...)` binds to
 * that inner `if`, not to the chain — branch paths silently never registered, and
 * the model reported a real compensation as illegitimate.
 */
function compensableSteps(spec: WorkflowSpec): Set<string> {
  const out = new Set<string>();
  for (const node of spec.nodes) {
    if (node.kind === 'step') {
      if (node.step.compensate) out.add(node.step.name);
    } else if (node.kind === 'parallel') {
      for (const s of node.steps) {
        if (s.compensate) out.add(s.name);
      }
    } else if (node.kind === 'branch') {
      for (const p of node.paths) {
        for (const s of p.steps) {
          if (s.compensate) out.add(s.name);
        }
      }
    } else if (node.kind === 'forEach') {
      if (node.step.compensate) {
        for (let i = 0; i < node.count; i++) out.add(`${node.step.name}:${i}`);
      }
    } else if (node.kind === 'subWorkflow') {
      // The child's own step compensates through the CHILD's unwind, and it writes
      // into the same ledger, so the parent's invariants must know the name is
      // legitimately compensable. Without this every sub-workflow rollback trips I7
      // ("compensated but declares no handler") and the model fails for the wrong
      // reason instead of finding real defects.
      if (node.step.compensate) out.add(node.step.name);
    }
  }
  return out;
}

function retryBudgets(spec: WorkflowSpec): Map<string, number> {
  const out = new Map<string, number>();
  for (const node of spec.nodes) {
    if (node.kind === 'step') out.set(node.step.name, node.step.retry);
    else if (node.kind === 'parallel') for (const s of node.steps) out.set(s.name, s.retry);
    else if (node.kind === 'branch')
      for (const p of node.paths) for (const s of p.steps) out.set(s.name, s.retry);
    else if (node.kind === 'forEach') out.set(node.step.name, node.step.retry);
  }
  return out;
}
