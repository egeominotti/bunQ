/**
 * Real-engine harness for the workflow state-machine model.
 *
 * All campaign runs share one SQLite file and use unique queue/workflow names. The
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
import { buildChildWorkflows, buildWorkflow } from './workflow-build';
import { emptyLedger, type Ledger, type WorkflowSpec } from './workflow-spec';
import { listAllExecutions } from './workflow-model-listing';

export interface WorkflowModel {
  /** The model starts every run before applying generated operator actions. */
  implicitStart: true;
}

export class RealWorkflow {
  private engine: Engine;
  private readonly dataPath: string;
  private readonly queueName: string;
  readonly ledger: Ledger = emptyLedger();
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
      maxNodeIndex: new Map(),
      terminalStates: new Map(),
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
    this.check();
  }

  async signal(event: string, payload: unknown): Promise<void> {
    if (!this.runId) return;
    try {
      await this.engine.signal(this.runId, event, payload);
      // Only accepted deliveries enter the oracle. Signals are first-wins, so a
      // rejected duplicate must preserve the original expected payload.
      this.invariants.delivered.set(event, payload);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (!/cannot receive the signal|was already received/.test(message)) throw err;
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

  async resumeCompensation(): Promise<void> {
    if (!this.runId) return;
    await this.resumeExecution(this.runId);
    this.check();
  }

  async abandonCompensation(): Promise<void> {
    if (!this.runId) return;
    await this.abandonExecution(this.runId);
    this.check();
  }

  execution(): Execution | null {
    return this.runId ? this.engine.getExecution(this.runId) : null;
  }

  check(): void {
    checkInvariants(this.invariants, this.execution(), this.relatedExecutions());
  }

  /**
   * Final liveness gate. Untimed gates are opened, timed gates are allowed to expire,
   * and parked rollbacks receive one retry followed by an explicit abandon. Thus
   * every generated graph must reach a real terminal state; `waiting` and
   * `compensation-stuck` cannot make an otherwise vacuous campaign pass.
   *
   * This model keeps real time: `simulatedClock` is process-global while the embedded
   * worker polls on real timers and may drain after force-close. Advancing it between
   * worker turns lets adjacent generated runs interfere. Direct clock properties test
   * the timer primitive; this integration uses bounded real delays until queue clocks
   * can be injected per engine.
   */
  async assertSettles(): Promise<void> {
    if (!this.runId) throw new Error('NON-VACUOUS violated: generated run was never started');
    await this.openUntimedGates();
    const resumed = new Set<string>();
    const deadline = Date.now() + 12_000;
    while (Date.now() < deadline) {
      const related = this.relatedExecutions();
      for (const exec of related.filter((candidate) => candidate.state === 'compensation-stuck')) {
        if (!resumed.has(exec.id)) {
          resumed.add(exec.id);
          await this.resumeExecution(exec.id);
        } else {
          await this.abandonExecution(exec.id);
        }
      }
      this.check();
      if (related.length > 0 && related.every((exec) => TERMINAL.has(exec.state))) return;
      await Bun.sleep(25);
    }
    this.check();
    const states = this.relatedExecutions()
      .map((exec) => `${exec.id}:${exec.state}`)
      .join(', ');
    throw new Error(
      `LIVENESS violated: executions did not terminate (${states})\n` +
        `spec=${JSON.stringify(this.spec)}\nledger=${JSON.stringify(this.ledger)}`
    );
  }

  async dispose(): Promise<void> {
    await this.engine.close(true);
  }

  private relatedExecutions(): Execution[] {
    if (!this.runId) return [];
    const all = listAllExecutions(this.engine);
    const ids = new Set([this.runId]);
    let changed = true;
    while (changed) {
      changed = false;
      for (const exec of all) {
        if (exec.parentExecutionId && ids.has(exec.parentExecutionId) && !ids.has(exec.id)) {
          ids.add(exec.id);
          changed = true;
        }
      }
    }
    return all.filter((exec) => ids.has(exec.id));
  }

  private async openUntimedGates(): Promise<void> {
    const events = new Set(
      this.spec.nodes
        .filter((node) => node.kind === 'waitFor' && node.timeout === undefined)
        .map((node) => node.event)
    );
    for (const event of events) {
      if (!this.invariants.delivered.has(event)) {
        await this.signal(event, { modelFinalizer: true });
      }
    }
  }

  private async resumeExecution(executionId: string): Promise<void> {
    if (this.engine.getExecution(executionId)?.state !== 'compensation-stuck') return;
    this.ledger.operatorActions.push({ kind: 'resume', executionId });
    try {
      await this.engine.resumeCompensation(executionId);
    } catch (error) {
      if (!operatorRace(error)) throw error;
    }
  }

  private async abandonExecution(executionId: string): Promise<void> {
    if (this.engine.getExecution(executionId)?.state !== 'compensation-stuck') return;
    this.ledger.operatorActions.push({ kind: 'abandon', executionId });
    try {
      await this.engine.abandonCompensation(executionId);
    } catch (error) {
      if (!operatorRace(error)) throw error;
    }
  }
}

const TERMINAL = new Set(['completed', 'failed']);

function operatorRace(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return /already being rolled back|not a parked unwind/.test(message);
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
