import { WorkflowEmitter } from '../../../src/client/workflow';
import { CoverageTracker, ensure, type CoverageMode } from '../support/tracker';

export function runWorkflowEmitterContract(mode: CoverageMode): CoverageTracker {
  const tracker = new CoverageTracker(mode, 'workflow-emitter-contract');
  const emitter = new WorkflowEmitter();
  const typed: string[] = [];
  const global: string[] = [];
  const typedListener = (event: { type: string }): void => {
    typed.push(event.type);
  };
  const globalListener = (event: { type: string }): void => {
    global.push(event.type);
  };

  tracker.call('WorkflowEmitter', 'on', () => emitter.on('step:started', typedListener));
  tracker.call('WorkflowEmitter', 'onAny', () => emitter.onAny(globalListener));
  tracker.call('WorkflowEmitter', 'emitStep', () =>
    emitter.emitStep('step:started', 'execution-1', 'workflow', 'step')
  );
  tracker.call('WorkflowEmitter', 'emitWorkflow', () =>
    emitter.emitWorkflow('workflow:started', 'execution-1', 'workflow', 'running')
  );
  tracker.call('WorkflowEmitter', 'emitSignal', () =>
    emitter.emitSignal('signal:received', 'execution-1', 'workflow', 'approval', { approved: true })
  );
  ensure(typed.length === 1, 'typed workflow emitter listener did not receive its event');
  ensure(global.length === 3, 'global workflow emitter listener did not receive every event');
  tracker.call('WorkflowEmitter', 'off', () => emitter.off('step:started', typedListener));
  tracker.call('WorkflowEmitter', 'offAny', () => emitter.offAny(globalListener));
  tracker.call('WorkflowEmitter', 'removeAllListeners', () => emitter.removeAllListeners());
  emitter.emitStep('step:started', 'execution-2', 'workflow', 'step');
  ensure(typed.length === 1 && global.length === 3, 'removed workflow listeners still fired');
  return tracker;
}
