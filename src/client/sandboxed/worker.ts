/** Public SandboxedWorker façade. Runtime lives in sandboxed/runtime/. */
import { SandboxedRecovery } from './runtime/recovery';

export class SandboxedWorker<T = unknown> extends SandboxedRecovery<T> {}
