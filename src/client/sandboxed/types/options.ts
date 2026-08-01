import type { SharedManager } from '../../manager';
import type { ConnectionOptions } from '../../types';

export interface SandboxedWorkerOptions {
  processor: string;
  concurrency?: number;
  maxMemory?: number;
  timeout?: number;
  autoRestart?: boolean;
  maxRestarts?: number;
  pollInterval?: number;
  manager?: SharedManager;
  connection?: ConnectionOptions;
  heartbeatInterval?: number;
  idleTimeout?: number;
  idleRecycleMs?: number;
  autoStart?: boolean;
  autoStartPollMs?: number;
}

export interface RequiredSandboxedWorkerOptions {
  processor: string;
  concurrency: number;
  maxMemory: number;
  timeout: number;
  autoRestart: boolean;
  maxRestarts: number;
  pollInterval: number;
}
