/**
 * Bunqueue — Circuit Breaker for worker protection
 */

import type { Worker } from '../worker/worker';
import type { CircuitBreakerConfig, CircuitState } from './types';

export class WorkerCircuitBreaker {
  private state: CircuitState = 'closed';
  private failures = 0;
  private timer: ReturnType<typeof setTimeout> | null = null;
  private destroyed = false;
  private readonly config: CircuitBreakerConfig;
  private readonly worker: Worker;

  constructor(config: CircuitBreakerConfig, worker: Worker) {
    this.config = config;
    this.worker = worker;
  }

  get currentState(): CircuitState {
    return this.state;
  }

  isOpen(): boolean {
    return this.state === 'open';
  }

  onSuccess(): void {
    if (this.destroyed) return;
    if (this.state === 'half-open') {
      this.state = 'closed';
      this.failures = 0;
      this.config.onClose?.();
    } else if (this.state === 'closed') {
      this.failures = 0;
    }
  }

  onFailure(): void {
    if (this.destroyed) return;
    this.failures++;
    const threshold = this.config.threshold ?? 5;

    if (this.state === 'half-open' || this.failures >= threshold) {
      this.open();
    }
  }

  private open(): void {
    if (this.destroyed) return;
    this.state = 'open';
    this.config.onOpen?.(this.failures);
    if (this.destroyed) return;
    this.worker.pause();

    const resetTimeout = this.config.resetTimeout ?? 30000;
    if (this.timer) clearTimeout(this.timer);
    const timer = setTimeout(() => {
      if (this.destroyed || this.timer !== timer) return;
      this.timer = null;
      this.state = 'half-open';
      this.config.onHalfOpen?.();
      if (this.destroyed) return;
      this.worker.resume();
    }, resetTimeout);
    this.timer = timer;
  }

  reset(): void {
    if (this.destroyed) return;
    this.state = 'closed';
    this.failures = 0;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (this.worker.isPaused()) {
      this.worker.resume();
    }
  }

  destroy(): void {
    this.destroyed = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  }
}
