/**
 * TCP Health Tracker
 * Monitors connection health with ping and latency tracking
 */

import type { ConnectionHealth } from './types';

/** Health tracker configuration */
export interface HealthConfig {
  pingInterval: number;
  maxPingFailures: number;
  /**
   * Consecutive command timeouts before the link is concluded dead (0 = off).
   * Optional for backward compatibility; defaults to 3 when omitted.
   */
  maxCommandTimeouts?: number;
}

/** Default consecutive command-timeout threshold when not configured. */
const DEFAULT_MAX_COMMAND_TIMEOUTS = 3;

/**
 * Tracks connection health metrics
 */
export class HealthTracker {
  private consecutivePingFailures = 0;
  private consecutiveCommandTimeouts = 0;
  private lastSuccessAt: number | null = null;
  private lastErrorAt: number | null = null;
  private connectedAt: number | null = null;
  private totalCommands = 0;
  private totalErrors = 0;
  private readonly latencyHistory: number[] = [];
  private pingTimer: ReturnType<typeof setInterval> | null = null;

  private static readonly MAX_LATENCY_HISTORY = 10;

  constructor(private readonly config: HealthConfig) {}

  /** Record successful command */
  recordSuccess(latencyMs: number): void {
    this.lastSuccessAt = Date.now();
    this.totalCommands++;
    // A real response proves the link is alive: the prior timeouts were not a
    // sustained run, so reset the dead-link counter ("consecutive" must mean it).
    this.consecutiveCommandTimeouts = 0;
    this.recordLatency(latencyMs);
  }

  /** Record command error */
  recordError(): void {
    this.lastErrorAt = Date.now();
    this.totalErrors++;
  }

  /** Record command sent (for total count) */
  recordCommandSent(): void {
    this.totalCommands++;
  }

  /** Record connection established */
  recordConnected(): void {
    this.connectedAt = Date.now();
    this.consecutivePingFailures = 0;
    this.consecutiveCommandTimeouts = 0;
  }

  /** Record ping success */
  recordPingSuccess(latencyMs: number): void {
    // A successful ping is also proof the link is alive — clear both suspicions.
    this.consecutivePingFailures = 0;
    this.consecutiveCommandTimeouts = 0;
    this.recordLatency(latencyMs);
  }

  /** Record ping failure, returns true if max failures reached */
  recordPingFailure(): boolean {
    this.consecutivePingFailures++;
    this.lastErrorAt = Date.now();
    this.totalErrors++;
    return this.consecutivePingFailures >= this.config.maxPingFailures;
  }

  /**
   * Record a command timeout. Returns true when the configured consecutive
   * threshold is reached (and the feature is enabled), signalling the caller to
   * force a reconnect. Any intervening success resets the counter, so this only
   * fires on a sustained run of timeouts — the signature of a dead/half-open
   * socket where writes succeed but no response ever comes back.
   */
  recordCommandTimeout(): boolean {
    const max = this.config.maxCommandTimeouts ?? DEFAULT_MAX_COMMAND_TIMEOUTS;
    if (max <= 0) return false;
    this.consecutiveCommandTimeouts++;
    return this.consecutiveCommandTimeouts >= max;
  }

  /** Get current health metrics */
  getHealth(state: 'connected' | 'connecting' | 'disconnected' | 'closed'): ConnectionHealth {
    const avgLatency =
      this.latencyHistory.length > 0
        ? this.latencyHistory.reduce((a, b) => a + b, 0) / this.latencyHistory.length
        : 0;

    return {
      healthy: state === 'connected' && this.consecutivePingFailures < this.config.maxPingFailures,
      state,
      lastSuccessAt: this.lastSuccessAt,
      lastErrorAt: this.lastErrorAt,
      avgLatencyMs: Math.round(avgLatency * 100) / 100,
      consecutivePingFailures: this.consecutivePingFailures,
      consecutiveCommandTimeouts: this.consecutiveCommandTimeouts,
      totalCommands: this.totalCommands,
      totalErrors: this.totalErrors,
      uptimeMs: this.connectedAt ? Date.now() - this.connectedAt : 0,
    };
  }

  /** Start ping timer */
  startPing(pingFn: () => Promise<void>): void {
    if (this.config.pingInterval <= 0) return;
    this.stopPing();
    this.pingTimer = setInterval(() => {
      void pingFn();
    }, this.config.pingInterval);
  }

  /** Stop ping timer */
  stopPing(): void {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
  }

  /** Record latency for averaging */
  private recordLatency(latencyMs: number): void {
    this.latencyHistory.push(latencyMs);
    if (this.latencyHistory.length > HealthTracker.MAX_LATENCY_HISTORY) {
      this.latencyHistory.shift();
    }
  }
}
