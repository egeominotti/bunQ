/**
 * bunqueue Cloud Agent
 * Main orchestrator that collects telemetry and sends it to bunqueue Cloud.
 *
 * Two channels:
 *   1. HTTP POST — all data (snapshots + events every N seconds)
 *   2. WebSocket — commands only (dashboard → bunqueue, bunqueue → command results)
 *
 * Zero overhead when disabled (BUNQUEUE_CLOUD_URL not set).
 */

import type { QueueManager } from '../../application/queueManager';
import type { JobEvent } from '../../domain/types/queue';
import type { CloudConfig, CloudEvent, CloudSnapshot } from './types';
import { loadCloudConfig } from './config';
import { collectSnapshot, type ServerHandles } from './snapshotCollector';
import { redactData } from './redact';
import { HttpSender } from './httpSender';
import { WsSender } from './wsSender';
import { handleCommand, type CommandContext } from './commandHandler';
import { cloudLog } from './logger';

const EVENT_BUFFER_MAX = 1000;

export class CloudAgent {
  private readonly config: CloudConfig;
  private readonly instanceId: string;
  private readonly startedAt = Date.now();
  private readonly httpSender: HttpSender;
  private readonly wsSender: WsSender | null;
  private snapshotTimer: ReturnType<typeof setInterval> | null = null;
  private statsUpdateTimer: ReturnType<typeof setInterval> | null = null;
  private unsubscribeEvents: (() => void) | null = null;
  private sequenceId = 0;
  private snapshotCount = 0;
  private stopped = false;
  private serverHandles?: ServerHandles;
  /** Event buffer — flushed into each HTTP snapshot */
  private readonly eventBuffer: CloudEvent[] = [];

  constructor(
    private readonly queueManager: QueueManager,
    config: CloudConfig
  ) {
    this.config = config;
    this.instanceId = config.instanceId;
    this.httpSender = new HttpSender(config);
    this.wsSender = config.useWebSocket ? new WsSender(config, this.instanceId) : null;
  }

  /** Set server handles for connection stats */
  setServerHandles(handles: ServerHandles): void {
    this.serverHandles = handles;
  }

  /** Create and start a Cloud agent if configured. Returns null if disabled. */
  static create(queueManager: QueueManager, dataPath?: string): CloudAgent | null {
    const config = loadCloudConfig(dataPath);
    if (!config) return null;

    const agent = new CloudAgent(queueManager, config);
    agent.start();
    return agent;
  }

  /** Create from pre-resolved config (used by config file flow) */
  static createFromConfig(queueManager: QueueManager, config: CloudConfig): CloudAgent {
    const agent = new CloudAgent(queueManager, config);
    agent.start();
    return agent;
  }

  /** Start both channels */
  start(): void {
    cloudLog.info('Connecting to dashboard', {
      url: this.config.url,
      instance: this.config.instanceName,
      id: this.instanceId,
      intervalMs: this.config.intervalMs,
      ws: this.config.useWebSocket,
    });

    // Channel 1: HTTP snapshots
    if (this.config.useHttp) {
      void this.sendSnapshot().then(() => {
        this.scheduleNext();
      });
    }

    // Channel 2: WebSocket — commands only (dashboard → bunqueue)
    if (this.wsSender) {
      if (this.config.remoteCommands) {
        cloudLog.info('Remote commands enabled');
        const commandContext: CommandContext = {
          getSnapshot: () => this.getSnapshot(),
        };
        this.wsSender.setCommandHandler((cmd) => {
          handleCommand(this.queueManager, cmd, commandContext)
            .then((result) => {
              this.wsSender?.sendRaw(result);
              // HTTP ingest disabled — dashboard uses WS commands
              // if (result.success) {
              //   this.sendImmediateSnapshot();
              // }
            })
            .catch((err: unknown) => {
              cloudLog.debug('Command handler error', { error: String(err) });
            });
        });
      }

      this.wsSender.connect();
    }

    // Subscribe to events — buffer for next HTTP snapshot
    this.subscribeToEvents();
  }

  /** Graceful shutdown: send final snapshot, close connections */
  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;

    if (this.snapshotTimer) {
      clearTimeout(this.snapshotTimer);
      this.snapshotTimer = null;
    }

    if (this.statsUpdateTimer) {
      clearInterval(this.statsUpdateTimer);
      this.statsUpdateTimer = null;
    }

    if (this.unsubscribeEvents) {
      this.unsubscribeEvents();
      this.unsubscribeEvents = null;
    }

    // Send final shutdown snapshot (best-effort, 2s timeout)
    try {
      const snapshot = await collectSnapshot({
        queueManager: this.queueManager,
        instanceId: this.instanceId,
        instanceName: this.config.instanceName,
        startedAt: this.startedAt,
        sequenceId: ++this.sequenceId,
        serverHandles: this.serverHandles,
        includeHeavy: true,
        redactFields: this.config.redactFields,
        includeJobData: this.config.includeJobData,
      });
      snapshot.shutdown = true;

      await Promise.race([this.httpSender.send(snapshot), Bun.sleep(2000)]);
    } catch {
      // Best-effort
    }

    this.wsSender?.stop();
    cloudLog.info('Disconnected from dashboard');
  }

  /** Compute next interval from last compressed payload size */
  private computeInterval(): number {
    const kb = this.httpSender.lastCompressedKB;
    if (kb < 50) return 5_000;
    if (kb < 200) return 10_000;
    if (kb < 500) return 20_000;
    return 30_000;
  }

  /** Schedule next snapshot with dynamic interval */
  private scheduleNext(): void {
    if (this.stopped) return;
    const intervalMs = this.computeInterval();
    cloudLog.debug('Next snapshot', { intervalMs, compressedKB: this.httpSender.lastCompressedKB });
    this.snapshotTimer = setTimeout(() => {
      void this.sendSnapshot().then(() => {
        this.scheduleNext();
      });
    }, intervalMs);
  }

  /** Collect and send a snapshot via HTTP */
  private async sendSnapshot(_forceHeavy = false): Promise<void> {
    try {
      this.snapshotCount++;

      const snapshot = await collectSnapshot({
        queueManager: this.queueManager,
        instanceId: this.instanceId,
        instanceName: this.config.instanceName,
        startedAt: this.startedAt,
        sequenceId: ++this.sequenceId,
        serverHandles: this.serverHandles,
        includeHeavy: true,
        redactFields: this.config.redactFields,
        includeJobData: this.config.includeJobData,
      });

      // Embed buffered events in snapshot
      if (this.eventBuffer.length > 0) {
        snapshot.events = this.eventBuffer.splice(0);
      }

      await this.httpSender.send(snapshot);
    } catch (err) {
      cloudLog.debug('Snapshot send failed', { error: String(err) });
    }
  }

  /** Subscribe to job events — buffer for next HTTP snapshot */
  private subscribeToEvents(): void {
    this.unsubscribeEvents = this.queueManager.subscribe((event: JobEvent) => {
      // Apply event filter
      if (
        this.config.eventFilter.length > 0 &&
        !this.config.eventFilter.includes(event.eventType)
      ) {
        return;
      }

      const cloudEvent: CloudEvent = {
        instanceId: this.instanceId,
        timestamp: event.timestamp,
        jobEvent: {
          eventType: event.eventType,
          queue: event.queue,
          jobId: event.jobId,
          error: event.error,
          progress: event.progress,
          data: this.config.includeJobData ? this.redactData(event.data) : undefined,
          prev: event.prev,
          delay: event.delay,
        },
      };

      // Ring buffer — drop oldest when full
      if (this.eventBuffer.length >= EVENT_BUFFER_MAX) {
        this.eventBuffer.shift();
      }
      this.eventBuffer.push(cloudEvent);
    });
  }

  /** Redact sensitive fields from job data (delegates to the shared helper) */
  private redactData(data: unknown): unknown {
    return redactData(data, this.config.redactFields);
  }

  getInstanceId(): string {
    return this.instanceId;
  }

  /** Collect a fresh snapshot on demand (used by snapshot:get command) */
  async getSnapshot(): Promise<CloudSnapshot> {
    const snapshot = await collectSnapshot({
      queueManager: this.queueManager,
      instanceId: this.instanceId,
      instanceName: this.config.instanceName,
      startedAt: this.startedAt,
      sequenceId: ++this.sequenceId,
      serverHandles: this.serverHandles,
      includeHeavy: true,
      redactFields: this.config.redactFields,
      includeJobData: this.config.includeJobData,
    });
    return snapshot;
  }
}
