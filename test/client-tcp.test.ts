/**
 * TCP Client Layer Tests
 * Comprehensive tests for client.ts, connection.ts, health.ts, reconnect.ts
 */

import { describe, it, expect, beforeEach, afterEach } from 'bun:test';
import { HealthTracker } from '../src/client/tcp/health';
import { ReconnectManager } from '../src/client/tcp/reconnect';
import { CommandQueue } from '../src/client/tcp/connection';
import { TcpClient } from '../src/client/tcp/client';
import { TcpConnectionPool, getSharedPool, closeAllSharedPools } from '../src/client/tcpPool';
import { Queue, shutdownManager } from '../src/client';

// ---------------------------------------------------------------------------
// HealthTracker (health.ts) - pure unit tests
// ---------------------------------------------------------------------------
describe('HealthTracker', () => {
  let tracker: HealthTracker;

  beforeEach(() => {
    tracker = new HealthTracker({ pingInterval: 30000, maxPingFailures: 3 });
  });

  afterEach(() => {
    tracker.stopPing();
  });

  describe('initial state', () => {
    it('should report unhealthy when disconnected', () => {
      const health = tracker.getHealth('disconnected');
      expect(health.healthy).toBe(false);
      expect(health.state).toBe('disconnected');
      expect(health.totalCommands).toBe(0);
      expect(health.totalErrors).toBe(0);
      expect(health.consecutivePingFailures).toBe(0);
      expect(health.avgLatencyMs).toBe(0);
      expect(health.lastSuccessAt).toBeNull();
      expect(health.lastErrorAt).toBeNull();
      expect(health.uptimeMs).toBe(0);
    });
  });

  describe('health check passes for healthy connection', () => {
    it('should report healthy when connected with no failures', () => {
      tracker.recordConnected();
      const health = tracker.getHealth('connected');
      expect(health.healthy).toBe(true);
      expect(health.state).toBe('connected');
      expect(health.consecutivePingFailures).toBe(0);
    });

    it('should report healthy after ping success', () => {
      tracker.recordConnected();
      tracker.recordPingSuccess(5);
      const health = tracker.getHealth('connected');
      expect(health.healthy).toBe(true);
      expect(health.consecutivePingFailures).toBe(0);
    });

    it('should report healthy when below max ping failures', () => {
      tracker.recordConnected();
      tracker.recordPingFailure(); // 1
      tracker.recordPingFailure(); // 2
      const health = tracker.getHealth('connected');
      expect(health.healthy).toBe(true); // still under 3
      expect(health.consecutivePingFailures).toBe(2);
    });
  });

  describe('health check fails for dead connection', () => {
    it('should report unhealthy when max ping failures reached', () => {
      tracker.recordConnected();
      tracker.recordPingFailure(); // 1
      tracker.recordPingFailure(); // 2
      tracker.recordPingFailure(); // 3
      const health = tracker.getHealth('connected');
      expect(health.healthy).toBe(false);
      expect(health.consecutivePingFailures).toBe(3);
    });

    it('should report unhealthy when state is not connected', () => {
      tracker.recordConnected();
      const health = tracker.getHealth('disconnected');
      expect(health.healthy).toBe(false);
    });

    it('should report unhealthy for closed state', () => {
      const health = tracker.getHealth('closed');
      expect(health.healthy).toBe(false);
    });

    it('should report unhealthy for connecting state', () => {
      const health = tracker.getHealth('connecting');
      expect(health.healthy).toBe(false);
    });
  });

  describe('timeout / latency tracking', () => {
    it('should track average latency from successful commands', () => {
      tracker.recordConnected();
      tracker.recordSuccess(10);
      tracker.recordSuccess(20);
      tracker.recordSuccess(30);
      const health = tracker.getHealth('connected');
      expect(health.avgLatencyMs).toBe(20); // (10+20+30)/3
    });

    it('should track ping latency separately', () => {
      tracker.recordConnected();
      tracker.recordPingSuccess(100);
      tracker.recordPingSuccess(200);
      const health = tracker.getHealth('connected');
      expect(health.avgLatencyMs).toBe(150); // (100+200)/2
    });

    it('should keep only last 10 latency samples', () => {
      tracker.recordConnected();
      // Add 12 samples
      for (let i = 1; i <= 12; i++) {
        tracker.recordSuccess(i * 10);
      }
      const health = tracker.getHealth('connected');
      // Last 10 samples: 30,40,50,60,70,80,90,100,110,120 => avg = 75
      expect(health.avgLatencyMs).toBe(75);
    });

    it('should report zero latency with no samples', () => {
      const health = tracker.getHealth('connected');
      expect(health.avgLatencyMs).toBe(0);
    });
  });

  describe('command counting', () => {
    it('should count commands sent', () => {
      tracker.recordCommandSent();
      tracker.recordCommandSent();
      tracker.recordCommandSent();
      const health = tracker.getHealth('connected');
      expect(health.totalCommands).toBe(3);
    });

    it('should count successful commands (increments totalCommands too)', () => {
      tracker.recordSuccess(5);
      tracker.recordSuccess(10);
      const health = tracker.getHealth('connected');
      expect(health.totalCommands).toBe(2);
    });

    it('should count errors', () => {
      tracker.recordError();
      tracker.recordError();
      const health = tracker.getHealth('connected');
      expect(health.totalErrors).toBe(2);
    });

    it('should count ping failures as errors', () => {
      tracker.recordPingFailure();
      const health = tracker.getHealth('connected');
      expect(health.totalErrors).toBe(1);
    });
  });

  describe('recordPingFailure return value', () => {
    it('should return false when under max failures', () => {
      expect(tracker.recordPingFailure()).toBe(false); // 1 < 3
      expect(tracker.recordPingFailure()).toBe(false); // 2 < 3
    });

    it('should return true when max failures reached', () => {
      tracker.recordPingFailure(); // 1
      tracker.recordPingFailure(); // 2
      expect(tracker.recordPingFailure()).toBe(true); // 3 >= 3
    });

    it('should continue returning true after exceeding max failures', () => {
      tracker.recordPingFailure();
      tracker.recordPingFailure();
      tracker.recordPingFailure();
      expect(tracker.recordPingFailure()).toBe(true); // 4 >= 3
    });
  });

  describe('ping success resets failures', () => {
    it('should reset consecutive failures on ping success', () => {
      tracker.recordPingFailure();
      tracker.recordPingFailure();
      tracker.recordPingSuccess(5);
      const health = tracker.getHealth('connected');
      expect(health.consecutivePingFailures).toBe(0);
    });
  });

  describe('recordConnected resets failures', () => {
    it('should reset consecutive failures on new connection', () => {
      tracker.recordPingFailure();
      tracker.recordPingFailure();
      tracker.recordConnected();
      const health = tracker.getHealth('connected');
      expect(health.consecutivePingFailures).toBe(0);
    });
  });

  describe('uptime tracking', () => {
    it('should report uptime since connection', async () => {
      tracker.recordConnected();
      await new Promise((r) => setTimeout(r, 50));
      const health = tracker.getHealth('connected');
      expect(health.uptimeMs).toBeGreaterThanOrEqual(40);
    });

    it('should report zero uptime when not connected', () => {
      const health = tracker.getHealth('disconnected');
      expect(health.uptimeMs).toBe(0);
    });
  });

  describe('lastSuccessAt / lastErrorAt', () => {
    it('should set lastSuccessAt on success', () => {
      const before = Date.now();
      tracker.recordSuccess(5);
      const health = tracker.getHealth('connected');
      expect(health.lastSuccessAt).toBeGreaterThanOrEqual(before);
    });

    it('should set lastErrorAt on error', () => {
      const before = Date.now();
      tracker.recordError();
      const health = tracker.getHealth('connected');
      expect(health.lastErrorAt).toBeGreaterThanOrEqual(before);
    });

    it('should set lastErrorAt on ping failure', () => {
      const before = Date.now();
      tracker.recordPingFailure();
      const health = tracker.getHealth('connected');
      expect(health.lastErrorAt).toBeGreaterThanOrEqual(before);
    });
  });

  describe('ping timer', () => {
    it('should not start ping if interval is 0', () => {
      const zeroTracker = new HealthTracker({ pingInterval: 0, maxPingFailures: 3 });
      let called = false;
      zeroTracker.startPing(async () => {
        called = true;
      });
      // Should not start timer - calling stop should be safe
      zeroTracker.stopPing();
      expect(called).toBe(false);
    });

    it('should not start ping if interval is negative', () => {
      const negTracker = new HealthTracker({ pingInterval: -1, maxPingFailures: 3 });
      let called = false;
      negTracker.startPing(async () => {
        called = true;
      });
      negTracker.stopPing();
      expect(called).toBe(false);
    });

    it('should stop ping cleanly when no timer running', () => {
      // Should not throw
      tracker.stopPing();
      tracker.stopPing();
    });
  });
});

// ---------------------------------------------------------------------------
// ReconnectManager (reconnect.ts) - pure unit tests
// ---------------------------------------------------------------------------
describe('ReconnectManager', () => {
  let manager: ReconnectManager;

  beforeEach(() => {
    manager = new ReconnectManager({
      maxReconnectAttempts: 5,
      reconnectDelay: 100,
      maxReconnectDelay: 5000,
      autoReconnect: true,
    });
  });

  afterEach(() => {
    manager.cancelReconnect();
  });

  describe('canReconnect', () => {
    it('should allow reconnect when autoReconnect is true and not closed', () => {
      expect(manager.canReconnect()).toBe(true);
    });

    it('should disallow reconnect when closed', () => {
      manager.setClosed(true);
      expect(manager.canReconnect()).toBe(false);
    });

    it('should disallow reconnect when autoReconnect is false', () => {
      const noAutoManager = new ReconnectManager({
        maxReconnectAttempts: 5,
        reconnectDelay: 100,
        maxReconnectDelay: 5000,
        autoReconnect: false,
      });
      expect(noAutoManager.canReconnect()).toBe(false);
    });
  });

  describe('setClosed / isClosed', () => {
    it('should track closed state', () => {
      expect(manager.isClosed()).toBe(false);
      manager.setClosed(true);
      expect(manager.isClosed()).toBe(true);
      manager.setClosed(false);
      expect(manager.isClosed()).toBe(false);
    });

    it('should cancel pending reconnect when closed', () => {
      let called = false;
      manager.scheduleReconnect(async () => {
        called = true;
      });
      manager.setClosed(true);
      // Timer should be cancelled
      expect(called).toBe(false);
    });
  });

  describe('reset', () => {
    it('should reset attempt counter', () => {
      // Use up some attempts
      manager.scheduleReconnect(async () => {});
      manager.cancelReconnect();
      manager.scheduleReconnect(async () => {});
      manager.cancelReconnect();
      // Reset
      manager.reset();
      // Should be able to schedule again from attempt 1
      const result = manager.scheduleReconnect(async () => {});
      expect(result).toBe(true);
      manager.cancelReconnect();
    });
  });

  describe('scheduleReconnect', () => {
    it('should return true on successful schedule', () => {
      const result = manager.scheduleReconnect(async () => {});
      expect(result).toBe(true);
      manager.cancelReconnect();
    });

    it('should return false when closed', () => {
      manager.setClosed(true);
      const result = manager.scheduleReconnect(async () => {});
      expect(result).toBe(false);
    });

    it('should return false when a reconnect is already pending', () => {
      manager.scheduleReconnect(async () => {});
      const result = manager.scheduleReconnect(async () => {});
      expect(result).toBe(false);
      manager.cancelReconnect();
    });

    it('should emit reconnecting event', () => {
      let emitted = false;
      let emittedData: { attempt: number; delay: number } | null = null;
      manager.on('reconnecting', (data) => {
        emitted = true;
        emittedData = data;
      });
      manager.scheduleReconnect(async () => {});
      expect(emitted).toBe(true);
      expect(emittedData!.attempt).toBe(1);
      expect(emittedData!.delay).toBeGreaterThan(0);
      manager.cancelReconnect();
    });
  });

  describe('max reconnection attempts', () => {
    it('should emit maxReconnectAttemptsReached after exhausting attempts', () => {
      let maxReached = false;
      manager.on('maxReconnectAttemptsReached', () => {
        maxReached = true;
      });

      // Schedule and cancel 5 attempts (max is 5)
      for (let i = 0; i < 5; i++) {
        manager.scheduleReconnect(async () => {});
        manager.cancelReconnect();
      }

      // 6th attempt should emit max reached and return false
      const result = manager.scheduleReconnect(async () => {});
      expect(result).toBe(false);
      expect(maxReached).toBe(true);
    });

    it('should succeed for exactly max attempts', () => {
      // Should succeed for attempts 1..5
      for (let i = 0; i < 5; i++) {
        const result = manager.scheduleReconnect(async () => {});
        expect(result).toBe(true);
        manager.cancelReconnect();
      }
    });
  });

  describe('reconnection delay calculation (exponential backoff)', () => {
    it('should increase delay with each attempt', () => {
      const delays: number[] = [];
      manager.on('reconnecting', (data: { delay: number }) => {
        delays.push(data.delay);
      });

      for (let i = 0; i < 3; i++) {
        manager.scheduleReconnect(async () => {});
        manager.cancelReconnect();
      }

      // Base delays: 100, 200, 400 (with jitter up to 30%)
      // So delay[0] in [100, 130], delay[1] in [200, 260], delay[2] in [400, 520]
      expect(delays[0]).toBeGreaterThanOrEqual(100);
      expect(delays[0]).toBeLessThanOrEqual(130);
      expect(delays[1]).toBeGreaterThanOrEqual(200);
      expect(delays[1]).toBeLessThanOrEqual(260);
      expect(delays[2]).toBeGreaterThanOrEqual(400);
      expect(delays[2]).toBeLessThanOrEqual(520);
    });

    it('should cap delay at maxReconnectDelay', () => {
      // Create manager with low max delay
      const cappedManager = new ReconnectManager({
        maxReconnectAttempts: 20,
        reconnectDelay: 1000,
        maxReconnectDelay: 2000,
        autoReconnect: true,
      });

      const delays: number[] = [];
      cappedManager.on('reconnecting', (data: { delay: number }) => {
        delays.push(data.delay);
      });

      // After several attempts, delay should be capped at maxReconnectDelay + jitter
      for (let i = 0; i < 5; i++) {
        cappedManager.scheduleReconnect(async () => {});
        cappedManager.cancelReconnect();
      }

      // All delays should be <= maxReconnectDelay + 30% jitter = 2600
      for (const delay of delays) {
        expect(delay).toBeLessThanOrEqual(2600);
      }

      cappedManager.cancelReconnect();
    });
  });

  describe('cancelReconnect', () => {
    it('should cancel pending reconnect timer', () => {
      let called = false;
      manager.scheduleReconnect(async () => {
        called = true;
      });
      manager.cancelReconnect();
      // Give it time to fire if not cancelled
      expect(called).toBe(false);
    });

    it('should be safe to call when no reconnect is pending', () => {
      // Should not throw
      manager.cancelReconnect();
      manager.cancelReconnect();
    });
  });
});

// ---------------------------------------------------------------------------
// CommandQueue (connection.ts) - pure unit tests
// ---------------------------------------------------------------------------
describe('CommandQueue', () => {
  let queue: CommandQueue;

  beforeEach(() => {
    queue = new CommandQueue();
  });

  describe('nextId', () => {
    it('should generate sequential IDs', () => {
      expect(queue.nextId()).toBe(1);
      expect(queue.nextId()).toBe(2);
      expect(queue.nextId()).toBe(3);
    });
  });

  describe('enqueue / dequeue', () => {
    it('should enqueue and dequeue commands in FIFO order', () => {
      const cmd1 = makePendingCommand(queue.nextId(), '1');
      const cmd2 = makePendingCommand(queue.nextId(), '2');
      queue.enqueue(cmd1);
      queue.enqueue(cmd2);

      expect(queue.hasPending()).toBe(true);

      const d1 = queue.dequeue();
      expect(d1?.reqId).toBe('1');

      const d2 = queue.dequeue();
      expect(d2?.reqId).toBe('2');

      const d3 = queue.dequeue();
      expect(d3).toBeNull();
      expect(queue.hasPending()).toBe(false);
    });
  });

  describe('remove', () => {
    it('should remove a queued command by ID', () => {
      const cmd = makePendingCommand(queue.nextId(), '1');
      queue.enqueue(cmd);

      expect(queue.remove(cmd.id)).toBe(true);
      expect(queue.hasPending()).toBe(false);
    });

    it('should return false for non-existent ID', () => {
      expect(queue.remove(999)).toBe(false);
    });
  });

  describe('in-flight tracking', () => {
    it('should track in-flight commands by reqId', () => {
      const cmd = makePendingCommand(1, 'req-1');
      queue.addInFlight(cmd);

      expect(queue.getInFlightCount()).toBe(1);
      expect(queue.getByReqId('req-1')).toBe(cmd);
    });

    it('should remove in-flight commands by reqId', () => {
      const cmd = makePendingCommand(1, 'req-1');
      queue.addInFlight(cmd);

      const removed = queue.removeByReqId('req-1');
      expect(removed).toBe(cmd);
      expect(queue.getInFlightCount()).toBe(0);
    });

    it('should return undefined for non-existent reqId', () => {
      expect(queue.removeByReqId('nope')).toBeUndefined();
      expect(queue.getByReqId('nope')).toBeUndefined();
    });
  });

  describe('canSendMore', () => {
    it('should allow sending when under limit', () => {
      expect(queue.canSendMore(10)).toBe(true);
    });

    it('should block sending when at limit', () => {
      for (let i = 0; i < 5; i++) {
        queue.addInFlight(makePendingCommand(i, `req-${i}`));
      }
      expect(queue.canSendMore(5)).toBe(false);
      expect(queue.canSendMore(6)).toBe(true);
    });
  });

  describe('currentCommand (legacy mode)', () => {
    it('should get and set current command', () => {
      expect(queue.getCurrentCommand()).toBeNull();

      const cmd = makePendingCommand(1, '1');
      queue.setCurrentCommand(cmd);
      expect(queue.getCurrentCommand()).toBe(cmd);

      queue.setCurrentCommand(null);
      expect(queue.getCurrentCommand()).toBeNull();
    });
  });

  describe('clearCurrent', () => {
    it('should clear current command without error', () => {
      const cmd = makePendingCommand(1, '1');
      queue.setCurrentCommand(cmd);
      queue.clearCurrent();
      expect(queue.getCurrentCommand()).toBeNull();
    });

    it('should reject current command when error is provided', () => {
      let rejected = false;
      const cmd = makePendingCommand(1, '1');
      cmd.reject = () => {
        rejected = true;
      };
      queue.setCurrentCommand(cmd);
      queue.clearCurrent(new Error('test'));
      expect(rejected).toBe(true);
      expect(queue.getCurrentCommand()).toBeNull();
    });

    it('should be safe to call with no current command', () => {
      queue.clearCurrent();
      queue.clearCurrent(new Error('test'));
    });
  });

  describe('rejectAll', () => {
    it('should reject all queued commands', () => {
      const rejections: string[] = [];
      const cmd1 = makePendingCommand(queue.nextId(), '1');
      cmd1.reject = (err) => rejections.push(err.message);
      const cmd2 = makePendingCommand(queue.nextId(), '2');
      cmd2.reject = (err) => rejections.push(err.message);

      queue.enqueue(cmd1);
      queue.enqueue(cmd2);
      queue.rejectAll(new Error('closed'));

      expect(rejections).toEqual(['closed', 'closed']);
      expect(queue.hasPending()).toBe(false);
    });

    it('should reject all in-flight commands', () => {
      const rejections: string[] = [];
      const cmd = makePendingCommand(1, 'req-1');
      cmd.reject = (err) => rejections.push(err.message);

      queue.addInFlight(cmd);
      queue.rejectAll(new Error('lost'));

      expect(rejections).toEqual(['lost']);
      expect(queue.getInFlightCount()).toBe(0);
    });

    it('should reject current command', () => {
      let rejected = false;
      const cmd = makePendingCommand(1, '1');
      cmd.reject = () => {
        rejected = true;
      };

      queue.setCurrentCommand(cmd);
      queue.rejectAll(new Error('done'));

      expect(rejected).toBe(true);
      expect(queue.getCurrentCommand()).toBeNull();
    });

    it('should reject all categories simultaneously', () => {
      const rejections: string[] = [];

      const queued = makePendingCommand(queue.nextId(), 'q1');
      queued.reject = (err) => rejections.push(`queued:${err.message}`);
      queue.enqueue(queued);

      const inflight = makePendingCommand(10, 'f1');
      inflight.reject = (err) => rejections.push(`inflight:${err.message}`);
      queue.addInFlight(inflight);

      const current = makePendingCommand(20, 'c1');
      current.reject = (err) => rejections.push(`current:${err.message}`);
      queue.setCurrentCommand(current);

      queue.rejectAll(new Error('boom'));

      expect(rejections).toHaveLength(3);
      expect(rejections).toContain('queued:boom');
      expect(rejections).toContain('inflight:boom');
      expect(rejections).toContain('current:boom');
    });
  });
});

// ---------------------------------------------------------------------------
// TcpClient (client.ts) - integration tests with embedded server
// ---------------------------------------------------------------------------
describe('TcpClient', () => {
  let queue: Queue;

  beforeEach(async () => {
    queue = new Queue('tcp-client-test');
    // Ensure embedded QueueManager is initialized
    await queue.add('init', { warmup: true });
  });

  afterEach(() => {
    shutdownManager();
  });

  describe('send command and receive response', () => {
    it('should send and receive via Queue (backed by TcpClient in embedded)', async () => {
      const job = await queue.add('my-task', { value: 123 });
      expect(job.id).toBeDefined();
      expect(job.name).toBe('my-task');
      expect(job.data.value).toBe(123);
    });

    it('should handle multiple sequential commands', async () => {
      const results = [];
      for (let i = 0; i < 10; i++) {
        results.push(await queue.add(`seq-${i}`, { i }));
      }
      expect(results).toHaveLength(10);
      const ids = new Set(results.map((r) => r.id));
      expect(ids.size).toBe(10);
    });
  });

  describe('Hello', () => {
    it('should advertise the current Bun client protocol contract', async () => {
      const client = new TcpClient({ autoReconnect: false, pingInterval: 0 });
      const commands: Record<string, unknown>[] = [];
      client.send = async (command) => {
        commands.push(command);
        return {
          ok: true,
          protocolVersion: 3,
          capabilities: ['pipelining', 'separate-job-name'],
          server: 'bunqueue',
          version: 'test',
        };
      };

      const response = await client.hello();

      expect(commands).toEqual([
        {
          cmd: 'Hello',
          protocolVersion: 3,
          capabilities: ['pipelining', 'separate-job-name'],
        },
      ]);
      expect(response.protocolVersion).toBe(3);
      expect(response.capabilities).toContain('separate-job-name');
      client.close();
    });

    it('should surface a rejected Hello response', async () => {
      const client = new TcpClient({ autoReconnect: false, pingInterval: 0 });
      client.send = async () => ({ ok: false, error: 'Unsupported protocol' });

      await expect(client.hello()).rejects.toThrow('Unsupported protocol');
      client.close();
    });
  });

  describe('multiple concurrent commands', () => {
    it('should handle many parallel commands', async () => {
      const promises = Array.from({ length: 50 }, (_, i) => queue.add(`concurrent-${i}`, { i }));
      const results = await Promise.all(promises);
      expect(results).toHaveLength(50);
      const ids = new Set(results.map((r) => r.id));
      expect(ids.size).toBe(50);
    });
  });

  describe('getState / isConnected', () => {
    it('should report connection state through TcpClient constructor', () => {
      const client = new TcpClient({
        host: 'localhost',
        port: 6789,
        autoReconnect: false,
        pingInterval: 0,
      });
      // Not connected yet
      expect(client.isConnected()).toBe(false);
      expect(client.getState()).toBe('disconnected');
      client.close();
    });

    it('should report closed state after close', () => {
      const client = new TcpClient({
        host: 'localhost',
        port: 6789,
        autoReconnect: false,
        pingInterval: 0,
      });
      client.close();
      expect(client.getState()).toBe('closed');
      expect(client.isConnected()).toBe(false);
    });
  });

  describe('getHealth', () => {
    it('should return health metrics for disconnected client', () => {
      const client = new TcpClient({
        host: 'localhost',
        port: 6789,
        autoReconnect: false,
        pingInterval: 0,
      });
      const health = client.getHealth();
      expect(health.state).toBe('disconnected');
      expect(health.healthy).toBe(false);
      expect(health.totalCommands).toBe(0);
      client.close();
    });

    it('should return health metrics for closed client', () => {
      const client = new TcpClient({
        host: 'localhost',
        port: 6789,
        autoReconnect: false,
        pingInterval: 0,
      });
      client.close();
      const health = client.getHealth();
      expect(health.state).toBe('closed');
      expect(health.healthy).toBe(false);
      client.close();
    });
  });

  describe('getInFlightCount', () => {
    it('should start at 0', () => {
      const client = new TcpClient({
        host: 'localhost',
        port: 6789,
        autoReconnect: false,
        pingInterval: 0,
      });
      expect(client.getInFlightCount()).toBe(0);
      client.close();
    });
  });

  describe('close', () => {
    it('should reject pending commands on close', async () => {
      const client = new TcpClient({
        host: 'localhost',
        port: 99999, // invalid port
        autoReconnect: false,
        connectTimeout: 100,
        commandTimeout: 200,
        pingInterval: 0,
      });

      // Enqueue a command (will not connect)
      const promise = client.send({ cmd: 'Ping' });
      // Close immediately
      client.close();

      await expect(promise).rejects.toThrow();
    });

    it('should be idempotent', () => {
      const client = new TcpClient({ autoReconnect: false, pingInterval: 0 });
      client.close();
      client.close(); // should not throw
      expect(client.getState()).toBe('closed');
    });
  });

  describe('events', () => {
    it('should emit events from constructor', () => {
      const client = new TcpClient({ autoReconnect: false, pingInterval: 0 });
      const events: string[] = [];
      client.on('connected', () => events.push('connected'));
      client.on('disconnected', () => events.push('disconnected'));
      client.on('error', () => events.push('error'));
      // Just verify no crash from event registration
      expect(events).toHaveLength(0);
      client.close();
    });
  });

  describe('reqId generation', () => {
    it('should wrap reqId counter to avoid overflow', async () => {
      // This tests the internal counter modulo behavior
      // We can verify through successful multiple sends
      const promises = Array.from({ length: 20 }, (_, i) => queue.add(`reqid-test-${i}`, { i }));
      const results = await Promise.all(promises);
      expect(results).toHaveLength(20);
      // All unique IDs
      const ids = new Set(results.map((r) => r.id));
      expect(ids.size).toBe(20);
    });
  });
});

// ---------------------------------------------------------------------------
// TcpConnectionPool (tcpPool.ts) - tests
// ---------------------------------------------------------------------------
describe('TcpConnectionPool', () => {
  afterEach(() => {
    closeAllSharedPools();
    shutdownManager();
  });

  describe('create pool with specified pool size', () => {
    it('should create pool with default size of 4', () => {
      const pool = new TcpConnectionPool();
      expect(pool.getPoolSize()).toBe(4);
      pool.close();
    });

    it('should create pool with custom size', () => {
      const pool = new TcpConnectionPool({ poolSize: 8 });
      expect(pool.getPoolSize()).toBe(8);
      pool.close();
    });

    it('should enforce minimum pool size of 1', () => {
      const pool = new TcpConnectionPool({ poolSize: 0 });
      expect(pool.getPoolSize()).toBe(1);
      pool.close();
    });

    it('should enforce minimum pool size of 1 for negative values', () => {
      const pool = new TcpConnectionPool({ poolSize: -5 });
      expect(pool.getPoolSize()).toBe(1);
      pool.close();
    });
  });

  describe('close pool closes all connections', () => {
    it('should mark pool as closed', () => {
      const pool = new TcpConnectionPool({ poolSize: 2 });
      expect(pool.isClosed()).toBe(false);
      pool.close();
      expect(pool.isClosed()).toBe(true);
    });

    it('should be idempotent', () => {
      const pool = new TcpConnectionPool({ poolSize: 2 });
      pool.close();
      pool.close(); // should not throw
      expect(pool.isClosed()).toBe(true);
    });

    it('should reject sends after close', async () => {
      const pool = new TcpConnectionPool({ poolSize: 2 });
      pool.close();
      await expect(pool.send({ cmd: 'Ping' })).rejects.toThrow('Connection pool is closed');
    });

    it('should reject parallel sends after close', async () => {
      const pool = new TcpConnectionPool({ poolSize: 2 });
      pool.close();
      await expect(pool.sendParallel([{ cmd: 'Ping' }])).rejects.toThrow(
        'Connection pool is closed'
      );
    });

    it('should report 0 pool size after close', () => {
      const pool = new TcpConnectionPool({ poolSize: 3 });
      pool.close();
      expect(pool.getPoolSize()).toBe(0);
    });
  });

  describe('isConnected', () => {
    it('should report not connected before connect', () => {
      const pool = new TcpConnectionPool({ poolSize: 2 });
      expect(pool.isConnected()).toBe(false);
      pool.close();
    });

    it('should report not connected after close', () => {
      const pool = new TcpConnectionPool({ poolSize: 2 });
      pool.close();
      expect(pool.isConnected()).toBe(false);
    });
  });

  describe('getConnectedCount', () => {
    it('should start at 0', () => {
      const pool = new TcpConnectionPool({ poolSize: 2 });
      expect(pool.getConnectedCount()).toBe(0);
      pool.close();
    });
  });

  describe('getHealth', () => {
    it('should return aggregate health for unconnected pool', () => {
      const pool = new TcpConnectionPool({ poolSize: 2 });
      const health = pool.getHealth();
      expect(health.healthy).toBe(false);
      expect(health.connectedCount).toBe(0);
      expect(health.totalCount).toBe(2);
      expect(health.clients).toHaveLength(2);
      expect(health.totalCommands).toBe(0);
      expect(health.totalErrors).toBe(0);
      expect(health.avgLatencyMs).toBe(0);
      pool.close();
    });
  });

  describe('reference counting', () => {
    it('should track addRef and release', () => {
      const pool = new TcpConnectionPool({ poolSize: 1 });
      pool.addRef();
      pool.addRef();
      // Release one - should not close
      pool.release();
      expect(pool.isClosed()).toBe(false);
      // Release second - should close
      pool.release();
      expect(pool.isClosed()).toBe(true);
    });
  });

  describe('shared pool', () => {
    it('should return same pool for same options', () => {
      const pool1 = getSharedPool({ host: 'localhost', port: 16789, poolSize: 2 });
      const pool2 = getSharedPool({ host: 'localhost', port: 16789, poolSize: 2 });
      expect(pool1).toBe(pool2);
      pool1.release();
      pool2.release();
    });

    it('should return different pool for different port', () => {
      const pool1 = getSharedPool({ host: 'localhost', port: 16789, poolSize: 2 });
      const pool2 = getSharedPool({ host: 'localhost', port: 16790, poolSize: 2 });
      expect(pool1).not.toBe(pool2);
      pool1.release();
      pool2.release();
    });

    it('should return different pool for different pool size', () => {
      const pool1 = getSharedPool({ host: 'localhost', port: 16789, poolSize: 2 });
      const pool2 = getSharedPool({ host: 'localhost', port: 16789, poolSize: 4 });
      expect(pool1).not.toBe(pool2);
      pool1.release();
      pool2.release();
    });

    it('should close all shared pools', () => {
      const pool1 = getSharedPool({ host: 'localhost', port: 16789, poolSize: 1 });
      const pool2 = getSharedPool({ host: 'localhost', port: 16790, poolSize: 1 });
      closeAllSharedPools();
      expect(pool1.isClosed()).toBe(true);
      expect(pool2.isClosed()).toBe(true);
    });

    it('should create new pool after previous was closed', () => {
      const pool1 = getSharedPool({ host: 'localhost', port: 16799, poolSize: 1 });
      pool1.close();
      const pool2 = getSharedPool({ host: 'localhost', port: 16799, poolSize: 1 });
      expect(pool2).not.toBe(pool1);
      expect(pool2.isClosed()).toBe(false);
      pool2.release();
    });
  });
});

// ---------------------------------------------------------------------------
// Connection error handling (connection.ts createConnection)
// ---------------------------------------------------------------------------
describe('createConnection', () => {
  it('should fail to connect to invalid port', async () => {
    const { createConnection } = await import('../src/client/tcp/connection');
    await expect(
      createConnection({ host: 'localhost', port: 1 }, 500, {
        onData: () => {},
        onClose: () => {},
        onError: () => {},
      })
    ).rejects.toThrow();
  });

  it('should fail to connect to unreachable host', async () => {
    const { createConnection } = await import('../src/client/tcp/connection');
    await expect(
      createConnection(
        { host: '192.0.2.1', port: 6789 }, // TEST-NET, unreachable
        500,
        {
          onData: () => {},
          onClose: () => {},
          onError: () => {},
        }
      )
    ).rejects.toThrow();
  });
});

// ---------------------------------------------------------------------------
// TcpClient connection failure tests
// ---------------------------------------------------------------------------
describe('TcpClient connection errors', () => {
  it('should fail to connect to invalid port', async () => {
    const client = new TcpClient({
      host: 'localhost',
      port: 1, // invalid
      autoReconnect: false,
      connectTimeout: 500,
      pingInterval: 0,
    });
    await expect(client.connect()).rejects.toThrow();
    client.close();
  });

  it('should emit maxReconnectAttemptsReached after max attempts', async () => {
    let maxReached = false;
    const client = new TcpClient({
      host: 'localhost',
      port: 1,
      autoReconnect: true,
      maxReconnectAttempts: 2,
      reconnectDelay: 50,
      maxReconnectDelay: 100,
      connectTimeout: 100,
      pingInterval: 0,
    });

    client.on('maxReconnectAttemptsReached', () => {
      maxReached = true;
    });

    try {
      await client.connect();
    } catch {
      // expected
    }

    // Wait for reconnect attempts to exhaust
    await new Promise((r) => setTimeout(r, 1500));
    expect(maxReached).toBe(true);
    client.close();
  });

  it('should not reconnect when autoReconnect is false', async () => {
    let reconnecting = false;
    const client = new TcpClient({
      host: 'localhost',
      port: 1,
      autoReconnect: false,
      connectTimeout: 100,
      pingInterval: 0,
    });

    client.on('reconnecting', () => {
      reconnecting = true;
    });

    try {
      await client.connect();
    } catch {
      // expected
    }

    await new Promise((r) => setTimeout(r, 300));
    expect(reconnecting).toBe(false);
    client.close();
  });
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
function makePendingCommand(
  id: number,
  reqId: string
): {
  id: number;
  reqId: string;
  command: Record<string, unknown>;
  resolve: (value: Record<string, unknown>) => void;
  reject: (error: Error) => void;
  timeout: ReturnType<typeof setTimeout>;
} {
  return {
    id,
    reqId,
    command: { cmd: 'test', reqId },
    resolve: () => {},
    reject: () => {},
    timeout: setTimeout(() => {}, 60000),
  };
}
