/**
 * Test: Issue #63 - disable stall config
 *
 * Verifies that:
 * 1. setStallConfig({ enabled: false }) actually disables stall detection
 * 2. getStallConfig() reflects the change
 * 3. Cloud queue:detail response includes the `enabled` field
 */
import { describe, it, expect, beforeAll, afterAll } from 'bun:test';
import { Queue } from '../src/client';

describe('Issue #63: disable stall config', () => {
  let queue: Queue;

  beforeAll(() => {
    queue = new Queue('issue-63-stall', { embedded: true });
  });

  afterAll(() => {
    queue.close();
  });

  it('should disable stall detection via setStallConfig({ enabled: false })', () => {
    queue.setStallConfig({ enabled: false });
    const config = queue.getStallConfig();
    expect(config.enabled).toBe(false);
    expect(config.stallInterval).toBe(30000); // other fields unchanged
    expect(config.maxStalls).toBe(3);
  });

  it('should re-enable stall detection via setStallConfig({ enabled: true })', () => {
    queue.setStallConfig({ enabled: false });
    expect(queue.getStallConfig().enabled).toBe(false);

    queue.setStallConfig({ enabled: true });
    expect(queue.getStallConfig().enabled).toBe(true);
  });

  it('getStallConfig should reflect setStallConfig immediately (issue #63 screenshot)', () => {
    // Reproduces the exact user scenario from the screenshot
    queue.setStallConfig({ enabled: false });
    const config = queue.getStallConfig();

    // BUG before fix: returned { enabled: true } always in TCP mode
    expect(config.enabled).toBe(false);
  });

  it('cloud queue:detail response should include enabled field in stallConfig', async () => {
    // Import cloud commands to test the response shape
    const { COMMANDS } = await import('../src/infrastructure/cloud/commands');
    const { getSharedManager } = await import('../src/client/manager');

    const manager = getSharedManager();
    const queueName = 'issue-63-stall';

    // Disable stall config
    manager.setStallConfig(queueName, { enabled: false });

    // Simulate cloud queue:detail command
    const handler = COMMANDS['queue:detail']!;
    const result = handler(manager, { type: 'queue:detail', queue: queueName });

    // The stallConfig in the response MUST include `enabled`
    expect(result).toBeDefined();
    expect((result as { stallConfig: { enabled: boolean } }).stallConfig.enabled).toBe(false);
  });
});
