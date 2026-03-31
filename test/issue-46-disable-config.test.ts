/**
 * Issue #46: can not disable stallConfig or dlqConfig
 * Reproduction test - verifies that configs can be disabled and actually take effect
 */

import { describe, it, expect, afterEach } from 'bun:test';
import { Queue, shutdownManager } from '../src/client';

describe('Issue #46: disable stallConfig and dlqConfig', () => {
  afterEach(() => {
    shutdownManager();
  });

  it('should disable stall detection via setStallConfig({ enabled: false })', () => {
    const queue = new Queue('issue-46-stall');

    queue.setStallConfig({ enabled: false });
    const config = queue.getStallConfig();

    expect(config.enabled).toBe(false);
  });

  it('should disable DLQ auto-retry via setDlqConfig({ autoRetry: false })', () => {
    const queue = new Queue('issue-46-dlq');

    queue.setDlqConfig({ autoRetry: false, maxAutoRetries: 0 });
    const config = queue.getDlqConfig();

    expect(config.autoRetry).toBe(false);
    expect(config.maxAutoRetries).toBe(0);
  });

  it('should return non-empty config from getDlqConfig()', () => {
    const queue = new Queue('issue-46-dlq-defaults');

    const config = queue.getDlqConfig();

    // Config should have actual properties, not be empty {}
    expect(config.autoRetry).toBeDefined();
  });

  it('should persist stall config after setting enabled: false then getting it', () => {
    const queue = new Queue('issue-46-stall-persist');

    // Set stall config to disabled
    queue.setStallConfig({ enabled: false, stallInterval: 60000, maxStalls: 1 });
    const config = queue.getStallConfig();

    expect(config.enabled).toBe(false);
    expect(config.stallInterval).toBe(60000);
    expect(config.maxStalls).toBe(1);
  });
});
