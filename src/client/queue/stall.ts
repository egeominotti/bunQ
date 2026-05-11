/**
 * Stall Detection Operations
 * setStallConfig, getStallConfig
 */

import type { TcpConnectionPool } from '../tcpPool';
import type { StallConfig } from '../types';
import * as dlqOps from './dlqOps';

interface StallContext {
  name: string;
  embedded: boolean;
  tcp: TcpConnectionPool | null;
}

const DEFAULT_STALL_CONFIG: StallConfig = {
  enabled: true,
  stallInterval: 30000,
  maxStalls: 3,
  gracePeriod: 5000,
};

/** Client-side cache for TCP mode (server is the source of truth) */
const tcpConfigCache = new Map<string, StallConfig>();

/** Set stall detection configuration */
export function setStallConfig(ctx: StallContext, config: Partial<StallConfig>): void {
  if (ctx.embedded) {
    dlqOps.setStallConfigEmbedded(ctx.name, config);
  } else if (ctx.tcp) {
    // Cache locally so getStallConfig() returns the correct value
    const current = tcpConfigCache.get(ctx.name) ?? { ...DEFAULT_STALL_CONFIG };
    tcpConfigCache.set(ctx.name, { ...current, ...config });
    void ctx.tcp.send({ cmd: 'SetStallConfig', queue: ctx.name, config });
  }
}

/** Get stall detection configuration */
export function getStallConfig(ctx: StallContext): StallConfig {
  if (ctx.embedded) {
    return dlqOps.getStallConfigEmbedded(ctx.name);
  }
  // Return cached config if available, otherwise defaults
  return tcpConfigCache.get(ctx.name) ?? { ...DEFAULT_STALL_CONFIG };
}

/** Get stall detection configuration (async, works in TCP mode) */
export async function getStallConfigAsync(ctx: StallContext): Promise<StallConfig> {
  if (ctx.embedded) {
    return dlqOps.getStallConfigEmbedded(ctx.name);
  }
  if (!ctx.tcp) {
    return { enabled: true, stallInterval: 30000, maxStalls: 3, gracePeriod: 5000 };
  }
  const response = await ctx.tcp.send({ cmd: 'GetStallConfig', queue: ctx.name });
  if (!response.ok) {
    return { enabled: true, stallInterval: 30000, maxStalls: 3, gracePeriod: 5000 };
  }
  return (response as { config: StallConfig }).config;
}
