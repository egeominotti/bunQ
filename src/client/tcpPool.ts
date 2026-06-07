/**
 * TCP Connection Pool
 * Manages multiple TCP connections for parallel operations
 */

import { TcpClient, type ConnectionOptions, type ConnectionHealth } from './tcpClient';

export interface PoolOptions extends Partial<ConnectionOptions> {
  /** Number of connections in pool (default: 4) */
  poolSize?: number;
}

/** Resolved pool options */
type ResolvedPoolOptions = Required<PoolOptions>;

/**
 * Connection pool for parallel TCP operations
 * Load-aware distribution - prefers connected clients with capacity
 */
export class TcpConnectionPool {
  private readonly clients: TcpClient[] = [];
  private readonly options: ResolvedPoolOptions;
  private currentIndex = 0;
  private closed = false;
  private refCount = 0; // Reference counting for shared pools
  private poolKey: string | null = null; // Track key for cleanup from sharedPools

  constructor(options: PoolOptions = {}) {
    const poolSize = Math.max(1, options.poolSize ?? 4); // Validate: at least 1
    this.options = {
      host: options.host ?? 'localhost',
      port: options.port ?? 6789,
      token: options.token ?? '',
      poolSize,
      maxReconnectAttempts: options.maxReconnectAttempts ?? Infinity,
      reconnectDelay: options.reconnectDelay ?? 100,
      maxReconnectDelay: options.maxReconnectDelay ?? 30000,
      connectTimeout: options.connectTimeout ?? 5000,
      commandTimeout: options.commandTimeout ?? 30000,
      autoReconnect: options.autoReconnect ?? true,
      pingInterval: options.pingInterval ?? 30000,
      maxPingFailures: options.maxPingFailures ?? 3,
      maxCommandTimeouts: options.maxCommandTimeouts ?? 3,
      pipelining: options.pipelining ?? true,
      maxInFlight: options.maxInFlight ?? 100,
    };

    // Create pool of connections
    for (let i = 0; i < this.options.poolSize; i++) {
      const client = new TcpClient({
        host: this.options.host,
        port: this.options.port,
        token: this.options.token,
        maxReconnectAttempts: this.options.maxReconnectAttempts,
        reconnectDelay: this.options.reconnectDelay,
        maxReconnectDelay: this.options.maxReconnectDelay,
        connectTimeout: this.options.connectTimeout,
        commandTimeout: this.options.commandTimeout,
        autoReconnect: this.options.autoReconnect,
        pingInterval: this.options.pingInterval,
        maxPingFailures: this.options.maxPingFailures,
        maxCommandTimeouts: this.options.maxCommandTimeouts,
      });
      this.clients.push(client);
    }
  }

  /** Connect all clients in pool */
  async connect(): Promise<void> {
    await Promise.all(this.clients.map((c) => c.connect()));
  }

  /** Get next client with load-aware selection */
  private getNextClient(): TcpClient {
    const len = this.clients.length;

    // First pass: try to find a connected client starting from current index
    for (let i = 0; i < len; i++) {
      const idx = (this.currentIndex + i) % len;
      const client = this.clients[idx];
      if (client.isConnected()) {
        this.currentIndex = (idx + 1) % len;
        return client;
      }
    }

    // All disconnected: fall back to round-robin (they'll reconnect)
    const client = this.clients[this.currentIndex];
    this.currentIndex = (this.currentIndex + 1) % len;
    return client;
  }

  /** Send command using next available connection */
  async send(command: Record<string, unknown>): Promise<Record<string, unknown>> {
    if (this.closed) {
      throw new Error('Connection pool is closed');
    }
    return this.getNextClient().send(command);
  }

  /** Send multiple commands in parallel across pool */
  async sendParallel(
    commands: Array<Record<string, unknown>>
  ): Promise<Array<Record<string, unknown>>> {
    if (this.closed) {
      throw new Error('Connection pool is closed');
    }

    // Distribute commands across clients
    const promises = commands.map((cmd, i) => {
      const client = this.clients[i % this.clients.length];
      return client.send(cmd);
    });

    return Promise.all(promises);
  }

  /** Check if any connection is ready */
  isConnected(): boolean {
    return this.clients.some((c) => c.isConnected());
  }

  /** Get number of connected clients */
  getConnectedCount(): number {
    return this.clients.filter((c) => c.isConnected()).length;
  }

  /** Get pool size */
  getPoolSize(): number {
    return this.clients.length;
  }

  /** Increment reference count (for shared pools) */
  addRef(): void {
    this.refCount++;
  }

  /** Decrement reference count and close if zero */
  release(): void {
    this.refCount--;
    if (this.refCount <= 0) {
      this.close();
    }
  }

  /** Set pool key for shared pool cleanup */
  setPoolKey(key: string): void {
    this.poolKey = key;
  }

  /** Close all connections */
  close(): void {
    if (this.closed) return;
    this.closed = true;
    // Remove from shared pools map if this was a shared pool
    if (this.poolKey) {
      sharedPools.delete(this.poolKey);
      this.poolKey = null;
    }
    for (const client of this.clients) {
      client.close();
    }
    this.clients.length = 0;
  }

  /** Check if pool is closed */
  isClosed(): boolean {
    return this.closed;
  }

  /** Get aggregated health metrics for the pool */
  getHealth(): {
    healthy: boolean;
    connectedCount: number;
    totalCount: number;
    clients: ConnectionHealth[];
    avgLatencyMs: number;
    totalCommands: number;
    totalErrors: number;
  } {
    const clientHealths = this.clients.map((c) => c.getHealth());
    const connectedCount = clientHealths.filter((h) => h.state === 'connected').length;
    const healthyCount = clientHealths.filter((h) => h.healthy).length;

    const totalCommands = clientHealths.reduce((sum, h) => sum + h.totalCommands, 0);
    const totalErrors = clientHealths.reduce((sum, h) => sum + h.totalErrors, 0);

    // Average latency across connected clients
    const latencies = clientHealths.filter((h) => h.avgLatencyMs > 0).map((h) => h.avgLatencyMs);
    const avgLatency =
      latencies.length > 0 ? latencies.reduce((a, b) => a + b, 0) / latencies.length : 0;

    return {
      healthy: healthyCount > 0, // At least one healthy connection
      connectedCount,
      totalCount: this.clients.length,
      clients: clientHealths,
      avgLatencyMs: Math.round(avgLatency * 100) / 100,
      totalCommands,
      totalErrors,
    };
  }
}

/** Shared pools by host:port key */
const sharedPools = new Map<string, TcpConnectionPool>();

/** Get pool key from options (includes all pool-differentiating params) */
function getPoolKey(options?: PoolOptions): string {
  const host = options?.host ?? 'localhost';
  const port = options?.port ?? 6789;
  const poolSize = options?.poolSize ?? 4;
  const token = options?.token ?? '';
  // Include poolSize and token hash to prevent sharing pools with different configs
  const tokenHash = token ? String(Number(Bun.hash(token)) & 0xffff) : '0';
  return `${host}:${port}:${poolSize}:${tokenHash}`;
}

/** Get or create shared connection pool */
export function getSharedPool(options?: PoolOptions): TcpConnectionPool {
  const key = getPoolKey(options);
  let pool = sharedPools.get(key);

  // Check if pool exists and is healthy (not just not closed)
  if (pool && !pool.isClosed()) {
    pool.addRef();
    return pool;
  }

  // If pool exists but is closed/errored, remove it first
  if (pool) {
    sharedPools.delete(key);
  }

  // Create new pool
  pool = new TcpConnectionPool(options);
  pool.setPoolKey(key); // Track key for cleanup on close
  sharedPools.set(key, pool);
  pool.addRef();
  return pool;
}

/** Release shared pool reference */
export function releaseSharedPool(pool: TcpConnectionPool): void {
  pool.release();
}

/** Close all shared pools */
export function closeAllSharedPools(): void {
  for (const pool of sharedPools.values()) {
    pool.close();
  }
  sharedPools.clear();
}
