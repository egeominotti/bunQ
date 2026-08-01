export interface CloudConfig {
  readonly url: string;
  readonly apiKey: string;
  readonly instanceId: string;
  readonly signingSecret: string | null;
  readonly instanceName: string;
  readonly intervalMs: number;
  readonly includeJobData: boolean;
  readonly redactFields: string[];
  readonly eventFilter: string[];
  readonly bufferSize: number;
  readonly circuitBreakerThreshold: number;
  readonly circuitBreakerResetMs: number;
  readonly useWebSocket: boolean;
  readonly useHttp: boolean;
  readonly dataPath: string | null;
  readonly remoteCommands: boolean;
}
