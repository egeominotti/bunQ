export interface Distribution {
  readonly count: number;
  readonly min: number;
  readonly p50: number;
  readonly p95: number;
  readonly p99: number;
  readonly max: number;
}

export interface DatabaseDelta {
  readonly blksHit: number;
  readonly blksRead: number;
  readonly deadlocks: number;
  readonly tempBytes: number;
  readonly walBytes: number;
  readonly xactCommit: number;
}

export interface PostgresVersionSample {
  readonly accepted: number;
  readonly ackLatencyMs: Distribution;
  readonly admissionJobsPerSecond: number;
  readonly admissionMs: number;
  readonly batchSize: number;
  readonly brokerClaims: readonly number[];
  readonly brokers: number;
  readonly completed: number;
  readonly consumerConnections: number;
  readonly databaseDelta: DatabaseDelta;
  readonly duplicateInvocations: number;
  readonly jobs: number;
  readonly lifecycleJobsPerSecond: number;
  readonly lifecycleMs: number;
  readonly postgresMajor: number;
  readonly postgresSettings: Readonly<Record<string, string>>;
  readonly postgresVersion: string;
  readonly pollIntervalMs: number;
  readonly producerConnections: number;
  readonly processingJobsPerSecond: number;
  readonly processingMs: number;
  readonly poolSize: number;
  readonly pullLatencyMs: Distribution;
  readonly pushLatencyMs: Distribution;
  readonly uniqueAccepted: number;
  readonly uniqueInvoked: number;
}

export interface MetricSummary {
  readonly count: number;
  readonly cvPercent: number;
  readonly max: number;
  readonly mean: number;
  readonly meanCi95High: number;
  readonly meanCi95Low: number;
  readonly median: number;
  readonly min: number;
  readonly p05: number;
  readonly p95: number;
}

export interface VersionTopologySummary {
  readonly admissionJobsPerSecond: MetricSummary;
  readonly brokers: number;
  readonly lifecycleJobsPerSecond: MetricSummary;
  readonly postgresMajor: number;
  readonly postgresVersion: string;
  readonly processingJobsPerSecond: MetricSummary;
  readonly samples: number;
}

export interface MatrixObservation {
  readonly measured: boolean;
  readonly round: number;
  readonly sample: PostgresVersionSample;
}

export interface PostgresVersionMatrixReport {
  readonly completedAt: string;
  readonly configuration: {
    readonly batchSize: number;
    readonly brokers: readonly number[];
    readonly consumerConnections: number;
    readonly jobs: number;
    readonly pollIntervalMs: number;
    readonly producerConnections: number;
    readonly poolSize: number;
    readonly runs: number;
    readonly versions: readonly number[];
    readonly warmups: number;
    readonly workMem: string;
  };
  readonly durationSeconds: number;
  readonly host: Readonly<Record<string, string | number>>;
  readonly observations: readonly MatrixObservation[];
  readonly startedAt: string;
  readonly summaries: readonly VersionTopologySummary[];
}
