import type {
  Distribution,
  MetricSummary,
  PostgresVersionSample,
  VersionTopologySummary,
} from './types';

function sorted(values: readonly number[]): number[] {
  return [...values].sort((left, right) => left - right);
}

function quantile(values: readonly number[], ratio: number): number {
  if (values.length === 0) return 0;
  const ordered = sorted(values);
  const position = (ordered.length - 1) * ratio;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  if (lower === upper) return ordered[lower];
  const weight = position - lower;
  return ordered[lower] * (1 - weight) + ordered[upper] * weight;
}

function round(value: number, digits = 2): number {
  const scale = 10 ** digits;
  return Math.round(value * scale) / scale;
}

function tCritical95(sampleCount: number): number {
  const values = [
    0, 12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228, 2.201, 2.179, 2.16,
    2.145, 2.131, 2.12, 2.11, 2.101, 2.093, 2.086,
  ];
  return values[Math.min(sampleCount - 1, values.length - 1)] ?? 1.96;
}

export function distribution(values: readonly number[]): Distribution {
  if (values.length === 0) {
    return { count: 0, min: 0, p50: 0, p95: 0, p99: 0, max: 0 };
  }
  const ordered = sorted(values);
  return {
    count: values.length,
    min: round(ordered[0], 3),
    p50: round(quantile(ordered, 0.5), 3),
    p95: round(quantile(ordered, 0.95), 3),
    p99: round(quantile(ordered, 0.99), 3),
    max: round(ordered.at(-1) ?? 0, 3),
  };
}

export function rotate<T>(values: readonly T[], offset: number): T[] {
  const normalized = offset % values.length;
  return [...values.slice(normalized), ...values.slice(0, normalized)];
}

export function summarize(values: readonly number[]): MetricSummary {
  if (values.length === 0) throw new Error('Cannot summarize an empty sample set');
  const ordered = sorted(values);
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance =
    values.length > 1
      ? values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / (values.length - 1)
      : 0;
  const standardDeviation = Math.sqrt(variance);
  const standardError = standardDeviation / Math.sqrt(values.length);
  const t95 = tCritical95(values.length);
  return {
    count: values.length,
    cvPercent: round(mean === 0 ? 0 : (standardDeviation / mean) * 100),
    max: round(ordered.at(-1) ?? 0),
    mean: round(mean),
    meanCi95High: round(mean + t95 * standardError),
    meanCi95Low: round(mean - t95 * standardError),
    median: round(quantile(ordered, 0.5)),
    min: round(ordered[0]),
    p05: round(quantile(ordered, 0.05)),
    p95: round(quantile(ordered, 0.95)),
  };
}

export function summarizeSamples(
  samples: readonly PostgresVersionSample[]
): VersionTopologySummary[] {
  const groups = Map.groupBy(samples, (sample) => `${sample.postgresMajor}:${sample.brokers}`);
  return [...groups.values()]
    .map((group) => ({
      admissionJobsPerSecond: summarize(group.map((sample) => sample.admissionJobsPerSecond)),
      brokers: group[0].brokers,
      lifecycleJobsPerSecond: summarize(group.map((sample) => sample.lifecycleJobsPerSecond)),
      postgresMajor: group[0].postgresMajor,
      postgresVersion: group[0].postgresVersion,
      processingJobsPerSecond: summarize(group.map((sample) => sample.processingJobsPerSecond)),
      samples: group.length,
    }))
    .sort(
      (left, right) => left.postgresMajor - right.postgresMajor || left.brokers - right.brokers
    );
}
