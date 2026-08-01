import type { Job } from '../types/job';
import type {
  BatchResponse,
  DataResponse,
  ErrorResponse,
  HelloResponse,
  JobCounts,
  JobCountsResponse,
  JobResponse,
  JobsResponse,
  MetricsData,
  MetricsResponse,
  NullableJobResponse,
  OkResponse,
  ProtocolCapability,
  PulledJobResponse,
  PulledJobsResponse,
  StatsData,
  StatsResponse,
} from '../types/responses/model';

export function ok(id?: string, reqId?: string): OkResponse {
  return { ok: true, id, reqId };
}
export function batch(ids: string[], reqId?: string): BatchResponse {
  return { ok: true, ids, reqId };
}
export function job(value: Job, reqId?: string): JobResponse {
  return { ok: true, job: value, reqId };
}
export function nullableJob(value: Job | null, reqId?: string): NullableJobResponse {
  return { ok: true, job: value, reqId };
}
export function pulledJob(
  value: Job | null,
  token: string | null,
  reqId?: string
): PulledJobResponse {
  return { ok: true, job: value, token, reqId };
}
export function pulledJobs(list: Job[], tokens: string[], reqId?: string): PulledJobsResponse {
  return { ok: true, jobs: list, tokens, reqId };
}
export function jobs(list: Job[], reqId?: string): JobsResponse {
  return { ok: true, jobs: list, reqId };
}
export function error(message: string, reqId?: string): ErrorResponse {
  return { ok: false, error: message, reqId };
}
export function hello(
  protocolVersion: number,
  capabilities: ProtocolCapability[],
  server: string,
  version: string,
  reqId?: string
): HelloResponse {
  return { ok: true, protocolVersion, capabilities, server, version, reqId };
}
export function data<T>(payload: T, reqId?: string): DataResponse<T> {
  return { ok: true, data: payload, reqId };
}
export function counts(value: JobCounts, reqId?: string): JobCountsResponse {
  return { ok: true, counts: value, reqId };
}
export function stats(value: StatsData, reqId?: string): StatsResponse {
  return { ok: true, stats: value, reqId };
}
export function metrics(value: MetricsData, reqId?: string): MetricsResponse {
  return { ok: true, metrics: value, reqId };
}
