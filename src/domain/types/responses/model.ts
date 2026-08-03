import type { Job, JobState } from '../job';
import type { ProtocolCapability } from '../protocol';

export type { ProtocolCapability } from '../protocol';

interface BaseResponse {
  readonly ok: boolean;
  readonly reqId?: string;
}
export interface OkResponse extends BaseResponse {
  readonly ok: true;
  readonly id?: string;
}
export interface BatchResponse extends BaseResponse {
  readonly ok: true;
  readonly ids: string[];
}
export interface JobResponse extends BaseResponse {
  readonly ok: true;
  readonly job: Job;
}
export interface NullableJobResponse extends BaseResponse {
  readonly ok: true;
  readonly job: Job | null;
}
export interface PulledJobResponse extends BaseResponse {
  readonly ok: true;
  readonly job: Job | null;
  readonly token: string | null;
}
export interface PulledJobsResponse extends BaseResponse {
  readonly ok: true;
  readonly jobs: Job[];
  readonly tokens: string[];
}
export interface JobsResponse extends BaseResponse {
  readonly ok: true;
  readonly jobs: Job[];
}
export interface StateResponse extends BaseResponse {
  readonly ok: true;
  readonly id: string;
  readonly state: JobState;
}
export interface ResultResponse extends BaseResponse {
  readonly ok: true;
  readonly id: string;
  readonly result: unknown;
}
export interface JobCounts {
  readonly waiting: number;
  readonly prioritized: number;
  readonly delayed: number;
  readonly active: number;
  readonly completed: number;
  readonly failed: number;
  readonly 'waiting-children': number;
  readonly paused: number;
}
export interface JobCountsResponse extends BaseResponse {
  readonly ok: true;
  readonly counts: JobCounts;
}
export interface QueueInfo {
  readonly name: string;
  readonly waiting: number;
  readonly delayed: number;
  readonly active: number;
  readonly paused: boolean;
}
export interface QueuesResponse extends BaseResponse {
  readonly ok: true;
  readonly queues: QueueInfo[];
}
export interface ProgressResponse extends BaseResponse {
  readonly ok: true;
  readonly progress: number;
  readonly message: string | null;
}
export interface BoolResponse extends BaseResponse {
  readonly ok: true;
  readonly value: boolean;
}
export interface CountResponse extends BaseResponse {
  readonly ok: true;
  readonly count: number;
  readonly ids?: string[];
}
export interface StatsData {
  readonly waiting: number;
  readonly active: number;
  readonly delayed: number;
  readonly dlq: number;
  readonly completed: number;
  readonly failed: number;
  readonly uptime: number;
  readonly pushPerSec: number;
  readonly pullPerSec: number;
}
export interface StatsResponse extends BaseResponse {
  readonly ok: true;
  readonly stats: StatsData;
}
export interface MetricsData {
  readonly totalPushed: number;
  readonly totalPulled: number;
  readonly totalCompleted: number;
  readonly totalFailed: number;
  readonly avgLatencyMs: number;
  readonly avgProcessingMs: number;
  readonly memoryUsageMb: number;
  readonly sqliteSizeMb: number;
  readonly activeConnections: number;
}
export interface MetricsResponse extends BaseResponse {
  readonly ok: true;
  readonly metrics: MetricsData;
}
export interface CronInfo {
  readonly name: string;
  readonly jobName?: string;
  readonly queue: string;
  readonly schedule: string | null;
  readonly repeatEvery: number | null;
  readonly nextRun: number;
  readonly executions: number;
  readonly maxLimit?: number | null;
  readonly timezone?: string | null;
  readonly priority?: number;
}
export interface CronResponse extends BaseResponse {
  readonly ok: true;
  readonly cron: CronInfo;
}
export interface CronListResponse extends BaseResponse {
  readonly ok: true;
  readonly crons: CronInfo[];
}
export interface ErrorResponse extends BaseResponse {
  readonly ok: false;
  readonly error: string;
}
export interface HelloResponse extends BaseResponse {
  readonly ok: true;
  readonly protocolVersion: number;
  readonly capabilities: ProtocolCapability[];
  readonly server: string;
  readonly version: string;
}
export interface DataResponse<T> extends BaseResponse {
  readonly ok: true;
  readonly data: T;
}

export type Response =
  | OkResponse
  | BatchResponse
  | JobResponse
  | NullableJobResponse
  | PulledJobResponse
  | PulledJobsResponse
  | JobsResponse
  | StateResponse
  | ResultResponse
  | JobCountsResponse
  | QueuesResponse
  | ProgressResponse
  | BoolResponse
  | CountResponse
  | StatsResponse
  | MetricsResponse
  | CronResponse
  | CronListResponse
  | HelloResponse
  | ErrorResponse
  | DataResponse<unknown>;
