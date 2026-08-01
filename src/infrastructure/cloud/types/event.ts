export interface CloudEvent {
  instanceId: string;
  timestamp: number;
  jobEvent?: {
    eventType: string;
    queue: string;
    jobId: string;
    error?: string;
    progress?: number;
    data?: unknown;
    prev?: string;
    delay?: number;
  };
}
