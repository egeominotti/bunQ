export interface PendingCommand {
  id: number;
  reqId: string;
  command: Record<string, unknown>;
  resolve: (value: Record<string, unknown>) => void;
  reject: (error: Error) => void;
  timeout: ReturnType<typeof setTimeout>;
  promise?: Promise<Record<string, unknown>>;
}
