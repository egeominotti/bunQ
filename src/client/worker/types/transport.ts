export interface PendingAck {
  id: string;
  result: unknown;
  token?: string;
  resolve: () => void;
  reject: (error: Error) => void;
}

export interface TcpConnection {
  send: (command: Record<string, unknown>) => Promise<Record<string, unknown>>;
}
