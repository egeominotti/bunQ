export interface WsData {
  id: string;
  authenticated: boolean;
  queueFilter: string | null;
  subscriptions: Set<string> | null;
}
