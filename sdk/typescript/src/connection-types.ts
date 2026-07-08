/** Connection option and message types. */

export type TlsOption = boolean | { caFile?: string; rejectUnauthorized?: boolean } | undefined;

export interface ConnectionOptions {
  host?: string;
  port?: number;
  token?: string;
  tls?: TlsOption;
  connectTimeoutMs?: number;
  commandTimeoutMs?: number;
}

export type Command = Record<string, unknown> & { cmd: string };
export type Response = Record<string, unknown> & { ok: boolean };

export interface Pending {
  resolve: (response: Response) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}
