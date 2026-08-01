/** Shared request metadata for every protocol command. */
export interface BaseCommand {
  readonly cmd: string;
  readonly reqId?: string;
}
