import { resolveToken } from '../client/resolveToken';

export interface GlobalOptions {
  host: string;
  port: number;
  token?: string;
  tls?: boolean | { rejectUnauthorized?: boolean; caFile?: string };
  json: boolean;
  help: boolean;
  version: boolean;
}

interface TlsFlagState {
  enabled: boolean;
  noVerify: boolean;
  caFile?: string;
}

interface HostPortState {
  host: string;
  port: number;
  hostExplicit: boolean;
  portExplicit: boolean;
}

function resolveEnvPort(currentPort: number): number {
  const envPort = Bun.env.TCP_PORT ?? Bun.env.BUNQUEUE_TCP_PORT ?? Bun.env.BQ_TCP_PORT;
  if (!envPort) return currentPort;
  const parsed = parseInt(envPort, 10);
  if (Number.isNaN(parsed) || parsed < 1 || parsed > 65535) {
    console.warn(`Warning: Invalid env port "${envPort}". Using ${currentPort}.`);
    return currentPort;
  }
  return parsed;
}

function resolveEnvHost(currentHost: string): string {
  return Bun.env.HOST ?? Bun.env.BUNQUEUE_HOST ?? Bun.env.BQ_HOST ?? currentHost;
}

function applyTlsFlag(
  arg: string,
  allArgs: string[],
  index: number,
  state: TlsFlagState,
  commandArgs: string[]
): number {
  if (arg === '--tls') {
    state.enabled = true;
    return index;
  }
  if (arg === '--tls-no-verify') {
    state.noVerify = true;
    return index;
  }
  if (arg === '--tls-ca') {
    const value = allArgs[index + 1];
    if (value === undefined || value.startsWith('-')) {
      console.warn('Warning: --tls-ca requires a file path. Option ignored.');
      return index;
    }
    state.caFile = value;
    return index + 1;
  }
  if (arg.startsWith('--tls-ca=')) {
    const value = arg.slice(9);
    if (value) state.caFile = value;
    else console.warn('Warning: --tls-ca= requires a file path. Option ignored.');
    return index;
  }
  commandArgs.push(arg);
  return index;
}

function buildTlsOption(state: TlsFlagState): GlobalOptions['tls'] {
  if (state.noVerify || state.caFile !== undefined) {
    return {
      ...(state.noVerify && { rejectUnauthorized: false }),
      ...(state.caFile !== undefined && { caFile: state.caFile }),
    };
  }
  return state.enabled ? true : undefined;
}

function commandOwnsShortT(commandArgs: string[]): boolean {
  return commandArgs[0] === 'pull' || (commandArgs[0] === 'job' && commandArgs[1] === 'wait');
}

function applyTokenFlag(
  arg: string,
  allArgs: string[],
  index: number,
  state: { token?: string },
  commandArgs: string[]
): number {
  if (arg === '-t' && commandOwnsShortT(commandArgs)) {
    commandArgs.push(arg);
    return index;
  }
  const value = allArgs[index + 1];
  if (value === undefined || value.startsWith('-')) {
    console.warn('Warning: --token requires a value. Token not set.');
    return index;
  }
  state.token = value;
  return index + 1;
}

function applyHostFlag(allArgs: string[], index: number, state: HostPortState): number {
  const value = allArgs[index + 1];
  if (value === undefined || value.startsWith('-')) {
    console.warn('Warning: --host requires a value. Using localhost.');
    return index;
  }
  state.host = value;
  state.hostExplicit = true;
  return index + 1;
}

function applyPortFlag(allArgs: string[], index: number, state: HostPortState): number {
  const value = allArgs[index + 1];
  if (value === undefined || value.startsWith('-')) {
    console.warn('Warning: --port requires a value. Using default port 6789.');
    return index;
  }
  const parsed = parseInt(value, 10);
  if (Number.isNaN(parsed) || parsed < 1 || parsed > 65535) {
    console.warn(`Warning: Invalid port "${value}". Using default port 6789.`);
    state.port = 6789;
  } else {
    state.port = parsed;
    state.portExplicit = true;
  }
  return index + 1;
}

function warnAmbiguousAttachedShort(arg: string, commandArgs: string[]): void {
  if (!/^-[Hpthv][^-\s]/.test(arg)) return;
  if (arg.startsWith('-t') && commandOwnsShortT(commandArgs)) return;
  console.warn(
    `Warning: "${arg}" looks like a short flag with an attached value; ` +
      `use the separated form ("${arg.slice(0, 2)} ${arg.slice(2)}") or the long form.`
  );
}

export function parseGlobalOptions(allArgs = process.argv.slice(2)): {
  options: GlobalOptions;
  commandArgs: string[];
} {
  const hp: HostPortState = {
    host: 'localhost',
    port: 6789,
    hostExplicit: false,
    portExplicit: false,
  };
  const tokenState: { token?: string } = {};
  const tlsState: TlsFlagState = { enabled: false, noVerify: false };
  const commandArgs: string[] = [];
  let json = false;
  let help = false;
  let version = false;

  for (let index = 0; index < allArgs.length; index++) {
    const arg = allArgs[index];
    if (arg === '--') {
      commandArgs.push(...allArgs.slice(index + 1));
      break;
    }
    if (arg === '--host' || arg === '-H') {
      index = applyHostFlag(allArgs, index, hp);
    } else if (arg === '--port' || arg === '-p') {
      index = applyPortFlag(allArgs, index, hp);
    } else if (arg === '--token' || arg === '-t') {
      index = applyTokenFlag(arg, allArgs, index, tokenState, commandArgs);
    } else if (arg.startsWith('--tls')) {
      index = applyTlsFlag(arg, allArgs, index, tlsState, commandArgs);
    } else if (arg === '--json') {
      json = true;
    } else if (arg === '--help' || (arg === '-h' && commandArgs.length === 0)) {
      help = true;
    } else if (arg === '--version' || (arg === '-v' && commandArgs.length === 0)) {
      version = true;
    } else if (arg.startsWith('--host=')) {
      hp.host = arg.slice(7);
      hp.hostExplicit = true;
    } else if (arg.startsWith('--port=')) {
      const raw = arg.slice(7);
      const parsed = parseInt(raw, 10);
      if (Number.isNaN(parsed) || parsed < 1 || parsed > 65535) {
        console.warn(`Warning: Invalid port "${raw}". Using default port 6789.`);
        hp.port = 6789;
      } else {
        hp.port = parsed;
        hp.portExplicit = true;
      }
    } else if (arg.startsWith('--token=')) {
      const value = arg.slice(8);
      if (value) tokenState.token = value;
      else console.warn('Warning: --token= requires a value. Token not set.');
    } else {
      warnAmbiguousAttachedShort(arg, commandArgs);
      commandArgs.push(arg);
    }
  }

  const serverMode =
    commandArgs[0] === 'start' || commandArgs.length === 0 || commandArgs[0]?.startsWith('-');
  if (serverMode) {
    if (hp.hostExplicit) commandArgs.push('--host', hp.host);
    if (hp.portExplicit) commandArgs.push('--tcp-port', String(hp.port));
  }

  return {
    options: {
      host: hp.hostExplicit ? hp.host : resolveEnvHost(hp.host),
      port: hp.portExplicit ? hp.port : resolveEnvPort(hp.port),
      token: resolveToken(tokenState.token),
      tls: buildTlsOption(tlsState),
      json,
      help,
      version,
    },
    commandArgs,
  };
}
