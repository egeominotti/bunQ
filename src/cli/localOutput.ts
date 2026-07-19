import { VERSION } from '../shared/version';

export interface LocalOutput {
  exitCode: number;
  json: unknown;
  text: string;
  stream?: 'stdout' | 'stderr';
}

export interface VersionInfo {
  client: { name: 'bunqueue'; version: string };
  server: {
    endpoint: string;
    reachable: boolean;
    version?: string;
  };
  mismatch: boolean;
}

/** The only writer used by local commands, keeping JSON to one stream and document. */
export function emitLocalOutput(output: LocalOutput, json: boolean): void {
  const text = json ? JSON.stringify(output.json, null, 2) : output.text;
  const writer = output.stream === 'stderr' ? console.error : console.log;
  writer(text);
}

export async function collectVersionInfo(host: string, httpPort: number): Promise<VersionInfo> {
  const server: VersionInfo['server'] = {
    endpoint: `${host}:${httpPort}`,
    reachable: false,
  };
  try {
    const response = await fetch(`http://${server.endpoint}/health`, {
      signal: AbortSignal.timeout(3000),
    });
    if (response.ok) {
      const data = (await response.json()) as { version?: string };
      server.reachable = true;
      if (data.version) server.version = data.version;
    }
  } catch {
    // Unreachable is part of the version result, not an exceptional path.
  }
  return {
    client: { name: 'bunqueue', version: VERSION },
    server,
    mismatch: server.version !== undefined && server.version !== VERSION,
  };
}

export function formatVersionInfoText(info: VersionInfo): string {
  const lines = [`Client: bunqueue v${info.client.version}`];
  if (!info.server.reachable) {
    lines.push(`Server: not reachable (${info.server.endpoint})`);
  } else if (info.server.version) {
    lines.push(`Server: bunqueue v${info.server.version}`);
    if (info.mismatch) {
      lines.push('', '⚠ Version mismatch! Update server or client to match.');
    }
  }
  return lines.join('\n');
}
