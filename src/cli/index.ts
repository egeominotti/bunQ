#!/usr/bin/env bun

import { executeCommand } from './client';
import { executeBackupCommand, isBackupCommand } from './commands/backup';
import { formatDoctorText, runDoctor } from './commands/doctor';
import { runServer } from './commands/server';
import { runHealthcheck } from './commands/healthcheck';
import {
  renderCronAddHelp,
  renderHelp,
  renderPushHelp,
  renderServerHelp,
  renderVersion,
} from './help';
import { parseGlobalOptions } from './globalOptions';
import {
  collectVersionInfo,
  emitLocalOutput,
  formatVersionInfoText,
  type LocalOutput,
} from './localOutput';
import { VERSION } from '../shared/version';

export { parseGlobalOptions } from './globalOptions';

function finish(output: LocalOutput, json: boolean): never {
  emitLocalOutput(output, json);
  process.exit(output.exitCode);
}

function helpFor(command: string | undefined, color: boolean): string {
  if (command === 'start') return renderServerHelp();
  if (command === 'push') return renderPushHelp();
  if (command === 'cron') return renderCronAddHelp();
  return renderHelp(color);
}

async function executeLocalCommand(
  command: string,
  args: string[],
  host: string,
  port: number,
  json: boolean
): Promise<boolean> {
  if (command === 'healthcheck') {
    process.exit(await runHealthcheck(args));
  }

  if (command === 'version') {
    const info = await collectVersionInfo(host, port + 1);
    finish(
      {
        exitCode: 0,
        json: { ok: true, ...info },
        text: formatVersionInfoText(info),
      },
      json
    );
  }

  if (command === 'doctor') {
    const report = await runDoctor(host, port + 1);
    finish(
      {
        exitCode: report.exitCode,
        json: { ok: report.exitCode === 0, ...report },
        text: formatDoctorText(report),
      },
      json
    );
  }

  if (!isBackupCommand(command)) return false;
  try {
    const result = await executeBackupCommand(args);
    finish(
      {
        exitCode: result.success ? 0 : 1,
        json: result,
        text: result.data
          ? `${result.message}\n${JSON.stringify(result.data, null, 2)}`
          : result.message,
      },
      json
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown error occurred';
    finish(
      {
        exitCode: 1,
        json: { success: false, message },
        text: `Error: ${message}`,
        stream: 'stderr',
      },
      json
    );
  }
}

export async function main(): Promise<void> {
  const { options, commandArgs } = parseGlobalOptions();
  const command = commandArgs[0];

  if (options.version) {
    finish(
      {
        exitCode: 0,
        json: { ok: true, version: VERSION },
        text: renderVersion(VERSION),
      },
      options.json
    );
  }

  if (options.help) {
    const help = helpFor(command, !options.json);
    finish(
      {
        exitCode: 0,
        json: { ok: true, help },
        text: help,
      },
      options.json
    );
  }

  if (!command || command === 'start' || command.startsWith('-')) {
    const serverArgs = command === 'start' ? commandArgs.slice(1) : commandArgs;
    await runServer(serverArgs, false);
    return;
  }

  if (
    await executeLocalCommand(
      command,
      commandArgs.slice(1),
      options.host,
      options.port,
      options.json
    )
  ) {
    return;
  }

  await executeCommand(command, commandArgs.slice(1), {
    host: options.host,
    port: options.port,
    token: options.token,
    tls: options.tls,
    json: options.json,
  });
}

if (import.meta.main) {
  main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : 'Unknown error occurred';
    const json = process.argv.includes('--json');
    emitLocalOutput(
      {
        exitCode: 1,
        json: { ok: false, error: message },
        text: `Fatal error: ${message}`,
        stream: 'stderr',
      },
      json
    );
    process.exit(1);
  });
}
