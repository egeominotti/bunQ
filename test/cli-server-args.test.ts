/**
 * CLI Server Arguments Bug Test
 *
 * Bug: `bunqueue start --host 127.0.0.1 -p 6666` ignores --host and -p flags.
 * Root cause: parseGlobalOptions() consumes --host and --port from argv,
 * removing them from commandArgs, but runServer() never receives these values.
 */

import { describe, test, expect, afterEach } from 'bun:test';
import { parseGlobalOptions } from '../src/cli/index';

describe('CLI server argument passing', () => {
  const originalArgv = process.argv;

  function setArgv(...args: string[]) {
    process.argv = ['bun', 'bunqueue', ...args];
  }

  afterEach(() => {
    process.argv = originalArgv;
  });

  test('--host flag should be available in commandArgs for start command', () => {
    setArgv('start', '--host', '127.0.0.1');
    const { commandArgs } = parseGlobalOptions();

    // The start command's server args (after removing 'start')
    const serverArgs = commandArgs.slice(1);

    // Bug: --host is consumed by parseGlobalOptions and removed from commandArgs,
    // so parseServerArgs never sees it. Either the host must be forwarded to
    // runServer or --host must remain in commandArgs for the start command.
    expect(serverArgs).toContain('--host');
    expect(serverArgs).toContain('127.0.0.1');
  });

  test('-p flag should be available in commandArgs for start command', () => {
    setArgv('start', '-p', '6666');
    const { commandArgs } = parseGlobalOptions();
    const serverArgs = commandArgs.slice(1);

    // Bug: -p is consumed as global --port and never reaches parseServerArgs
    expect(
      serverArgs.includes('-p') ||
        serverArgs.includes('--port') ||
        serverArgs.includes('--tcp-port')
    ).toBe(true);
    expect(serverArgs).toContain('6666');
  });

  test('--host and --port should work together for start command', () => {
    setArgv('start', '--host', '127.0.0.1', '-p', '6666');
    const { commandArgs } = parseGlobalOptions();
    const serverArgs = commandArgs.slice(1);

    // Both flags should be available to the server
    expect(serverArgs).toContain('127.0.0.1');
    expect(serverArgs).toContain('6666');
  });

  test('--host for non-start commands should still work as global option', () => {
    setArgv('push', '--host', '10.0.0.1', 'myqueue', '{"data":1}');
    const { options, commandArgs } = parseGlobalOptions();

    // For client commands, --host IS a global option and should be in options
    expect(options.host).toBe('10.0.0.1');
    expect(commandArgs).toEqual(['push', 'myqueue', '{"data":1}']);
  });

  test('-p for non-start commands should still work as global option', () => {
    setArgv('push', '-p', '7777', 'myqueue', '{"data":1}');
    const { options, commandArgs } = parseGlobalOptions();

    // For client commands, -p IS a global option and should be in options
    expect(options.port).toBe(7777);
    expect(commandArgs).toEqual(['push', 'myqueue', '{"data":1}']);
  });

  test('--host=VALUE syntax should work for start command', () => {
    setArgv('start', '--host=127.0.0.1');
    const { commandArgs } = parseGlobalOptions();
    const serverArgs = commandArgs.slice(1);
    expect(serverArgs).toContain('--host');
    expect(serverArgs).toContain('127.0.0.1');
  });

  test('--port=VALUE syntax should work for start command', () => {
    setArgv('start', '--port=6666');
    const { commandArgs } = parseGlobalOptions();
    const serverArgs = commandArgs.slice(1);
    expect(
      serverArgs.includes('--port') ||
        serverArgs.includes('--tcp-port')
    ).toBe(true);
    expect(serverArgs).toContain('6666');
  });

  test('parseServerArgs should correctly use re-injected flags', () => {
    setArgv('start', '--host', '10.0.0.1', '-p', '7777');
    const { commandArgs } = parseGlobalOptions();
    const serverArgs = commandArgs.slice(1);

    // Import parseServerArgs to verify end-to-end
    const { parseArgs } = require('node:util');
    const { values } = parseArgs({
      args: serverArgs,
      options: {
        'tcp-port': { type: 'string' },
        'http-port': { type: 'string' },
        host: { type: 'string' },
      },
      allowPositionals: false,
      strict: false,
    });

    expect(values.host).toBe('10.0.0.1');
    expect(values['tcp-port']).toBe('7777');
  });
});
