#!/usr/bin/env bun
/**
 * bunqueue - High-performance job queue server for Bun
 * Main entry point - routes to CLI for client commands or starts server
 */

// Only run startup dispatch when this file is the program entry point.
// Issue #85: re-exporting `defineConfig` means user config files import this
// module; without this guard, the top-level dispatch would re-run the CLI/server
// on every import and cause "Failed to listen at 0.0.0.0".
if (import.meta.main) {
  const firstArg = process.argv[2];

  // The server boots ONLY for a bare `bunqueue` invocation. `start` and
  // flag-led argv go through the CLI, which detects server mode itself and
  // boots the same full server (shared bootstrap). Every other first argument
  // is a CLI command: known ones execute, unknown ones print
  // "Unknown command: X" and exit 1 — a typo must never silently boot a server.
  if (!firstArg) {
    void startServer();
  } else {
    void import('./cli/index').then(({ main }) => main());
  }
}

import { Logger, type LogLevel } from './shared/logger';
import { loadConfigFile, resolveServerConfig } from './config';
import { bootServer } from './infrastructure/server/bootstrap';
export { defineConfig } from './config';
export type { BunqueueConfig } from './config';

/** Start the server (direct mode) */
async function startServer(): Promise<void> {
  // Load config file (bunqueue.config.ts) if present, then merge with env vars
  const fileConfig = await loadConfigFile();
  const config = resolveServerConfig(fileConfig);
  await bootServer(fileConfig, config);
}

// Logger env-var bootstrap only applies when this file is the entry point.
// Imported consumers (e.g. user config files using `defineConfig`) must not
// have their process Logger state mutated as a side effect — see Issue #85.
if (import.meta.main) {
  if (Bun.env.LOG_FORMAT === 'json') {
    Logger.enableJsonMode();
  }

  if (Bun.env.LOG_LEVEL) {
    const validLevels: LogLevel[] = ['debug', 'info', 'warn', 'error'];
    const level = Bun.env.LOG_LEVEL.toLowerCase();
    if (validLevels.includes(level as LogLevel)) {
      Logger.setLevel(level as LogLevel);
    }
  }
}
