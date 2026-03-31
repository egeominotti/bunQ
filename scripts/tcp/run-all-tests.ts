#!/usr/bin/env bun
/**
 * Run All TCP Functional Tests
 * Starts the server, runs tests, then shuts down
 */

import { spawn, type Subprocess } from 'bun';
import { readdir } from 'fs/promises';

const SCRIPTS_DIR = import.meta.dir;
const PROJECT_ROOT = `${SCRIPTS_DIR}/../..`;
const TCP_PORT = 16789; // Use different port to avoid conflicts
const HTTP_PORT = 16790;

let serverProcess: Subprocess | null = null;

async function waitForServer(maxWaitMs = 10000): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    try {
      const response = await fetch(`http://localhost:${HTTP_PORT}/health`);
      if (response.ok) return true;
    } catch {
      // Server not ready yet
    }
    await Bun.sleep(100);
  }
  return false;
}

async function startServer(): Promise<void> {
  console.log('🚀 Starting bunqueue server...');

  serverProcess = spawn(['bun', 'run', 'src/main.ts'], {
    cwd: PROJECT_ROOT,
    stdout: 'pipe',
    stderr: 'pipe',
    env: {
      ...process.env,
      TCP_PORT: String(TCP_PORT),
      HTTP_PORT: String(HTTP_PORT),
      // No DATA_PATH = in-memory mode
    },
  });

  const ready = await waitForServer();
  if (!ready) {
    throw new Error('Server failed to start within timeout');
  }
  console.log(`✅ Server ready on TCP:${TCP_PORT} HTTP:${HTTP_PORT}\n`);
}

async function stopServer(): Promise<void> {
  if (serverProcess) {
    console.log('\n🛑 Stopping server...');
    serverProcess.kill();
    await serverProcess.exited;
    serverProcess = null;
  }
}

async function runTest(scriptPath: string): Promise<{ name: string; success: boolean; output: string }> {
  const name = scriptPath.replace('.ts', '').replace('test-', '');

  try {
    const proc = spawn(['bun', 'run', scriptPath], {
      cwd: SCRIPTS_DIR,
      stdout: 'pipe',
      stderr: 'pipe',
      env: {
        ...process.env,
        TCP_PORT: String(TCP_PORT),
        HTTP_PORT: String(HTTP_PORT),
        // NO BUNQUEUE_EMBEDDED - use TCP mode
      },
    });

    const output = await new Response(proc.stdout).text();
    const stderr = await new Response(proc.stderr).text();
    const exitCode = await proc.exited;

    return {
      name,
      success: exitCode === 0,
      output: output + (stderr ? `\nSTDERR: ${stderr}` : ''),
    };
  } catch (e) {
    return {
      name,
      success: false,
      output: `Error running test: ${e}`,
    };
  }
}

async function main() {
  console.log('╔════════════════════════════════════════════════════════════╗');
  console.log('║         bunqueue TCP Functional Test Suite                 ║');
  console.log('╚════════════════════════════════════════════════════════════╝\n');

  try {
    await startServer();

    // Get all test files
    const files = await readdir(SCRIPTS_DIR);
    const testFiles = files
      .filter(f => f.startsWith('test-') && f.endsWith('.ts'))
      .sort();

    console.log(`Found ${testFiles.length} test files:\n`);
    testFiles.forEach(f => console.log(`  • ${f}`));
    console.log('\n' + '─'.repeat(60) + '\n');

    const results: Array<{ name: string; success: boolean; passed: number; failed: number }> = [];

    for (const file of testFiles) {
      console.log(`\n▶ Running: ${file}\n`);

      const result = await runTest(file);

      // Extract passed/failed counts from output
      const passedMatch = result.output.match(/Passed: (\d+)/);
      const failedMatch = result.output.match(/Failed: (\d+)/);

      const passed = passedMatch ? parseInt(passedMatch[1]) : 0;
      const failed = failedMatch ? parseInt(failedMatch[1]) : (result.success ? 0 : 1);

      results.push({
        name: result.name,
        success: result.success,
        passed,
        failed,
      });

      // Print condensed output
      const lines = result.output.split('\n');
      const summaryStart = lines.findIndex(l => l.includes('=== Summary ==='));
      if (summaryStart > 0) {
        console.log(lines.slice(summaryStart).join('\n'));
      }

      const status = result.success ? '✅ PASSED' : '❌ FAILED';
      console.log(`\n${status}: ${file}`);
      console.log('─'.repeat(60));
    }

    // Final summary
    console.log('\n' + '═'.repeat(60));
    console.log('\n📊 FINAL SUMMARY (TCP Mode)\n');

    const totalPassed = results.reduce((sum, r) => sum + r.passed, 0);
    const totalFailed = results.reduce((sum, r) => sum + r.failed, 0);
    const totalTests = results.length;
    const passedTests = results.filter(r => r.success).length;
    const failedTests = results.filter(r => !r.success).length;

    console.log('┌─────────────────────────────────────┬────────┬────────┐');
    console.log('│ Test Suite                          │ Passed │ Failed │');
    console.log('├─────────────────────────────────────┼────────┼────────┤');

    for (const r of results) {
      const name = r.name.padEnd(35);
      const passed = String(r.passed).padStart(6);
      const failed = String(r.failed).padStart(6);
      const status = r.success ? '✓' : '✗';
      console.log(`│ ${status} ${name} │ ${passed} │ ${failed} │`);
    }

    console.log('├─────────────────────────────────────┼────────┼────────┤');
    console.log(`│ ${'TOTAL'.padEnd(35)} │ ${String(totalPassed).padStart(6)} │ ${String(totalFailed).padStart(6)} │`);
    console.log('└─────────────────────────────────────┴────────┴────────┘');

    console.log(`\n📁 Test Suites: ${passedTests}/${totalTests} passed`);
    console.log(`📋 Individual Tests: ${totalPassed}/${totalPassed + totalFailed} passed`);

    await stopServer();

    if (failedTests > 0) {
      console.log('\n❌ Some tests failed!\n');
      console.log('Failed suites:');
      results.filter(r => !r.success).forEach(r => {
        console.log(`  • ${r.name}`);
      });
      process.exit(1);
    } else {
      console.log('\n✅ All TCP tests passed!\n');
      process.exit(0);
    }
  } catch (e) {
    console.error('Fatal error:', e);
    await stopServer();
    process.exit(1);
  }
}

// Handle interrupts
process.on('SIGINT', async () => {
  await stopServer();
  process.exit(1);
});

main().catch(console.error);
