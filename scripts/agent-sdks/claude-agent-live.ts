/**
 * LIVE — Claude Agent SDK inside a bunqueue saga, executed against the real API.
 *
 * `query()` spawns the Claude Code harness and needs both a network and credentials, so
 * it cannot run inside the isolated test gate. This script is how the documented example
 * is proven: it runs the exact code that appears on the Agent SDKs guide page and prints
 * what actually happened.
 *
 *   bun scripts/agent-sdks/claude-agent-live.ts
 *
 * Credentials come from the local Claude Code login, or from ANTHROPIC_API_KEY.
 * The agent is confined to a fresh temporary directory; nothing in this repo is touched.
 */

import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { query } from '@anthropic-ai/claude-agent-sdk';
import { Engine, Workflow } from '../../src/client/workflow';
import { shutdownManager } from '../../src/client';

const MODEL = Bun.env.AGENT_MODEL ?? 'claude-sonnet-5';

function log(scenario: string, msg: string) {
  console.log(`[${scenario}] ${msg}`);
}

/**
 * One agent turn. Returns the session id so the workflow can journal it, which is what
 * makes a later turn a continuation rather than a fresh conversation.
 */
async function agentTurn(prompt: string, cwd: string, resume?: string) {
  const q = query({
    prompt,
    options: {
      model: MODEL,
      cwd,
      maxTurns: 6,
      allowedTools: ['Read', 'Write', 'Edit'],
      permissionMode: 'bypassPermissions',
      ...(resume ? { resume } : {}),
    },
  });

  let sessionId = '';
  let text = '';
  for await (const message of q) {
    if (message.type === 'system' && message.subtype === 'init') sessionId = message.session_id;
    if (message.type === 'result' && message.subtype === 'success') {
      text = message.result;
      sessionId = message.session_id;
    }
  }
  return { sessionId, text };
}

async function settle(engine: Engine, id: string, want: string, ms = 180_000) {
  const deadline = Date.now() + ms;
  while (engine.getExecution(id)?.state !== want && Date.now() < deadline) await Bun.sleep(200);
  return engine.getExecution(id)?.state;
}

// ---------------------------------------------------------------------------
// S1 — a rejected review unwinds the effects around a real agent turn
// ---------------------------------------------------------------------------

async function scenario1() {
  const S = 'S1';
  const cwd = await mkdtemp(join(tmpdir(), 'bq-agent-'));
  await writeFile(join(cwd, 'VERSION.txt'), 'lodash 4.17.20\n');

  // Stands in for the forge. Real code would call the GitHub API here.
  const forge = { branches: new Set<string>(), prs: new Map<string, 'open' | 'closed'>() };

  const flow = new Workflow<{ repo: string }>('dependency-upgrade')
    .step(
      'create-branch',
      async (ctx) => {
        forge.branches.add(`${ctx.input.repo}#bot/upgrade`);
        log(S, `branch created (${forge.branches.size} open)`);
        return { branch: 'bot/upgrade' };
      },
      {
        compensate: async (ctx) => {
          forge.branches.delete(`${ctx.input.repo}#bot/upgrade`);
          log(S, 'compensate: branch deleted');
        },
      }
    )
    .step(
      'open-draft-pr',
      async () => {
        forge.prs.set('PR-7', 'open');
        log(S, 'draft PR opened');
        return { pr: 'PR-7' };
      },
      {
        compensate: async () => {
          forge.prs.set('PR-7', 'closed');
          log(S, 'compensate: PR closed');
        },
      }
    )
    .step('agent-edit', async () => {
      const r = await agentTurn(
        'Edit VERSION.txt so it reads exactly "lodash 4.17.21". Then reply with the single word DONE.',
        cwd
      );
      log(S, `agent session ${r.sessionId.slice(0, 8)} replied: ${r.text.trim().slice(0, 40)}`);
      return { sessionId: r.sessionId, reply: r.text };
    })
    .waitFor('code-review', { timeout: 600_000 })
    .step(
      'merge',
      async (ctx) => {
        const decision = ctx.signals['code-review'] as { approved: boolean };
        if (!decision.approved) throw new Error('review rejected the upgrade');
        return { merged: true };
      },
      { retry: 1 }
    );

  const engine = new Engine({ embedded: true });
  engine.register(flow);
  const run = await engine.start('dependency-upgrade', { repo: 'acme/api' });

  if ((await settle(engine, run.id, 'waiting')) !== 'waiting') {
    throw new Error(`S1 never parked: ${engine.getExecution(run.id)?.state}`);
  }
  log(S, `parked at the review gate, PR is ${forge.prs.get('PR-7')}`);
  log(S, `file on disk: ${(await readFile(join(cwd, 'VERSION.txt'), 'utf8')).trim()}`);

  await engine.signal(run.id, 'code-review', { approved: false });
  if ((await settle(engine, run.id, 'failed')) !== 'failed') {
    throw new Error(`S1 did not fail: ${engine.getExecution(run.id)?.state}`);
  }

  const exec = engine.getExecution(run.id);
  const ok =
    forge.prs.get('PR-7') === 'closed' &&
    forge.branches.size === 0 &&
    exec?.steps['open-draft-pr'].compensation?.status === 'compensated' &&
    exec?.steps['create-branch'].compensation?.status === 'compensated';

  log(
    S,
    `rollbackStatus=${exec?.rollbackStatus} prs=${forge.prs.get('PR-7')} branches=${forge.branches.size}`
  );
  await engine.close(true);
  return ok;
}

// ---------------------------------------------------------------------------
// S2 — the journaled session id makes the second turn a continuation
// ---------------------------------------------------------------------------

async function scenario2() {
  const S = 'S2';
  const cwd = await mkdtemp(join(tmpdir(), 'bq-agent-'));

  const flow = new Workflow('agent-two-phase')
    .step('turn-1', async () => {
      const r = await agentTurn('Remember this codeword: FLAMINGO. Reply with just: OK', cwd);
      log(S, `turn 1 session ${r.sessionId.slice(0, 8)} → ${r.text.trim().slice(0, 30)}`);
      return { sessionId: r.sessionId, text: r.text };
    })
    .step('turn-2', async (ctx) => {
      const prev = ctx.steps['turn-1'] as { sessionId: string };
      // The id came out of the journal, not out of a variable in this process.
      const r = await agentTurn(
        'What was the codeword? Reply with just the word.',
        cwd,
        prev.sessionId
      );
      log(S, `turn 2 session ${r.sessionId.slice(0, 8)} → ${r.text.trim().slice(0, 30)}`);
      return { sessionId: r.sessionId, text: r.text };
    });

  const engine = new Engine({ embedded: true });
  engine.register(flow);
  const run = await engine.start('agent-two-phase');

  if ((await settle(engine, run.id, 'completed')) !== 'completed') {
    const e = engine.getExecution(run.id);
    log(S, `state=${e?.state} err=${JSON.stringify(e?.steps)}`);
    await engine.close(true);
    return false;
  }

  const exec = engine.getExecution(run.id);
  const t1 = exec?.steps['turn-1'].result as { sessionId: string };
  const t2 = exec?.steps['turn-2'].result as { sessionId: string; text: string };
  // The continuation recalled something only turn 1 was told.
  const remembered = t2.text.toUpperCase().includes('FLAMINGO');
  log(S, `same session=${t2.sessionId === t1.sessionId} remembered=${remembered}`);

  await engine.close(true);
  return remembered;
}

// ---------------------------------------------------------------------------

const results: Array<[string, boolean]> = [];
results.push(['S1 rejected review unwinds around a live agent turn', await scenario1()]);
results.push(['S2 journaled session id continues the conversation', await scenario2()]);

console.log('\n────────────────────────────────────────');
for (const [name, ok] of results) console.log(`${ok ? 'PASS' : 'FAIL'}  ${name}`);
const passed = results.filter(([, ok]) => ok).length;
console.log(`${passed}/${results.length} passed`);
console.log('────────────────────────────────────────');

await shutdownManager();
process.exit(passed === results.length ? 0 : 1);
