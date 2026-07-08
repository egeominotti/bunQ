/** E2E: auth — dedicated server with AUTH_TOKENS (own fixture, not shared). */

import { AuthError, CommandError, Connection, Queue } from '../dist/index.js';
import { assert, qname, startServer, test } from './harness.ts';

test('auth: valid token, wrong token → AuthError, no token → rejected', async () => {
  const { port, proc } = await startServer({ AUTH_TOKENS: 'secret-token' });
  try {
    // 1. valid token works
    const authed = new Queue(qname('auth'), { host: '127.0.0.1', port, token: 'secret-token' });
    const job = await authed.add('ok', { x: 1 });
    const state = await authed.getJobState(job.id);
    assert(state === 'waiting' || state === 'prioritized', 'authenticated add works');
    authed.close();

    // 2. wrong token → AuthError on connect
    const wrong = new Connection({ host: '127.0.0.1', port, token: 'wrong-token' });
    let authErr = false;
    try {
      await wrong.connect();
    } catch (err) {
      authErr = err instanceof AuthError;
    }
    assert(authErr, 'wrong token raises AuthError');
    wrong.close();

    // 3. no token → commands rejected
    const anon = new Connection({ host: '127.0.0.1', port });
    let rejected = false;
    try {
      await anon.call({ cmd: 'PUSH', queue: 'authless', data: { name: 't' } });
    } catch (err) {
      rejected = err instanceof CommandError;
    }
    assert(rejected, 'unauthenticated command rejected with CommandError');
    anon.close();
  } finally {
    proc.kill();
  }
});
