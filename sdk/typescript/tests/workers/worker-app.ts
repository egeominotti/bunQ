/** Workers e2e — entry: routes each path to a scenario, returns JSON.
 * Runs inside workerd (the real Cloudflare Workers runtime) via wrangler. */

import { auth, consumeBatch, cron, dlqRoundtrip, flows } from './routes-advanced.ts';
import { apiAdminExtras, apiChildren, apiJobMethods, apiMoves } from './routes-api.ts';
import {
  addAndQuery,
  bigPayload,
  bulkAndCount,
  controls,
  type Env,
  pipeline,
  unicodePayload,
} from './routes-basic.ts';
import { simpleMode } from './routes-simple.ts';

type Route = (env: Env) => Promise<Record<string, unknown>>;

const ROUTES: Record<string, Route> = {
  '/add-query': addAndQuery,
  '/bulk': bulkAndCount,
  '/controls': controls,
  '/big': bigPayload,
  '/unicode': unicodePayload,
  '/pipeline': pipeline,
  '/consume': consumeBatch,
  '/dlq': dlqRoundtrip,
  '/flows': flows,
  '/cron': cron,
  '/auth': auth,
  '/api-moves': apiMoves,
  '/api-job-methods': apiJobMethods,
  '/api-children': apiChildren,
  '/api-admin-extras': apiAdminExtras,
  '/simple-mode': simpleMode,
};

export default {
  async fetch(req: Request, env: Env): Promise<Response> {
    const path = new URL(req.url).pathname;
    if (path === '/routes') return Response.json({ ok: true, routes: Object.keys(ROUTES) });
    const route = ROUTES[path];
    if (!route)
      return Response.json({ ok: false, error: `unknown route ${path}` }, { status: 404 });
    try {
      const result = await route(env);
      return Response.json({ ok: true, ...result });
    } catch (err) {
      return Response.json(
        { ok: false, error: err instanceof Error ? `${err.name}: ${err.message}` : String(err) },
        { status: 500 }
      );
    }
  },
};
