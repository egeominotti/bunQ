import type { Engine, Execution } from '../../src/client/workflow';

/** Read every page so lifecycle invariants never silently stop at the default 100 rows. */
export function listAllExecutions(engine: Engine): Execution[] {
  const executions: Execution[] = [];
  const limit = 1000;
  for (let offset = 0; ; offset += limit) {
    const page = engine.listExecutions(undefined, undefined, { limit, offset });
    executions.push(...page);
    if (page.length < limit) return executions;
  }
}
