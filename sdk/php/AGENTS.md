# PHP SDK agent guide

These instructions apply inside `sdk/php/`. Read [INVARIANTS.md](INVARIANTS.md)
before editing protocol, queue, worker, or flow behavior.

## Change discipline

- Keep PHP 8.1 syntax compatibility even though mutation testing runs on 8.4.
  Use strict types, PSR-4, one class per file, two-space indentation, single
  quotes, and no source file over 300 lines.
- Runtime code may depend only on `rybakit/msgpack`. Eris/PHPUnit are test
  dependencies; Infection/PCOV stay out of the PHP 8.1 dependency solve.
- Start a confirmed bug fix with a regression that fails for the expected
  reason. For invalid input, assert both the exception and that the injected
  broker caller was never invoked.
- Preserve command/envelope spelling from the server handler. Unknown options
  must throw; never “support” a field by silently dropping it.
- Flow changes belong in the pure planner or snapshot validator where possible.
  A valid non-empty plan crosses the transport boundary once with `PUSHF`.
  Topology fields, reserved markers, list shapes, ID/queue validation, and
  snapshot cardinality move together.
- Update [README.md](README.md), [CHANGELOG.md](CHANGELOG.md),
  [INVARIANTS.md](INVARIANTS.md), and the module map in [CLAUDE.md](CLAUDE.md)
  when a public or correctness contract changes.

## Focused verification

```bash
composer install --no-interaction --no-progress
composer validate --strict
composer test:property
for f in $(find src tests -name '*.php'); do php -l "$f"; done
```

Eris prints a replay command. Preserve its `ERIS_SEED` and minimized input,
then convert a confirmed defect into a deterministic test.

Mutation uses the verified Infection 0.34.1 PHAR and PCOV on PHP 8.4:

```bash
composer mutation
```

Do not lower the MSI thresholds to pass a campaign. Classify every survivor as
killed, equivalent, or requiring a regression. Review
`build/infection-summary.json` and the full JSON/log reports.

## Broker and release gates

```bash
php tests/run-e2e.php
cd ../conformance && bun runner.ts --driver "php drivers/php.php"
cd ../.. && bun run test:sandbox:sdk
```

The e2e runner starts with the property suite. Close all connections and workers
in `finally`, use unique queues, and clean broker state. If the isolated SDK
sandbox cannot run, report it as a blocker; native tests are diagnostic, not a
replacement.
