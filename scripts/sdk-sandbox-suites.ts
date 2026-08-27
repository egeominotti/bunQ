export const sdkSandboxSuites = [
  {
    name: 'typescript',
    command: [
      'bash',
      '-c',
      "cd sdk/typescript && bun run build && bun run check && bun run test:property && mkdir -p /tmp/typescript-package && bun pm pack --destination /tmp/typescript-package && bun tests/integration.ts && bun tests/e2e.ts && bun run test:workers && cd ../conformance && env -u BUNQUEUE_CONFORMANCE_POSTGRES_URL bun runner.ts --driver 'bun drivers/typescript.ts' && bun runner.ts --driver 'bun drivers/typescript.ts'",
    ],
  },
  {
    name: 'python',
    command: [
      'bash',
      '-c',
      "cd sdk/python && python -m compileall -q bunqueue tests && python -m pytest tests/test_flow_plan_property.py -q && python -m build --no-isolation --outdir /tmp/python-package && python tests/test_integration.py && python tests/run_e2e.py && cd ../conformance && env -u BUNQUEUE_CONFORMANCE_POSTGRES_URL bun runner.ts --driver 'python drivers/python.py' && bun runner.ts --driver 'python drivers/python.py'",
    ],
  },
  {
    name: 'php',
    command: [
      'bash',
      '-c',
      "cd sdk/php && composer validate --strict --no-check-publish && find src tests -name '*.php' -print0 | xargs -0 -n1 php -l && composer test:property && php tests/run-e2e.php && cd ../conformance && env -u BUNQUEUE_CONFORMANCE_POSTGRES_URL bun runner.ts --driver 'php drivers/php.php' && bun runner.ts --driver 'php drivers/php.php'",
    ],
  },
  {
    name: 'go',
    command: [
      'bash',
      '-c',
      "cd sdk/go && test -z \"$(gofmt -l .)\" && go vet ./... && go list ./... && go test -v ./... && go test -race -run 'Hardening|Regression|Worker' ./... && cd ../conformance && env -u BUNQUEUE_CONFORMANCE_POSTGRES_URL bun runner.ts --driver './drivers/go-driver' && bun runner.ts --driver './drivers/go-driver'",
    ],
  },
  {
    name: 'rust',
    command: [
      'bash',
      '-c',
      "cd sdk/rust && cargo fmt --check && cargo clippy --locked --offline --all-targets -- -D warnings && cargo test --locked --offline && cargo package --locked --offline --allow-dirty --no-verify && cd ../conformance && env -u BUNQUEUE_CONFORMANCE_POSTGRES_URL bun runner.ts --driver 'cargo run --locked --offline --quiet --manifest-path ../rust/Cargo.toml --example conformance-driver' && bun runner.ts --driver 'cargo run --locked --offline --quiet --manifest-path ../rust/Cargo.toml --example conformance-driver'",
    ],
  },
  {
    name: 'elixir',
    command: [
      'bash',
      '-c',
      "cd sdk/elixir && mix format --check-formatted && mix compile --warnings-as-errors && mix test --slowest 20 && mix hex.build && cd ../conformance && env -u BUNQUEUE_CONFORMANCE_POSTGRES_URL bun runner.ts --driver 'cd ../elixir && mix run ../conformance/drivers/elixir.exs' && bun runner.ts --driver 'cd ../elixir && mix run ../conformance/drivers/elixir.exs'",
    ],
  },
] as const;

export type SdkSandboxSuite = (typeof sdkSandboxSuites)[number];
