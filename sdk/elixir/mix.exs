defmodule Bunqueue.MixProject do
  use Mix.Project

  def project do
    [
      app: :bunqueue_client,
      version: "0.1.1",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      test_ignore_filters: [~r{test/support/}],
      aliases: aliases(),
      deps: deps(),
      description: "Official Elixir client for the bunqueue wire protocol",
      package: [
        licenses: ["MIT"],
        links: %{"Source" => "https://github.com/egeominotti/bunqueue"},
        files: ~w(lib mix.exs README.md CHANGELOG.md LICENSE)
      ]
    ]
  end

  def application do
    [extra_applications: [:logger, :ssl, :public_key, :crypto]]
  end

  defp deps do
    [
      {:msgpax, "~> 2.4"},
      {:jason, "~> 1.4", only: [:dev, :test]},
      {:stream_data, "== 1.4.0", only: :test},
      {:muex, "== 0.8.1", only: [:dev, :test], runtime: false}
    ]
  end

  defp aliases do
    [
      mutants: [
        "cmd mix muex --files lib/bunqueue/flow_planner.ex --test-paths test/flow_planner_property_test.exs --no-filter --fail-at 100",
        "cmd mix muex --files lib/bunqueue/flow_snapshots.ex --test-paths test/flow_snapshots_test.exs --no-filter --fail-at 100"
      ]
    ]
  end
end
