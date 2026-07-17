defmodule Bunqueue.MixProject do
  use Mix.Project

  def project do
    [
      app: :bunqueue_client,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      test_ignore_filters: [~r{test/support/}],
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
    [extra_applications: [:logger, :ssl, :public_key]]
  end

  defp deps do
    [
      {:msgpax, "~> 2.4"},
      {:jason, "~> 1.4", only: [:dev, :test]}
    ]
  end
end
