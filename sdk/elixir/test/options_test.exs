defmodule Bunqueue.OptionsTest do
  use ExUnit.Case, async: true

  alias Bunqueue.Options

  test "renames documented public options to wire fields" do
    assert Options.job(attempts: 4, jobId: "single") == %{
             "maxAttempts" => 4,
             "jobId" => "single"
           }

    assert Options.job(%{"jobId" => "bulk"}, true)["customId"] == "bulk"
  end

  test "scheduler limit and timezone reach their exact wire names" do
    assert Options.scheduler(limit: 3, tz: "UTC") == %{
             "maxLimit" => 3,
             "timezone" => "UTC"
           }
  end

  test "unknown options are rejected rather than silently dropped" do
    assert_raise ArgumentError, ~r/unknown job option/, fn ->
      Options.job(typo: true)
    end

    assert_raise ArgumentError, ~r/not supported/, fn ->
      Options.scheduler_job(priority: 1)
    end
  end
end
