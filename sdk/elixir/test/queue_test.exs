defmodule Bunqueue.QueueTest do
  use ExUnit.Case, async: true

  alias Bunqueue.Queue

  alias Bunqueue.Job

  test "keeps the job name separate from object user data" do
    assert Queue.job_payload("send-email", %{to: "a@b.c"}) == %{
             "name" => "send-email",
             "data" => %{to: "a@b.c"}
           }

    assert Queue.job_payload("job-name", %{name: "user-name"}) == %{
             "name" => "job-name",
             "data" => %{name: "user-name"}
           }
  end

  test "preserves scalar and list payloads without wrapping them" do
    assert Queue.job_payload("scalar", 42) == %{"name" => "scalar", "data" => 42}
    assert Queue.job_payload("list", [1, 2]) == %{"name" => "list", "data" => [1, 2]}
  end

  test "job prefers top-level name and only unwraps legacy data" do
    modern =
      Job.from_wire(
        %{"name" => "modern-op", "data" => %{"name" => "user-name", "value" => 1}},
        self()
      )

    legacy = Job.from_wire(%{"data" => %{"name" => "legacy-op", "value" => 2}}, self())
    scalar = Job.from_wire(%{"name" => "scalar-op", "data" => false}, self())

    assert {modern.name, modern.data} == {"modern-op", %{"name" => "user-name", "value" => 1}}
    assert {legacy.name, legacy.data} == {"legacy-op", %{"value" => 2}}
    assert {scalar.name, scalar.data} == {"scalar-op", false}
  end
end
