defmodule Bunqueue.QueueTest do
  use ExUnit.Case, async: true

  alias Bunqueue.Queue

  test "places the job name inside data and lets the user name win" do
    assert Queue.job_payload("send-email", %{to: "a@b.c"}) == %{
             "name" => "send-email",
             "to" => "a@b.c"
           }

    assert Queue.job_payload("job-name", %{name: "user-name"})["name"] == "user-name"
  end

  test "wraps scalar and list payloads without losing them" do
    assert Queue.job_payload("scalar", 42) == %{"name" => "scalar", "payload" => 42}
    assert Queue.job_payload("list", [1, 2]) == %{"name" => "list", "payload" => [1, 2]}
  end
end
