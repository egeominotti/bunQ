defmodule Bunqueue.ErrorsTest do
  use ExUnit.Case, async: true

  alias Bunqueue.CommandError

  test "not-found matching is typed and case insensitive" do
    error = %CommandError{message: "Job Not Found", command: "GetJob"}
    assert CommandError.not_found?(error)
    refute CommandError.not_found?(%CommandError{message: "permission denied"})
    refute CommandError.not_found?(RuntimeError.exception("not found"))
  end
end
