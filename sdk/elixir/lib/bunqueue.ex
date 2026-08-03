defmodule Bunqueue do
  @moduledoc """
  Official Elixir client for bunqueue protocol version 3.

  `Bunqueue.Queue` is the producing and administration API, while
  `Bunqueue.Worker` implements lease-aware job consumption.
  """

  alias Bunqueue.{Connection, Queue}

  @spec queue(String.t(), keyword()) :: Queue.t()
  def queue(name, options \\ []) do
    {:ok, connection} = Connection.start_link(options)
    Queue.new(name, connection, true)
  end
end
