defmodule Bunqueue.ConnectionError do
  defexception [:message, :reason]
end

defmodule Bunqueue.CommandError do
  defexception [:message, :command, :response]

  @spec not_found?(Exception.t()) :: boolean()
  def not_found?(%__MODULE__{message: message}) do
    message |> String.downcase() |> String.contains?("not found")
  end

  def not_found?(_), do: false
end

defmodule Bunqueue.AuthenticationError do
  defexception [:message]
end

defmodule Bunqueue.TimeoutError do
  defexception [:message, :timeout]
end

defmodule Bunqueue.ProtocolError do
  defexception [:message]
end

defmodule Bunqueue.UnrecoverableError do
  defexception [:message]
end
