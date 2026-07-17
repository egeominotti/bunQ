defmodule Bunqueue.Connection do
  @moduledoc """
  OTP-safe, sequential bunqueue connection.

  A `GenServer` owns the socket, so commands from multiple processes cannot
  interleave frames. Broken or timed-out streams are discarded and reconnect
  lazily on the next command.
  """

  use GenServer

  alias Bunqueue.{
    AuthenticationError,
    CommandError,
    ConnectionError,
    ProtocolError,
    Telemetry,
    TimeoutError,
    Transport,
    Wire
  }

  @default_timeout 30_000
  @max_frame_size 64 * 1024 * 1024
  @type option ::
          {:host, String.t()}
          | {:port, :inet.port_number()}
          | {:token, String.t()}
          | {:timeout, non_neg_integer()}
          | {:tls, boolean()}
          | {:ca_file, String.t()}
          | {:verify, boolean()}
          | {:event_handler, (map() -> any())}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(options \\ []), do: GenServer.start_link(__MODULE__, options)

  @spec call(GenServer.server(), map(), non_neg_integer() | nil) ::
          {:ok, map()} | {:error, Exception.t()}
  def call(connection, command, timeout \\ nil) do
    command_timeout = normalize_timeout(timeout || connection_timeout(connection))
    do_call(connection, command, command_timeout)
  end

  defp do_call(connection, command, command_timeout) do
    GenServer.call(connection, {:command, command, command_timeout}, command_timeout + 2_000)
  catch
    :exit, {:timeout, _} ->
      GenServer.cast(connection, :disconnect)
      {:error, %TimeoutError{message: "command timed out", timeout: command_timeout}}

    :exit, reason ->
      {:error, %ConnectionError{message: "connection process exited", reason: reason}}
  end

  @spec call!(GenServer.server(), map(), non_neg_integer() | nil) :: map()
  def call!(connection, command, timeout \\ nil) do
    case call(connection, command, timeout) do
      {:ok, response} -> response
      {:error, error} -> raise error
    end
  end

  @spec generation(GenServer.server()) :: non_neg_integer()
  def generation(connection), do: GenServer.call(connection, :generation)

  @spec close(GenServer.server()) :: :ok
  def close(connection) do
    if Process.alive?(connection), do: GenServer.stop(connection, :normal)
    :ok
  end

  @impl true
  def init(options) do
    {:ok,
     %{
       host: Keyword.get(options, :host, "127.0.0.1"),
       port: Keyword.get(options, :port, 6789),
       token: Keyword.get(options, :token),
       timeout: normalize_timeout(Keyword.get(options, :timeout, @default_timeout)),
       tls: Keyword.get(options, :tls, false),
       ca_file: Keyword.get(options, :ca_file),
       verify: Keyword.get(options, :verify, true),
       event_handler: Keyword.get(options, :event_handler),
       socket: nil,
       generation: 0,
       request: 0
     }}
  end

  @impl true
  def handle_call(:timeout, _from, state), do: {:reply, state.timeout, state}
  def handle_call(:generation, _from, state), do: {:reply, state.generation, state}

  def handle_call({:command, command, timeout}, _from, state) do
    started_at = System.monotonic_time(:microsecond)

    case ensure_connected(state, timeout) do
      {:ok, connected} ->
        case exchange(connected, command, timeout) do
          {:ok, response, next} ->
            Telemetry.emit(next, "command", %{
              command: command_name(command),
              ok: response["ok"] == true,
              duration_us: elapsed(started_at),
              req_id: response["reqId"]
            })

            {:reply, classify(response, command), next}

          {:error, error, next} ->
            emit_failure(next, error, command, started_at)
            {:reply, {:error, error}, Transport.close(next)}
        end

      {:error, error, next} ->
        emit_failure(next, error, command, started_at)
        {:reply, {:error, error}, Transport.close(next)}
    end
  end

  @impl true
  def handle_cast(:disconnect, state), do: {:noreply, Transport.close(state)}

  @impl true
  def terminate(_reason, state) do
    Telemetry.emit(state, "close", %{})
    Transport.close(state)
  end

  defp ensure_connected(%{socket: nil} = state, timeout) do
    with {:ok, socket} <- Transport.connect(state, timeout) do
      next = %{state | socket: socket, generation: state.generation + 1}
      if state.generation > 0, do: Telemetry.emit(next, "reconnect", %{})

      Telemetry.emit(next, "connected", %{
        host: state.host,
        port: state.port,
        transport: if(state.tls, do: "tls", else: "tcp")
      })

      authenticate(next, timeout)
    else
      {:error, error} -> {:error, connection_error(error), state}
    end
  end

  defp ensure_connected(state, _timeout), do: {:ok, state}

  defp authenticate(%{token: nil} = state, _timeout), do: {:ok, state}

  defp authenticate(state, timeout) do
    case exchange(state, %{"cmd" => "Auth", "token" => state.token}, timeout) do
      {:ok, %{"ok" => true}, next} ->
        Telemetry.emit(next, "auth", %{ok: true})
        {:ok, next}

      {:ok, response, next} ->
        message = Map.get(response, "error", "authentication failed")
        Telemetry.emit(next, "auth", %{ok: false, error: to_string(message)})
        {:error, %AuthenticationError{message: to_string(message)}, next}

      {:error, error, next} ->
        {:error, error, next}
    end
  end

  defp exchange(state, command, timeout) do
    request = state.request + 1
    do_exchange(state, command, timeout, request)
  end

  defp do_exchange(state, command, timeout, request) do
    req_id = "elixir-#{System.unique_integer([:positive])}-#{request}"
    envelope = command |> stringify_keys() |> Map.put("reqId", req_id)

    with {:ok, frame} <- Wire.encode(envelope),
         :ok <- Transport.send(state, frame),
         {:ok, <<length::unsigned-big-32>>} <- Transport.recv(state, 4, timeout),
         :ok <- validate_length(length),
         {:ok, payload} <- Transport.recv(state, length, timeout),
         {:ok, response} <- Wire.decode(payload),
         :ok <- validate_req_id(response, req_id) do
      {:ok, response, %{state | request: request}}
    else
      {:error, error} ->
        {:error, normalize_error(error, timeout), %{state | request: request}}
    end
  rescue
    error -> {:error, connection_error(error), %{state | request: request}}
  end

  defp classify(%{"ok" => true} = response, _command), do: {:ok, response}

  defp classify(response, command) do
    {:error,
     %CommandError{
       message: to_string(Map.get(response, "error", "command failed")),
       command: Map.get(command, "cmd") || Map.get(command, :cmd),
       response: response
     }}
  end

  defp validate_length(length) when length <= @max_frame_size, do: :ok

  defp validate_length(_length),
    do: {:error, %ProtocolError{message: "incoming frame exceeds 64 MiB"}}

  defp validate_req_id(%{"reqId" => req_id}, req_id), do: :ok

  defp validate_req_id(_response, _expected),
    do: {:error, %ProtocolError{message: "response reqId mismatch"}}

  defp normalize_error(%_{} = error, _timeout), do: error

  defp normalize_error(:timeout, timeout),
    do: %TimeoutError{message: "command timed out after #{timeout}ms", timeout: timeout}

  defp normalize_error(reason, _timeout), do: connection_error(reason)

  defp connection_error(%_{} = error),
    do: %ConnectionError{message: Exception.message(error), reason: error}

  defp connection_error(reason),
    do: %ConnectionError{message: "connection failed: #{inspect(reason)}", reason: reason}

  defp stringify_keys(map) do
    Map.new(map, fn {key, value} -> {to_string(key), value} end)
  end

  defp emit_failure(state, error, command, started_at) do
    event = if match?(%TimeoutError{}, error), do: "timeout", else: "error"

    Telemetry.emit(state, event, %{
      command: command_name(command),
      duration_us: elapsed(started_at),
      error: Exception.message(error)
    })
  end

  defp command_name(command), do: Map.get(command, "cmd") || Map.get(command, :cmd)
  defp elapsed(started_at), do: System.monotonic_time(:microsecond) - started_at

  defp connection_timeout(connection) do
    GenServer.call(connection, :timeout)
  catch
    :exit, _reason -> @default_timeout
  end

  defp normalize_timeout(value) when is_number(value) and value >= 0, do: trunc(value)
  defp normalize_timeout(_value), do: @default_timeout
end
