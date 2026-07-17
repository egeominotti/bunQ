defmodule Bunqueue.TestBroker do
  @moduledoc false

  def start!(options \\ []) do
    tcp_port = Keyword.get(options, :port, free_port())
    http_port = free_port()
    token = Keyword.get(options, :token)
    data_dir = Path.join(System.tmp_dir!(), "bunqueue-elixir-#{unique_id()}")
    File.mkdir_p!(data_dir)
    tls = if Keyword.get(options, :tls, false), do: tls_files(data_dir), else: nil
    parent = self()

    owner =
      spawn_link(fn ->
        port = open_broker(tcp_port, http_port, data_dir, token, tls)
        send(parent, {:broker_opened, self()})
        owner_loop(port)
      end)

    receive do
      {:broker_opened, ^owner} -> :ok
    after
      2_000 -> raise "broker process did not open"
    end

    broker =
      %{
        owner: owner,
        port: tcp_port,
        http_port: http_port,
        data_dir: data_dir,
        token: token,
        tls: tls
      }
      |> Map.merge(tls || %{})

    wait_ready!(broker, token)
    broker
  end

  def stop(%{owner: owner, data_dir: data_dir}) do
    monitor = Process.monitor(owner)
    send(owner, :stop)

    receive do
      {:DOWN, ^monitor, :process, ^owner, _reason} -> :ok
    after
      2_000 -> Process.exit(owner, :kill)
    end

    File.rm_rf(data_dir)
    :ok
  end

  def crash(%{owner: owner}) do
    monitor = Process.monitor(owner)
    send(owner, :crash)

    receive do
      {:DOWN, ^monitor, :process, ^owner, _reason} -> :ok
    after
      2_000 -> Process.exit(owner, :kill)
    end

    :ok
  end

  def restart(broker) do
    parent = self()

    owner =
      spawn_link(fn ->
        port =
          open_broker(
            broker.port,
            broker.http_port,
            broker.data_dir,
            broker.token,
            broker.tls
          )

        send(parent, {:broker_reopened, self()})
        owner_loop(port)
      end)

    receive do
      {:broker_reopened, ^owner} -> :ok
    after
      2_000 -> raise "broker process did not reopen"
    end

    restarted = %{broker | owner: owner}
    wait_ready!(restarted, broker.token)
    restarted
  end

  defp open_broker(tcp_port, http_port, data_dir, token, tls) do
    bun = System.find_executable("bun") || raise "bun executable not found"
    root = Path.expand("../../../..", __DIR__)

    env =
      [
        {~c"TCP_PORT", chars(tcp_port)},
        {~c"HTTP_PORT", chars(http_port)},
        {~c"BUNQUEUE_DATA_PATH", chars(Path.join(data_dir, "bunq.db"))}
      ]
      |> maybe_env(~c"AUTH_TOKENS", token)
      |> maybe_env(~c"TLS_CERT_FILE", tls && tls.cert_file)
      |> maybe_env(~c"TLS_KEY_FILE", tls && tls.key_file)

    Port.open(
      {:spawn_executable, bun},
      [
        :binary,
        :exit_status,
        :stderr_to_stdout,
        args: ["src/main.ts"],
        cd: root,
        env: env
      ]
    )
  end

  defp owner_loop(port) do
    receive do
      :stop ->
        terminate_port(port)

      :crash ->
        kill_port(port)

      {^port, {:data, _output}} ->
        owner_loop(port)

      {^port, {:exit_status, status}} ->
        exit({:broker_exit, status})
    end
  end

  defp terminate_port(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} ->
        os_kill(pid, "TERM")

        receive do
          {^port, {:exit_status, _status}} ->
            :ok
        after
          500 ->
            os_kill(pid, "KILL")
        end

      nil ->
        :ok
    end

    if Port.info(port), do: Port.close(port)
  end

  defp kill_port(port) do
    case Port.info(port, :os_pid) do
      {:os_pid, pid} ->
        os_kill(pid, "KILL")

        receive do
          {^port, {:exit_status, _status}} -> :ok
        after
          500 -> :ok
        end

      nil ->
        :ok
    end

    if Port.info(port), do: Port.close(port)
  end

  defp os_kill(pid, signal) do
    :os.cmd(String.to_charlist("kill -#{signal} #{pid}"))
    :ok
  end

  defp wait_ready!(broker, token) do
    deadline = System.monotonic_time(:millisecond) + 15_000
    wait_ready(broker, token, deadline)
  end

  defp wait_ready(broker, token, deadline) do
    options =
      [host: "127.0.0.1", port: broker.port, token: token, timeout: 100]
      |> maybe_tls_options(broker)

    {:ok, connection} = Bunqueue.Connection.start_link(options)
    result = Bunqueue.Connection.call(connection, %{"cmd" => "Ping"}, 100)
    Bunqueue.Connection.close(connection)

    cond do
      match?({:ok, _response}, result) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        stop(broker)
        raise "broker did not become ready"

      true ->
        Process.sleep(50)
        wait_ready(broker, token, deadline)
    end
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end

  defp maybe_env(env, _key, nil), do: env
  defp maybe_env(env, key, value), do: [{key, chars(value)} | env]

  defp maybe_tls_options(options, %{ca_file: ca_file}),
    do: Keyword.merge(options, tls: true, ca_file: ca_file)

  defp maybe_tls_options(options, _broker), do: options

  defp tls_files(data_dir) do
    ca_key = Path.join(data_dir, "ca.key")
    ca_file = Path.join(data_dir, "ca.crt")
    key_file = Path.join(data_dir, "server.key")
    request = Path.join(data_dir, "server.csr")
    cert_file = Path.join(data_dir, "server.crt")
    wrong_ca_file = Path.join(data_dir, "wrong-ca.crt")
    wrong_ca_key = Path.join(data_dir, "wrong-ca.key")

    openssl!(
      ~w(req -x509 -newkey rsa:2048 -nodes -keyout #{ca_key} -out #{ca_file}) ++
        ["-subj", "/CN=bunqueue-test-ca", "-days", "1"]
    )

    openssl!(
      ~w(req -newkey rsa:2048 -nodes -keyout #{key_file} -out #{request}) ++
        ["-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"]
    )

    openssl!(
      ~w(x509 -req -in #{request} -CA #{ca_file} -CAkey #{ca_key}) ++
        ~w(-CAcreateserial -out #{cert_file} -days 1 -copy_extensions copy)
    )

    openssl!(
      ~w(req -x509 -newkey rsa:2048 -nodes -keyout #{wrong_ca_key} -out #{wrong_ca_file}) ++
        ["-subj", "/CN=wrong-test-ca", "-days", "1"]
    )

    %{
      ca_file: ca_file,
      wrong_ca_file: wrong_ca_file,
      cert_file: cert_file,
      key_file: key_file
    }
  end

  defp openssl!(arguments) do
    case System.cmd("openssl", arguments, stderr_to_stdout: true) do
      {_output, 0} -> :ok
      {output, status} -> raise "openssl failed (#{status}): #{output}"
    end
  end

  defp chars(value), do: value |> to_string() |> String.to_charlist()
  defp unique_id, do: System.unique_integer([:positive, :monotonic])
end
