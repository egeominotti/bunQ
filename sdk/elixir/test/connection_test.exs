defmodule Bunqueue.ConnectionTest do
  use ExUnit.Case

  alias Bunqueue.{AuthenticationError, Connection, ConnectionError, TimeoutError}

  test "writes reqId frames and emits connected and command events" do
    parent = self()
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])
    {:ok, port} = :inet.port(listener)

    server =
      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)
        {:ok, <<length::unsigned-big-32>>} = :gen_tcp.recv(socket, 4, 1_000)
        {:ok, body} = :gen_tcp.recv(socket, length, 1_000)
        request = Msgpax.unpack!(body)

        response =
          Msgpax.pack!(%{"ok" => true, "reqId" => request["reqId"], "data" => %{"pong" => true}},
            iodata: false
          )

        :ok = :gen_tcp.send(socket, <<byte_size(response)::unsigned-big-32, response::binary>>)
        :gen_tcp.close(socket)
      end)

    monitor = Process.monitor(server)

    {:ok, connection} =
      Connection.start_link(
        port: port,
        event_handler: &send(parent, {:event, &1})
      )

    assert {:ok, %{"data" => %{"pong" => true}}} =
             Connection.call(connection, %{"cmd" => "Ping"})

    assert_receive {:event, %{event: "connected"}}, 500
    assert_receive {:event, %{event: "command", command: "Ping", ok: true}}, 500
    Connection.close(connection)
    assert_receive {:event, %{event: "close", generation: 1}}, 500
    :gen_tcp.close(listener)
    assert_receive {:DOWN, ^monitor, :process, ^server, :normal}, 500
  end

  test "returns typed connection and timeout errors" do
    {:ok, connection} = Connection.start_link(port: 1, timeout: 10)
    assert {:error, %ConnectionError{}} = Connection.call(connection, %{"cmd" => "Ping"})
    Connection.close(connection)

    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :raw, reuseaddr: true])
    {:ok, port} = :inet.port(listener)

    server =
      spawn_link(fn ->
        {:ok, socket} = :gen_tcp.accept(listener)
        {:ok, _header} = :gen_tcp.recv(socket, 4, 1_000)
        Process.sleep(100)
        :gen_tcp.close(socket)
      end)

    monitor = Process.monitor(server)
    {:ok, connection} = Connection.start_link(port: port, timeout: 10)
    assert {:error, %TimeoutError{}} = Connection.call(connection, %{"cmd" => "Ping"})
    Connection.close(connection)
    :gen_tcp.close(listener)
    assert_receive {:DOWN, ^monitor, :process, ^server, :normal}, 500
  end

  test "authenticates first against a token-protected broker" do
    broker = Bunqueue.TestBroker.start!(token: "elixir-secret")
    on_exit(fn -> Bunqueue.TestBroker.stop(broker) end)

    assert {:ok, valid} =
             Connection.start_link(
               host: "127.0.0.1",
               port: broker.port,
               token: "elixir-secret"
             )

    assert {:ok, _response} = Connection.call(valid, %{"cmd" => "Ping"})
    Connection.close(valid)

    assert {:ok, invalid} =
             Connection.start_link(
               host: "127.0.0.1",
               port: broker.port,
               token: "wrong-secret"
             )

    assert {:error, %AuthenticationError{}} =
             Connection.call(invalid, %{"cmd" => "Ping"})

    Connection.close(invalid)
  end

  test "reconnects lazily after the broker closes an established socket" do
    first = Bunqueue.TestBroker.start!()
    parent = self()

    {:ok, connection} =
      Connection.start_link(
        host: "127.0.0.1",
        port: first.port,
        timeout: 500,
        event_handler: &send(parent, {:event, &1})
      )

    assert {:ok, _response} = Connection.call(connection, %{"cmd" => "Ping"})
    assert Connection.generation(connection) == 1
    Bunqueue.TestBroker.stop(first)
    Process.sleep(100)

    second = Bunqueue.TestBroker.start!(port: first.port)
    on_exit(fn -> Bunqueue.TestBroker.stop(second) end)

    assert {:error, %ConnectionError{}} =
             Connection.call(connection, %{"cmd" => "Ping"})

    assert {:ok, _response} = Connection.call(connection, %{"cmd" => "Ping"})
    assert Connection.generation(connection) == 2
    assert_receive {:event, %{event: "reconnect", generation: 2}}, 1_000
    Connection.close(connection)
  end

  test "verifies TLS with the configured CA and rejects the wrong CA" do
    broker = Bunqueue.TestBroker.start!(tls: true)
    on_exit(fn -> Bunqueue.TestBroker.stop(broker) end)

    assert {:ok, trusted} =
             Connection.start_link(
               host: "127.0.0.1",
               port: broker.port,
               tls: true,
               ca_file: broker.ca_file
             )

    assert {:ok, _response} = Connection.call(trusted, %{"cmd" => "Ping"})
    Connection.close(trusted)

    assert {:ok, untrusted} =
             Connection.start_link(
               host: "127.0.0.1",
               port: broker.port,
               tls: true,
               ca_file: broker.wrong_ca_file,
               timeout: 1_000
             )

    assert {:error, %ConnectionError{}} =
             Connection.call(untrusted, %{"cmd" => "Ping"})

    Connection.close(untrusted)
  end
end
