defmodule Bunqueue.Transport do
  @moduledoc false

  def connect(%{tls: false} = state, timeout) do
    options = [:binary, active: false, packet: :raw, nodelay: true]
    :gen_tcp.connect(String.to_charlist(state.host), state.port, options, timeout)
  end

  def connect(state, timeout) do
    verify = if state.verify, do: :verify_peer, else: :verify_none

    options =
      [
        :binary,
        active: false,
        packet: :raw,
        verify: verify,
        server_name_indication: String.to_charlist(state.host)
      ] ++ trust_options(state)

    :ssl.connect(String.to_charlist(state.host), state.port, options, timeout)
  end

  def send(%{tls: true, socket: socket}, data), do: :ssl.send(socket, data)
  def send(%{socket: socket}, data), do: :gen_tcp.send(socket, data)

  def recv(%{tls: true, socket: socket}, length, timeout),
    do: :ssl.recv(socket, length, timeout)

  def recv(%{socket: socket}, length, timeout),
    do: :gen_tcp.recv(socket, length, timeout)

  def close(%{socket: nil} = state), do: state

  def close(%{tls: true, socket: socket} = state) do
    :ssl.close(socket)
    %{state | socket: nil}
  end

  def close(%{socket: socket} = state) do
    :gen_tcp.close(socket)
    %{state | socket: nil}
  end

  defp trust_options(%{verify: false}), do: []

  defp trust_options(%{ca_file: path}) when is_binary(path) do
    [
      cacertfile: String.to_charlist(path),
      customize_hostname_check: [
        match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
      ]
    ]
  end

  defp trust_options(_state) do
    [
      cacerts: :public_key.cacerts_get(),
      customize_hostname_check: [
        match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
      ]
    ]
  end
end
