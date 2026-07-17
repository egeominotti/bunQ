defmodule Bunqueue.Wire do
  @moduledoc false
  @max_frame_size 64 * 1024 * 1024
  @int32_min -2_147_483_648
  @int32_max 2_147_483_647

  defmodule ExtUnpacker do
    @behaviour Msgpax.Ext.Unpacker

    @impl true
    def unpack(%Msgpax.Ext{type: 0}), do: {:ok, nil}
    def unpack(_extension), do: :error
  end

  @spec max_frame_size() :: pos_integer()
  def max_frame_size, do: @max_frame_size

  @spec encode(map()) :: {:ok, binary()} | {:error, Exception.t()}
  def encode(command) when is_map(command) do
    try do
      with safe <- js_safe(command),
           {:ok, packed} <- Msgpax.pack(safe, iodata: false),
           true <- byte_size(packed) <= @max_frame_size do
        {:ok, <<byte_size(packed)::unsigned-big-32, packed::binary>>}
      else
        false ->
          {:error,
           %Bunqueue.ProtocolError{
             message: "outgoing frame exceeds #{@max_frame_size} bytes"
           }}

        {:error, reason} ->
          {:error, %Bunqueue.ProtocolError{message: Exception.message(reason)}}
      end
    rescue
      error ->
        {:error,
         %Bunqueue.ProtocolError{
           message: "outgoing payload is not encodable: #{Exception.message(error)}"
         }}
    end
  end

  @spec decode(binary()) :: {:ok, map()} | {:error, Exception.t()}
  def decode(payload) when byte_size(payload) <= @max_frame_size do
    case Msgpax.unpack(payload, ext: ExtUnpacker) do
      {:ok, value} when is_map(value) -> {:ok, value}
      {:ok, _} -> {:error, %Bunqueue.ProtocolError{message: "response is not a map"}}
      {:error, reason} -> {:error, %Bunqueue.ProtocolError{message: Exception.message(reason)}}
    end
  end

  def decode(_payload) do
    {:error, %Bunqueue.ProtocolError{message: "incoming frame exceeds #{@max_frame_size} bytes"}}
  end

  @spec js_safe(term()) :: term()
  def js_safe(value) when is_integer(value) and (value < @int32_min or value > @int32_max),
    do: value / 1

  def js_safe(value) when is_map(value) do
    Map.new(value, fn {key, nested} -> {to_string(key), js_safe(nested)} end)
  end

  def js_safe(value) when is_list(value), do: Enum.map(value, &js_safe/1)
  def js_safe(value) when is_tuple(value), do: value |> Tuple.to_list() |> Enum.map(&js_safe/1)
  def js_safe(value), do: value
end
