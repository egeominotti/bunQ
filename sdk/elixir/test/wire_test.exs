defmodule Bunqueue.WireTest do
  use ExUnit.Case, async: true

  alias Bunqueue.Wire

  test "frames a MessagePack map with a big-endian length" do
    assert {:ok, <<length::unsigned-big-32, body::binary>>} =
             Wire.encode(%{"cmd" => "Ping"})

    assert length == byte_size(body)
    assert Msgpax.unpack!(body) == %{"cmd" => "Ping"}
  end

  test "converts every integer outside int32 to float recursively" do
    safe =
      Wire.js_safe(%{
        small: 42,
        big: 9_999_999_999_999,
        nested: [%{low: -2_147_483_649}]
      })

    assert safe["small"] === 42
    assert safe["big"] === 9_999_999_999_999.0
    assert safe["nested"] == [%{"low" => -2_147_483_649.0}]
  end

  test "decodes server ext type zero as nil" do
    payload = <<0x81, 0xA1, ?x, 0xD4, 0x00, 0x00>>
    assert Wire.decode(payload) == {:ok, %{"x" => nil}}
  end

  test "rejects an oversized incoming body before decoding" do
    oversized = :binary.copy(<<0>>, Wire.max_frame_size() + 1)
    assert {:error, %Bunqueue.ProtocolError{}} = Wire.decode(oversized)
  end

  test "rejects an oversized encoded body before framing" do
    oversized = :binary.copy("x", Wire.max_frame_size())
    assert {:error, %Bunqueue.ProtocolError{}} = Wire.encode(%{"data" => oversized})
  end

  test "maps an integer beyond float64 range to a protocol error instead of crashing" do
    huge = Integer.pow(10, 400)
    assert {:error, %Bunqueue.ProtocolError{}} = Wire.encode(%{"huge" => huge})
  end
end
