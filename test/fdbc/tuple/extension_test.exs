defmodule FDBC.Tuple.ExtensionTest do
  use ExUnit.Case, async: true

  alias FDBC.Tuple
  alias FDBC.Tuple.Extension

  defmodule Datetime do
    use Extension

    alias FDBC.Tuple

    @datetime_typecode 0x40

    def decode(<<@datetime_typecode, length::integer-size(8)>> <> encoded) do
      {value, encoded} =
        case encoded do
          <<value::binary-size(^length), encoded>> -> {value, encoded}
          <<value::binary-size(^length)>> -> {value, <<>>}
        end

      [timestamp] = Tuple.unpack(value)
      {:ok, datetime} = DateTime.from_unix(timestamp)
      {{:datetime, datetime}, encoded}
    end

    def encode({:datetime, data}) do
      timestamp = DateTime.to_unix(data)
      packed = Tuple.pack([timestamp])
      <<@datetime_typecode>> <> <<byte_size(packed)::integer-big-8>> <> packed
    end

    def identify(%DateTime{}) do
      :datetime
    end
  end

  describe "decode/1" do
    test "successfully decodes supported type code" do
      assert {{:datetime, ~U[1990-01-01T00:00:00Z]}, <<>>} =
               Datetime.decode("@\x05\x18%\x9E\x9D\x80")
    end

    test "raises on unsupported type code" do
      assert_raise ArgumentError, fn ->
        Datetime.decode(<<0x00>>)
      end
    end

    test "raises on unexpected end of output" do
      assert_raise ArgumentError, fn ->
        Datetime.decode(<<>>)
      end
    end
  end

  describe "encode/1" do
    test "successfully encodes supported type" do
      assert "@\x05\x18%\x9E\x9D\x80" == Datetime.encode({:datetime, ~U[1990-01-01T00:00:00Z]})
    end

    test "raises on unknown identity" do
      assert_raise ArgumentError, fn ->
        Datetime.encode({:uuid, "foo"})
      end
    end

    test "raises on unsupported data type" do
      assert_raise ArgumentError, fn ->
        Datetime.encode("foobar")
      end
    end
  end

  describe "identify/1" do
    test "successfully identifies data" do
      assert :datetime == Datetime.identify(~U[1990-01-01T00:00:00Z])
    end

    test "raises on unsupported data type" do
      assert_raise ArgumentError, fn ->
        Datetime.identify("foobar")
      end
    end
  end

  describe "test in tuple layer" do
    test "pack/2" do
      assert "@\x05\x18%\x9E\x9D\x80" ==
               Tuple.pack([~U[1990-01-01T00:00:00Z]], extension: Datetime)
    end

    test "unpack/2" do
      assert [~U[1990-01-01T00:00:00Z]] ==
               Tuple.unpack("@\x05\x18%\x9E\x9D\x80", extension: Datetime)
    end
  end
end
