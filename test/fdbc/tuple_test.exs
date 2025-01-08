defmodule FDBC.TupleTest do
  use ExUnit.Case, async: true

  alias FDBC.Tuple
  alias FDBC.Tuple.Versionstamp

  describe "pack/2" do
    test "pack null value" do
      assert <<0x00>> == Tuple.pack([nil])
    end

    test "pack byte string" do
      assert "\x01foo\x00\xffbar\x00" == Tuple.pack(["foo\x00bar"])
    end

    test "pack unicode string" do
      assert "\x02FÔO\x00\xFFbar\x00" == Tuple.pack([{:string, "F\u00d4O\u0000bar"}])
    end

    test "pack nested tuple" do
      assert "\x05\x01foo\x00\xffbar\x00\x00\xff\x05\x00\x00" ==
               Tuple.pack([["foo\x00bar", nil, []]])
    end

    test "pack integer" do
      assert "\x0b\xf6\x00\x00\x00\x00\x00\x00\x00\x00\x00" ==
               Tuple.pack([-0xFFFFFFFFFFFFFFFFFF])

      assert "\x11\xabK\x93" == Tuple.pack([-5_551_212])

      assert "\x14" == Tuple.pack([0])

      assert "\x17T\xB4l" == Tuple.pack([5_551_212])

      assert "\x1d\x09\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF" ==
               Tuple.pack([0xFFFFFFFFFFFFFFFFFF])
    end

    test "pack float" do
      assert "\x20\x3d\xd7\xff\xff" == Tuple.pack([-42.0])
    end

    test "pack false" do
      assert "\x26" == Tuple.pack([false])
    end

    test "pack true" do
      assert "\x27" == Tuple.pack([true])
    end

    test "pack uuid" do
      assert "\x30\x3d\xa3\x11\x24\xb2\x41\x11\xef\xb8\xdd\x1f\xe7\x6e\x26\xf9\xcf" ==
               Tuple.pack(["3da31124-b241-11ef-b8dd-1fe76e26f9cf"])
    end

    test "pack versionstamp" do
      assert "\x33\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x00\x00\x01\x00\x00\x00" ==
               Tuple.pack([Versionstamp.new()])
    end
  end

  describe "unpack/2" do
    test "unpack null value" do
      assert [nil] == Tuple.unpack("\x00")
    end

    test "unpack byte string" do
      assert ["foo\x00bar"] == Tuple.unpack("\x01foo\x00\xffbar\x00")
    end

    test "unpack unicode string" do
      assert ["F\u00d4O\u0000bar"] == Tuple.unpack("\x02FÔO\x00\xFFbar\x00")
    end

    test "unpack nested tuple" do
      assert [["foo\x00bar", nil, []]] ==
               Tuple.unpack("\x05\x01foo\x00\xffbar\x00\x00\xff\x05\x00\x00")
    end

    test "unpack integer" do
      assert [-0xFFFFFFFFFFFFFFFFFF] ==
               Tuple.unpack("\x0b\xf6\x00\x00\x00\x00\x00\x00\x00\x00\x00")

      assert [-5_551_212] == Tuple.unpack("\x11\xabK\x93")

      assert [0] == Tuple.unpack("\x14")

      assert [5_551_212] == Tuple.unpack("\x17T\xB4l")

      assert [0xFFFFFFFFFFFFFFFFFF] ==
               Tuple.unpack("\x1d\x09\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF")
    end

    test "unpack float" do
      assert [-42.0] == Tuple.unpack("\x20\x3d\xd7\xff\xff")
    end

    test "unpack false" do
      assert [false] == Tuple.unpack("\x26")
    end

    test "unpack true" do
      assert [true] == Tuple.unpack("\x27")
    end

    test "unpack uuid" do
      assert ["3da31124-b241-11ef-b8dd-1fe76e26f9cf"] ==
               Tuple.unpack(
                 "\x30\x3d\xa3\x11\x24\xb2\x41\x11\xef\xb8\xdd\x1f\xe7\x6e\x26\xf9\xcf"
               )
    end
  end
end
