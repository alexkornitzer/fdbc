defmodule FDBC.SubspaceTest do
  use ExUnit.Case, async: true

  alias FDBC.Subspace

  describe "new/1" do
    test "creates binary prefix correctly" do
      subspace = Subspace.new("a")
      assert "a" == subspace.key
    end

    test "creates tuple prefix correctly" do
      subspace = Subspace.new(["a", "b", "c"])
      assert "\x01a\0\x01b\0\x01c\0" == subspace.key
    end
  end

  describe "concat/2" do
    test "successfully concatenates two subspaces" do
      a = Subspace.new("a")
      b = Subspace.new("b")
      ab = Subspace.concat(a, b)
      assert "ab" == ab.key
    end
  end

  describe "contains?/2" do
    test "returns true when subspace contains key" do
      subspace = Subspace.new(["a", "b", "c"])
      assert true == Subspace.contains?(subspace, "\x01a\0\x01b\0\x01c\0\x01foobar\0")
    end

    test "returns false when subspace does not contain key" do
      subspace = Subspace.new(["a", "b", "c"])
      assert false == Subspace.contains?(subspace, "\x01a\0\x01b\0\x01b\0\x01foobar\0")
    end
  end

  describe "pack/2" do
    test "subspace packs correctly" do
      subspace = Subspace.new(["a", "b", "c"])
      assert "\x01a\0\x01b\0\x01c\0\x01foobar\0" == Subspace.pack(subspace, ["foobar"])
    end
  end

  describe "range/2" do
    test "builds range correctly" do
      subspace = Subspace.new(["a", "b", "c"])
      {start, stop} = Subspace.range(subspace)
      assert "\x01a\0\x01b\0\x01c\0\x00" == start
      assert "\x01a\0\x01b\0\x01c\0\xFF" == stop
    end
  end

  describe "unpack/2" do
    test "subspace unpacks correctly" do
      subspace = Subspace.new(["a", "b", "c"])
      assert ["foobar"] = Subspace.unpack(subspace, "\x01a\0\x01b\0\x01c\0\x01foobar\0")
    end
  end
end
