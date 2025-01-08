defmodule FDBC.Tuple.VersionstampTest do
  use ExUnit.Case, async: true

  alias FDBC.Tuple.Versionstamp

  describe "new/2" do
    test "creates a 10 byte versionstamp" do
      versionstamp = Versionstamp.new(<<0, 1, 2, 3, 4, 5, 6, 7, 8, 9>>)
      assert versionstamp.tr == <<0, 1, 2, 3, 4, 5, 6, 7, 8, 9>>
      assert versionstamp.user == 0
    end

    test "creates a 12 byte versionstamp" do
      versionstamp = Versionstamp.new(<<0, 1, 2, 3, 4, 5, 6, 7, 8, 9>>, 100)
      assert versionstamp.tr == <<0, 1, 2, 3, 4, 5, 6, 7, 8, 9>>
      assert versionstamp.user == 100
    end

    test "creates an incomplete versionstamp" do
      versionstamp = Versionstamp.new()
      assert versionstamp.tr == <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>
      assert versionstamp.user == 0
    end

    test "fails to when user version is too large" do
      assert_raise FunctionClauseError, fn ->
        Versionstamp.new(nil, 0xFFFFFF)
      end
    end
  end

  describe "dump/1" do
    test "dumps a 12 byte versionstamp" do
      versionstamp = Versionstamp.new(<<0, 1, 2, 3, 4, 5, 6, 7, 8, 9>>, 100)
      assert Versionstamp.dump(versionstamp) == <<0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 100>>
    end
  end

  describe "load/1" do
    test "loads a 10 byte versionstamp" do
      versionstamp = Versionstamp.load(<<0, 1, 2, 3, 4, 5, 6, 7, 8, 9>>)
      assert versionstamp.tr == <<0, 1, 2, 3, 4, 5, 6, 7, 8, 9>>
      assert versionstamp.user == 0
    end

    test "loads a 12 byte versionstamp" do
      versionstamp = Versionstamp.load(<<0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 100>>)
      assert versionstamp.tr == <<0, 1, 2, 3, 4, 5, 6, 7, 8, 9>>
      assert versionstamp.user == 100
    end
  end

  describe "incomplete?/1" do
    test "return true when incomplete" do
      versionstamp = Versionstamp.new()
      assert Versionstamp.incomplete?(versionstamp)
    end

    test "return false when complete" do
      versionstamp = Versionstamp.new(<<0, 1, 2, 3, 4, 5, 6, 7, 8, 9>>)
      refute Versionstamp.incomplete?(versionstamp)
    end
  end
end
