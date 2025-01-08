defmodule FDBC.KeySelectorTest do
  use ExUnit.Case, async: true

  alias FDBC.KeySelector

  describe "new/3" do
    test "set correct default values" do
      key_selector = KeySelector.new("foo")
      assert "foo" == key_selector.key
      assert 0 == key_selector.offset
      assert 0 == key_selector.or_equal
    end
  end

  describe "less_than/1" do
    test "set correct values" do
      key_selector = KeySelector.less_than("foo")
      assert "foo" == key_selector.key
      assert 0 == key_selector.offset
      assert 0 == key_selector.or_equal
    end
  end

  describe "less_than_or_equal/1" do
    test "set correct values" do
      key_selector = KeySelector.less_than_or_equal("foo")
      assert "foo" == key_selector.key
      assert 0 == key_selector.offset
      assert 1 == key_selector.or_equal
    end
  end

  describe "greater_than/1" do
    test "set correct values" do
      key_selector = KeySelector.greater_than("foo")
      assert "foo" == key_selector.key
      assert 1 == key_selector.offset
      assert 1 == key_selector.or_equal
    end
  end

  describe "greater_than_or_equal/1" do
    test "set correct values" do
      key_selector = KeySelector.greater_than_or_equal("foo")
      assert "foo" == key_selector.key
      assert 1 == key_selector.offset
      assert 0 == key_selector.or_equal
    end
  end
end
