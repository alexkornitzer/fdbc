defmodule FDBC.NetworkTest do
  use ExUnit.Case

  alias FDBC.Network

  test "ensure that the network thread can only start once" do
    assert_raise FDBC.Error, "Network can be configured only once", fn -> Network.start() end
  end
end
