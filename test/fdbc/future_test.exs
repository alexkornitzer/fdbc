defmodule FutureTest do
  use ExUnit.Case, async: true

  alias FDBC.Database
  alias FDBC.Future
  alias FDBC.Transaction

  setup do
    tr = Database.create() |> Transaction.create()
    Transaction.clear_range(tr, <<>>, <<0xFF>>)
    :ok = Transaction.commit(tr)
  end

  test "await many" do
    tr = Database.create() |> Transaction.create()
    :ok = Transaction.set(tr, "A", "a")
    :ok = Transaction.set(tr, "B", "b")
    :ok = Transaction.set(tr, "C", "c")

    futures = [
      Transaction.async_get(tr, "A"),
      Transaction.async_get(tr, "B"),
      Transaction.async_get(tr, "C")
    ]

    assert ["a", "b", "c"] == Future.await_many(futures)
    :ok = Transaction.commit(tr)
  end
end
