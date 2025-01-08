defmodule FDBCTest do
  use ExUnit.Case, async: true

  alias FDBC.Database
  alias FDBC.Transaction

  describe "transact/3" do
    test "get and set using transact" do
      db = Database.create()

      FDBC.transact(db, fn tr ->
        Transaction.set(tr, "foo", "bar")
        assert "bar" == Transaction.get(tr, "foo")
      end)
    end
  end
end
