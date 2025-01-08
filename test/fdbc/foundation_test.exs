defmodule FDBC.FoundationTest do
  use ExUnit.Case, async: true

  alias FDBC.Database
  alias FDBC.Directory
  alias FDBC.Foundation
  alias FDBC.Transaction

  defmodule Test do
    use Foundation,
      otp_app: :test,
      options: [
        transaction_retry_limit: 100,
        transaction_timeout: 60_000
      ]
  end

  setup_all do
    Application.put_env(:test, Test, cluster: nil, directory: "example")

    {:ok, pid} = GenServer.start_link(Test, :ok)

    on_exit(:stop_foundation, fn ->
      db = Test.database()
      :ok = Directory.new() |> Directory.remove(db, ["example"])
      Process.exit(pid, :normal)
    end)
  end

  describe "database/0" do
    test "gets access to the database instance" do
      assert %Database{} = Test.database()
    end
  end

  describe "transact/3" do
    test "get and set using transact" do
      Test.transact(fn tr, dir ->
        assert ["example"] = Directory.path(dir)
        Transaction.set(tr, "foo", "bar")
        assert "bar" == Transaction.get(tr, "foo")
      end)
    end
  end
end
