defmodule FDBC.DatabaseTest do
  use ExUnit.Case, async: true

  alias FDBC.Database

  describe "create/2" do
    test "successfully creates database" do
      assert(%Database{} = Database.create())
    end
  end

  describe "change/2" do
    test "set location cache size" do
      db = Database.create()
      assert %Database{} = Database.change(db, location_cache_size: 1_000)
    end

    test "set max watches" do
      db = Database.create()
      assert %Database{} = Database.change(db, max_watches: 10)
    end

    test "set machine id" do
      db = Database.create()
      assert %Database{} = Database.change(db, machine_id: "deadbeef")
    end

    test "set datacenter id" do
      db = Database.create()
      assert %Database{} = Database.change(db, datacenter_id: "deadbeef")
    end

    test "set snapshot ryw to true" do
      db = Database.create()
      assert %Database{} = Database.change(db, snapshot_ryw: true)
    end

    test "set snapshot ryw to false" do
      db = Database.create()
      assert %Database{} = Database.change(db, snapshot_ryw: false)
    end

    test "set transaction logging max field length" do
      db = Database.create()
      assert %Database{} = Database.change(db, transaction_logging_max_field_length: 10)
    end

    test "set transaction timeout" do
      db = Database.create()
      assert %Database{} = Database.change(db, transaction_timeout: 1_000)
    end

    test "set transaction retry limit" do
      db = Database.create()
      assert %Database{} = Database.change(db, transaction_retry_limit: 5)
    end

    test "set transaction max retry delay" do
      db = Database.create()
      assert %Database{} = Database.change(db, transaction_max_retry_delay: 10)
    end

    test "set transaction size limit" do
      db = Database.create()
      assert %Database{} = Database.change(db, transaction_size_limit: 1024)
    end

    test "set transaction casual read risky" do
      db = Database.create()
      assert %Database{} = Database.change(db, transaction_causal_read_risky: true)
    end

    test "set transaction automatic idempotency" do
      db = Database.create()
      assert %Database{} = Database.change(db, transaction_automatic_idempotency: true)
    end

    test "set transaction bypass unreadable" do
      db = Database.create()
      assert %Database{} = Database.change(db, transaction_bypass_unreadable: true)
    end

    test "set transaction used during commit protection disable" do
      db = Database.create()

      assert %Database{} =
               Database.change(db, transaction_used_during_commit_protection_disable: true)
    end

    test "set transaction report conflicting keys" do
      db = Database.create()
      assert %Database{} = Database.change(db, transaction_report_conflicting_keys: true)
    end

    test "set use config database" do
      db = Database.create()
      assert %Database{} = Database.change(db, use_config_database: true)
    end

    test "set test causal read risky" do
      db = Database.create()
      assert %Database{} = Database.change(db, test_causal_read_risky: 50)
    end
  end
end
