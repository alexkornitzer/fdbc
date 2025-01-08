defmodule FDBC.TransactionTest do
  use ExUnit.Case, async: true

  alias FDBC.Database
  alias FDBC.Future
  alias FDBC.KeySelector
  alias FDBC.Tenant
  alias FDBC.Transaction

  setup_all do
    db = Database.create()

    on_exit(:clear_keyspace, fn ->
      tr = Transaction.create(db)
      Transaction.clear_range(tr, <<>>, <<0xFF>>)
      :ok = Transaction.commit(tr)
    end)

    [db: db]
  end

  describe "create/2" do
    test "database can create a transaction", context do
      assert(%Transaction{} = Transaction.create(context.db))
    end

    test "tenant can create a transaction", context do
      tenant = Tenant.open(context.db, "tenant")
      assert(%Transaction{} = Transaction.create(tenant))
    end
  end

  describe "change/1" do
    test "set causal write risky", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, causal_write_risky: true)
    end

    test "set causal read risky", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, causal_read_risky: true)
    end

    test "set causal read disable", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, causal_read_disable: true)
    end

    test "set next write no write conflict range", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, next_write_no_write_conflict_range: true)
    end

    test "set read your writes disable", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, read_your_writes_disabled: true)
    end

    test "set enable read server side cache", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, read_server_side_cache: true)
    end

    test "set disable read server side cache", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, read_server_side_cache: false)
    end

    test "set read priority to normal", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, read_priority: :normal)
    end

    test "set read priority to low", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, read_priority: :low)
    end

    test "set read priority to high", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, read_priority: :high)
    end

    test "set durability to datacenter", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, durability: :datacenter)
    end

    test "set durability to risky", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, durability: :risky)
    end

    test "set priority to immediate", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, priority: :immediate)
    end

    test "set priority to batch", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, priority: :batch)
    end

    test "set access system keys", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, access_system_keys: true)
    end

    test "set read system keys", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, read_system_keys: true)
    end

    test "set raw access", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, raw_access: true)
    end

    test "set bypass storage quota", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, bypass_storage_quota: true)
    end

    test "set debug dump", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, debug_dump: true)
    end

    test "set debug transaction identifier", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, debug_transaction_identifier: "foobar")
    end

    test "set log transaction", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, log_transaction: true)
    end

    test "set transaction logging max field length", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, transaction_logging_max_field_length: 100)
    end

    test "set server request tracing", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, server_request_tracing: true)
    end

    test "set timeout", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, timeout: 60)
    end

    test "set retry limit", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, retry_limit: 60)
    end

    test "set max retry delay", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, max_retry_delay: 60)
    end

    test "set size limit", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, size_limit: 60)
    end

    test "set snapshot ryw enabled", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, snapshot_ryw: true)
    end

    test "set snapshot ryw disabled", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, snapshot_ryw: false)
    end

    test "set lock aware", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, lock_aware: true)
    end

    test "set used during commit protection disable", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, used_during_commit_protection_disable: true)
    end

    test "set read lock aware", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, read_lock_aware: true)
    end

    test "set first in batch", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, first_in_batch: true)
    end

    test "set report conflicting keys", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, report_conflicting_keys: true)
    end

    test "set special key space relaxed", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, special_key_space_relaxed: true)
    end

    test "set special key space enable writes", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, special_key_space_enable_writes: true)
    end

    test "set tag", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, tag: "tag")
    end

    test "set auto throttle tag", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, auto_throttle_tag: "tag")
    end

    test "set span parent", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, span_parent: "0x0FDBC00B073000000LL")
    end

    test "set expensive clear cost estimation enabled", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, expensive_clear_cost_estimation_enable: true)
    end

    test "set bypass unreadable", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, bypass_unreadable: true)
    end

    test "set grv cache enabled", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, use_grv_cache: true)
    end

    test "set grv cache disabled", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, use_grv_cache: false)
    end

    test "set authorization token", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, authorization_token: "foobar")
    end

    test "set enable replica consistency check", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, enable_replica_consistency_check: true)
    end

    test "set consistency check required replicas", context do
      tr = Transaction.create(context.db)
      assert %Transaction{} = Transaction.change(tr, consistency_check_required_replicas: 4)
    end
  end

  describe "add_conflict_key/3" do
    test "can add a read conflict", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.add_conflict_key(tr, "foo", :read)
    end

    test "can add a write conflict", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.add_conflict_key(tr, "foo", :write)
    end
  end

  describe "add_conflict_range/4" do
    test "can add a read conflict", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.add_conflict_range(tr, "bar", "foo", :read)
    end

    test "can add a write conflict", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.add_conflict_range(tr, "bar", "foo", :write)
    end
  end

  describe "atomic_op/4" do
    # TODO: These should really all check the operation actually succeeded...
    test "can perform `:add`", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.atomic_op(tr, :add, "foo", <<1::integer-little>>)
    end

    test "can perform `:append_if_fits`", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.atomic_op(tr, :append_if_fits, "foo", "bar")
    end

    test "can perform `:bit_and`", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.atomic_op(tr, :bit_and, "foo", <<100>>)
    end

    test "can perform `:bit_or`", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.atomic_op(tr, :bit_or, "foo", <<100>>)
    end

    test "can perform `:bit_xor`", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.atomic_op(tr, :bit_xor, "foo", <<100>>)
    end

    test "can perform `:byte_max`", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.atomic_op(tr, :byte_max, "foo", "bar")
    end

    test "can perform `:byte_min`", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.atomic_op(tr, :byte_min, "foo", "bar")
    end

    test "can perform `:compare_and_clear`", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.atomic_op(tr, :compare_and_clear, "foo", "bar")
    end

    test "can perform `:max`", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.atomic_op(tr, :max, "foo", <<100>>)
    end

    test "can perform `:min`", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.atomic_op(tr, :min, "foo", <<100>>)
    end

    test "can perform `:set_versionstamped_key`", context do
      tr = Transaction.create(context.db)

      :ok =
        Transaction.atomic_op(
          tr,
          :set_versionstamped_key,
          <<0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x00, 0x00,
            0x00>>,
          <<>>
        )
    end

    test "can perform `:set_versionstamped_value`", context do
      tr = Transaction.create(context.db)

      :ok =
        Transaction.atomic_op(
          tr,
          :set_versionstamped_value,
          "foo",
          <<0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x00, 0x00,
            0x00>>
        )
    end
  end

  describe "cancel/1" do
    test "successfully cancels transaction", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.cancel(tr)
    end
  end

  describe "clear/3" do
    test "successfully clears key", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.clear(tr, "foo")
    end
  end

  describe "clear_range/4" do
    test "successfully clears range", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.clear_range(tr, "bar", "foo")
    end
  end

  describe "clear_starts_with/3" do
    test "successfully clears using prefix", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.clear_starts_with(tr, "foo")
    end
  end

  describe "get/3" do
    test "succeeded gets value", context do
      tr = Transaction.create(context.db)
      assert nil == Transaction.get(tr, "foobar")
    end
  end

  describe "get_addresses_for_key/2" do
    test "successfully gets host for stored key", context do
      tr = Transaction.create(context.db)
      Transaction.set(tr, "foo", "bar")
      addresses = Transaction.get_addresses_for_key(tr, "foo")
      assert addresses == ["127.0.0.1:4500"]
    end
  end

  describe "get_approximate_size/1" do
    test "successfully get size of transaction so far", context do
      tr = Transaction.create(context.db)
      size = Transaction.get_estimated_range_size(tr, "foo", "goo")
      assert size == 0
    end
  end

  describe "get_committed_version/1" do
    test "successfully get committed version", context do
      tr = Transaction.create(context.db)
      assert Transaction.get_committed_version(tr)
    end
  end

  describe "get_estimated_range_size/3" do
    test "successfully get estimated range size", context do
      tr = Transaction.create(context.db)
      assert 0 == Transaction.get_estimated_range_size(tr, "bar", "foo")
    end
  end

  describe "get_key/3" do
    test "successfully gets key for selector", context do
      tr = Transaction.create(context.db)
      Transaction.set(tr, "foo", "bar")
      assert "foo" = Transaction.get_key(tr, KeySelector.greater_than_or_equal("foo"))
    end
  end

  describe "get_metadata_version/1" do
    test "successfully gets metadata version", context do
      tr = Transaction.create(context.db)
      assert nil == Transaction.get_metadata_version(tr)
    end
  end

  describe "get_range/4" do
    test "successfully gets kv pairs for given range", context do
      tr = Transaction.create(context.db)
      Transaction.set(tr, "aa", "1")
      Transaction.set(tr, "ab", "2")
      Transaction.set(tr, "ac", "3")
      Transaction.set(tr, "ad", "4")
      Transaction.set(tr, "ae", "5")
      Transaction.set(tr, "af", "6")
      pairs = Transaction.get_range(tr, "aa", "az")

      assert pairs == [
               {"aa", "1"},
               {"ab", "2"},
               {"ac", "3"},
               {"ad", "4"},
               {"ae", "5"},
               {"af", "6"}
             ]
    end
  end

  describe "get_range_split_points/4" do
    test "successfully gets split points", context do
      tr = Transaction.create(context.db)
      keys = Transaction.get_range_split_points(tr, "foo", "goo", 1)
      assert keys == ["foo", "goo"]
    end
  end

  describe "get_read_version/1" do
    test "successfully gets read version", context do
      tr = Transaction.create(context.db)
      assert Transaction.get_read_version(tr)
    end
  end

  describe "get_starts_with/3" do
    test "successfully gets kv pairs starting with given prefix", context do
      tr = Transaction.create(context.db)
      pairs = Transaction.get_starts_with(tr, "abcd")
      assert pairs == []
    end
  end

  describe "get_tag_throttled_duration/1" do
    test "successfully gets duration", context do
      tr = Transaction.create(context.db)
      assert 0.0 == Transaction.get_tag_throttled_duration(tr)
    end
  end

  describe "get_total_cost/1" do
    test "successfully gets total cost", context do
      tr = Transaction.create(context.db)
      assert 0 == Transaction.get_total_cost(tr)
    end
  end

  describe "async_get_versionstamp/1" do
    test "successfully gets versionstamp", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.set(tr, "foo", "bar")
      future = Transaction.async_get_versionstamp(tr)
      :ok = Transaction.commit(tr)
      assert Future.resolve(future)
    end

    test "errors on a read only transaction", context do
      tr = Transaction.create(context.db)
      future = Transaction.async_get_versionstamp(tr)
      :ok = Transaction.commit(tr)

      assert_raise FDBC.Error, fn ->
        assert Future.resolve(future)
      end
    end

    test "timesout when incorrectly called", context do
      tr = Transaction.create(context.db)

      assert_raise FDBC.Error, fn ->
        Transaction.async_get_versionstamp(tr) |> Future.await(500)
      end
    end
  end

  describe "reset/1" do
    test "successfully resets transaction", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.reset(tr)
    end
  end

  describe "set/4" do
    test "successfully sets value for key", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.set(tr, "foo", "bar")
    end
  end

  describe "set_metadata_version/1" do
    test "successfully sets metadata version", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.set_metadata_version(tr)
    end
  end

  describe "set_read_version/2" do
    test "successfully sets read version", context do
      tr = Transaction.create(context.db)
      :ok = Transaction.set_read_version(tr, 1)
    end
  end

  describe "stream/4" do
    test "successfully streams kv pairs for given range", context do
      tr = Transaction.create(context.db)
      Transaction.set(tr, "aa", "1")
      Transaction.set(tr, "ab", "2")
      Transaction.set(tr, "ac", "3")
      Transaction.set(tr, "ad", "4")
      Transaction.set(tr, "ae", "5")
      Transaction.set(tr, "af", "6")
      pairs = Transaction.stream(tr, "aa", "az", limit: 2) |> Enum.to_list()

      assert pairs == [
               {"aa", "1"},
               {"ab", "2"},
               {"ac", "3"},
               {"ad", "4"},
               {"ae", "5"},
               {"af", "6"}
             ]
    end
  end

  describe "stream_starts_with/3" do
    test "successfully streams kv pairs for given prefix", context do
      tr = Transaction.create(context.db)
      Transaction.set(tr, "aa", "1")
      Transaction.set(tr, "ab", "2")
      Transaction.set(tr, "ac", "3")
      Transaction.set(tr, "ad", "4")
      Transaction.set(tr, "ae", "5")
      Transaction.set(tr, "af", "6")
      pairs = Transaction.stream_starts_with(tr, "a", limit: 2) |> Enum.to_list()

      assert pairs == [
               {"aa", "1"},
               {"ab", "2"},
               {"ac", "3"},
               {"ad", "4"},
               {"ae", "5"},
               {"af", "6"}
             ]
    end
  end

  describe "watch/2" do
    test "successfully reports a value change on a watched key", context do
      tr = Transaction.create(context.db)
      watcher = Transaction.watch(tr, "watch")
      :ok = Transaction.set(tr, "watch", "value")
      assert nil == Future.await(watcher, 500)
      :ok = Future.cancel(watcher)
    end
  end
end
