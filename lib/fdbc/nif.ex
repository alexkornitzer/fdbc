defmodule FDBC.NIF do
  @moduledoc false
  @on_load :load_nifs

  @app Mix.Project.config()[:app]

  defp load_nifs do
    path = :filename.join(:code.priv_dir(@app), ~c"lib/libfdbcnif")
    :erlang.load_nif(path, 0)
  end

  def select_api_version(_runtime_version) do
    :erlang.nif_error("NIF select_api_version/1 not implemented")
  end

  def get_max_api_version() do
    :erlang.nif_error("NIF get_max_api_version/0 not implemented")
  end

  def network_set_option(_option) do
    :erlang.nif_error("NIF network_set_option/1 not implemented")
  end

  def network_set_option(_option, _value) do
    :erlang.nif_error("NIF network_set_option/2 not implemented")
  end

  def setup_network() do
    :erlang.nif_error("NIF setup_network/0 not implemented")
  end

  def run_network() do
    :erlang.nif_error("NIF run_network/0 not implemented")
  end

  def stop_network() do
    :erlang.nif_error("NIF stop_network/0 not implemented")
  end

  def future_resolve(_future, _ref) do
    :erlang.nif_error("NIF future_resolve/2 not implemented")
  end

  def future_cancel(_future) do
    :erlang.nif_error("NIF future_cancel/1 not implemented")
  end

  def future_is_ready(_future) do
    :erlang.nif_error("NIF future_is_ready/1 not implemented")
  end

  def create_database(_cluster_file_path) do
    :erlang.nif_error("NIF create_database/1 not implemented")
  end

  def database_set_option(_database, _option) do
    :erlang.nif_error("NIF database_set_option/2 not implemented")
  end

  def database_set_option(_database, _option, _value) do
    :erlang.nif_error("NIF database_set_option/3 not implemented")
  end

  def database_open_tenant(_database, _tenant) do
    :erlang.nif_error("NIF database_open_tenant/2 not implemented")
  end

  def database_create_transaction(_database) do
    :erlang.nif_error("NIF database_create_transaction/1 not implemented")
  end

  def database_reboot_worker(_database, _address, _check, _duration) do
    :erlang.nif_error("NIF database_reboot_worker/4 not implemented")
  end

  def database_force_recovery_with_data_loss(_database, _dc_id) do
    :erlang.nif_error("NIF database_force_recovery_with_data_loss/2 not implemented")
  end

  def database_create_snapshot(_database, _command) do
    :erlang.nif_error("NIF database_create_snapshot/2 not implemented")
  end

  def database_get_main_thread_busyness(_database) do
    :erlang.nif_error("NIF database_get_main_thread_busyness/1 not implemented")
  end

  def database_get_client_status(_database) do
    :erlang.nif_error("NIF database_get_client_status/1 not implemented")
  end

  def tenant_create_transaction(_tenant) do
    :erlang.nif_error("NIF tenant_create_transaction/1 not implemented")
  end

  def tenant_get_id(_tenant) do
    :erlang.nif_error("NIF tenant_get_id/1 not implemented")
  end

  def transaction_set_option(_transaction, _option) do
    :erlang.nif_error("NIF transaction_set_option/2 not implemented")
  end

  def transaction_set_option(_transaction, _option, _value) do
    :erlang.nif_error("NIF transaction_set_option/3 not implemented")
  end

  def transaction_set_read_version(_transaction, _version) do
    :erlang.nif_error("NIF transaction_set_read_version/2 not implemented")
  end

  def transaction_get_read_version(_transaction) do
    :erlang.nif_error("NIF transaction_get_read_version/1 not implemented")
  end

  def transaction_get(_transaction, _key, _snapshot) do
    :erlang.nif_error("NIF transaction_get/3 not implemented")
  end

  def transaction_get_estimated_range_size_bytes(_transaction, _begin_key, _end_key) do
    :erlang.nif_error("NIF transaction_get_estimated_range_size_bytes/3 not implemented")
  end

  def transaction_get_range_split_points(_transaction, _begin_key, _end_key, _chunk_size) do
    :erlang.nif_error("NIF transaction_get_range_split_points/4 not implemented")
  end

  def transaction_get_key(_transaction, _key, _or_equal, _offset, _snapshot) do
    :erlang.nif_error("NIF transaction_get_key/5 not implemented")
  end

  def transaction_get_addresses_for_key(_transaction, _key) do
    :erlang.nif_error("NIF transaction_get_addresses_for_key/2 not implemented")
  end

  def transaction_get_range(
        _transaction,
        _begin_key,
        _begin_or_equal,
        _begin_offset,
        _end_key,
        _end_or_equal,
        _end_offset,
        _limit,
        _target_bytes,
        _mode,
        _iteration,
        _snapshot,
        _reverse
      ) do
    :erlang.nif_error("NIF transaction_get_range/13 not implemented")
  end

  def transaction_set(_transaction, _key, _value) do
    :erlang.nif_error("NIF transaction_set/3 not implemented")
  end

  def transaction_clear(_transaction, _key) do
    :erlang.nif_error("NIF transaction_clear/2 not implemented")
  end

  def transaction_clear_range(_transaction, _begin_key, _end_key) do
    :erlang.nif_error("NIF transaction_clear_range/3 not implemented")
  end

  def transaction_atomic_op(_transaction, _key, _param, _type) do
    :erlang.nif_error("NIF transaction_atomic_op/4 not implemented")
  end

  def transaction_commit(_transaction) do
    :erlang.nif_error("NIF transaction_commit/1 not implemented")
  end

  def transaction_get_committed_version(_transaction) do
    :erlang.nif_error("NIF transaction_get_committed_version/1 not implemented")
  end

  def transaction_get_tag_throttled_duration(_transaction) do
    :erlang.nif_error("NIF transaction_get_tag_throttled_duration/1 not implemented")
  end

  def transaction_get_total_cost(_transaction) do
    :erlang.nif_error("NIF transaction_get_total_cost/1 not implemented")
  end

  def transaction_get_approximate_size(_transaction) do
    :erlang.nif_error("NIF transaction_get_approximate_size/1 not implemented")
  end

  def transaction_get_versionstamp(_transaction) do
    :erlang.nif_error("NIF transaction_get_versionstamp/1 not implemented")
  end

  def transaction_watch(_transaction, _key) do
    :erlang.nif_error("NIF transaction_watch/2 not implemented")
  end

  def transaction_on_error(_transaction, _error) do
    :erlang.nif_error("NIF transaction_on_error/2 not implemented")
  end

  def transaction_reset(_transaction) do
    :erlang.nif_error("NIF transaction_reset/1 not implemented")
  end

  def transaction_cancel(_transaction) do
    :erlang.nif_error("NIF transaction_cancel/1 not implemented")
  end

  def transaction_add_conflict_range(_transaction, _begin_key, _end_key, _type) do
    :erlang.nif_error("NIF transaction_add_conflict_range/4 not implemented")
  end
end
