defmodule FDBC.Database do
  @moduledoc """
  An `FDBC.Database` represents a FoundationDB database — a mutable,
  lexicographically ordered mapping from binary keys to binary values.
  Modifications to a database are performed via transactions.
  """

  alias FDBC.NIF

  defstruct [:resource]

  @type t :: %__MODULE__{}

  @doc """
  Creates a database instance used to interact with the cluster.

  If no cluster file is provided, then FoundationDB automatically [determines a
  cluster file](https://apple.github.io/foundationdb/administration.html#specifying-a-cluster-file)
  with which to connect to a cluster.

  A single client can use this function multiple times to connect to different
  clusters simultaneously, with each invocation requiring its own cluster file.

  ## Options

    * `:datacenter_id` - (binary) Specify the datacenter ID that was passed to
      fdbserver processes running in the same datacenter as this client, for
      better location-aware load balancing.

    * `:location_cache_size` - (integer) Set the size of the client location
      cache. Raising this value can boost performance in very large databases
      where clients acc  ess data in a near-random pattern. Defaults to `100000`.

    * `:machine_id` - (binary) Specify the machine ID that was passed to
      fdbserver processes running on the same machine as this client, for
      better location-aware load balancing.

    * `:max_watches` - (integer) Set the maximum number of watches allowed to be
      outstanding on a database connection. Increasing this number could result
      in increased resource usage. Reducing this number will not cancel any
      outstanding watches. Defaults to `10000` and cannot be larger than `1000000`.

    * `:snapshot_ryw` - (boolean) Specify whether snapshot read operations will
      see the results of writes done in the same transaction. Defaults to `true`.

    * `:test_causal_read_risky` - (integer) Enables verification of causal read
      risky by checking whether clients are able to read stale data when they
      detect a recovery, and logging an error if so. Must be between 0 and 100
      expressing the probability a client will verify it can't read stale data.

    * `:transaction_automatic_idempotency` - (true) Set a random idempotency id
      for all transactions. See the transaction option description for more
      information. This feature is in development and not ready for general
      use.

    * `:transaction_bypass_unreadable` - (true) Allows ``get`` operations to
      read from sections of keyspace that have become unreadable because of
      versionstamp operations. This sets the ``bypass_unreadable`` option of
      each transaction created by this database. See the transaction option
      description for more information.

    * `:transaction_causal_read_risky` - (true) The read version will be
      committed, and usually will be the latest committed, but might not be the
      latest committed in the event of a simultaneous fault and misbehaving
      clock.

    * `:transaction_logging_max_field_length` - (integer) Sets the maximum
      escaped length of key and value fields to be logged to the trace file via
      the `:log_transaction` option. This sets the `:transaction_logging_max_field_length`
      option of each transaction created by this database. See the transaction
      option description for more information.

    * `:transaction_max_retry_delay` - (integer) Set the maximum amount of
      backoff delay incurred in the call to `on_error` if the error is retryable.
      This sets the `:max_retry_delay` option of each transaction created by
      this database. See the transaction option description for more information.

    * `:transaction_report_conflicting_keys` - (true) Enables conflicting key
      reporting on all transactions, allowing them to retrieve the keys that
      are conflicting with other transactions.

    * `:transaction_retry_limit` - (integer) Set a maximum number of retries
      after which additional calls to `on_error` will throw the most recently
      seen error code. This sets the `:retry_limit` option of each transaction
      created by this database. See the transaction option description for more
      information.

    * `:transaction_size_limit` - (integer) Set the maximum transaction size in
      bytes. This sets the `:size_limit` option on each transaction created by
      this database. See the transaction option description for more information.

    * `:transaction_timeout` - (integer) Set a timeout in milliseconds which,
      when elapsed, will cause each transaction automatically to be cancelled.
      This sets the `:timeout` option of each transaction created by this
      database. See the transaction option description for more information.
      Using this option requires that the API version is 610 or higher.

    * `:transaction_used_during_commit_protection_disable` - (true) By default,
      operations that are performed on a transaction while it is being
      committed will not only fail themselves, but they will attempt to fail
      other in-flight operations (such as the commit) as well. This behavior is
      intended to help developers discover situations where operations could be
      unintentionally executed after the transaction has been reset. Setting
      this option removes that protection, causing only the offending operation
      to fail.

    * `:use_config_database` - (true) Use configuration database.
  """
  @spec create(String.t() | nil, keyword()) :: t
  def create(cluster_file_path \\ nil, opts \\ []) do
    case NIF.create_database(cluster_file_path) do
      {:ok, resource} ->
        Enum.each(opts, fn {k, v} -> set_option(resource, k, v) end)
        %__MODULE__{resource: resource}

      {:error, code, reason} ->
        raise FDBC.Error, message: reason, code: code
    end
  end

  @doc """
  Change the options on the database instance after its initial creation.

  See `create/2` for the list of options.
  """
  @spec change(t, keyword) :: t
  def change(%__MODULE__{} = db, opts) do
    Enum.each(opts, fn {k, v} -> set_option(db.resource, k, v) end)
    db
  end

  defp set_option(resource, :location_cache_size, size) when is_integer(size) do
    :ok = NIF.database_set_option(resource, 10, <<size::64-signed-little-integer>>)
    :ok
  end

  defp set_option(resource, :max_watches, max) when is_integer(max) do
    :ok = NIF.database_set_option(resource, 20, <<max::64-signed-little-integer>>)
    :ok
  end

  defp set_option(resource, :machine_id, id) when is_binary(id) do
    :ok = NIF.database_set_option(resource, 21, id)
    :ok
  end

  defp set_option(resource, :datacenter_id, id) when is_binary(id) do
    :ok = NIF.database_set_option(resource, 22, id)
    :ok
  end

  defp set_option(resource, :snapshot_ryw, enable) when is_boolean(enable) do
    :ok =
      case enable do
        true -> NIF.database_set_option(resource, 26)
        false -> NIF.database_set_option(resource, 27)
      end

    :ok
  end

  defp set_option(resource, :transaction_logging_max_field_length, max) when is_integer(max) do
    :ok = NIF.database_set_option(resource, 405, <<max::64-signed-little-integer>>)
    :ok
  end

  defp set_option(resource, :transaction_timeout, timeout) when is_integer(timeout) do
    :ok = NIF.database_set_option(resource, 500, <<timeout::64-signed-little-integer>>)
    :ok
  end

  defp set_option(resource, :transaction_retry_limit, limit) when is_integer(limit) do
    :ok = NIF.database_set_option(resource, 501, <<limit::64-signed-little-integer>>)
    :ok
  end

  defp set_option(resource, :transaction_max_retry_delay, delay) when is_integer(delay) do
    :ok = NIF.database_set_option(resource, 502, <<delay::64-signed-little-integer>>)
    :ok
  end

  defp set_option(resource, :transaction_size_limit, limit) when is_integer(limit) do
    :ok = NIF.database_set_option(resource, 503, <<limit::64-signed-little-integer>>)
    :ok
  end

  defp set_option(resource, :transaction_causal_read_risky, true) do
    :ok = NIF.database_set_option(resource, 504)
    :ok
  end

  defp set_option(resource, :transaction_automatic_idempotency, true) do
    :ok = NIF.database_set_option(resource, 506)
    :ok
  end

  defp set_option(resource, :transaction_bypass_unreadable, true) do
    :ok = NIF.database_set_option(resource, 700)
    :ok
  end

  defp set_option(resource, :transaction_used_during_commit_protection_disable, true) do
    :ok = NIF.database_set_option(resource, 701)
    :ok
  end

  defp set_option(resource, :transaction_report_conflicting_keys, true) do
    :ok = NIF.database_set_option(resource, 702)
    :ok
  end

  defp set_option(resource, :use_config_database, true) do
    :ok = NIF.database_set_option(resource, 800)
    :ok
  end

  defp set_option(resource, :test_causal_read_risky, percentage)
       when is_integer(percentage) and percentage >= 0 and percentage <= 100 do
    :ok = NIF.database_set_option(resource, 900, <<percentage::64-signed-little-integer>>)
    :ok
  end
end
