defmodule FDBC.Transaction do
  @moduledoc """
  In FoundationDB, a transaction is a mutable snapshot of a database. All read
  and write operations on a transaction see and modify an otherwise-unchanging
  version of the database and only change the underlying database if and when
  the transaction is committed. Read operations do see the effects of previous
  write operations on the same transaction. Committing a transaction usually
  succeeds in the absence of
  [conflicts](https://apple.github.io/foundationdb/developer-guide.html#conflict-ranges).

  Transactions group operations into a unit with the properties of atomicity,
  isolation, and durability. Transactions also provide the ability to maintain
  an application’s invariants or integrity constraints, supporting the property
  of consistency. Together these properties are known as
  [ACID](https://apple.github.io/foundationdb/developer-guide.html#acid).

  Transactions are also causally consistent: once a transaction has been
  successfully committed, all subsequently created transactions will see the
  modifications made by it.

  Applications must provide error handling and an appropriate retry loop around
  the application code for a transaction. `FDBC` provides a convenience
  function `FDBC.transact/3` which will do just that when passed a
  `FDBC.Database`. The function roughly does the following:

  ```elixir
  tr = Transaction.create(db)
  def transact(tr, fun) do
    result = fun.(transaction)
    :ok = FDBC.Transaction.commit(transaction)
    result
  rescue
    e in FDBC.Error ->
      :ok = FDBC.Transaction.on_error(tr, e)
      transact(tr, fun)
  end
  ```

  This convenience function allows for transactional blocks to be handled like
  so:

  ```elixir
  db = FDBC.Database.create()
  FDBC.transact(db, fn tr ->
    :ok = Transaction.set(tr, "foo", "bar")
  end)
  ```

  In reality the `FDBC.transact/3` is more versatile than this and can be
  consulted for further details.

  ## Futures

  This library unlike [upstream implementations](https://apple.github.io/foundationdb/developer-guide.html#developer-guide-programming-with-futures)
  does not support implicit asynchronicity, it must be explicity used.

  There are two ways in which to achieve this, using `Task` or by using the
  `async_*` variants along with `FDBC.Future`.

  Using `Task` the example in the link above can be achieved like so:

  ```elixir
  tasks = [
    Task.async(fn -> FDBC.Transaction.get(tr, "A") end),
    Task.async(fn -> FDBC.Transaction.get(tr, "B") end),
  ]
  result = Task.await_many(tasks) |> Enum.reduce(0, fn x, acc -> acc <> x end)
  IO.inspect(result)
  ```

  Using the `async_*` variants it can be achieved with the following:

  ```elixir
  futures = [
    FDBC.Transaction.async_get(tr, "A"),
    FDBC.Transaction.async_get(tr, "B"),
  ]
  result = FDBC.Futures.await_many(futures) |> Enum.reduce(0, fn x, acc -> acc <> x end)
  IO.inspect(result)
  ```

  The main difference with the two approaches is that `Task` will spawn each
  operation in a new process while `FDBC.Future` will operate within the
  calling process.

  ## Watches

  It is possible to [watch keys](https://apple.github.io/foundationdb/developer-guide.html#watches) for
  value changes. The most obvious way to handle this is via a `GenServer` like
  implementation:

  ```elixir
  defmodule Watcher do
    use GenServer

    alias FDBC.{Database, Future, Transaction}

    def start_link(opts) do
      GenServer.start_link(__MODULE__, :ok, opts)
    end

    def watch(key) do
      GenServer.call(__MODULE__, {:watch, key})
    end

    def init(_opts) do
      {:ok, %{tasks: %{}}}
    end

    def handle_call({:watch, key}, _from, state) do
      {future, task} = start_watch(key)
      state = put_in(state.tasks[task.ref], key)
      {:reply, :ok, state}
    end

    def handle_info({ref, nil}, state) do
      Process.demonitor(ref, [:flush])
      {key, state} = pop_in(state.tasks[ref])
      IO.puts("Value for key \#{inspect(key)} has changed")
      task = start_watch(key)
      state = put_in(state.tasks[task.ref], key)
      {:noreply, state}
    end

    def handle_info({:DOWN, ref, _, _, reason}, state) do
      {_key, state} = pop_in(state.tasks[ref])
      # Should check reason to see if watcher was cancelled or errored...
      {:noreply, state}
    end

    defp start_watch(key) do
      tr = Database.create() |> Transaction.create()
      future = Transaction.watch(tr, key)
      :ok = Transaction.commit(tr)

      Task.Supervisor.async_nolink(Example.TaskSupervisor, fn ->
        Future.resolve(future)
      end)
    end
  end
  ```

  The above would then need to be added to the application's supervision tree:

  ```elixir
  children = [
    {Task.Supervisor, name: Example.TaskSupervisor},
    {Watcher, name: Example.Watcher}
  ]

  Supervisor.start_link(children, strategy: :one_for_one)
  ```
  """

  alias FDBC.Database
  alias FDBC.Future
  alias FDBC.KeySelector
  alias FDBC.NIF
  alias FDBC.Tenant

  defstruct [:resource]

  @type t :: %__MODULE__{}

  @type mutation ::
          :add
          | :append_if_fits
          | :bit_and
          | :bit_or
          | :bit_xor
          | :byte_max
          | :byte_min
          | :compare_and_clear
          | :max
          | :min
          | :set_versionstamped_key
          | :set_versionstamped_value

  @doc """
  Creates a new transaction on the given database or tenant.

  It is valid to pass a transaction through this function in order to modify
  its options.

  ## Options

    * `:access_system_keys` - (true) Allows this transaction to read and modify
      system keys (those that start with the byte 0xFF). Implies `:raw_access`.

    * `:authorization_token` - (true) Attach given authorization token to the
      transaction such that subsequent tenant-aware requests are authorized.

    * `:auto_throttle_tag` - (binary) Adds a tag to the transaction that can be
      used to apply manual or automatic targeted throttling. At most 5 tags can
      be set on a transaction.

    * `:bypass_storage_quota` - (true) Allows this transaction to bypass
      storage quota enforcement. Should only be used for transactions that
      directly or indirectly decrease the size of the tenant group's data.

    * `:bypass_unreadable` - (true) Allows `get` operations to read from
      sections of keyspace that have become unreadable because of versionstamp
      operations. These reads will view versionstamp operations as if they were
      set operations that did not fill in the versionstamp.

    * `:causal_read_disable` - (true) Disable causal reads.

    * `:causal_read_risky` - (true) The read version will be committed, and
      usually will be the latest committed, but might not be the latest
      committed in the event of a simultaneous fault and misbehaving clock.

    * `:causal_write_risky` - (true) The transaction, if not self-conflicting,
      may be committed a second time after commit succeeds, in the event of a
      fault.

    * `:consistency_check_required_replicas` - (integer) Specifies the number
      of storage server replica results that the load balancer needs to compare
      when `:enable_replica_consistency_check` option is set.

    * `:debug_transaction_identifier` - (binary) Sets a client provided
      identifier for the transaction that will be used in scenarios like
      tracing or profiling. Client trace logging or transaction profiling must
      be separately enabled.

    * `:enable_replica_consistency_check` - (true) Enables replica consistency
      check, which compares the results returned by storage server replicas (as
      many as specified by co  nsistency_check_required_replicas option) for a
      given read request, in client-side load balancer.

    * `:expensive_clear_cost_estimation_enable` - (true) Asks storage servers
      for how many bytes a clear key range contains. Otherwise uses the
      location cache to roughly estimate this.

    * `:first_in_batch` - (true) No other transactions will be applied before
      this transaction within the same commit version.

    * `:lock_aware` - (true) The transaction can read and write to locked
      databases, and is responsible for checking that it took the lock.

    * `:log_transaction` - (true) Enables tracing for this transaction and logs
      results to the client trace logs. The `:debug_transaction_identifier`
      option must be set before using this option, and client trace logging
      must be enabled to get log output.

    * `:max_retry_delay` - (integer) Set the maximum amount of backoff delay
      incurred in the call to `on_error` if the error is retryable. If the
      maximum retry delay is less than the current retry delay of the
      transaction, then the current retry delay will be clamped to the maximum
      retry delay. Prior to API version 610, like all other transaction
      options, the maximum retry delay must be reset after a call to
      `on_error`. If the API version is 610 or greater, the retry limit is not
      reset after an `on_error` call. Note that at all API versions, it is
      safe and legal to set the maximum retry delay each time the transaction
      begins, so most code written assuming the older behavior can be upgraded
      to the newer behavior without requiring any modification, and the caller
      is not required to implement special logic in retry loops to only
      conditionally set this option. Defaults to `1000`.

    * `:next_write_no_write_conflict_range` - (true) The next write performed
      on this transaction will not generate a write conflict range. As a
      result, other transactions which read the key(s) being modified by the
      next write will not conflict with this transaction. Care needs to be
      taken when using this option on a transaction that is shared between
      multiple threads. When setting this option, write conflict ranges will be
      disabled on the next write operation, regardless of what thread it is on.

    * `:priority` - (atom) Set the priority for the transaction. Valid values
      are `:immediate` and `:batch`. Where `:immediate` specifies that this
      transaction should be treated as highest priority and that lower priority
      transactions should block behind this one. Use is discouraged outside of
      low-level tools. While `:batch` specifies that this transaction should be
      treated as low priority and that default priority transactions will be
      processed first. Batch priority transactions will also be throttled at
      load levels smaller than for other types of transactions and may be fully
      cut off in the event of machine failures. Useful for doing batch work
      simultaneously with latency-sensitive work.

    * `:raw_access` - (true) Allows this transaction to access the raw
      key-space when tenant mode is on.

    * `:read_lock_aware` - (true) The transaction can read from locked
      databases.

    * `:read_priority` - (atom) Set the priority for subsequent read requests
      in this transaction. Valid values are `:low`, `:normal`, and `:high`.
      Defaults to `:normal`.

    * `:read_server_side_cache` - (boolean) Whether the storage server should
      cache disk blocks needed for subsequent read requests in this
      transaction. Defaults to `true`.

    * `:read_system_keys` - (true) Allows this transaction to read system keys
      (those that start with the byte 0xFF). Implies `:raw_access`.

    * `:read_your_writes_disabled` - (true) Reads performed by a transaction
      will not see any prior mutations that occurred in that transaction,
      instead seeing the value which was in the database at the transaction's
      read version. This option may provide a small performance benefit for the
      client, but also disables a number of client-side optimizations which are
      beneficial for transactions which tend to read and write the same keys
      within a single transaction. It is an error to set this option after
      performing any reads or writes on the transaction.

    * `:report_conflicting_keys` - (true) The transaction can retrieve keys
      that are conflicting with other transactions.

    * `:retry_limit` - (integer) Set a maximum number of retries after which
      additional calls to `on_error` will throw the most recently seen error
      code. If set to `-1`, will disable the retry limit. Prior to API version
      610, like all other transaction options, the retry limit must be reset
      after a call to `on_error`. If the API version is 610 or greater, the
      retry limit is not reset after an `on_error` call. Note that at all API
      versions, it is safe and legal to set the retry limit each time the trans
      action begins, so most code written assuming the older behavior can be
      upgraded to the newer behavior without requiring any modification, and
      the caller is not required to implement special logic in retry loops to
      only conditionally set this option.

    * `:server_request_tracing` - (true) Sets an identifier for server tracing
      of this transaction. When committed, this identifier triggers logging
      when each part of the transaction authority encounters it, which is
      helpful in diagnosing slowness in misbehaving clusters. The identifier is
      randomly generated. When there is also a `:debug_transaction_identifier`,
      both IDs are logged together.

    * `:size_limit` - (integer) Set the transaction size limit in bytes. The
      size is calculated by combining the sizes of all keys and values written
      or mutated, all key ranges cleared, and all read and write conflict
      ranges. (In other words, it includes the total size of all data included
      in the request to the cluster to commit the transaction.) Large
      transactions can cause performance problems on FoundationDB clusters, so
      setting this limit to a smaller value than the default can help prevent
      the client from accidentally degrading the cluster's performance. This
      value must be at least 32 and cannot be set to higher than 10,000,000,
      the default transaction size limit.

    * `:snapshot_ryw` - (boolean) Allow snapshot read operations will see the
      results of writes done in the same transaction. Defaults to `true`.

    * `:span_parent` - (binary) Adds a parent to the Span of this transaction.
      Used for transaction tracing. A span can be identified with a 33 bytes
      serialized binary format which consists of: 8 bytes protocol version,
      e.g. ``0x0FDBC00B073000000LL`` in little-endian format, 16 bytes trace id,
      8 bytes span id, 1 byte set to 1 if sampling is enabled.

    * `:special_key_space_enable_writes` - (true) By default, users are not
      allowed to write to special keys. Enable this option will implicitly
      enable all options required to achieve the configuration change.

    * `:special_key_space_relaxed` - (true) By default, the special key space
      will only allow users to read from exactly one module (a subspace in the
      special key space). Use this option to allow reading from zero or more
      modules. Users who set this option should be prepared for new modules,
      which may have different behaviors than the modules they're currently
      reading. For example, a new module might block or return an error.

    * `:tag` - (binary) Adds a tag to the transaction that can be used to apply
      manual targeted throttling. At most 5 tags can be set on a transaction.

    * `:timeout` - (integer) Set a timeout in milliseconds which, when elapsed,
      will cause the transaction automatically to be cancelled. If set to `0`,
      will disable all timeouts. All pending and any future uses of the
      transaction will throw a  n exception. The transaction can be used again
      after it is reset. Prior to API version 610, like all other transaction
      options, the timeout must be reset after a call to `on_error`. If the
      API version is 610 or greater, the timeout is not reset after an
      `on_error` call. This allows the user to specify a longer timeout on
      specific transactions than the default timeout specified through the
      `:transaction_timeout` database option without the shorter database
      timeout cancelling transactions that encounter a retryable error. Note
      that at all API versions, it is safe and legal to set the timeout each
      time the transaction begins, so most code written assum  ing the older
      behavior can be upgraded to the newer behavior without requiring any
      modification, and the caller is not required to i  mplement special logic
      in retry loops to only conditionally set this option.

    * `:transaction_logging_max_field_length` - (integer) Sets the maximum
      escaped length of key and value fields to be logged to the trace file via the
      `:log_transaction` option, after which the field will be truncated. A
      negative value disables truncation.

    * `:use_grv_cache` - (boolean) Allows this transaction to use cached GRV
      from the database context. Upon first usage, starts a background updater
      to periodically update the cache to avoid stale read versions. The
      disable_client_bypass option must also be set. Defaults to `false`.

    * `:used_during_commit_protection_disable` - (true) By default, operations
      that are performed on a transaction while it is being committed will not
      only fail themselves, but the  y will attempt to fail other in-flight
      operations (such as the commit) as well. This behavior is intended to
      help developers discove  r situations where operations could be
      unintentionally executed after the transaction has been reset. Setting
      this option removes th  at protection, causing only the offending
      operation to fail.
  """
  @spec create(Database.t() | Tenant.t(), keyword) :: t
  def create(database_or_tenant, opts \\ [])

  def create(%Database{} = database, opts) do
    case NIF.database_create_transaction(database.resource) do
      {:ok, resource} ->
        Enum.each(opts, fn {k, v} -> set_option(resource, k, v) end)
        %__MODULE__{resource: resource}

      {:error, code, reason} ->
        raise FDBC.Error, message: reason, code: code
    end
  end

  def create(%Tenant{} = tenant, opts) do
    case NIF.tenant_create_transaction(tenant.resource) do
      {:ok, resource} ->
        Enum.each(opts, fn {k, v} -> set_option(resource, k, v) end)
        %__MODULE__{resource: resource}

      {:error, code, reason} ->
        raise FDBC.Error, message: reason, code: code
    end
  end

  @doc """
  Change the options on the transaction after its initial creation.

  See `create/2` for the list of options.
  """
  @spec change(t, keyword) :: t
  def change(%__MODULE__{} = transaction, opts) do
    Enum.each(opts, fn {k, v} -> set_option(transaction.resource, k, v) end)
    transaction
  end

  @doc """
  Attempts to commit the transaction to the database.

  The commit may or may not succeed - in particular, if a conflicting
  transaction previously committed, then the commit must fail in order to
  preserve transactional isolation. If the commit does succeed, the transaction
  is durably committed to the database and all subsequently started
  transactions will observe its effects.
  """
  @spec commit(t) :: :ok
  def commit(%__MODULE__{resource: resource}) do
    resource = NIF.transaction_commit(resource)

    with nil <- %Future{resource: resource} |> Future.resolve() do
      :ok
    end
  end

  @doc """
  Adds a conflict key to a transaction without performing the associated read or write.

  Works the same way as `add_conflict_range/4` by creating the range on a single key.
  """
  @spec add_conflict_key(t, binary, :read | :write) :: :ok
  def add_conflict_key(transaction, key, op) do
    :ok = add_conflict_range(transaction, key, key <> <<0x00>>, op)
    :ok
  end

  @doc """
  Adds a [conflict range](https://apple.github.io/foundationdb/developer-guide.html#conflict-ranges)
  to a transaction without performing the associated read or write.

  If `:read` is used, this function adds a range of keys to the transaction’s
  read conflict ranges as if you had read the range. As a result, other
  transactions that write a key in this range could cause the transaction to
  fail with a conflict.

  If `:write` is used, this function adds a range of keys to the transaction’s
  write conflict ranges as if you had cleared the range. As a result, other
  transactions that concurrently read a key in this range could fail with a
  conflict.
  """
  @spec add_conflict_range(t, binary, binary, :read | :write) :: :ok
  def add_conflict_range(%__MODULE__{resource: resource}, start, stop, op) do
    op =
      case op do
        :read -> 0
        :write -> 1
      end

    case NIF.transaction_add_conflict_range(resource, start, stop, op) do
      :ok -> :ok
      {:error, code, reason} -> raise FDBC.Error, message: reason, code: code
    end

    :ok
  end

  @doc """
  Perform a mutation as an atomic operation against the database.

  To be more specific an atomic operation modifies the database snapshot
  represented by transaction to perform the operation indicated by `op` with
  operand `param` to the value stored by the given `key`.

  An atomic operation is a single database command that carries out several
  logical steps: reading the value of a key, performing a transformation on
  that value, and writing the result. Different atomic operations perform
  different transformations. Like other database operations, an atomic
  operation is used within a transaction; however, its use within a transaction
  will not cause the transaction to conflict.

  Atomic operations do not expose the current value of the key to the client
  but simply send the database the transformation to apply. In regard to
  conflict checking, an atomic operation is equivalent to a write without a
  read. It can only cause other transactions performing reads of the key to
  conflict.

  By combining these logical steps into a single, read-free operation,
  FoundationDB can guarantee that the transaction will not conflict due to the
  operation. This makes atomic operations ideal for operating on keys that are
  frequently modified. A common example is the use of a key-value pair as a
  counter.

  ## Mutations

    * `:add` - Performs an addition of little-endian integers. If the existing
      value in the database is not present or shorter than `param`, it is first
      extended to the length of `param` with zero bytes. If `param` is shorter
      than the existing value in the database, the existing value is truncated
      to match the length of `param`. The integers to be added must be stored
      in a little-endian representation. They can be signed in two's complement
      representation or unsigned. You can add to an integer at a known offset
      in the value by prepending the appropriate number of zero bytes to
      `param` and padding with zero bytes to match the length of the value.
      However, this offset technique requires that you know the addition will
      not cause the integer field within the value to overflow.

    * `:append_if_fits` - Appends `param` to the end of the existing value
      already in the database at the given key (or creates the key and sets the
      value to `param` if the key is empty). This will only append the value if
      the final concatenated value size is less than or equal to the maximum
      value size. WARNING: No error is surfaced back to the user if the final
      value is too large because the mutation will not be applied until after
      the transaction has been committed. Therefore, it is only safe to use
      this mutation type if one can guarantee that one will keep the total
      value size under the maximum size.

    * `:bit_and` - Performs a bitwise `and` operation. If the existing value in
      the database is not present, then `param` is stored in the database. If
      the existing value in the database is shorter than `param`, it is first
      extended to the length of `param` with zero bytes. If `param` is shorter
      than the existing value in the database, the existing value is truncated
      to match the length of `param`.

    * `:bit_or` - Performs a bitwise `or` operation. If the existing value in
      the database is not present or shorter than `param`, it is first extended
      to the length of `param` with zero bytes. If `param` is shorter than the
      existing value in the database, the existing value is truncated to match
      the length of `param`.

    * `:bit_xor` - Performs a bitwise `xor` operation. If the existing value in
      the database is not present or shorter than `param`, it is first extended
      to the length of `param` with zero bytes. If `param` is shorter than the
      existing value in the database, the existing value is truncated to match
      the length of `param`.

    * `:byte_max` - Performs lexicographic comparison of byte strings. If the
      existing value in the database is not present, then `param` is stored.
      Otherwise the larger of the two values is then stored in the database.

    * `:byte_min` - Performs lexicographic comparison of byte strings. If the
      existing value in the database is not present, then `param` is stored.
      Otherwise the smaller of the two values is then stored in the database.

    * `:compare_and_clear` - Performs an atomic `compare and clear` operation.
      If the existing value in the database is equal to the given value, then
      given key is cleared.

    * `:max` - Performs a little-endian comparison of byte strings. If the
      existing value in the database is not present or shorter than `param`, it
      is first extended to the length of `param` with zero bytes. If `param` is
      shorter than the existing value in the database, the existing value is
      truncated to match the length of `param`. The larger of the two values is
      then stored in the database.

    * `:min` - Performs a little-endian comparison of byte strings. If the
      existing value in the database is not present, then `param` is stored in
      the database. If the existing value in the database is shorter than
      `param`, it is first extended to the length of `param` with zero bytes.
      If `param` is shorter than the existing value in the database, the
      existing value is truncated to match the length of `param`. The smaller
      of the two values is then stored in the database.

    * `:set_versionstamped_key` - Transforms `key` using a versionstamp for the
      transaction. Sets the transformed key in the database to `param`. The key
      is transformed by removing the final four bytes from the key and reading
      those as a little-Endian 32-bit integer to get a position `pos`. The 10
      bytes of the key from `pos` to `pos + 10` are replaced with the
      versionstamp of the transaction used. The first byte of the key is
      position 0. A versionstamp is a 10 byte, unique, monotonically (but not
      sequentially) increasing value for each committed transaction. The first
      8 bytes are the committed version of the database (serialized in
      big-Endian order). The last 2 bytes are monotonic in the serialization
      order for transactions.

    * `:set_versionstamped_value` - Transforms `param` using a versionstamp for the
      transaction. Sets the `key` given to the transformed `param`. The
      parameter is transformed by removing the final four bytes from `param`
      and reading those as a little-Endian 32-bit integer to get a position
      `pos`. The 10 bytes of the parameter from `pos` to `pos + 10` are
      replaced with the versionstamp of the transaction used. The first byte of
      the parameter is position 0. A versionstamp is a 10 byte, unique,
      monotonically (but not sequentially) increasing value for each committed
      transaction. The first 8 bytes are the committed version of the database
      (serialized in big-Endian order). The last 2 bytes are monotonic in the
      serialization order for transactions.
  """
  @spec atomic_op(t, mutation, binary, binary) :: :ok
  def atomic_op(transaction, op, key, param)

  def atomic_op(transaction, :add, key, param), do: do_atomic_op(transaction, key, param, 2)
  def atomic_op(transaction, :bit_and, key, param), do: do_atomic_op(transaction, key, param, 6)
  def atomic_op(transaction, :bit_or, key, param), do: do_atomic_op(transaction, key, param, 7)
  def atomic_op(transaction, :bit_xor, key, param), do: do_atomic_op(transaction, key, param, 8)

  def atomic_op(transaction, :append_if_fits, key, param),
    do: do_atomic_op(transaction, key, param, 9)

  def atomic_op(transaction, :max, key, param), do: do_atomic_op(transaction, key, param, 12)
  def atomic_op(transaction, :min, key, param), do: do_atomic_op(transaction, key, param, 13)

  def atomic_op(transaction, :set_versionstamped_key, key, param),
    do: do_atomic_op(transaction, key, param, 14)

  def atomic_op(transaction, :set_versionstamped_value, key, param),
    do: do_atomic_op(transaction, key, param, 15)

  def atomic_op(transaction, :byte_min, key, param), do: do_atomic_op(transaction, key, param, 16)
  def atomic_op(transaction, :byte_max, key, param), do: do_atomic_op(transaction, key, param, 17)

  def atomic_op(transaction, :compare_and_clear, key, param),
    do: do_atomic_op(transaction, key, param, 20)

  defp do_atomic_op(%__MODULE__{resource: resource}, key, param, op) do
    :ok = NIF.transaction_atomic_op(resource, key, param, op)
    :ok
  end

  @doc """
  Cancels the transaction.

  All pending or future uses of the transaction will raise a transaction
  cancelled exception. The transaction can be used again after it is `reset/1`.
  """
  @spec cancel(t) :: :ok
  def cancel(%__MODULE__{resource: resource}) do
    :ok = NIF.transaction_cancel(resource)
    :ok
  end

  @doc """
  Clear the given key from the database.

  Modify the database snapshot represented by transaction to remove the given
  key from the database. If the key was not previously present in the database,
  there is no effect.

  ## Options

    * `:no_write_conflict_range` - (true) The operation will not generate a
      write conflict range. As a result, other transactions which read the
      key(s) being modified by the next write will not conflict with this
      transaction. NOTE: This is equivalent to setting
      `:next_write_no_write_conflict_range` on the transaction followed by
      calling this function.
  """
  @spec clear(t, binary, keyword) :: :ok
  def clear(%__MODULE__{resource: resource}, key, opts \\ []) do
    if Keyword.get(opts, :no_write_conflict_range, false) do
      :ok = set_option(resource, :next_write_no_write_conflict_range, true)
    end

    :ok = NIF.transaction_clear(resource, key)
    :ok
  end

  @doc """
  Clear the given range from the database.

  Modify the database snapshot represented by transaction to remove all keys
  (if any) which are lexicographically greater than or equal to the given begin
  key and lexicographically less than the given end_key.

  Range clears are efficient with FoundationDB – clearing large amounts of data
  will be fast. However, this will not immediately free up disk - data for the
  deleted range is cleaned up in the background. For purposes of computing the
  transaction size, only the begin and end keys of a clear range are counted.
  The size of the data stored in the range does not count against the
  transaction size limit.

  ## Options

    * `:no_write_conflict_range` - (true) The operation will not generate a
      write conflict range. As a result, other transactions which read the
      key(s) being modified by the next write will not conflict with this
      transaction. NOTE: This is equivalent to setting
      `:next_write_no_write_conflict_range` on the transaction followed by
      calling this function.
  """
  @spec clear_range(t, binary, binary, keyword) :: :ok
  def clear_range(%__MODULE__{resource: resource}, start, stop, opts \\ []) do
    if Keyword.get(opts, :no_write_conflict_range, false) do
      :ok = set_option(resource, :next_write_no_write_conflict_range, true)
    end

    :ok = NIF.transaction_clear_range(resource, start, stop)
    :ok
  end

  @doc """
  Clear all keys starting with the given prefix from the database.

  Calls `clear_range/4` under the hood turning the `prefix` into a start/stop
  pair that will clear a range of keys that is equivalent to clearing by
  prefix.

  ## Options

    * `:no_write_conflict_range` - (true) The operation will not generate a
      write conflict range. As a result, other transactions which read the
      key(s) being modified by the next write will not conflict with this
      transaction. NOTE: This is equivalent to setting
      `:next_write_no_write_conflict_range` on the transaction followed by
      calling this function.
  """
  @spec clear_starts_with(t, binary, keyword) :: :ok
  def clear_starts_with(transaction, prefix, opts \\ []) do
    clear_range(transaction, prefix, increment_key(prefix), opts)
  end

  @doc """
  Get a value from the database.

  Reads a value from the database snapshot represented by the transaction.

  ## Options

    * `:snapshot` - (boolean) Perform the get as a [snapshot read](https://apple.github.io/foundationdb/api-c.html#snapshots).
  """
  @spec get(t, binary, keyword) :: binary | nil
  def get(transaction, key, opts \\ []) do
    async_get(transaction, key, opts) |> Future.resolve()
  end

  @doc """
  The same as `get/3` except it returns the unresolved future.
  """
  @spec async_get(t, binary, keyword) :: Future.t(binary | nil)
  def async_get(%__MODULE__{resource: resource}, key, opts \\ []) do
    snapshot =
      case Keyword.get(opts, :snapshot, false) do
        false -> 0
        true -> 1
      end

    resource = NIF.transaction_get(resource, key, snapshot)
    %Future{resource: resource}
  end

  @doc """
  Returns the storage server adressess storing the given key.

  Returns a list of public network addresses as strings, one for each of the
  storage servers responsible for storing key_name and its associated value.
  """
  @spec get_addresses_for_key(t, binary) :: [binary]
  def get_addresses_for_key(transaction, key) do
    async_get_addresses_for_key(transaction, key) |> Future.resolve()
  end

  @doc """
  The same as `get_addresses_for_key/2` except it returns the unresolved future.
  """
  @spec async_get_addresses_for_key(t, binary) :: Future.t([binary])
  def async_get_addresses_for_key(%__MODULE__{resource: resource}, key) do
    resource = NIF.transaction_get_addresses_for_key(resource, key)

    %Future{resource: resource}
  end

  @doc """
  Returns the approximate transaction size so far.

  This is a summation of the estimated size of mutations, read conflict ranges,
  and write conflict ranges.
  """
  @spec get_approximate_size(t) :: integer
  def get_approximate_size(transaction) do
    async_get_approximate_size(transaction) |> Future.resolve()
  end

  @doc """
  The same as `get_approximate_size/1` except it returns the unresolved future.
  """
  @spec async_get_approximate_size(t) :: Future.t(integer)
  def async_get_approximate_size(%__MODULE__{resource: resource}) do
    resource = NIF.transaction_get_approximate_size(resource)

    %Future{resource: resource}
  end

  @doc """
  Returns the database version number for the commited transaction.

  Retrieves the database version number at which a given transaction was
  committed. `commit/1` must have been called on transaction. Read-only
  transactions do not modify the database when committed and will have a
  committed version of -1. Keep in mind that a transaction which reads keys and
  then sets them to their current values may be optimized to a read-only
  transaction.

  Note that database versions are not necessarily unique to a given transaction
  and so cannot be used to determine in what order two transactions completed.
  The only use for this function is to manually enforce causal consistency when
  calling `set_read_version/2` on another subsequent transaction.
  """
  @spec get_committed_version(t) :: integer
  def get_committed_version(%__MODULE__{resource: resource}) do
    case NIF.transaction_get_committed_version(resource) do
      {:ok, result} -> result
      {:error, code, reason} -> raise FDBC.Error, message: reason, code: code
    end
  end

  @doc """
  Returns an estimated byte size of the key range.

  > #### Note {: .info}
  > The estimated size is calculated based on the sampling done by FDB server.
    The sampling algorithm works roughly in this way: the larger the key-value
    pair is, the more likely it would be sampled and the more accurate its
    sampled size would be. And due to that reason it is recommended to use this
    API to query against large ranges for accuracy considerations. For a rough
    reference, if the returned size is larger than 3MB, one can consider the
    size to be accurate.

  """
  @spec get_estimated_range_size(t, binary, binary) :: integer
  def get_estimated_range_size(transaction, start, stop) do
    async_get_estimated_range_size(transaction, start, stop) |> Future.resolve()
  end

  @doc """
  The same as `get_estimated_range_size/3` except it returns the unresolved future.
  """
  @spec async_get_estimated_range_size(t, binary, binary) :: Future.t(integer)
  def async_get_estimated_range_size(%__MODULE__{resource: resource}, start, stop) do
    resource = NIF.transaction_get_estimated_range_size_bytes(resource, start, stop)

    %Future{resource: resource}
  end

  @doc """
  Returns the first key in the database that matches the given key selector.

  ## Options

    * `:snapshot` - (boolean) Perform the get as a [snapshot read](https://apple.github.io/foundationdb/api-c.html#snapshots).
  """
  @spec get_key(t, KeySelector.t(), keyword) :: binary
  def get_key(transaction, key_selector, opts \\ []) do
    async_get_key(transaction, key_selector, opts) |> Future.resolve()
  end

  @doc """
  The same as `get_key/3` except it returns the unresolved future.
  """
  @spec async_get_key(t, KeySelector.t(), keyword) :: Future.t(binary)
  def async_get_key(
        %__MODULE__{resource: resource},
        %KeySelector{key: key, or_equal: or_equal, offset: offset},
        opts \\ []
      ) do
    snapshot =
      case Keyword.get(opts, :snapshot, false) do
        false -> 0
        true -> 1
      end

    resource = NIF.transaction_get_key(resource, key, or_equal, offset, snapshot)

    %Future{resource: resource}
  end

  @doc """
  Returns the metadata version.

  The metadata version key `\\xff/metadataVersion` is a key intended to help
  layers deal with hot keys. The value of this key is sent to clients along
  with the read version from the proxy, so a client can read its value without
  communicating with a storage server.

  It is stored as a versionstamp, and can be `nil` if its yet to be utilised.
  """
  @spec get_metadata_version(t) :: binary | nil
  def get_metadata_version(transaction) do
    get(transaction, "\xff/metadataVersion")
  end

  @doc """
  Returns all the key-value pairs for the given range.

  ## Options

    * `:limit` - (integer) Indicates the maximum number of key-value pairs to
      return.

    * `:mode` - (atom) The mode in which to return the data to the caller.
      Defaults to `:iterator`.

      * `:exact` - The client has passed a specific row limit and wants that
        many rows delivered in a single batch. Because of iterator operation in
        client drivers make request batches transparent to the user, consider
        `:want_all` instead. A row `:limit` must be specified if this mode is
        used.

      * `:iterator` - The client doesn't know how much of the range it is
        likely to used and wants different performance concerns to be balanced.
        Only a small portion of data is transferred to the client initially (in
        order to minimize costs if the client doesn't read the entire range),
        and as the caller iterates over more items in the range larger batches
        will be transferred in order to minimize latency. After enough
        iterations, the iterator mode will eventually reach the same byte limit
        as `:want_all`.

      * `:large` - Transfer data in batches large enough to be, in a
        high-concurrency environment, nearly as efficient as possible. If the
        client stops iteration early, some disk and network bandwidth may be
        wasted. The batch size may still be too small to allow a single client
        to get high throughput from the database, so if that is what you need
        consider the `:serial` instead.

      * `:medium` - Transfer data in batches sized in between small and large.

      * `:serial` - Transfer data in batches large enough that an individual
        client can get reasonable read bandwidth from the database. If the
        client stops iteration early, considerable disk and network bandwidth
        may be wasted.

      * `:small` - Transfer data in batches small enough to not be much more
        expensive than reading individual rows, to minimize cost if iteration
        stops early.

      * `:want_all` - Client intends to consume the entire range and would like
        it all transferred as early as possible.

    * `:reverse` - (boolean) The key-value pairs will be returned in reverse
      lexicographical order beginning at the end of the range. Reading ranges
      in reverse is supported natively by the database and should have minimal
      extra cost.

    * `:snapshot` - (boolean) Perform the get as a [snapshot read](https://apple.github.io/foundationdb/api-c.html#snapshots).
  """
  @spec get_range(t, binary | KeySelector.t(), binary | KeySelector.t(), keyword) ::
          [{binary, binary}]
  def get_range(transaction, start, stop, opts \\ [])

  def get_range(
        %__MODULE__{} = transaction,
        %KeySelector{} = start,
        %KeySelector{} = stop,
        opts
      ) do
    mode =
      case opts[:mode] || :iterator do
        :want_all -> -2
        :iterator -> -1
        :exact -> 0
        :small -> 1
        :medium -> 2
        :large -> 3
        :serial -> 4
      end

    reverse =
      case Keyword.get(opts, :reverse, false) do
        true -> 1
        false -> 0
      end

    snapshot =
      case Keyword.get(opts, :snapshot, false) do
        true -> 1
        false -> 0
      end

    do_range(
      transaction,
      start,
      stop,
      1,
      opts[:limit] || 0,
      mode,
      reverse,
      snapshot
    )
    |> List.flatten()
    |> Enum.reverse()
  end

  def get_range(transaction, start, %KeySelector{} = stop, opts)
      when is_binary(start) do
    get_range(
      transaction,
      KeySelector.greater_than_or_equal(start),
      stop,
      opts
    )
  end

  def get_range(transaction, %KeySelector{} = start, stop, opts)
      when is_binary(stop) do
    get_range(
      transaction,
      start,
      KeySelector.greater_than_or_equal(stop),
      opts
    )
  end

  def get_range(transaction, start, stop, opts)
      when is_binary(start) and is_binary(stop) do
    get_range(
      transaction,
      KeySelector.greater_than_or_equal(start),
      KeySelector.greater_than_or_equal(stop),
      opts
    )
  end

  defp do_range(
         %__MODULE__{resource: resource} = transaction,
         %KeySelector{key: start_key, or_equal: start_or_equal, offset: start_offset} = start,
         %KeySelector{key: stop_key, or_equal: stop_or_equal, offset: stop_offset} =
           stop,
         iteration,
         limit,
         mode,
         reverse,
         snapshot
       ) do
    resource =
      NIF.transaction_get_range(
        resource,
        start_key,
        start_or_equal,
        start_offset,
        stop_key,
        stop_or_equal,
        stop_offset,
        limit,
        0,
        mode,
        iteration,
        snapshot,
        reverse
      )

    case %Future{resource: resource} |> Future.resolve() do
      {0, results} ->
        results

      {1, results} ->
        [{key, _value} | _] = results

        {start, stop} =
          case reverse do
            1 -> {start, KeySelector.greater_than_or_equal(key)}
            0 -> {KeySelector.greater_than(key), stop}
          end

        left = limit - length(results)

        if limit == 0 || left > 0 do
          limit =
            case limit do
              0 -> 0
              _ -> left
            end

          [
            do_range(
              transaction,
              start,
              stop,
              iteration + 1,
              limit,
              mode,
              reverse,
              snapshot
            )
            | results
          ]
        else
          results
        end
    end
  end

  @doc """
  Returns a list of keys that can split the given range into roughly equally sized chunks based on chunk `size`.

  The returned split points contain the start key and end key of the given range.
  """
  @spec get_range_split_points(t, binary, binary, non_neg_integer) :: [binary]
  def get_range_split_points(%__MODULE__{resource: resource}, start, stop, size) do
    resource = NIF.transaction_get_range_split_points(resource, start, stop, size)

    %Future{resource: resource} |> Future.resolve() |> Enum.reverse()
  end

  @doc """
  Returns the transaction snapshot read version.

  The transaction obtains a snapshot read version automatically at the time of
  the first call to `get_*()` (including this one) and (unless causal
  consistency has been deliberately compromised by transaction options) is
  guaranteed to represent all transactions which were reported committed before
  that call.
  """
  @spec get_read_version(t) :: integer
  def get_read_version(transaction) do
    async_get_read_version(transaction) |> Future.resolve()
  end

  @doc """
  The same as `get_read_version/1` except it returns the unresolved future.
  """
  @spec async_get_read_version(t) :: Future.t(integer)
  def async_get_read_version(%__MODULE__{resource: resource}) do
    resource = NIF.transaction_get_read_version(resource)

    %Future{resource: resource}
  end

  @doc """
  Returns all the key-value pairs the start with the given prefix.

  This function calls `get_range/4` and therefore supports the same options as
  it.
  """
  @spec get_starts_with(t, binary, keyword) :: [{binary, binary}]
  def get_starts_with(transaction, prefix, opts \\ []) do
    get_range(transaction, prefix, increment_key(prefix), opts)
  end

  @doc """
  Returns the time in seconds that the transaction was throttled by the tag
  throttler.
  """
  @spec get_tag_throttled_duration(t) :: float
  def get_tag_throttled_duration(transaction) do
    async_get_tag_throttled_duration(transaction) |> Future.resolve()
  end

  @doc """
  The same as `get_tag_throttled_duration/1` except it returns the unresolved future.
  """
  @spec async_get_tag_throttled_duration(t) :: Future.t(float)
  def async_get_tag_throttled_duration(%__MODULE__{resource: resource}) do
    resource = NIF.transaction_get_tag_throttled_duration(resource)

    %Future{resource: resource}
  end

  @doc """
  Returns the cost of the transaction so far in bytes.

  The cost is computed by the tag throttler, and used for tag throttling if
  throughput quotas are specified.
  """
  @spec get_total_cost(t) :: integer
  def get_total_cost(transaction) do
    async_get_total_cost(transaction) |> Future.resolve()
  end

  @doc """
  The same as `get_total_cost/1` except it returns the unresolved future.
  """
  @spec async_get_total_cost(t) :: Future.t(integer)
  def async_get_total_cost(%__MODULE__{resource: resource}) do
    resource = NIF.transaction_get_total_cost(resource)

    %Future{resource: resource}
  end

  @doc """
  Returns a future that will resolve to the versionstamp used by the transaction.

  The underlying future will be ready only after the successful completion of a
  call to `commit/1`. Read-only transactions do not modify the database when
  committed and will result in the underlying future completing with an error.
  Keep in mind that a transaction which reads keys and then sets them to their
  current values may be optimized to a read-only transaction.

  > #### Warning {: .warning}
  > It must be called before `commit/1` but resolved after it.
  """
  @spec async_get_versionstamp(t) :: Future.t(binary)
  def async_get_versionstamp(%__MODULE__{resource: resource}) do
    resource = NIF.transaction_get_versionstamp(resource)

    %Future{resource: resource}
  end

  @doc """
  Implements the recommended retry and backoff behavior for a transaction.

  This function knows which of the error codes generated by other
  `FDBC.Transaction` functions represent temporary error conditions and which
  represent application errors that should be handled by the application. It
  also implements an exponential backoff strategy to avoid swamping the
  database cluster with excessive retries when there is a high level of
  conflict between transactions.
  """
  @spec on_error(t, FDBC.Error.t()) :: :ok
  def on_error(%__MODULE__{resource: resource}, error) do
    resource = NIF.transaction_on_error(resource, error.code)

    with nil <- %Future{resource: resource} |> Future.resolve() do
      :ok
    end
  end

  @doc """
  Reset the transaction to its initial state.

  It is not necessary to call `reset/1` when handling an error with
  `on_error/2` since the transaction has already been reset.
  """
  @spec reset(t) :: :ok
  def reset(%__MODULE__{resource: resource}) do
    :ok = NIF.transaction_reset(resource)
    :ok
  end

  @doc """
  Set the value for a given key.

  ## Options

    * `:no_write_conflict_range` - (true) The operation will not generate a
      write conflict range. As a result, other transactions which read the
      key(s) being modified by the next write will not conflict with this
      transaction. NOTE: This is equivalent to setting
      `:next_write_no_write_conflict_range` on the transaction followed by
      calling this function.
  """
  @spec set(t, binary, binary, keyword) :: :ok
  def set(%__MODULE__{resource: resource}, key, value, opts \\ []) do
    if Keyword.get(opts, :no_write_conflict_range, false) do
      :ok = set_option(resource, :next_write_no_write_conflict_range, true)
    end

    :ok = NIF.transaction_set(resource, key, value)
    :ok
  end

  @doc """
  Sets the metadata version.

  It takes no value as the database is responsible for setting it.
  """
  @spec set_metadata_version(t) :: :ok
  def set_metadata_version(transaction) do
    atomic_op(
      transaction,
      :set_versionstamped_value,
      "\xff/metadataVersion",
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    )
  end

  @doc """
  Sets the snapshot read version.

  This is not needed in simple cases. If the given version is too old,
  subsequent reads will fail with error code 'transaction_too_old'; if it is too
  new, subsequent reads may be delayed indefinitely and/or fail with
  error code 'future_version'. If any of get_*() have been called on this
  transaction already, the result is undefined.
  """
  @spec set_read_version(t, integer) :: :ok
  def set_read_version(%__MODULE__{resource: resource}, version) do
    case NIF.transaction_set_read_version(resource, version) do
      :ok -> :ok
      {:error, code, reason} -> raise FDBC.Error, message: reason, code: code
    end

    :ok
  end

  @doc """
  Streams all the key-value pairs for the given range.

  This function supports the same options as `get_range/4`.
  """
  @spec stream(t, binary | KeySelector.t(), binary | KeySelector.t(), keyword()) ::
          Enumerable.t({binary, binary})
  def stream(transaction, start, stop, opts \\ [])

  def stream(
        %__MODULE__{resource: resource},
        %KeySelector{} = start,
        %KeySelector{} = stop,
        opts
      ) do
    mode =
      case opts[:mode] || :iterator do
        :want_all -> -2
        :iterator -> -1
        :exact -> 0
        :small -> 1
        :medium -> 2
        :large -> 3
        :serial -> 4
      end

    limit = Keyword.get(opts, :limit, 0)

    reverse =
      case Keyword.get(opts, :reverse, false) do
        true -> 1
        false -> 0
      end

    snapshot =
      case Keyword.get(opts, :snapshot, false) do
        true -> 1
        false -> 0
      end

    Stream.resource(
      fn -> {start, stop, 1} end,
      fn state ->
        case state do
          nil ->
            {:halt, nil}

          {start, stop, iteration} ->
            resource =
              NIF.transaction_get_range(
                resource,
                start.key,
                start.or_equal,
                start.offset,
                stop.key,
                stop.or_equal,
                stop.offset,
                limit,
                0,
                mode,
                iteration,
                snapshot,
                reverse
              )

            case %Future{resource: resource} |> Future.resolve() do
              {0, results} ->
                {results |> Enum.reverse(), nil}

              {1, results} ->
                [{key, _value} | _] = results

                {start, stop} =
                  case reverse do
                    1 -> {start, KeySelector.greater_than_or_equal(key)}
                    0 -> {KeySelector.greater_than(key), stop}
                  end

                {results |> Enum.reverse(), {start, stop, iteration + 1}}
            end
        end
      end,
      fn _ -> :ok end
    )
  end

  def stream(transaction, start, %KeySelector{} = stop, opts)
      when is_binary(start) do
    stream(
      transaction,
      KeySelector.greater_than_or_equal(start),
      stop,
      opts
    )
  end

  def stream(transaction, %KeySelector{} = start, stop, opts)
      when is_binary(stop) do
    stream(
      transaction,
      start,
      KeySelector.greater_than_or_equal(stop),
      opts
    )
  end

  def stream(transaction, start, stop, opts)
      when is_binary(start) and is_binary(stop) do
    stream(
      transaction,
      KeySelector.greater_than_or_equal(start),
      KeySelector.greater_than_or_equal(stop),
      opts
    )
  end

  @doc """
  Stream all the key-value pairs the start with the given prefix.

  This function calls `get_range/4` and therefore supports the same options as
  it.
  """
  @spec stream_starts_with(t, binary, keyword) :: Enumerable.t({binary, binary})
  def stream_starts_with(transaction, prefix, opts \\ []) do
    stream(transaction, prefix, increment_key(prefix), opts)
  end

  @doc """
  Watch for a change on the given key's value.

  A watch’s behavior is relative to the transaction that created it. A watch
  will report a change in relation to the key’s value as readable by that
  transaction. The initial value used for comparison is either that of the
  transaction’s read version or the value as modified by the transaction itself
  prior to the creation of the watch. If the value changes and then changes
  back to its initial value, the watch might not report the change.

  Until the transaction that created it has been committed, a watch will not
  report changes made by other transactions. In contrast, a watch will
  immediately report changes made by the transaction itself. Watches cannot be
  created if the transaction has set the `:read_your_writes_disabled`
  transaction option, and an attempt to do so will return an watches_disabled
  error.

  If the transaction used to create a watch encounters an error during commit,
  then the watch will be set with that error. A transaction whose commit result
  is unknown will set all of its watches with the commit_unknown_result error.
  If an uncommitted transaction is reset or destroyed, then any watches it
  created will be set with the transaction_cancelled error.

  Returns an `FDBC.Future` representing an empty value that will be set once
  the watch has detected a change to the value at the specified key.

  By default, each database connection can have no more than 10,000 watches
  that have not yet reported a change. When this number is exceeded, an attempt
  to create a watch will return a too_many_watches error. This limit can be
  changed using the `:max_watches` database option. Because a watch outlives the
  transaction that creates it, any watch that is no longer needed should be
  cancelled by calling `cancel/1` on its returned future.
  """
  @spec watch(t, binary) :: Future.t(nil)
  def watch(%__MODULE__{resource: resource}, key) do
    resource = NIF.transaction_watch(resource, key)
    %Future{resource: resource}
  end

  ## Helpers

  # Not private as its needed for testing.
  @doc false
  def increment_key(key) when is_binary(key) do
    size = byte_size(key)

    {key, size} =
      case key do
        <<key::binary-size(^size - 1), 0xFF>> -> {key, size - 1}
        _ -> {key, size}
      end

    if size == 0 do
      raise "key must contain at least one byte not equal to 0xFF"
    end

    <<x::binary-size(^size - 1), y>> = key
    <<x::binary, y + 1>>
  end

  defp set_option(resource, :causal_write_risky, true) do
    :ok = NIF.transaction_set_option(resource, 10)
    :ok
  end

  defp set_option(resource, :causal_read_risky, true) do
    :ok = NIF.transaction_set_option(resource, 20)
    :ok
  end

  defp set_option(resource, :causal_read_disable, true) do
    :ok = NIF.transaction_set_option(resource, 21)
    :ok
  end

  defp set_option(resource, :next_write_no_write_conflict_range, true) do
    :ok = NIF.transaction_set_option(resource, 30)
    :ok
  end

  defp set_option(resource, :read_your_writes_disabled, true) do
    :ok = NIF.transaction_set_option(resource, 51)
    :ok
  end

  defp set_option(resource, :read_server_side_cache, enable) when is_boolean(enable) do
    :ok =
      case enable do
        true -> NIF.transaction_set_option(resource, 507)
        false -> NIF.transaction_set_option(resource, 508)
      end

    :ok
  end

  defp set_option(resource, :read_priority, :normal) do
    :ok = NIF.transaction_set_option(resource, 509)
    :ok
  end

  defp set_option(resource, :read_priority, :low) do
    :ok = NIF.transaction_set_option(resource, 510)
    :ok
  end

  defp set_option(resource, :read_priority, :high) do
    :ok = NIF.transaction_set_option(resource, 511)
    :ok
  end

  defp set_option(resource, :durability, :datacenter) do
    :ok = NIF.transaction_set_option(resource, 110)
    :ok
  end

  defp set_option(resource, :durability, :risky) do
    :ok = NIF.transaction_set_option(resource, 120)
    :ok
  end

  defp set_option(resource, :priority, :immediate) do
    :ok = NIF.transaction_set_option(resource, 200)
    :ok
  end

  defp set_option(resource, :priority, :batch) do
    :ok = NIF.transaction_set_option(resource, 201)
    :ok
  end

  defp set_option(resource, :access_system_keys, true) do
    :ok = NIF.transaction_set_option(resource, 301)
    :ok
  end

  defp set_option(resource, :read_system_keys, true) do
    :ok = NIF.transaction_set_option(resource, 302)
    :ok
  end

  defp set_option(resource, :raw_access, true) do
    :ok = NIF.transaction_set_option(resource, 303)
    :ok
  end

  defp set_option(resource, :bypass_storage_quota, true) do
    :ok = NIF.transaction_set_option(resource, 304)
    :ok
  end

  defp set_option(resource, :debug_dump, true) do
    :ok = NIF.transaction_set_option(resource, 400)
    :ok
  end

  defp set_option(resource, :debug_transaction_identifier, identifier)
       when is_binary(identifier) do
    :ok = NIF.transaction_set_option(resource, 403, identifier)
    :ok
  end

  defp set_option(resource, :log_transaction, true) do
    :ok = NIF.transaction_set_option(resource, 404)
    :ok
  end

  defp set_option(resource, :transaction_logging_max_field_length, length)
       when is_integer(length) do
    :ok = NIF.transaction_set_option(resource, 405, <<length::64-signed-little-integer>>)
    :ok
  end

  defp set_option(resource, :server_request_tracing, true) do
    :ok = NIF.transaction_set_option(resource, 406)
    :ok
  end

  defp set_option(resource, :timeout, value) when is_integer(value) do
    :ok = NIF.transaction_set_option(resource, 500, <<value::64-signed-little-integer>>)
    :ok
  end

  defp set_option(resource, :retry_limit, limit) when is_integer(limit) do
    :ok = NIF.transaction_set_option(resource, 501, <<limit::64-signed-little-integer>>)
    :ok
  end

  defp set_option(resource, :max_retry_delay, delay) when is_integer(delay) do
    :ok = NIF.transaction_set_option(resource, 502, <<delay::64-signed-little-integer>>)
    :ok
  end

  defp set_option(resource, :size_limit, limit) when is_integer(limit) do
    :ok = NIF.transaction_set_option(resource, 503, <<limit::64-signed-little-integer>>)
    :ok
  end

  defp set_option(resource, :snapshot_ryw, enable) when is_boolean(enable) do
    :ok =
      case enable do
        true -> NIF.transaction_set_option(resource, 600)
        false -> NIF.transaction_set_option(resource, 601)
      end

    :ok
  end

  defp set_option(resource, :lock_aware, true) do
    :ok = NIF.transaction_set_option(resource, 700)
    :ok
  end

  defp set_option(resource, :used_during_commit_protection_disable, true) do
    :ok = NIF.transaction_set_option(resource, 701)
    :ok
  end

  defp set_option(resource, :read_lock_aware, true) do
    :ok = NIF.transaction_set_option(resource, 702)
    :ok
  end

  defp set_option(resource, :first_in_batch, true) do
    :ok = NIF.transaction_set_option(resource, 710)
    :ok
  end

  defp set_option(resource, :report_conflicting_keys, true) do
    :ok = NIF.transaction_set_option(resource, 712)
    :ok
  end

  defp set_option(resource, :special_key_space_relaxed, true) do
    :ok = NIF.transaction_set_option(resource, 713)
    :ok
  end

  defp set_option(resource, :special_key_space_enable_writes, true) do
    :ok = NIF.transaction_set_option(resource, 714)
    :ok
  end

  defp set_option(resource, :tag, tag) when is_binary(tag) do
    :ok = NIF.transaction_set_option(resource, 800, tag)
    :ok
  end

  defp set_option(resource, :auto_throttle_tag, tag) when is_binary(tag) do
    :ok = NIF.transaction_set_option(resource, 801, tag)
    :ok
  end

  defp set_option(resource, :span_parent, parent) when is_binary(parent) do
    :ok = NIF.transaction_set_option(resource, 900, parent)
    :ok
  end

  defp set_option(resource, :expensive_clear_cost_estimation_enable, true) do
    :ok = NIF.transaction_set_option(resource, 1000)
    :ok
  end

  defp set_option(resource, :bypass_unreadable, true) do
    :ok = NIF.transaction_set_option(resource, 1100)
    :ok
  end

  defp set_option(resource, :use_grv_cache, enable) when is_boolean(enable) do
    :ok =
      case enable do
        true -> NIF.transaction_set_option(resource, 1101)
        false -> NIF.transaction_set_option(resource, 1102)
      end

    :ok
  end

  defp set_option(resource, :authorization_token, token) when is_binary(token) do
    :ok = NIF.transaction_set_option(resource, 2000, token)
    :ok
  end

  defp set_option(resource, :enable_replica_consistency_check, true) do
    :ok = NIF.transaction_set_option(resource, 4000)
    :ok
  end

  defp set_option(resource, :consistency_check_required_replicas, replicas)
       when is_integer(replicas) do
    :ok = NIF.transaction_set_option(resource, 4001, <<replicas::64-signed-little-integer>>)
    :ok
  end
end
