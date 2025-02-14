# Implements the tester for use by the upstream bindingtester.
#
# https://github.com/apple/foundationdb/blob/main/bindings/bindingtester
#
# This can be run by applying the patch set `bindingtester.patch` and running
# then running the `run_tester_loop.sh`.

defmodule Stack do
  alias FDBC.Future
  alias FDBC.Tuple

  def new(), do: []

  def peek(stack, opts \\ [])
  def peek([], _), do: nil

  def peek([{idx, item} | _], opts) do
    if Keyword.get(opts, :indexed) do
      {idx, item}
    else
      item
    end
  end

  def pop(stack, opts \\ [])
  def pop([], _), do: {nil, []}

  def pop([{idx, %Future{} = f} | tail], opts) do
    item =
      try do
        case Future.resolve(f) do
          nil -> {:binary, "RESULT_NOT_PRESENT"}
          item -> {:binary, item}
        end
      rescue
        # If we let these errors bubble to the top we break the expected output
        # of the stack machine, so like upstream we handle them here!
        e in FDBC.Error ->
          item = Tuple.pack(["ERROR", to_string(e.code)])
          {:binary, item}
      end

    if Keyword.get(opts, :indexed) do
      {{idx, item}, tail}
    else
      {item, tail}
    end
  end

  def pop([{idx, item} | tail], opts) do
    if Keyword.get(opts, :indexed) do
      {{idx, item}, tail}
    else
      {item, tail}
    end
  end

  def pop_many(stack, 0), do: {[], stack}

  def pop_many(stack, count) do
    {pairs, stack} =
      Enum.reduce(1..count, {[], stack}, fn _, {pairs, stack} ->
        {pair, stack} = pop(stack)
        {[pair | pairs], stack}
      end)

    {Enum.reverse(pairs), stack}
  end

  def push(stack, id, item), do: [{id, item} | stack]

  def swap(stack, idx) do
    x = Enum.at(stack, 0)
    y = Enum.at(stack, idx)

    stack
    |> List.replace_at(0, y)
    |> List.replace_at(idx, x)
  end
end

defmodule Transactions do
  use Agent

  def start_link() do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  def get(name) do
    Agent.get(__MODULE__, &Map.get(&1, name))
  end

  def put(name, tr) do
    Agent.update(__MODULE__, &Map.put(&1, name, tr))
  end
end

defmodule Machine do
  alias FDBC.Database
  alias FDBC.Directory
  alias FDBC.Error
  alias FDBC.Future
  alias FDBC.KeySelector
  alias FDBC.Subspace
  alias FDBC.Tenant
  alias FDBC.Tuple
  alias FDBC.Transaction

  def start(db, prefix) do
    state = %{
      db: db,
      directories: [
        Directory.new()
      ],
      directories_act_idx: 0,
      directories_err_idx: 0,
      name: prefix,
      read_version: 0,
      processes: [],
      stack: [],
      subspace: Subspace.new([prefix]),
      tenant: nil
    }

    {start, stop} = Subspace.range(state.subspace)

    tr = Transaction.create(state.db)

    state =
      Transaction.stream(tr, start, stop)
      |> Enum.with_index()
      |> Enum.reduce(state, fn {{_k, v}, idx}, state ->
        instruction = Tuple.unpack(v, keyed: true)
        execute(state, idx, instruction)
      end)

    :ok = Transaction.commit(tr)

    for {pid, reference} <- state.processes do
      receive do
        {:DOWN, ^reference, :process, ^pid, :normal} ->
          :ok
      end
    end
  end

  ## Data Operations

  def execute(machine, id, [{_, "PUSH"}, pair]) do
    %{machine | stack: Stack.push(machine.stack, id, pair)}
  end

  def execute(machine, _id, [{_, "DUP"}]) do
    {id, pair} = Stack.peek(machine.stack, indexed: true)
    %{machine | stack: Stack.push(machine.stack, id, pair)}
  end

  def execute(machine, _id, [{_, "EMPTY_STACK"}]) do
    %{machine | stack: Stack.new()}
  end

  def execute(machine, _id, [{_, "SWAP"}]) do
    {{_, idx}, stack} = Stack.pop(machine.stack)
    %{machine | stack: Stack.swap(stack, idx)}
  end

  def execute(machine, _id, [{_, "POP"}]) do
    {_, stack} = Stack.pop(machine.stack)
    %{machine | stack: stack}
  end

  def execute(machine, id, [{_, "SUB"}]) do
    {{t, a}, stack} = Stack.pop(machine.stack)
    {{^t, b}, stack} = Stack.pop(stack)
    %{machine | stack: Stack.push(stack, id, {t, a - b})}
  end

  def execute(machine, id, [{_, "CONCAT"}]) do
    {{t, a}, stack} = Stack.pop(machine.stack)
    {{^t, b}, stack} = Stack.pop(stack)
    %{machine | stack: Stack.push(stack, id, {t, a <> b})}
  end

  def execute(machine, _id, [{_, "LOG_STACK"}]) do
    {{_, prefix}, stack} = Stack.pop(machine.stack)
    subspace = Subspace.new(prefix)

    Enum.reverse(stack)
    |> Enum.map(fn item ->
      {x, _} = Stack.pop([item], indexed: true)
      x
    end)
    |> Enum.with_index()
    |> Enum.chunk_every(100)
    |> Enum.each(fn chunk ->
      FDBC.transact(machine.db, fn tr ->
        Enum.each(chunk, fn {{id, pair}, idx} ->
          key = Subspace.pack(subspace, [idx, id])

          val =
            case Tuple.pack([pair]) do
              i when byte_size(i) > 40_000 -> binary_part(i, 0, 40_000)
              i -> i
            end

          Transaction.set(tr, key, val)
        end)
      end)
    end)

    %{machine | stack: []}
  end

  ## FoundationDB Operations

  def execute(machine, _id, [{_, "NEW_TRANSACTION"}]) do
    tr = Transaction.create(machine.tenant || machine.db)
    Transactions.put(machine.name, tr)
    machine
  end

  def execute(machine, _id, [{_, "USE_TRANSACTION"}]) do
    {{_, name}, stack} = Stack.pop(machine.stack)

    unless Transactions.get(name) do
      tr = Transaction.create(machine.db)
      Transactions.put(name, tr)
    end

    %{machine | name: name, stack: stack}
  end

  def execute(machine, id, [{_, "ON_ERROR"}]) do
    {{_, code}, stack} = Stack.pop(machine.stack)

    catch_error(machine, id, stack, fn ->
      tr = Transactions.get(machine.name)
      :ok = Transaction.on_error(tr, %Error{code: code})
      %{machine | stack: Stack.push(stack, id, {:binary, "RESULT_NOT_PRESENT"})}
    end)
  end

  def execute(machine, id, [{_, "GET"}]), do: do_get(machine, id)
  def execute(machine, id, [{_, "GET_DATABASE"}]), do: do_get(machine, id, :database)
  def execute(machine, id, [{_, "GET_SNAPSHOT"}]), do: do_get(machine, id, :snapshot)
  def execute(machine, id, [{_, "GET_TENANT"}]), do: do_get(machine, id, :tenant)

  def execute(machine, id, [{_, "GET_ESTIMATED_RANGE_SIZE"}]) do
    {pairs, stack} = Stack.pop_many(machine.stack, 2)
    [start, stop] = Keyword.values(pairs)

    catch_error(machine, id, stack, fn ->
      tr = Transactions.get(machine.name)
      _ = Transaction.get_estimated_range_size(tr, start, stop)
      %{machine | stack: Stack.push(stack, id, {:binary, "GOT_ESTIMATED_RANGE_SIZE"})}
    end)
  end

  def execute(machine, id, [{_, "GET_RANGE_SPLIT_POINTS"}]) do
    {pairs, stack} = Stack.pop_many(machine.stack, 3)
    [start, stop, size] = Keyword.values(pairs)

    catch_error(machine, id, stack, fn ->
      tr = Transactions.get(machine.name)
      _ = Transaction.get_range_split_points(tr, start, stop, size)
      %{machine | stack: Stack.push(stack, id, {:binary, "GOT_RANGE_SPLIT_POINTS"})}
    end)
  end

  def execute(machine, id, [{_, "GET_KEY"}]), do: do_get_key(machine, id)
  def execute(machine, id, [{_, "GET_KEY_DATABASE"}]), do: do_get_key(machine, id, :database)
  def execute(machine, id, [{_, "GET_KEY_SNAPSHOT"}]), do: do_get_key(machine, id, :snapshot)
  def execute(machine, id, [{_, "GET_KEY_TENANT"}]), do: do_get_key(machine, id, :tenant)

  def execute(machine, id, [{_, "GET_RANGE"}]), do: do_get_range(machine, id)
  def execute(machine, id, [{_, "GET_RANGE_DATABASE"}]), do: do_get_range(machine, id, :database)
  def execute(machine, id, [{_, "GET_RANGE_SNAPSHOT"}]), do: do_get_range(machine, id, :snapshot)
  def execute(machine, id, [{_, "GET_RANGE_TENANT"}]), do: do_get_range(machine, id, :tenant)

  def execute(machine, id, [{_, "GET_RANGE_STARTS_WITH"}]), do: do_get_starts_with(machine, id)

  def execute(machine, id, [{_, "GET_RANGE_STARTS_WITH_DATABASE"}]),
    do: do_get_starts_with(machine, id, :database)

  def execute(machine, id, [{_, "GET_RANGE_STARTS_WITH_SNAPSHOT"}]),
    do: do_get_starts_with(machine, id, :snapshot)

  def execute(machine, id, [{_, "GET_RANGE_STARTS_WITH_TENANT"}]),
    do: do_get_starts_with(machine, id, :tenant)

  def execute(machine, id, [{_, "GET_RANGE_SELECTOR"}]), do: do_get_range_selector(machine, id)

  def execute(machine, id, [{_, "GET_RANGE_SELECTOR_DATABASE"}]),
    do: do_get_range_selector(machine, id, :database)

  def execute(machine, id, [{_, "GET_RANGE_SELECTOR_SNAPSHOT"}]),
    do: do_get_range_selector(machine, id, :snapshot)

  def execute(machine, id, [{_, "GET_RANGE_SELECTOR_TENANT"}]),
    do: do_get_range_selector(machine, id, :tenant)

  def execute(machine, id, [{_, "GET_READ_VERSION"}]) do
    catch_error(machine, id, machine.stack, fn ->
      tr = Transactions.get(machine.name)
      version = Transaction.get_read_version(tr)

      %{
        machine
        | stack: Stack.push(machine.stack, id, {:binary, "GOT_READ_VERSION"}),
          read_version: version
      }
    end)
  end

  def execute(machine, id, [{_, "GET_READ_VERSION_SNAPSHOT"}]) do
    # There is no snapshot variant...
    execute(machine, id, [{:binary, "GET_READ_VERSION"}])
  end

  def execute(machine, id, [{_, "GET_VERSIONSTAMP"}]) do
    catch_error(machine, id, machine.stack, fn ->
      tr = Transactions.get(machine.name)
      future = Transaction.async_get_versionstamp(tr)
      %{machine | stack: Stack.push(machine.stack, id, future)}
    end)
  end

  def execute(machine, id, [{_, "SET"}]), do: do_set(machine, id)
  def execute(machine, id, [{_, "SET_DATABASE"}]), do: do_set(machine, id, :database)
  def execute(machine, id, [{_, "SET_TENANT"}]), do: do_set(machine, id, :tenant)

  def execute(machine, id, [{_, "SET_READ_VERSION"}]) do
    catch_error(machine, id, machine.stack, fn ->
      tr = Transactions.get(machine.name)
      :ok = Transaction.set_read_version(tr, machine.read_version)
      machine
    end)
  end

  def execute(machine, id, [{_, "CLEAR"}]), do: do_clear(machine, id)
  def execute(machine, id, [{_, "CLEAR_DATABASE"}]), do: do_clear(machine, id, :database)
  def execute(machine, id, [{_, "CLEAR_TENANT"}]), do: do_clear(machine, id, :tenant)

  def execute(machine, id, [{_, "CLEAR_RANGE"}]), do: do_clear_range(machine, id)

  def execute(machine, id, [{_, "CLEAR_RANGE_DATABASE"}]),
    do: do_clear_range(machine, id, :database)

  def execute(machine, id, [{_, "CLEAR_RANGE_TENANT"}]), do: do_clear_range(machine, id, :tenant)

  def execute(machine, id, [{_, "CLEAR_RANGE_STARTS_WITH"}]),
    do: do_clear_starts_with(machine, id)

  def execute(machine, id, [{_, "CLEAR_RANGE_STARTS_WITH_DATABASE"}]),
    do: do_clear_starts_with(machine, id, :database)

  def execute(machine, id, [{_, "CLEAR_RANGE_STARTS_WITH_TENANT"}]),
    do: do_clear_starts_with(machine, id, :tenant)

  def execute(machine, id, [{_, "ATOMIC_OP"}]), do: do_atomic_op(machine, id)
  def execute(machine, id, [{_, "ATOMIC_OP_DATABASE"}]), do: do_atomic_op(machine, id, :database)
  def execute(machine, id, [{_, "ATOMIC_OP_TENANT"}]), do: do_atomic_op(machine, id, :tenant)

  def execute(machine, id, [{_, "READ_CONFLICT_RANGE"}]) do
    {pairs, stack} = Stack.pop_many(machine.stack, 2)
    [start, stop] = Keyword.values(pairs)

    catch_error(machine, id, stack, fn ->
      tr = Transactions.get(machine.name)
      :ok = Transaction.add_conflict_range(tr, start, stop, :read)
      %{machine | stack: Stack.push(stack, id, {:binary, "SET_CONFLICT_RANGE"})}
    end)
  end

  def execute(machine, id, [{_, "WRITE_CONFLICT_RANGE"}]) do
    {pairs, stack} = Stack.pop_many(machine.stack, 2)
    [start, stop] = Keyword.values(pairs)

    catch_error(machine, id, stack, fn ->
      tr = Transactions.get(machine.name)
      :ok = Transaction.add_conflict_range(tr, start, stop, :write)
      %{machine | stack: Stack.push(stack, id, {:binary, "SET_CONFLICT_RANGE"})}
    end)
  end

  def execute(machine, id, [{_, "READ_CONFLICT_KEY"}]) do
    {{_, key}, stack} = Stack.pop(machine.stack)

    catch_error(machine, id, stack, fn ->
      tr = Transactions.get(machine.name)
      :ok = Transaction.add_conflict_key(tr, key, :read)
      %{machine | stack: Stack.push(stack, id, {:binary, "SET_CONFLICT_KEY"})}
    end)
  end

  def execute(machine, id, [{_, "WRITE_CONFLICT_KEY"}]) do
    {{_, key}, stack} = Stack.pop(machine.stack)

    catch_error(machine, id, stack, fn ->
      tr = Transactions.get(machine.name)
      :ok = Transaction.add_conflict_key(tr, key, :write)
      %{machine | stack: Stack.push(stack, id, {:binary, "SET_CONFLICT_KEY"})}
    end)
  end

  def execute(machine, id, [{_, "DISABLE_WRITE_CONFLICT"}]) do
    catch_error(machine, id, machine.stack, fn ->
      tr = Transactions.get(machine.name)
      Transaction.change(tr, next_write_no_write_conflict_range: true)
      machine
    end)
  end

  def execute(machine, id, [{_, "COMMIT"}]) do
    catch_error(machine, id, machine.stack, fn ->
      tr = Transactions.get(machine.name)
      :ok = Transaction.commit(tr)
      %{machine | stack: Stack.push(machine.stack, id, {:binary, "RESULT_NOT_PRESENT"})}
    end)
  end

  def execute(machine, _id, [{_, "RESET"}]) do
    tr = Transactions.get(machine.name)
    :ok = Transaction.reset(tr)
    machine
  end

  def execute(machine, _id, [{_, "CANCEL"}]) do
    tr = Transactions.get(machine.name)
    :ok = Transaction.cancel(tr)
    machine
  end

  def execute(machine, id, [{_, "GET_COMMITTED_VERSION"}]) do
    catch_error(machine, id, machine.stack, fn ->
      tr = Transactions.get(machine.name)
      version = Transaction.get_committed_version(tr)

      %{
        machine
        | read_version: version,
          stack: Stack.push(machine.stack, id, {:binary, "GOT_COMMITTED_VERSION"})
      }
    end)
  end

  def execute(machine, id, [{_, "GET_APPROXIMATE_SIZE"}]) do
    catch_error(machine, id, machine.stack, fn ->
      tr = Transactions.get(machine.name)
      _ = Transaction.get_approximate_size(tr)
      %{machine | stack: Stack.push(machine.stack, id, {:binary, "GOT_APPROXIMATE_SIZE"})}
    end)
  end

  def execute(machine, _id, [{_, "WAIT_FUTURE"}]) do
    # Future resolution is handled in pop
    {{id, pair}, stack} = Stack.pop(machine.stack, indexed: true)
    %{machine | stack: Stack.push(stack, id, pair)}
  end

  ## Tuple Operations

  def execute(machine, id, [{_, "TUPLE_PACK"}]) do
    {{_, count}, stack} = Stack.pop(machine.stack)
    {pairs, stack} = Stack.pop_many(stack, count)
    item = Tuple.pack(pairs, keyed: true)
    %{machine | stack: Stack.push(stack, id, {:binary, item})}
  end

  def execute(machine, id, [{_, "TUPLE_PACK_WITH_VERSIONSTAMP"}]) do
    {pairs, stack} = Stack.pop_many(machine.stack, 2)
    [prefix, count] = Keyword.values(pairs)
    {pairs, stack} = Stack.pop_many(stack, count)

    {item, stack} =
      try do
        item =
          Subspace.new(prefix)
          |> Subspace.pack(pairs, keyed: true, versionstamp: true)

        stack = Stack.push(stack, id, {:binary, "OK"})
        {item, stack}
      rescue
        e in ArgumentError ->
          case e.message do
            "tuple is missing an incomplete versionstamp" ->
              {"ERROR: NONE", stack}

            "tuple contains more than one incomplete versionstamp" ->
              {"ERROR: MULTIPLE", stack}

            _ ->
              reraise e, __STACKTRACE__
          end
      end

    %{machine | stack: Stack.push(stack, id, {:binary, item})}
  end

  def execute(machine, id, [{_, "TUPLE_UNPACK"}]) do
    {{_, item}, stack} = Stack.pop(machine.stack)

    stack =
      Tuple.unpack(item, keyed: true)
      |> Enum.reduce(stack, fn data, stack ->
        item = Tuple.pack([data], keyed: true)
        Stack.push(stack, id, {:binary, item})
      end)

    %{machine | stack: stack}
  end

  def execute(machine, id, [{_, "TUPLE_RANGE"}]) do
    {{_, count}, stack} = Stack.pop(machine.stack)
    {pairs, stack} = Stack.pop_many(stack, count)
    {start, stop} = Tuple.range(pairs, keyed: true)
    stack = Stack.push(stack, id, {:binary, start})
    %{machine | stack: Stack.push(stack, id, {:binary, stop})}
  end

  def execute(machine, id, [{_, "TUPLE_SORT"}]) do
    {{_, count}, stack} = Stack.pop(machine.stack)
    {pairs, stack} = Stack.pop_many(stack, count)

    stack =
      Keyword.values(pairs)
      |> Enum.sort()
      |> Enum.reduce(stack, fn item, stack ->
        Stack.push(stack, id, {:binary, item})
      end)

    %{machine | stack: stack}
  end

  def execute(machine, id, [{_, "ENCODE_FLOAT"}]) do
    {{_, item}, stack} = Stack.pop(machine.stack)

    item =
      case item do
        <<0::1, 0xFF::8, 0::23>> -> :infinity
        <<1::1, 0xFF::8, 0::23>> -> :neg_infinity
        <<0::1, 0xFF::8, _::23>> -> :nan
        <<1::1, 0xFF::8, _::23>> -> :neg_nan
        <<item::float-big-32>> -> item
      end

    %{machine | stack: Stack.push(stack, id, {:float, item})}
  end

  def execute(machine, id, [{_, "ENCODE_DOUBLE"}]) do
    {{_, item}, stack} = Stack.pop(machine.stack)

    item =
      case item do
        <<0::1, 0x7FF::11, 0::52>> -> :infinity
        <<1::1, 0x7FF::11, 0::52>> -> :neg_infinity
        <<0::1, 0x7FF::11, _::52>> -> :nan
        <<1::1, 0x7FF::11, _::52>> -> :neg_nan
        <<item::float-big-64>> -> item
      end

    %{machine | stack: Stack.push(stack, id, {:double, item})}
  end

  def execute(machine, id, [{_, "DECODE_FLOAT"}]) do
    {{_, item}, stack} = Stack.pop(machine.stack)

    item =
      case item do
        :infinity -> <<0::1, 0xFF::8, 0::1, 0::22>>
        :neg_infinity -> <<1::1, 0xFF::8, 0::1, 0::22>>
        :nan -> <<0::1, 0xFF::8, 1::1, 0::22>>
        :neg_nan -> <<1::1, 0xFF::8, 1::1, 0::22>>
        _ -> <<item::float-big-32>>
      end

    %{machine | stack: Stack.push(stack, id, {:binary, item})}
  end

  def execute(machine, id, [{_, "DECODE_DOUBLE"}]) do
    {{_, item}, stack} = Stack.pop(machine.stack)

    item =
      case item do
        :infinity -> <<0::1, 0x7FF::11, 0::1, 0::51>>
        :neg_infinity -> <<1::1, 0x7FF::11, 0::1, 0::51>>
        :nan -> <<0::1, 0x7FF::11, 1::1, 0::51>>
        :neg_nan -> <<1::1, 0x7FF::11, 1::1, 0::51>>
        _ -> <<item::float-big-64>>
      end

    %{machine | stack: Stack.push(stack, id, {:binary, item})}
  end

  ## Thread Operations

  def execute(machine, _id, [{_, "START_THREAD"}]) do
    {{_, prefix}, stack} = Stack.pop(machine.stack)
    result = spawn_monitor(fn -> Machine.start(machine.db, prefix) end)
    %{machine | processes: [result | machine.processes], stack: stack}
  end

  def execute(machine, id, [{_, "WAIT_EMPTY"}]) do
    {{_, prefix}, stack} = Stack.pop(machine.stack)

    FDBC.transact(machine.db, fn tr ->
      unless Transaction.get_starts_with(tr, prefix) == [] do
        raise Error,
          code: 1020,
          message: "Transaction not committed due to conflict with another transaction"
      end
    end)

    %{machine | stack: Stack.push(stack, id, {:binary, "WAITED_FOR_EMPTY"})}
  end

  ## Miscellaneous

  def execute(machine, _id, [{_, "UNIT_TESTS"}]) do
    # Ignored as these are achieved with `mix test`
    machine
  end

  ## Tenant Operations

  def execute(machine, id, [{_, "TENANT_CREATE"}]) do
    {{_, name}, stack} = Stack.pop(machine.stack)

    catch_error(machine, id, stack, fn ->
      :ok = Tenant.create(machine.db, name)
      %{machine | stack: Stack.push(stack, id, {:binary, "RESULT_NOT_PRESENT"})}
    end)
  end

  def execute(machine, id, [{_, "TENANT_DELETE"}]) do
    {{_, name}, stack} = Stack.pop(machine.stack)

    catch_error(machine, id, stack, fn ->
      :ok = Tenant.delete(machine.db, name)
      %{machine | stack: Stack.push(stack, id, {:binary, "RESULT_NOT_PRESENT"})}
    end)
  end

  def execute(machine, id, [{_, "TENANT_SET_ACTIVE"}]) do
    {{_, name}, stack} = Stack.pop(machine.stack)

    tenant = Tenant.open(machine.db, name)
    machine = %{machine | tenant: tenant}

    catch_error(machine, id, stack, fn ->
      _id = Tenant.get_id(tenant)
      %{machine | stack: Stack.push(stack, id, {:binary, "SET_ACTIVE_TENANT"})}
    end)
  end

  def execute(machine, _id, [{_, "TENANT_CLEAR_ACTIVE"}]) do
    %{machine | tenant: nil}
  end

  def execute(machine, id, [{_, "TENANT_LIST"}]) do
    {pairs, stack} = Stack.pop_many(machine.stack, 3)
    [start, stop, limit] = Keyword.values(pairs)
    limit = clamp_limit(limit)

    catch_error(machine, id, stack, fn ->
      value =
        FDBC.transact(machine.db, fn tr ->
          Tenant.list(tr, start, stop, limit: limit)
          |> Enum.map(fn {k, _} -> k end)
          |> Tuple.pack()
        end)

      %{machine | stack: Stack.push(stack, id, {:binary, value})}
    end)
  end

  def execute(machine, id, [{_, "TENANT_GET_ID"}]) do
    catch_error(machine, id, machine.stack, fn ->
      item =
        case machine.tenant do
          nil ->
            "NO_ACTIVE_TENANT"

          tenant ->
            _id = Tenant.get_id(tenant)
            "GOT_TENANT_ID"
        end

      %{machine | stack: Stack.push(machine.stack, id, {:binary, item})}
    end)
  end

  ## Directory Operations

  def execute(machine, _id, [{_, "DIRECTORY_CREATE_SUBSPACE"}]) do
    {{_, count}, stack} = Stack.pop(machine.stack)
    {path, stack} = Stack.pop_many(stack, count)
    {{_, raw_prefix}, stack} = Stack.pop(stack)

    path =
      case path do
        path when is_list(path) -> path
        _ -> [path]
      end

    subspace = Subspace.new(raw_prefix) |> Subspace.concat(path)
    %{machine | directories: machine.directories ++ [subspace], stack: stack}
  end

  def execute(machine, _id, [{_, "DIRECTORY_CREATE_LAYER"}]) do
    {pairs, stack} = Stack.pop_many(machine.stack, 3)
    [index1, index2, allow_manual_prefixes] = Keyword.values(pairs)
    content = Enum.at(machine.directories, index2)
    metadata = Enum.at(machine.directories, index1)

    if content == nil || metadata == nil do
      %{machine | directories: machine.directories ++ [nil]}
    else
      directory =
        Directory.new(
          allow_manual_prefixes: allow_manual_prefixes == 1,
          partition: %{
            content: content,
            metadata: metadata
          }
        )

      %{machine | directories: machine.directories ++ [directory], stack: stack}
    end
  end

  def execute(machine, id, [{_, "DIRECTORY_CREATE_OR_OPEN"}]),
    do: do_directory_open(machine, id, nil, true)

  def execute(machine, id, [{_, "DIRECTORY_CREATE_OR_OPEN_DATABASE"}]),
    do: do_directory_open(machine, id, :database, true)

  def execute(machine, id, [{_, "DIRECTORY_CREATE"}]), do: do_directory_create(machine, id)

  def execute(machine, id, [{_, "DIRECTORY_CREATE_DATABASE"}]),
    do: do_directory_create(machine, id, :database)

  def execute(machine, id, [{_, "DIRECTORY_OPEN"}]), do: do_directory_open(machine, id)

  def execute(machine, id, [{_, "DIRECTORY_OPEN_DATABASE"}]),
    do: do_directory_open(machine, id, :database)

  def execute(machine, id, [{_, "DIRECTORY_OPEN_SNAPSHOT"}]),
    do: do_directory_open(machine, id, :snapshot)

  def execute(machine, _id, [{_, "DIRECTORY_CHANGE"}]) do
    {{_, index}, stack} = Stack.pop(machine.stack)

    if Enum.at(machine.directories, index) do
      %{machine | directories_act_idx: index, stack: stack}
    else
      %{machine | directories_act_idx: machine.directories_err_idx, stack: stack}
    end
  end

  def execute(machine, _id, [{_, "DIRECTORY_SET_ERROR_INDEX"}]) do
    {{_, index}, stack} = Stack.pop(machine.stack)
    %{machine | directories_err_idx: index, stack: stack}
  end

  def execute(machine, id, [{_, "DIRECTORY_MOVE"}]), do: do_directory_move(machine, id)

  def execute(machine, id, [{_, "DIRECTORY_MOVE_DATABASE"}]),
    do: do_directory_move(machine, id, :database)

  def execute(machine, id, [{_, "DIRECTORY_MOVE_TO"}]), do: do_directory_move_to(machine, id)

  def execute(machine, id, [{_, "DIRECTORY_MOVE_TO_DATABASE"}]),
    do: do_directory_move_to(machine, id, :database)

  def execute(machine, id, [{_, "DIRECTORY_REMOVE"}]), do: do_directory_remove(machine, id)

  def execute(machine, id, [{_, "DIRECTORY_REMOVE_DATABASE"}]),
    do: do_directory_remove(machine, id, :database)

  def execute(machine, id, [{_, "DIRECTORY_REMOVE_IF_EXISTS"}]),
    do: do_directory_remove(machine, id, nil, true)

  def execute(machine, id, [{_, "DIRECTORY_REMOVE_IF_EXISTS_DATABASE"}]),
    do: do_directory_remove(machine, id, :database, true)

  def execute(machine, id, [{_, "DIRECTORY_LIST"}]), do: do_directory_list(machine, id)

  def execute(machine, id, [{_, "DIRECTORY_LIST_DATABASE"}]),
    do: do_directory_list(machine, id, :database)

  def execute(machine, id, [{_, "DIRECTORY_LIST_SNAPSHOT"}]),
    do: do_directory_list(machine, id, :snapshot)

  def execute(machine, id, [{_, "DIRECTORY_EXISTS"}]), do: do_directory_exists(machine, id)

  def execute(machine, id, [{_, "DIRECTORY_EXISTS_DATABASE"}]),
    do: do_directory_exists(machine, id, :database)

  def execute(machine, id, [{_, "DIRECTORY_EXISTS_SNAPSHOT"}]),
    do: do_directory_exists(machine, id, :snapshot)

  def execute(machine, id, [{_, "DIRECTORY_PACK_KEY"}]) do
    {{_, count}, stack} = Stack.pop(machine.stack)
    {tuple, stack} = Stack.pop_many(stack, count)
    directory = Enum.at(machine.directories, machine.directories_act_idx)

    tuple =
      case tuple do
        tuple when is_list(tuple) -> tuple
        _ -> [tuple]
      end

    catch_directory_error(machine, id, stack, fn ->
      key =
        case directory do
          %Directory{} -> Directory.subspace!(directory)
          %Subspace{} -> directory
        end
        |> Subspace.pack(tuple)

      %{machine | stack: Stack.push(stack, id, {:binary, key})}
    end)
  end

  def execute(machine, id, [{_, "DIRECTORY_UNPACK_KEY"}]) do
    {{_, key}, stack} = Stack.pop(machine.stack)
    directory = Enum.at(machine.directories, machine.directories_act_idx)

    catch_directory_error(machine, id, stack, fn ->
      subspace =
        case directory do
          %Directory{} -> Directory.subspace!(directory)
          %Subspace{} -> directory
        end

      stack =
        subspace
        |> Subspace.unpack(key, keyed: true)
        |> Enum.reduce(stack, fn pair, stack ->
          Stack.push(stack, id, pair)
        end)

      %{machine | stack: stack}
    end)
  end

  def execute(machine, id, [{_, "DIRECTORY_RANGE"}]) do
    {{_, count}, stack} = Stack.pop(machine.stack)
    {tuple, stack} = Stack.pop_many(stack, count)
    directory = Enum.at(machine.directories, machine.directories_act_idx)

    catch_directory_error(machine, id, stack, fn ->
      subspace =
        case directory do
          %Directory{} -> Directory.subspace!(directory)
          %Subspace{} -> directory
        end

      {start, stop} = Subspace.range(subspace, tuple)

      %{
        machine
        | stack: Stack.push(stack, id, {:binary, start}) |> Stack.push(id, {:binary, stop})
      }
    end)
  end

  def execute(machine, id, [{_, "DIRECTORY_CONTAINS"}]) do
    {{_, key}, stack} = Stack.pop(machine.stack)
    directory = Enum.at(machine.directories, machine.directories_act_idx)

    catch_directory_error(machine, id, stack, fn ->
      subspace =
        case directory do
          %Directory{} -> Directory.subspace!(directory)
          %Subspace{} -> directory
        end

      result =
        case Subspace.contains?(subspace, key) do
          true -> 1
          false -> 0
        end

      %{machine | stack: Stack.push(stack, id, {:integer, result})}
    end)
  end

  def execute(machine, id, [{_, "DIRECTORY_OPEN_SUBSPACE"}]) do
    {{_, count}, stack} = Stack.pop(machine.stack)
    {tuple, stack} = Stack.pop_many(stack, count)
    directory = Enum.at(machine.directories, machine.directories_act_idx)

    catch_directory_error(
      machine,
      id,
      stack,
      fn ->
        subspace =
          case directory do
            %Directory{} -> Directory.subspace!(directory)
            %Subspace{} -> directory
          end

        subspace = Subspace.concat(subspace, tuple)
        %{machine | directories: machine.directories ++ [subspace], stack: stack}
      end,
      true
    )
  end

  def execute(machine, id, [{_, "DIRECTORY_LOG_SUBSPACE"}]) do
    {{_, prefix}, stack} = Stack.pop(machine.stack)
    directory = Enum.at(machine.directories, machine.directories_act_idx)
    key = prefix <> Tuple.pack([machine.directories_act_idx])

    catch_directory_error(machine, id, stack, fn ->
      value =
        case directory do
          %Directory{} -> Directory.subspace!(directory).key
          %Subspace{} -> directory.key
        end

      tr = Transactions.get(machine.name)
      :ok = Transaction.set(tr, key, value)
      %{machine | stack: stack}
    end)
  end

  def execute(machine, id, [{_, "DIRECTORY_LOG_DIRECTORY"}]) do
    {{_, prefix}, stack} = Stack.pop(machine.stack)
    log_subspace = Subspace.new(prefix) |> Subspace.concat([machine.directories_act_idx])
    directory = Enum.at(machine.directories, machine.directories_act_idx)

    catch_directory_error(machine, id, stack, fn ->
      tr = Transactions.get(machine.name)
      key = Subspace.pack(log_subspace, [{:string, "path"}])
      value = Directory.path(directory) |> Enum.map(fn x -> {:string, x} end) |> Tuple.pack()
      :ok = Transaction.set(tr, key, value)
      key = Subspace.pack(log_subspace, [{:string, "layer"}])
      value = Tuple.pack([{:binary, directory.file_system.label}])
      :ok = Transaction.set(tr, key, value)
      key = Subspace.pack(log_subspace, [{:string, "exists"}])

      value =
        case Directory.exists?(directory, tr) do
          true -> 1
          false -> 0
        end

      :ok = Transaction.set(tr, key, Tuple.pack([value]))
      key = Subspace.pack(log_subspace, [{:string, "children"}])

      value =
        case value do
          1 -> Directory.list!(directory, tr) |> Enum.map(fn x -> {:string, x} end)
          0 -> []
        end

      :ok = Transaction.set(tr, key, Tuple.pack(value))
      %{machine | stack: stack}
    end)
  end

  def execute(machine, id, [{_, "DIRECTORY_STRIP_PREFIX"}]) do
    {{_, binary}, stack} = Stack.pop(machine.stack)
    directory = Enum.at(machine.directories, machine.directories_act_idx)

    catch_directory_error(machine, id, stack, fn ->
      key =
        case directory do
          %Directory{} -> Directory.subspace!(directory).key
          %Subspace{} -> directory.key
        end

      value =
        case binary do
          <<^key::binary, rest::binary>> -> rest
          _ -> raise ArgumentError, "binary does not start with directory key as prefix"
        end

      %{machine | stack: Stack.push(stack, id, {:binary, value})}
    end)
  end

  ## Helpers

  def do_atomic_op(machine, id, kind \\ nil) do
    {pairs, stack} = Stack.pop_many(machine.stack, 3)
    [op, key, value] = Keyword.values(pairs)
    op = op |> String.downcase() |> String.to_atom()

    catch_error(machine, id, stack, fn ->
      {fun, _} = transactor(machine, kind)
      :ok = fun.(&Transaction.atomic_op(&1, op, key, value))

      if kind do
        %{machine | stack: Stack.push(stack, id, {:binary, "RESULT_NOT_PRESENT"})}
      else
        %{machine | stack: stack}
      end
    end)
  end

  def do_clear(machine, id, kind \\ nil) do
    {{_, key}, stack} = Stack.pop(machine.stack)

    catch_error(machine, id, stack, fn ->
      {fun, _} = transactor(machine, kind)
      :ok = fun.(&Transaction.clear(&1, key))

      if kind do
        %{machine | stack: Stack.push(stack, id, {:binary, "RESULT_NOT_PRESENT"})}
      else
        %{machine | stack: stack}
      end
    end)
  end

  def do_clear_range(machine, id, kind \\ nil) do
    {pairs, stack} = Stack.pop_many(machine.stack, 2)
    [start, stop] = Keyword.values(pairs)

    catch_error(machine, id, stack, fn ->
      {fun, _} = transactor(machine, kind)
      :ok = fun.(&Transaction.clear_range(&1, start, stop))

      if kind do
        %{machine | stack: Stack.push(stack, id, {:binary, "RESULT_NOT_PRESENT"})}
      else
        %{machine | stack: stack}
      end
    end)
  end

  def do_clear_starts_with(machine, id, kind \\ nil) do
    {{_, prefix}, stack} = Stack.pop(machine.stack)

    catch_error(machine, id, stack, fn ->
      {fun, _} = transactor(machine, kind)
      :ok = fun.(&Transaction.clear_starts_with(&1, prefix))

      if kind do
        %{machine | stack: Stack.push(stack, id, {:binary, "RESULT_NOT_PRESENT"})}
      else
        %{machine | stack: stack}
      end
    end)
  end

  def do_directory_create(machine, id, kind \\ nil) do
    {{_, count}, stack} = Stack.pop(machine.stack)
    {path, stack} = Stack.pop_many(stack, count)
    {pairs, stack} = Stack.pop_many(stack, 2)
    [layer, prefix] = Keyword.values(pairs)

    {label, partition} =
      case layer do
        "partition" -> {nil, true}
        x -> {x, false}
      end

    directory = Enum.at(machine.directories, machine.directories_act_idx)

    db_or_tr =
      case kind do
        nil ->
          Transactions.get(machine.name)

        :database ->
          machine.db
      end

    catch_directory_error(
      machine,
      id,
      stack,
      fn ->
        :ok =
          Directory.create!(directory, db_or_tr, path,
            label: label,
            parents: true,
            partition: partition,
            prefix: prefix
          )

        dir = Directory.open!(directory, db_or_tr, path)

        %{machine | directories: machine.directories ++ [dir], stack: stack}
      end,
      true
    )
  end

  def do_directory_exists(machine, id, kind \\ nil) do
    {{_, count}, stack} = Stack.pop(machine.stack)

    db_or_tr =
      case kind do
        nil ->
          Transactions.get(machine.name)

        :database ->
          machine.db

        :snapshot ->
          Transactions.get(machine.name) |> Transaction.snapshot()
      end

    directory = Enum.at(machine.directories, machine.directories_act_idx)

    case count do
      0 ->
        catch_directory_error(machine, id, stack, fn ->
          item =
            case Directory.exists?(directory, db_or_tr) do
              false -> 0
              true -> 1
            end

          %{machine | stack: Stack.push(stack, id, {:integer, item})}
        end)

      1 ->
        {{_, count}, stack} = Stack.pop(stack)
        {path, stack} = Stack.pop_many(stack, count)

        catch_directory_error(machine, id, stack, fn ->
          item =
            case Directory.exists?(directory, db_or_tr, path) do
              false -> 0
              true -> 1
            end

          %{machine | stack: Stack.push(stack, id, {:integer, item})}
        end)
    end
  end

  def do_directory_list(machine, id, kind \\ nil) do
    {{_, count}, stack} = Stack.pop(machine.stack)

    db_or_tr =
      case kind do
        nil ->
          Transactions.get(machine.name)

        :database ->
          machine.db

        :snapshot ->
          Transactions.get(machine.name) |> Transaction.snapshot()
      end

    directory = Enum.at(machine.directories, machine.directories_act_idx)

    case count do
      0 ->
        catch_directory_error(machine, id, stack, fn ->
          item = Directory.list!(directory, db_or_tr) |> Enum.map(&{:string, &1}) |> Tuple.pack()
          %{machine | stack: Stack.push(stack, id, {:binary, item})}
        end)

      1 ->
        {{_, count}, stack} = Stack.pop(stack)
        {path, stack} = Stack.pop_many(stack, count)

        catch_directory_error(machine, id, stack, fn ->
          item =
            Directory.list!(directory, db_or_tr, path) |> Enum.map(&{:string, &1}) |> Tuple.pack()

          %{machine | stack: Stack.push(stack, id, {:binary, item})}
        end)
    end
  end

  def do_directory_move(machine, id, kind \\ nil) do
    {{_, count}, stack} = Stack.pop(machine.stack)
    {old_path, stack} = Stack.pop_many(stack, count)
    {{_, count}, stack} = Stack.pop(stack)
    {new_path, stack} = Stack.pop_many(stack, count)
    directory = Enum.at(machine.directories, machine.directories_act_idx)

    db_or_tr =
      case kind do
        nil ->
          Transactions.get(machine.name)

        :database ->
          machine.db
      end

    catch_directory_error(
      machine,
      id,
      stack,
      fn ->
        :ok = Directory.move!(directory, db_or_tr, old_path, new_path)
        {:ok, dir} = Directory.open(directory, db_or_tr, new_path)
        %{machine | directories: machine.directories ++ [dir], stack: stack}
      end,
      true
    )
  end

  def do_directory_move_to(machine, id, kind \\ nil) do
    {{_, count}, stack} = Stack.pop(machine.stack)
    {new_path, stack} = Stack.pop_many(stack, count)
    directory = Enum.at(machine.directories, machine.directories_act_idx)

    db_or_tr =
      case kind do
        nil ->
          Transactions.get(machine.name)

        :database ->
          machine.db
      end

    catch_directory_error(
      machine,
      id,
      stack,
      fn ->
        root = Directory.new(partition: directory.file_system.partition)
        path = Directory.path(directory)
        :ok = Directory.move!(root, db_or_tr, path, new_path)
        {:ok, dir} = Directory.open(root, db_or_tr, new_path)
        %{machine | directories: machine.directories ++ [dir], stack: stack}
      end,
      true
    )
  end

  def do_directory_open(machine, id, kind \\ nil, create \\ false) do
    {{_, count}, stack} = Stack.pop(machine.stack)
    {path, stack} = Stack.pop_many(stack, count)
    {{_, layer}, stack} = Stack.pop(stack)

    {label, partition} =
      case layer do
        "partition" -> {"partition", true}
        x -> {x, false}
      end

    directory = Enum.at(machine.directories, machine.directories_act_idx)

    db_or_tr =
      case kind do
        nil ->
          Transactions.get(machine.name)

        :database ->
          machine.db

        :snapshot ->
          Transactions.get(machine.name) |> Transaction.snapshot()
      end

    catch_directory_error(
      machine,
      id,
      stack,
      fn ->
        dir =
          Directory.open!(directory, db_or_tr, path,
            create: create,
            label: label,
            parents: true,
            partition: partition
          )

        if label != dir.file_system.label do
          raise ArgumentError, "provided label does not match that of the directory"
        end

        %{machine | directories: machine.directories ++ [dir], stack: stack}
      end,
      true
    )
  end

  def do_directory_remove(machine, id, kind \\ nil, exists \\ false) do
    {{_, count}, stack} = Stack.pop(machine.stack)
    directory = Enum.at(machine.directories, machine.directories_act_idx)

    db_or_tr =
      case kind do
        nil ->
          Transactions.get(machine.name)

        :database ->
          machine.db
      end

    case count do
      0 ->
        catch_directory_error(machine, id, stack, fn ->
          root = Directory.new(partition: directory.file_system.partition)
          path = Directory.path(directory)

          if exists == false || Directory.exists?(root, db_or_tr, path) do
            :ok = Directory.remove!(root, db_or_tr, path)
          end

          %{machine | stack: stack}
        end)

      1 ->
        {{_, count}, stack} = Stack.pop(stack)
        {path, stack} = Stack.pop_many(stack, count)

        catch_directory_error(machine, id, stack, fn ->
          case path do
            [] ->
              root = Directory.new(partition: directory.file_system.partition)
              path = Directory.path(directory)

              if exists == false || Directory.exists?(root, db_or_tr, path) do
                :ok = Directory.remove!(root, db_or_tr, path)
              end

            path ->
              if exists == false || Directory.exists?(directory, db_or_tr, path) do
                :ok = Directory.remove!(directory, db_or_tr, path)
              end
          end

          %{machine | stack: stack}
        end)
    end
  end

  defp do_get(machine, id, kind \\ nil) do
    {{_, key}, stack} = Stack.pop(machine.stack)

    catch_error(machine, id, stack, fn ->
      {fun, opts} = transactor(machine, kind)
      result = fun.(&Transaction.get(&1, key, opts))

      item =
        if result do
          {:binary, result}
        else
          {:binary, "RESULT_NOT_PRESENT"}
        end

      %{machine | stack: Stack.push(stack, id, item)}
    end)
  end

  defp do_get_key(machine, id, kind \\ nil) do
    {pairs, stack} = Stack.pop_many(machine.stack, 4)
    [key, or_equal, offset, prefix] = Keyword.values(pairs)

    or_equal = or_equal == 1
    selector = KeySelector.new(key, or_equal, offset)

    catch_error(machine, id, stack, fn ->
      {fun, opts} = transactor(machine, kind)
      result = fun.(&Transaction.get_key(&1, selector, opts))

      item =
        cond do
          String.starts_with?(result, prefix) -> result
          result < prefix -> prefix
          true -> Transaction.increment_key(prefix)
        end

      %{machine | stack: Stack.push(stack, id, {:binary, item})}
    end)
  end

  defp do_get_range(machine, id, kind \\ nil) do
    {pairs, stack} = Stack.pop_many(machine.stack, 5)
    [start, stop, limit, reverse, mode] = Keyword.values(pairs)

    limit = clamp_limit(limit)
    mode = range_int_to_mode(mode)
    reverse = reverse == 1

    catch_error(machine, id, stack, fn ->
      {fun, opts} = transactor(machine, kind)

      value =
        fun.(
          &Transaction.get_range(
            &1,
            start,
            stop,
            opts ++ [limit: limit, mode: mode, reverse: reverse]
          )
        )

      value =
        value
        |> Enum.flat_map(fn {k, v} -> [k, v] end)
        |> Tuple.pack()

      %{machine | stack: Stack.push(stack, id, {:binary, value})}
    end)
  end

  defp do_get_range_selector(machine, id, kind \\ nil) do
    {pairs, stack} = Stack.pop_many(machine.stack, 10)

    [
      start_key,
      start_or_equal,
      start_offset,
      stop_key,
      stop_or_equal,
      stop_offset,
      limit,
      reverse,
      mode,
      prefix
    ] = Keyword.values(pairs)

    limit = clamp_limit(limit)
    mode = range_int_to_mode(mode)
    reverse = reverse == 1
    start_or_equal = start_or_equal == 1
    stop_or_equal = stop_or_equal == 1

    start = KeySelector.new(start_key, start_or_equal, start_offset)
    stop = KeySelector.new(stop_key, stop_or_equal, stop_offset)

    catch_error(machine, id, stack, fn ->
      {fun, opts} = transactor(machine, kind)

      value =
        fun.(
          &Transaction.get_range(
            &1,
            start,
            stop,
            opts ++ [limit: limit, mode: mode, reverse: reverse]
          )
        )
        |> Enum.filter(fn {k, _v} -> String.starts_with?(k, prefix) end)
        |> Enum.flat_map(fn {k, v} -> [k, v] end)
        |> Tuple.pack()

      %{machine | stack: Stack.push(stack, id, {:binary, value})}
    end)
  end

  defp do_get_starts_with(machine, id, kind \\ nil) do
    {pairs, stack} = Stack.pop_many(machine.stack, 4)
    [prefix, limit, reverse, mode] = Keyword.values(pairs)

    mode = range_int_to_mode(mode)
    reverse = reverse == 1

    catch_error(machine, id, stack, fn ->
      {fun, opts} = transactor(machine, kind)

      value =
        fun.(
          &Transaction.get_starts_with(
            &1,
            prefix,
            opts ++ [limit: limit, mode: mode, reverse: reverse]
          )
        )
        |> Enum.flat_map(fn {k, v} -> [k, v] end)
        |> Tuple.pack()

      %{machine | stack: Stack.push(stack, id, {:binary, value})}
    end)
  end

  defp do_set(machine, id, kind \\ nil) do
    {pairs, stack} = Stack.pop_many(machine.stack, 2)
    [key, value] = Keyword.values(pairs)

    catch_error(machine, id, stack, fn ->
      {fun, _} = transactor(machine, kind)
      :ok = fun.(&Transaction.set(&1, key, value))

      if kind do
        %{machine | stack: Stack.push(stack, id, {:binary, "RESULT_NOT_PRESENT"})}
      else
        %{machine | stack: stack}
      end
    end)
  end

  defp catch_directory_error(machine, id, stack, fun, append \\ false) do
    try do
      fun.()
    rescue
      _ in [ArgumentError, FDBC.Error, RuntimeError] ->
        if append do
          %{
            machine
            | directories: machine.directories ++ [nil],
              stack: Stack.push(stack, id, {:binary, "DIRECTORY_ERROR"})
          }
        else
          %{machine | stack: Stack.push(stack, id, {:binary, "DIRECTORY_ERROR"})}
        end
    end
  end

  defp catch_error(machine, id, stack, fun) do
    try do
      fun.()
    rescue
      e in FDBC.Error ->
        item = Tuple.pack(["ERROR", to_string(e.code)])
        %{machine | stack: Stack.push(stack, id, {:binary, item})}
    end
  end

  defp clamp_limit(limit) do
    # We don't clamp because the Python implementation relies on the overflow
    # being treated as a side effect... and that is the gold standard that
    # output is compared with... eurgh...
    <<truncated::integer-32>> = <<limit::integer-32>>

    cond do
      truncated < -0x7FFFFFFF -> -0x7FFFFFFF
      truncated > 0x7FFFFFFF -> -0x7FFFFFFF
      true -> truncated
    end
  end

  defp range_int_to_mode(value) do
    case value do
      -2 -> :want_all
      -1 -> :iterator
      0 -> :exact
      1 -> :small
      2 -> :medium
      3 -> :large
      4 -> :serial
    end
  end

  defp transactor(machine, kind) do
    case kind do
      nil ->
        {fn action -> action.(Transactions.get(machine.name)) end, []}

      :database ->
        {fn action -> FDBC.transact(machine.db, &action.(&1)) end, []}

      :snapshot ->
        {fn action -> action.(Transactions.get(machine.name)) end, [snapshot: true]}

      :tenant ->
        {fn action -> FDBC.transact(machine.tenant || machine.db, &action.(&1)) end, []}
    end
  end
end

defmodule Tester do
  alias FDBC.Database
  alias FDBC.Subspace

  def run(prefix, cluster) do
    {:ok, _} = Transactions.start_link()
    db = Database.create(cluster)
    Machine.start(db, prefix)
  end
end

[prefix, api_version, cluster] = System.argv()
:ok = FDBC.api_version(String.to_integer(api_version))
:ok = FDBC.Network.start()
Tester.run(prefix, cluster)
:ok = FDBC.Network.stop()
