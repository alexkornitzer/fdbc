# Implements the tester for use by the upstream bindingtester.
#
# https://github.com/apple/foundationdb/blob/main/bindings/bindingtester
#
# This can be run by adding the tester.exs to the upstream `known_testers.py` file:
#
# ```diff
# diff --git a/bindings/bindingtester/known_testers.py b/bindings/bindingtester/known_testers.py
# index e7e5f99ed..808f7d31c 100644
# --- a/bindings/bindingtester/known_testers.py
# +++ b/bindings/bindingtester/known_testers.py
# @@ -130,4 +130,13 @@ testers = {
#          MAX_API_VERSION,
#          directory_snapshot_ops_enabled=False,
#      ),
# +    "elixir": Tester(
# +        "elixir",
# +        "mix test/tester.exs",
# +        2040,
# +        700,
# +        MAX_API_VERSION,
# +        types=ALL_TYPES,
# +        tenants_enabled=True,
# +    ),
#  }
# ```

defmodule Stack do
  alias FDBC.Future
  alias FDBC.Tuple

  def new(), do: []

  def peek([]), do: nil
  def peek([head | _]), do: head

  def pop([]), do: {nil, []}

  def pop([{id, %Future{} = f} | tail]) do
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

    {{id, item}, tail}
  end

  def pop([head | tail]), do: {head, tail}

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
  alias FDBC.Error
  alias FDBC.Future
  alias FDBC.KeySelector
  alias FDBC.Subspace
  alias FDBC.Tuple
  alias FDBC.Transaction

  def start(db, prefix) do
    state = %{
      db: db,
      name: prefix,
      read_version: 0,
      processes: [],
      stack: [],
      subspace: Subspace.new([prefix])
    }

    {start, stop} = Subspace.range(state.subspace)

    tr = Transaction.create(state.db)

    state =
      Transaction.stream(tr, start, stop)
      |> Enum.with_index()
      |> Enum.reduce(state, fn {{_k, v}, idx}, state ->
        instruction = Tuple.unpack(v, keyed: true)

        try do
          execute(state, idx, instruction)
        rescue
          e in FDBC.Error ->
            item = Tuple.pack(["ERROR", to_string(e.code)])
            %{state | stack: Stack.push(state.stack, idx, {:binary, item})}
        end
      end)

    :ok = Transaction.commit(tr)

    for {pid, reference} <- state.processes do
      receive do
        {:DOWN, ^reference, :process, ^pid, reason} ->
          :normal = reason
          :ok
      end
    end
  end

  def execute(machine, id, [{_, "PUSH"}, pair]) do
    %{machine | stack: Stack.push(machine.stack, id, pair)}
  end

  def execute(machine, id, [{_, "DUP"}]) do
    {_, item} = Stack.peek(machine.stack)
    %{machine | stack: Stack.push(machine.stack, id, item)}
  end

  def execute(machine, _id, [{_, "EMPTY_STACK"}]) do
    %{machine | stack: Stack.new()}
  end

  def execute(machine, _id, [{_, "SWAP"}]) do
    {{_, {_, idx}}, stack} = Stack.pop(machine.stack)
    %{machine | stack: Stack.swap(stack, idx)}
  end

  def execute(machine, _id, [{_, "POP"}]) do
    {_, stack} = Stack.pop(machine.stack)
    %{machine | stack: stack}
  end

  def execute(machine, id, [{_, "SUB"}]) do
    {{_, {t, a}}, stack} = Stack.pop(machine.stack)
    {{_, {^t, b}}, stack} = Stack.pop(stack)
    %{machine | stack: Stack.push(stack, id, {t, a - b})}
  end

  def execute(machine, id, [{_, "CONCAT"}]) do
    {{_, a}, stack} = Stack.pop(machine.stack)
    {{_, b}, stack} = Stack.pop(stack)

    # Because one of the above could be a future due to
    # `async_get_versionstamp` we have to check and resolve if so...

    {t, a} =
      case a do
        %Future{} ->
          case Future.resolve(a) do
            nil -> {:binary, "RESULT_NOT_PRESENT"}
            item -> {:binary, item}
          end

        _ ->
          a
      end

    {^t, b} =
      case b do
        %Future{} ->
          case Future.resolve(b) do
            nil -> {:binary, "RESULT_NOT_PRESENT"}
            item -> {:binary, item}
          end

        _ ->
          b
      end

    %{machine | stack: Stack.push(stack, id, {t, a <> b})}
  end

  def execute(machine, _id, [{_, "LOG_STACK"}]) do
    {{_, {_, prefix}}, stack} = Stack.pop(machine.stack)
    subspace = Subspace.new(prefix)

    Enum.reverse(stack)
    |> Enum.map(fn
      {id, item} ->
        {id,
         case(item) do
           %Future{} ->
             case Future.resolve(item) do
               nil -> "RESULT_NOT_PRESENT"
               item -> item
             end

           {_, item} ->
             item
         end}
    end)
    |> Enum.with_index()
    |> Enum.chunk_every(100)
    |> Enum.each(fn chunk ->
      FDBC.transact(machine.db, fn tr ->
        Enum.each(chunk, fn {{id, value}, idx} ->
          key = Subspace.pack(subspace, [idx, id])

          val =
            case Tuple.pack([value]) do
              i when byte_size(i) > 4_000 -> binary_part(i, 0, 4_000)
              i -> i
            end

          Transaction.set(tr, key, val)
        end)
      end)
    end)

    %{machine | stack: []}
  end

  def execute(machine, _id, [{_, "NEW_TRANSACTION"}]) do
    tr = Transaction.create(machine.db)
    Transactions.put(machine.name, tr)
    machine
  end

  def execute(machine, _id, [{_, "USE_TRANSACTION"}]) do
    {{_, {_, name}}, stack} = Stack.pop(machine.stack)

    unless Transactions.get(name) do
      tr = Transaction.create(machine.db)
      Transactions.put(name, tr)
    end

    %{machine | name: name, stack: stack}
  end

  def execute(machine, id, [{_, "ON_ERROR"}]) do
    {{_, {_, code}}, stack} = Stack.pop(machine.stack)
    tr = Transactions.get(machine.name)
    :ok = Transaction.on_error(tr, %Error{code: code})
    %{machine | stack: Stack.push(stack, id, {:binary, "RESULT_NOT_PRESENT"})}
  end

  def execute(machine, id, [{_, "GET"}]) do
    {{_, {_, key}}, stack} = Stack.pop(machine.stack)
    tr = Transactions.get(machine.name)

    item =
      case Transaction.get(tr, key) do
        nil -> {:binary, "RESULT_NOT_PRESENT"}
        item -> {:binary, item}
      end

    %{machine | stack: Stack.push(stack, id, item)}
  end

  def execute(machine, id, [{_, "GET_DATABASE"}]) do
    {{_, {_, key}}, stack} = Stack.pop(machine.stack)

    item =
      case FDBC.transact(machine.db, &Transaction.get(&1, key)) do
        nil -> {:binary, "RESULT_NOT_PRESENT"}
        item -> {:binary, item}
      end

    %{machine | stack: Stack.push(stack, id, item)}
  end

  def execute(machine, id, [{_, "GET_SNAPSHOT"}]) do
    {{_, {_, key}}, stack} = Stack.pop(machine.stack)
    tr = Transactions.get(machine.name)

    item =
      case Transaction.get(tr, key, snapshot: true) do
        nil -> {:binary, "RESULT_NOT_PRESENT"}
        item -> {:binary, item}
      end

    %{machine | stack: Stack.push(stack, id, item)}
  end

  def execute(machine, id, [{_, "GET_ESTIMATED_RANGE_SIZE"}]) do
    {{_, {_, start}}, stack} = Stack.pop(machine.stack)
    {{_, {_, stop}}, stack} = Stack.pop(stack)
    tr = Transactions.get(machine.name)
    _ = Transaction.get_estimated_range_size(tr, start, stop)
    %{machine | stack: Stack.push(stack, id, {:binary, "GOT_ESTIMATED_RANGE_SIZE"})}
  end

  def execute(machine, id, [{_, "GET_RANGE_SPLIT_POINTS"}]) do
    {{_, {_, start}}, stack} = Stack.pop(machine.stack)
    {{_, {_, stop}}, stack} = Stack.pop(stack)
    {{_, {_, size}}, stack} = Stack.pop(stack)
    tr = Transactions.get(machine.name)
    _ = Transaction.get_range_split_points(tr, start, stop, size)
    %{machine | stack: Stack.push(stack, id, {:binary, "GET_RANGE_SPLIT_POINTS"})}
  end

  def execute(machine, id, [{_, "GET_KEY"}]) do
    {{_, {_, key}}, stack} = Stack.pop(machine.stack)
    {{_, {_, or_equal}}, stack} = Stack.pop(stack)
    {{_, {_, offset}}, stack} = Stack.pop(stack)
    {{_, {_, prefix}}, stack} = Stack.pop(stack)

    or_equal = or_equal == 1

    selector = KeySelector.new(key, or_equal, offset)
    tr = Transactions.get(machine.name)
    result = Transaction.get_key(tr, selector)

    result =
      cond do
        String.starts_with?(result, prefix) -> result
        result < prefix -> prefix
        true -> Transaction.increment_key(prefix)
      end

    %{machine | stack: Stack.push(stack, id, {:binary, result})}
  end

  def execute(machine, id, [{_, "GET_KEY_DATABASE"}]) do
    {{_, {_, key}}, stack} = Stack.pop(machine.stack)
    {{_, {_, or_equal}}, stack} = Stack.pop(stack)
    {{_, {_, offset}}, stack} = Stack.pop(stack)
    {{_, {_, prefix}}, stack} = Stack.pop(stack)

    or_equal = or_equal == 1

    selector = KeySelector.new(key, or_equal, offset)
    result = FDBC.transact(machine.db, &Transaction.get_key(&1, selector))

    result =
      cond do
        String.starts_with?(result, prefix) -> result
        result < prefix -> prefix
        true -> Transaction.increment_key(prefix)
      end

    %{machine | stack: Stack.push(stack, id, {:binary, result})}
  end

  def execute(machine, id, [{_, "GET_KEY_SNAPSHOT"}]) do
    {{_, {_, key}}, stack} = Stack.pop(machine.stack)
    {{_, {_, or_equal}}, stack} = Stack.pop(stack)
    {{_, {_, offset}}, stack} = Stack.pop(stack)
    {{_, {_, prefix}}, stack} = Stack.pop(stack)

    or_equal = or_equal == 1

    selector = KeySelector.new(key, or_equal, offset)
    tr = Transactions.get(machine.name)
    result = Transaction.get_key(tr, selector, snapshot: true)

    result =
      cond do
        String.starts_with?(result, prefix) -> result
        result < prefix -> prefix
        true -> Transaction.increment_key(prefix)
      end

    %{machine | stack: Stack.push(stack, id, {:binary, result})}
  end

  def execute(machine, id, [{_, "GET_RANGE"}]) do
    {{_, {_, start}}, stack} = Stack.pop(machine.stack)
    {{_, {_, stop}}, stack} = Stack.pop(stack)
    {{_, {_, limit}}, stack} = Stack.pop(stack)
    {{_, {_, reverse}}, stack} = Stack.pop(stack)
    {{_, {_, mode}}, stack} = Stack.pop(stack)

    mode = int_to_mode(mode)
    reverse = reverse == 1

    tr = Transactions.get(machine.name)

    value =
      Transaction.get_range(tr, start, stop, limit: limit, mode: mode, reverse: reverse)
      |> Enum.flat_map(fn {k, v} -> [k, v] end)
      |> Tuple.pack()

    %{machine | stack: Stack.push(stack, id, {:binary, value})}
  end

  def execute(machine, id, [{_, "GET_RANGE_DATABASE"}]) do
    {{_, {_, start}}, stack} = Stack.pop(machine.stack)
    {{_, {_, stop}}, stack} = Stack.pop(stack)
    {{_, {_, limit}}, stack} = Stack.pop(stack)
    {{_, {_, reverse}}, stack} = Stack.pop(stack)
    {{_, {_, mode}}, stack} = Stack.pop(stack)

    mode = int_to_mode(mode)
    reverse = reverse == 1

    value =
      FDBC.transact(
        machine.db,
        &Transaction.get_range(&1, start, stop, limit: limit, mode: mode, reverse: reverse)
      )
      |> Enum.flat_map(fn {k, v} -> [k, v] end)
      |> Tuple.pack()

    %{machine | stack: Stack.push(stack, id, {:binary, value})}
  end

  def execute(machine, id, [{_, "GET_RANGE_SNAPSHOT"}]) do
    {{_, {_, start}}, stack} = Stack.pop(machine.stack)
    {{_, {_, stop}}, stack} = Stack.pop(stack)
    {{_, {_, limit}}, stack} = Stack.pop(stack)
    {{_, {_, reverse}}, stack} = Stack.pop(stack)
    {{_, {_, mode}}, stack} = Stack.pop(stack)

    mode = int_to_mode(mode)
    reverse = reverse == 1

    tr = Transactions.get(machine.name)

    value =
      Transaction.get_range(tr, start, stop,
        limit: limit,
        mode: mode,
        reverse: reverse,
        snapshot: true
      )
      |> Enum.flat_map(fn {k, v} -> [k, v] end)
      |> Tuple.pack()

    %{machine | stack: Stack.push(stack, id, {:binary, value})}
  end

  def execute(machine, id, [{_, "GET_RANGE_STARTS_WITH"}]) do
    {{_, {_, prefix}}, stack} = Stack.pop(machine.stack)
    {{_, {_, limit}}, stack} = Stack.pop(stack)
    {{_, {_, reverse}}, stack} = Stack.pop(stack)
    {{_, {_, mode}}, stack} = Stack.pop(stack)

    mode = int_to_mode(mode)
    reverse = reverse == 1

    tr = Transactions.get(machine.name)

    value =
      Transaction.get_starts_with(tr, prefix, limit: limit, mode: mode, reverse: reverse)
      |> Enum.flat_map(fn {k, v} -> [k, v] end)
      |> Tuple.pack()

    %{machine | stack: Stack.push(stack, id, {:binary, value})}
  end

  def execute(machine, id, [{_, "GET_RANGE_STARTS_WITH_DATABASE"}]) do
    {{_, {_, prefix}}, stack} = Stack.pop(machine.stack)
    {{_, {_, limit}}, stack} = Stack.pop(stack)
    {{_, {_, reverse}}, stack} = Stack.pop(stack)
    {{_, {_, mode}}, stack} = Stack.pop(stack)

    mode = int_to_mode(mode)
    reverse = reverse == 1

    value =
      FDBC.transact(
        machine.db,
        &Transaction.get_starts_with(&1, prefix, limit: limit, mode: mode, reverse: reverse)
      )
      |> Enum.flat_map(fn {k, v} -> [k, v] end)
      |> Tuple.pack()

    %{machine | stack: Stack.push(stack, id, {:binary, value})}
  end

  def execute(machine, id, [{_, "GET_RANGE_STARTS_WITH_SNAPSHOT"}]) do
    {{_, {_, prefix}}, stack} = Stack.pop(machine.stack)
    {{_, {_, limit}}, stack} = Stack.pop(stack)
    {{_, {_, reverse}}, stack} = Stack.pop(stack)
    {{_, {_, mode}}, stack} = Stack.pop(stack)

    mode = int_to_mode(mode)
    reverse = reverse == 1

    tr = Transactions.get(machine.name)

    value =
      Transaction.get_starts_with(tr, prefix,
        limit: limit,
        mode: mode,
        reverse: reverse,
        snapshot: true
      )
      |> Enum.flat_map(fn {k, v} -> [k, v] end)
      |> Tuple.pack()

    %{machine | stack: Stack.push(stack, id, {:binary, value})}
  end

  def execute(machine, id, [{_, "GET_RANGE_SELECTOR"}]) do
    {{_, {_, start_key}}, stack} = Stack.pop(machine.stack)
    {{_, {_, start_or_equal}}, stack} = Stack.pop(stack)
    {{_, {_, start_offset}}, stack} = Stack.pop(stack)
    {{_, {_, stop_key}}, stack} = Stack.pop(stack)
    {{_, {_, stop_or_equal}}, stack} = Stack.pop(stack)
    {{_, {_, stop_offset}}, stack} = Stack.pop(stack)
    {{_, {_, limit}}, stack} = Stack.pop(stack)
    {{_, {_, reverse}}, stack} = Stack.pop(stack)
    {{_, {_, mode}}, stack} = Stack.pop(stack)
    {{_, {_, prefix}}, stack} = Stack.pop(stack)

    mode = int_to_mode(mode)
    reverse = reverse == 1
    start_or_equal = start_or_equal == 1
    stop_or_equal = stop_or_equal == 1

    start = KeySelector.new(start_key, start_or_equal, start_offset)
    stop = KeySelector.new(stop_key, stop_or_equal, stop_offset)

    tr = Transactions.get(machine.name)

    value =
      Transaction.get_range(tr, start, stop, limit: limit, mode: mode, reverse: reverse)
      |> Enum.filter(fn {k, _v} -> String.starts_with?(k, prefix) end)
      |> Enum.flat_map(fn {k, v} -> [k, v] end)
      |> Tuple.pack()

    %{machine | stack: Stack.push(stack, id, {:binary, value})}
  end

  def execute(machine, id, [{_, "GET_RANGE_SELECTOR_DATABASE"}]) do
    {{_, {_, start_key}}, stack} = Stack.pop(machine.stack)
    {{_, {_, start_or_equal}}, stack} = Stack.pop(stack)
    {{_, {_, start_offset}}, stack} = Stack.pop(stack)
    {{_, {_, stop_key}}, stack} = Stack.pop(stack)
    {{_, {_, stop_or_equal}}, stack} = Stack.pop(stack)
    {{_, {_, stop_offset}}, stack} = Stack.pop(stack)
    {{_, {_, limit}}, stack} = Stack.pop(stack)
    {{_, {_, reverse}}, stack} = Stack.pop(stack)
    {{_, {_, mode}}, stack} = Stack.pop(stack)
    {{_, {_, prefix}}, stack} = Stack.pop(stack)

    mode = int_to_mode(mode)
    reverse = reverse == 1
    start_or_equal = start_or_equal == 1
    stop_or_equal = stop_or_equal == 1

    start = KeySelector.new(start_key, start_or_equal, start_offset)
    stop = KeySelector.new(stop_key, stop_or_equal, stop_offset)

    value =
      FDBC.transact(
        machine.db,
        &Transaction.get_range(&1, start, stop, limit: limit, mode: mode, reverse: reverse)
      )
      |> Enum.filter(fn {k, _v} -> String.starts_with?(k, prefix) end)
      |> Enum.flat_map(fn {k, v} -> [k, v] end)
      |> Tuple.pack()

    %{machine | stack: Stack.push(stack, id, {:binary, value})}
  end

  def execute(machine, id, [{_, "GET_RANGE_SELECTOR_SNAPSHOT"}]) do
    {{_, {_, start_key}}, stack} = Stack.pop(machine.stack)
    {{_, {_, start_or_equal}}, stack} = Stack.pop(stack)
    {{_, {_, start_offset}}, stack} = Stack.pop(stack)
    {{_, {_, stop_key}}, stack} = Stack.pop(stack)
    {{_, {_, stop_or_equal}}, stack} = Stack.pop(stack)
    {{_, {_, stop_offset}}, stack} = Stack.pop(stack)
    {{_, {_, limit}}, stack} = Stack.pop(stack)
    {{_, {_, reverse}}, stack} = Stack.pop(stack)
    {{_, {_, mode}}, stack} = Stack.pop(stack)
    {{_, {_, prefix}}, stack} = Stack.pop(stack)

    mode = int_to_mode(mode)
    reverse = reverse == 1
    start_or_equal = start_or_equal == 1
    stop_or_equal = stop_or_equal == 1

    start = KeySelector.new(start_key, start_or_equal, start_offset)
    stop = KeySelector.new(stop_key, stop_or_equal, stop_offset)

    tr = Transactions.get(machine.name)

    value =
      Transaction.get_range(tr, start, stop,
        limit: limit,
        mode: mode,
        reverse: reverse,
        snapshot: true
      )
      |> Enum.filter(fn {k, _v} -> String.starts_with?(k, prefix) end)
      |> Enum.flat_map(fn {k, v} -> [k, v] end)
      |> Tuple.pack()

    %{machine | stack: Stack.push(stack, id, {:binary, value})}
  end

  def execute(machine, id, [{_, "GET_READ_VERSION"}]) do
    tr = Transactions.get(machine.name)
    version = Transaction.get_read_version(tr)

    %{
      machine
      | stack: Stack.push(machine.stack, id, {:binary, "GOT_READ_VERSION"}),
        read_version: version
    }
  end

  def execute(machine, id, [{_, "GET_READ_VERSION_SNAPSHOT"}]) do
    # There is no snapshot variant...
    execute(machine, id, [{:binary, "GET_READ_VERSION"}])
  end

  def execute(machine, id, [{_, "GET_VERSIONSTAMP"}]) do
    tr = Transactions.get(machine.name)
    future = Transaction.async_get_versionstamp(tr)
    %{machine | stack: Stack.push(machine.stack, id, future)}
  end

  def execute(machine, _id, [{_, "SET"}]) do
    {{_, {_, key}}, stack} = Stack.pop(machine.stack)
    {{_, {_, value}}, stack} = Stack.pop(stack)
    tr = Transactions.get(machine.name)
    :ok = Transaction.set(tr, key, value)
    %{machine | stack: stack}
  end

  def execute(machine, id, [{_, "SET_DATABASE"}]) do
    {{_, {_, key}}, stack} = Stack.pop(machine.stack)
    {{_, {_, value}}, stack} = Stack.pop(stack)
    :ok = FDBC.transact(machine.db, &Transaction.set(&1, key, value))
    %{machine | stack: Stack.push(stack, id, {:binary, "RESULT_NOT_PRESENT"})}
  end

  def execute(machine, _id, [{_, "SET_READ_VERSION"}]) do
    tr = Transactions.get(machine.name)
    :ok = Transaction.set_read_version(tr, machine.read_version)
    machine
  end

  def execute(machine, _id, [{_, "CLEAR"}]) do
    {{_, {_, key}}, stack} = Stack.pop(machine.stack)
    tr = Transactions.get(machine.name)
    :ok = Transaction.clear(tr, key)
    %{machine | stack: stack}
  end

  def execute(machine, id, [{_, "CLEAR_DATABASE"}]) do
    {{_, {_, key}}, stack} = Stack.pop(machine.stack)
    :ok = FDBC.transact(machine.db, &Transaction.clear(&1, key))
    %{machine | stack: Stack.push(stack, id, {:binary, "RESULT_NOT_PRESENT"})}
  end

  def execute(machine, _id, [{_, "CLEAR_RANGE"}]) do
    {{_, {_, start}}, stack} = Stack.pop(machine.stack)
    {{_, {_, stop}}, stack} = Stack.pop(stack)
    tr = Transactions.get(machine.name)
    :ok = Transaction.clear_range(tr, start, stop)
    %{machine | stack: stack}
  end

  def execute(machine, id, [{_, "CLEAR_RANGE_DATABASE"}]) do
    {{_, {_, start}}, stack} = Stack.pop(machine.stack)
    {{_, {_, stop}}, stack} = Stack.pop(stack)
    :ok = FDBC.transact(machine.db, &Transaction.clear_range(&1, start, stop))
    %{machine | stack: Stack.push(stack, id, {:binary, "RESULT_NOT_PRESENT"})}
  end

  def execute(machine, _id, [{_, "CLEAR_RANGE_STARTS_WITH"}]) do
    {{_, {_, prefix}}, stack} = Stack.pop(machine.stack)
    tr = Transactions.get(machine.name)
    :ok = Transaction.clear_starts_with(tr, prefix)
    %{machine | stack: stack}
  end

  def execute(machine, id, [{_, "CLEAR_RANGE_STARTS_WITH_DATABASE"}]) do
    {{_, {_, prefix}}, stack} = Stack.pop(machine.stack)
    :ok = FDBC.transact(machine.db, &Transaction.clear_starts_with(&1, prefix))
    %{machine | stack: Stack.push(stack, id, {:binary, "RESULT_NOT_PRESENT"})}
  end

  def execute(machine, _id, [{_, "ATOMIC_OP"}]) do
    {{_, {_, op}}, stack} = Stack.pop(machine.stack)
    {{_, {_, key}}, stack} = Stack.pop(stack)
    {{_, {_, value}}, stack} = Stack.pop(stack)
    op = String.downcase(op) |> String.to_atom()
    tr = Transactions.get(machine.name)
    :ok = Transaction.atomic_op(tr, op, key, value)
    %{machine | stack: stack}
  end

  def execute(machine, id, [{_, "ATOMIC_OP_DATABASE"}]) do
    {{_, {_, op}}, stack} = Stack.pop(machine.stack)
    {{_, {_, key}}, stack} = Stack.pop(stack)
    {{_, {_, value}}, stack} = Stack.pop(stack)
    :ok = FDBC.transact(machine.db, &Transaction.atomic_op(&1, op, key, value))
    %{machine | stack: Stack.push(stack, id, {:binary, "RESULT_NOT_PRESENT"})}
  end

  def execute(machine, id, [{_, "READ_CONFLICT_RANGE"}]) do
    {{_, {_, start}}, stack} = Stack.pop(machine.stack)
    {{_, {_, stop}}, stack} = Stack.pop(stack)
    tr = Transactions.get(machine.name)
    :ok = Transaction.add_conflict_range(tr, start, stop, :read)
    %{machine | stack: Stack.push(stack, id, {:binary, "SET_CONFLICT_RANGE"})}
  end

  def execute(machine, id, [{_, "WRITE_CONFLICT_RANGE"}]) do
    {{_, {_, start}}, stack} = Stack.pop(machine.stack)
    {{_, {_, stop}}, stack} = Stack.pop(stack)
    tr = Transactions.get(machine.name)
    :ok = Transaction.add_conflict_range(tr, start, stop, :write)
    %{machine | stack: Stack.push(stack, id, {:binary, "SET_CONFLICT_RANGE"})}
  end

  def execute(machine, id, [{_, "READ_CONFLICT_KEY"}]) do
    {{_, {_, key}}, stack} = Stack.pop(machine.stack)
    tr = Transactions.get(machine.name)
    :ok = Transaction.add_conflict_key(tr, key, :read)
    %{machine | stack: Stack.push(stack, id, {:binary, "SET_CONFLICT_KEY"})}
  end

  def execute(machine, id, [{_, "WRITE_CONFLICT_KEY"}]) do
    {{_, {_, key}}, stack} = Stack.pop(machine.stack)
    tr = Transactions.get(machine.name)
    :ok = Transaction.add_conflict_key(tr, key, :write)
    %{machine | stack: Stack.push(stack, id, {:binary, "SET_CONFLICT_KEY"})}
  end

  def execute(machine, _id, [{_, "DISABLE_WRITE_CONFLICT"}]) do
    tr = Transactions.get(machine.name)
    Transaction.change(tr, next_write_no_write_conflict_range: true)
    machine
  end

  def execute(machine, id, [{_, "COMMIT"}]) do
    tr = Transactions.get(machine.name)
    :ok = Transaction.commit(tr)
    %{machine | stack: Stack.push(machine.stack, id, {:binary, "RESULT_NOT_PRESENT"})}
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
    tr = Transactions.get(machine.name)
    version = Transaction.get_committed_version(tr)

    %{
      machine
      | read_version: version,
        stack: Stack.push(machine.stack, id, {:binary, "GOT_COMMITTED_VERSION"})
    }
  end

  def execute(machine, id, [{_, "GET_APPROXIMATE_SIZE"}]) do
    tr = Transactions.get(machine.name)
    _ = Transaction.get_approximate_size(tr)
    %{machine | stack: Stack.push(machine.stack, id, {:binary, "GOT_APPROXIMATE_SIZE"})}
  end

  def execute(machine, _id, [{_, "WAIT_FUTURE"}]) do
    # Future resolution is handled in pop
    {{id, item}, stack} = Stack.pop(machine.stack)
    %{machine | stack: Stack.push(stack, id, item)}
  end

  def execute(machine, id, [{_, "TUPLE_PACK"}]) do
    {{_, {_, count}}, stack} = Stack.pop(machine.stack)

    {pairs, stack} =
      Enum.reduce(0..(count - 1), {[], stack}, fn _, {pairs, stack} ->
        {{_, pair}, stack} = Stack.pop(stack)
        {[pair | pairs], stack}
      end)

    item = Enum.reverse(pairs) |> Tuple.pack()
    %{machine | stack: Stack.push(stack, id, {:binary, item})}
  end

  def execute(machine, id, [{_, "TUPLE_PACK_WITH_VERSIONSTAMP"}]) do
    {{_, {_, prefix}}, stack} = Stack.pop(machine.stack)
    {{_, {_, count}}, stack} = Stack.pop(stack)

    {pairs, stack} =
      Enum.reduce(0..(count - 1), {[], stack}, fn _, {pairs, stack} ->
        {{_, pair}, stack} = Stack.pop(stack)
        {[pair | pairs], stack}
      end)

    {item, stack} =
      try do
        item = Subspace.new(prefix) |> Subspace.pack(Enum.reverse(pairs), versionstamp: true)
        stack = Stack.push(stack, id, {:binary, item})
        {"OK", stack}
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
    {{_, {_, item}}, stack} = Stack.pop(machine.stack)

    stack =
      Tuple.unpack(item, keyed: true)
      |> Enum.reduce(stack, fn data, stack ->
        item = Tuple.pack([data], keyed: true)
        Stack.push(stack, id, {:binary, item})
      end)

    %{machine | stack: stack}
  end

  def execute(machine, id, [{_, "TUPLE_RANGE"}]) do
    {{_, {_, count}}, stack} = Stack.pop(machine.stack)

    {pairs, stack} =
      Enum.reduce(0..(count - 1), {[], stack}, fn _, {pairs, stack} ->
        {{_, pair}, stack} = Stack.pop(stack)
        {[pair | pairs], stack}
      end)

    {start, stop} = Enum.reverse(pairs) |> Tuple.range()
    stack = Stack.push(stack, id, {:binary, start})
    %{machine | stack: Stack.push(stack, id, {:binary, stop})}
  end

  def execute(machine, id, [{_, "TUPLE_SORT"}]) do
    {{_, {_, count}}, stack} = Stack.pop(machine.stack)

    {items, stack} =
      Enum.reduce(0..(count - 1), {[], stack}, fn _, {items, stack} ->
        {{_, {_, item}}, stack} = Stack.pop(stack)
        {[item | items], stack}
      end)

    stack =
      Enum.sort(items)
      |> Enum.reduce(stack, fn item, stack ->
        Stack.push(stack, id, {:binary, item})
      end)

    %{machine | stack: stack}
  end

  def execute(machine, id, [{_, "ENCODE_FLOAT"}]) do
    {{_, {_, item}}, stack} = Stack.pop(machine.stack)
    <<item::float-big-32>> = item
    %{machine | stack: Stack.push(stack, id, {:float, item})}
  end

  def execute(machine, id, [{_, "ENCODE_DOUBLE"}]) do
    {{_, {_, item}}, stack} = Stack.pop(machine.stack)
    <<item::float-big-64>> = item
    %{machine | stack: Stack.push(stack, id, {:double, item})}
  end

  def execute(machine, id, [{_, "DECODE_FLOAT"}]) do
    {{_, {_, item}}, stack} = Stack.pop(machine.stack)
    <<item::float-big-32>> = item
    %{machine | stack: Stack.push(stack, id, {:binary, item})}
  end

  def execute(machine, id, [{_, "DECODE_DOUBLE"}]) do
    {{_, {_, item}}, stack} = Stack.pop(machine.stack)
    <<item::float-big-64>> = item
    %{machine | stack: Stack.push(stack, id, {:binary, item})}
  end

  def execute(machine, _id, [{_, "START_THREAD"}]) do
    {{_, {_, prefix}}, stack} = Stack.pop(machine.stack)
    result = spawn_monitor(fn -> Machine.start(machine.db, prefix) end)
    %{machine | processes: [result | machine.processes], stack: stack}
  end

  def execute(machine, id, [{_, "WAIT_EMPTY"}]) do
    {{_, {_, prefix}}, stack} = Stack.pop(machine.stack)

    FDBC.transact(machine.db, fn tr ->
      unless Transaction.get_starts_with(tr, prefix) == [] do
        raise Error,
          code: 1020,
          message: "Transaction not committed due to conflict with another transaction"
      end
    end)

    %{machine | stack: Stack.push(stack, id, {:binary, "WAITED_FOR_EMPTY"})}
  end

  def execute(machine, _id, [{_, "UNIT_TESTS"}]) do
    machine
  end

  ## Helpers

  defp int_to_mode(value) do
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
