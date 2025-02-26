alias FDBC.Network
alias FDBC.Transaction

defmodule Operations do
  @behaviour Benchee.Formatter

  def format(suite, _opts) do
    Enum.flat_map(suite.scenarios, fn s ->
      runs = s.run_time_data.statistics.ips

      if s.after_scenario do
        opts = s.after_scenario.(nil)
        ops = Keyword.get(opts, :ops, 1)
        ["#{s.name}: #{floor(ops * runs)} ops/second"]
      else
        []
      end
    end)
  end

  def write([], _opts), do: nil

  def write(any, _opts) do
    IO.puts("\nOperations:")
    Enum.each(any, &IO.puts(&1))
    IO.puts("\n")
  end
end

defmodule Helpers do
  @prefix "fdbc"

  def initialise() do
    db = FDBC.Database.create()

    FDBC.transact(db, fn tr ->
      Transaction.clear_starts_with(tr, @prefix)
    end)

    tasks =
      Enum.chunk_every(1..1_000_000, 10_000)
      |> Enum.map(fn chunk ->
        Task.async(fn ->
          FDBC.transact(db, fn tr ->
            Enum.each(chunk, fn i ->
              key = @prefix <> <<i::integer-size(12)-unit(8)>>
              value = random_value()
              Transaction.set(tr, key, value)
            end)
          end)
        end)
      end)

    _ = Task.await_many(tasks, 60_000)
    db
  end

  def as_key(i) do
    @prefix <> <<i::integer-size(12)-unit(8)>>
  end

  def get_pairs(count) do
    Enum.reduce(1..count, [], fn _, acc -> [{random_key(), random_value()} | acc] end)
  end

  def random_binary(size) do
    Enum.take_random(0x00..0xFF, size) |> Enum.reduce(<<>>, fn v, acc -> acc <> <<v>> end)
  end

  def random_key() do
    i = Enum.random(1..1_000_000)
    @prefix <> <<i::integer-size(12)-unit(8)>>
  end

  def random_value() do
    Enum.random(8..100) |> random_binary()
  end
end

:ok = FDBC.api_version(730)
:ok = Network.start()

db = Helpers.initialise()

Benchee.run(
  %{
    "single-core write test #1" => {
      fn {k, v} ->
        FDBC.transact(db, fn tr ->
          Transaction.set(tr, k, v)
        end)
      end,
      after_scenario: fn _ -> [ops: 1] end,
      before_each: fn _ ->
        [kv] = Helpers.get_pairs(1)
        kv
      end
    },
    "single-core write test #2" => {
      fn pairs ->
        FDBC.transact(db, fn tr ->
          Enum.each(pairs, fn {k, v} ->
            Transaction.set(tr, k, v)
          end)
        end)
      end,
      after_scenario: fn _ -> [ops: 10] end, before_each: fn _ -> Helpers.get_pairs(10) end
    }
  },
  formatters: [Benchee.Formatters.Console, Operations],
  parallel: 1,
  profile_after: false
)

Benchee.run(
  %{
    "single-core write test #3" => {
      fn pairs ->
        FDBC.transact(db, fn tr ->
          Enum.each(pairs, fn {k, v} ->
            Transaction.set(tr, k, v)
          end)
        end)
      end,
      after_scenario: fn _ -> [ops: 10 * 100] end, before_each: fn _ -> Helpers.get_pairs(10) end
    },
    "single-core read test" => {
      fn pairs ->
        FDBC.transact(db, fn tr ->
          Enum.each(pairs, fn {k, _} ->
            _ = Transaction.get(tr, k)
          end)
        end)
      end,
      after_scenario: fn _ -> [ops: 10 * 100] end, before_each: fn _ -> Helpers.get_pairs(10) end
    },
    "single-core 90/10 test" => {
      fn pairs ->
        FDBC.transact(db, fn tr ->
          [{k, v} | tail] = pairs

          Enum.each(tail, fn {k, _} ->
            Transaction.get(tr, k)
          end)

          Transaction.set(tr, k, v)
        end)
      end,
      after_scenario: fn _ -> [ops: 10 * 100] end, before_each: fn _ -> Helpers.get_pairs(10) end
    },
    "single-core range read test" => {
      fn {start, stop} ->
        FDBC.transact(db, fn tr ->
          Transaction.get_range(tr, start, stop)
        end)
      end,
      after_scenario: fn _ -> [ops: 1_000 * 100] end,
      before_each: fn _ ->
        [i] = Enum.take_random(1..(1_000_000 - 1_000), 1)
        start = Helpers.as_key(i)
        stop = Helpers.as_key(i + 999)
        {start, stop}
      end
    }
  },
  formatters: [Benchee.Formatters.Console, Operations],
  parallel: 100,
  profile_after: false
)

:ok = Network.stop()
