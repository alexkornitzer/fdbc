defmodule FDBC.Directory.HighContentionAllocator do
  @moduledoc false

  alias FDBC.Subspace
  alias FDBC.Transaction
  alias FDBC.Tuple

  @counters 0
  @recent 1

  @spec allocate(Subspace.t(), Transaction.t()) :: binary
  def allocate(%Subspace{} = subspace, %Transaction{} = tr) do
    candidate = find(subspace, tr)
    Tuple.pack([candidate])
  end

  defp find(%Subspace{} = subspace, %Transaction{} = tr) do
    key = subspace |> Subspace.pack([@counters])

    [_, start] =
      case Transaction.get_range(tr, key <> <<0x00>>, key <> <<0xFF>>, limit: 1, reverse: true) do
        [] -> [@counters, 0]
        [{k, _}] -> Subspace.unpack(subspace, k)
      end

    candidates = range(tr, subspace, start, false)

    case evaluate(tr, subspace, candidates) do
      nil -> find(subspace, tr)
      candidate -> candidate
    end
  end

  defp evaluate(%Transaction{} = tr, %Subspace{} = subspace, candidates) do
    candidate = Enum.random(candidates)

    {latest, value} =
      lock(tr, fn ->
        key = subspace |> Subspace.pack([@counters])

        [_, latest_counter] =
          case Transaction.get_range(tr, key <> <<0x00>>, key <> <<0xFF>>,
                 limit: 1,
                 reverse: true
               ) do
            [] -> [@counters, nil]
            [{k, _}] -> Subspace.unpack(subspace, k)
          end

        candidate_value = Transaction.get(tr, Subspace.pack(subspace, [@recent, candidate]))

        :ok =
          Transaction.set(tr, Subspace.pack(subspace, [@recent, candidate]), "",
            no_write_conflict_range: true
          )

        {latest_counter, candidate_value}
      end)

    cond do
      latest > candidates.first ->
        nil

      value == nil ->
        key = Subspace.pack(subspace, [@recent, candidate])
        :ok = Transaction.add_conflict_key(tr, key, :write)
        candidate

      true ->
        evaluate(tr, subspace, candidates)
    end
  end

  defp lock(%Transaction{resource: resource}, callback) do
    :global.trans({resource, self()}, callback, [Node.self()])
  end

  defp range(%Transaction{} = tr, subspace, start, windows_advanced) do
    count =
      lock(tr, fn ->
        if windows_advanced do
          :ok =
            Transaction.clear_range(
              tr,
              Subspace.pack(subspace, [@counters]),
              Subspace.pack(subspace, [@counters, start])
            )

          :ok =
            Transaction.clear_range(
              tr,
              Subspace.pack(subspace, [@recent]),
              Subspace.pack(subspace, [@recent, start]),
              no_write_conflict_range: true
            )
        end

        :ok =
          Transaction.atomic_op(
            tr,
            :add,
            Subspace.pack(subspace, [@counters, start]),
            <<1::integer-little-size(64)>>
          )

        Transaction.get(
          tr,
          Subspace.pack(subspace, [@counters, start]),
          snapshot: true
        )
      end)

    count =
      case count do
        nil -> 0
        <<count::integer-little-64>> -> count
      end

    window = window_size(start)

    if count * 2 < window do
      # We drop down by one because `Enum.random/1` is inclusive which is used
      # in `evaluate/3` to pick a candidate from the pool.
      start..(start + window - 1)
    else
      range(tr, subspace, start + window, true)
    end
  end

  defp window_size(start) do
    cond do
      start < 255 -> 64
      start < 65535 -> 1024
      true -> 8192
    end
  end
end
