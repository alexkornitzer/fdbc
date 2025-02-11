defmodule FDBC.Future do
  @moduledoc """
  Most functions in the FoundationDB API are asynchronous, meaning that they
  may return to the caller before actually delivering their result. An
  `FDBC.Future` object represents a result value or error to be delivered at
  some future time. A future can be awaited on or periodically checked to see
  if it is ready. Once a Future is ready it can be resolved to get its
  underlying value, which is dependant on the function that created the future.

  See `FDBC.Transaction` for examples on how futures can be used.
  """

  alias FDBC.NIF

  defstruct [:resource]

  @type t :: %__MODULE__{}
  @type t(_result) :: t()

  @doc """
  Cancels a future and its associated asynchronous operation.

  If called before the future is ready, attempts to `resolve/1` the future will
  result in a `FDBC.Error` exception with
  [operation_cancelled](https://apple.github.io/foundationdb/api-error-codes.html#developer-guide-error-codes)
  error.

  Cancelling a future which is already ready has no effect.

  > #### Note {: .info}
  > Even if a future is not ready, its associated asynchronous operation may
    have succesfully completed and be unable to be cancelled.

  """
  @spec cancel(t) :: :ok
  def cancel(%__MODULE__{resource: resource}) do
    :ok = NIF.future_cancel(resource)
    :ok
  end

  @doc """
  Returns `true` if the future is ready, where a Future is ready if it has been
  set to a value or an error.
  """
  @spec ready?(t) :: boolean
  def ready?(%__MODULE__{resource: resource}) do
    case NIF.future_is_ready(resource) do
      0 -> false
      1 -> true
    end
  end

  @doc """
  Resolves a future returning the result for its given type.

  On error it raises a `FDBC.Error` exception.
  """
  @spec resolve(t) :: any
  def resolve(future), do: await(future, :infinity)

  @doc """
  Awaits a future returning the result for its given type.

  This is similar to `resolve/1` except that a timeout can be set.

  On error it raises a `FDBC.Error` exception.
  """
  @spec await(t, timeout) :: any
  def await(%__MODULE__{resource: resource}, timeout \\ 5000)
      when timeout == :infinity or (is_integer(timeout) and timeout >= 0) do
    ref = make_ref()

    :ok =
      case NIF.future_resolve(resource, ref) do
        :ok -> :ok
        {:error, code, reason} -> raise FDBC.Error, message: reason, code: code
      end

    receive do
      {^ref, {:ok, result}} -> result
      {^ref, {:ok, more, result}} -> {more, result}
      {^ref, {:error, code, reason}} -> raise FDBC.Error, message: reason, code: code
    after
      timeout ->
        raise FDBC.Error, code: 1004, message: "Operation timed out"
    end
  end

  @doc """
  Awaits multiple futures returning their results.

  This function receives a list of futures and waits for their results in the
  given time interval. It returns a list of the results, in the same order as
  the futures supplied in the `futures` input argument.

  A timeout, in milliseconds or `:infinity`, can be given with a default value
  of `5000`. If the timeout is exceeded, then a `FDBC.Error` with a value of
  timed out is raised.

  On error it raises a `FDBC.Error` exception.
  """
  @spec await_many([t], timeout) :: [term]
  def await_many(futures, timeout \\ 5000)
      when timeout == :infinity or (is_integer(timeout) and timeout >= 0) do
    refs =
      Enum.reduce(futures, [], fn f, acc ->
        ref = make_ref()

        :ok =
          case NIF.future_resolve(f.resource, ref) do
            :ok -> :ok
            {:error, code, reason} -> raise FDBC.Error, message: reason, code: code
          end

        [ref | acc]
      end)
      |> Enum.reverse()

    awaiting = for ref <- refs, into: %{}, do: {ref, true}

    do_await_many(refs, timeout, awaiting, %{})
  end

  defp do_await_many(refs, _timeout, awaiting, results) when map_size(awaiting) == 0 do
    for ref <- refs, do: Map.fetch!(results, ref)
  end

  defp do_await_many(refs, timeout, awaiting, results) do
    receive do
      {ref, reply} when is_map_key(awaiting, ref) ->
        {ref, result} =
          case reply do
            {:ok, result} -> {ref, result}
            {:ok, more, result} -> {ref, {more, result}}
            {:error, code, reason} -> raise FDBC.Error, message: reason, code: code
          end

        do_await_many(refs, timeout, Map.delete(awaiting, ref), Map.put(results, ref, result))
    after
      timeout ->
        raise FDBC.Error, code: 1004, message: "Operation timed out"
    end
  end
end
