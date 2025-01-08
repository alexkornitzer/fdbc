defmodule FDBC.KeySelector do
  @moduledoc """
  FoundationDB’s lexicographically ordered data model permits finding keys
  based on their order (for example, finding the first key in the database
  greater than a given key). Key selectors represent a description of a key in
  the database that could be resolved to an actual key by
  `FDBC.Transaction.get_key/3` or used directly as the beginning or end of a
  range in `FDBC.Transaction.get_range/4`.

  For example:

  ```elixir
  # Selects the first key following apple.
  FDBC.KeySelector.greater_than('apple')

  # Selects the second key following apple.
  selector = FDBC.KeySelector.greater_than('apple')
  %{selector | selector.offset + 1}
  ```

  > #### Note {: .info}
  > The current version of FoundationDB does not resolve key selectors with
  large offsets in O(1) time. See the [Key selectors with large offsets are
  slow](https://apple.github.io/foundationdb/known-limitations.html#dont-use-key-selectors-for-paging)
  known limitation.

  All possible key selectors can be constructed using one of the four 'common
  form; constructors and a positive or negative offset. Alternatively, general
  key selectors can be manually constructed by specifying:

    * A reference key (binary)
    * An equality flag (boolean)
    * An offset (integer)

  To 'resolve' these key selectors FoundationDB first finds the last key less
  than the reference key (or equal to the reference key, if the equality flag
  is true), then moves forward a number of keys equal to the offset (or
  backwards, if the offset is negative). If a key selector would otherwise
  describe a key off the beginning of the database (before the first key), it
  instead resolves to the empty key `<<>>`. If it would otherwise describe a key
  off the end of the database (after the last key), it instead resolves to the
  key `'\\xFF'` (or `'\\xFF\\xFF'` if the transaction has been granted access
  to the system keys).
  """

  defstruct [:key, :or_equal, :offset]

  @type t :: %__MODULE__{
          key: binary,
          offset: integer,
          or_equal: integer
        }

  @doc """
  Returns a key selector for use in key or range based transactions.

  The value of `or_equal` is used to decide whether to include matching the
  `key` itself or just on the key immediate before it.

  The value of `offset` determines which key to select after the match on the `key`.

  > #### Note {: .info}
  > FoundationDB does not efficiently resolve key selectors with large offsets,
  so [Key selectors with large offsets are slow](https://apple.github.io/foundationdb/known-limitations.html#dont-use-key-selectors-for-paging).
  """
  @spec new(binary, boolean, integer) :: t
  def new(key, or_equal \\ false, offset \\ 0) when is_binary(key) do
    or_equal =
      case or_equal do
        false -> 0
        true -> 1
      end

    %__MODULE__{key: key, or_equal: or_equal, offset: offset}
  end

  @doc """
  Returns a key selector that will select the lexicographically greatest key
  present in the database which is lexicographically strictly less than the
  given byte string.
  """
  @spec less_than(binary) :: t
  def less_than(key) when is_binary(key) do
    %__MODULE__{key: key, or_equal: 0, offset: 0}
  end

  @doc """
  Returns a key selector that will select the lexicographically greatest key
  present in the database which is lexicographically less than or equal to the
  given byte string key.
  """
  @spec less_than_or_equal(binary) :: t
  def less_than_or_equal(key) when is_binary(key) do
    %__MODULE__{key: key, or_equal: 1, offset: 0}
  end

  @doc """
  Returns a key selector that will select the lexicographically least key
  present in the database which is lexicographically strictly greater than the
  given byte string key.
  """
  @spec greater_than(binary) :: t
  def greater_than(key) when is_binary(key) do
    %__MODULE__{key: key, or_equal: 1, offset: 1}
  end

  @doc """
  Returns a key selector that will select the lexicographically least key
  present in the database which is lexicographically greater than or equal to
  the given byte string key.
  """
  @spec greater_than_or_equal(binary) :: t
  def greater_than_or_equal(key) when is_binary(key) do
    %__MODULE__{key: key, or_equal: 0, offset: 1}
  end
end
