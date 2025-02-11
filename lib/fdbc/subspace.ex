defmodule FDBC.Subspace do
  @moduledoc """
  Subspaces provide the recommended way to define
  [namespaces](https://apple.github.io/foundationdb/developer-guide.html#developer-guide-namespace-management)
  for different categories of data. As a best practice, you should always use
  at least one subspace as a namespace for your application data.

  An instance of the class stores a prefix used to identify the namespace and
  automatically prepends it when encoding tuples into keys. Likewise, it
  removes the prefix when decoding keys.

  For example, suppose your application tracks profile data for your users. You
  could store the data in a `user_space` subspace that would make `'user'` the
  first element of each tuple. A back-end function might have the form:

  ```elixir
  defmodule Example do
    @user_space FDBC.Subspace(["user"])

    def set_user_data(db_or_tr, key, value) do
      FDBC.transact(db_or_tr, fn tr ->
        FDBC.Transaction.set(tr, FDBC.Subspace.pack(@user_space, [key]), value)
      end)
    end
  end
  ```
  """

  alias FDBC.Tuple

  defstruct [:key]

  @type t :: %__MODULE__{
          key: binary
        }

  @doc """
  Creates a new subspace with the given prefix.

  If the prefix is a `binary` it is encoded raw, otherwise a tuple must be
  provided.
  """
  @spec new(binary | [any]) :: t
  def new(prefix)

  def new(prefix) when is_binary(prefix) do
    %__MODULE__{key: prefix}
  end

  def new(prefix) when is_list(prefix) do
    key = Tuple.pack(prefix)
    new(key)
  end

  @doc """
  Returns a new subspace that is the concatination of two subspaces.

  The second subspace is appended to the first subspace.
  """
  @spec concat(t, t | [any]) :: t
  def concat(s1, s2)

  def concat(%__MODULE__{} = s1, %__MODULE__{} = s2) do
    new(s1.key <> s2.key)
  end

  def concat(%__MODULE__{} = s1, tuple) when is_list(tuple) do
    concat(s1, new(tuple))
  end

  @doc """
  Returns true if the subspace is the logical prefix of the given key.
  """
  @spec contains?(t, binary) :: boolean
  def contains?(%__MODULE__{key: prefix}, key) do
    case key do
      <<^prefix::binary, _::binary>> -> true
      ^prefix -> true
      _ -> false
    end
  end

  @doc """
  Returns a key encoding the specified tuple prefixed by the subspace.

  ## Options

    * `:extension` - a module that implements the behaviour `FDBC.Tuple.Extension`
      allowing the Tuple layer to be extended with user type codes.

    * `:keyed` - if present, then the tuple is returned as a keyword list, such
      as `[{string: "flip"}, {string: "flop"}]`.

    * `:versionstamp` - if present, then the tuple must contain one incomplete
      versionstamp whose postition will be encoded on the end of the key to
      enable compatability with versionstamp operations; An `ArgumentError`
      will be raised if no incomplete versionstamp is found or if more than one
      is found. By default packing a tuple with an incomplete versionstamp will
      raise an `ArgumentError`.
  """
  @spec pack(t, [any], keyword) :: binary
  def pack(%__MODULE__{key: prefix}, tuple, opts \\ []) do
    key = Tuple.pack(tuple, opts)

    if Keyword.get(opts, :versionstamp) do
      size = byte_size(key) - 4
      <<key::binary-size(size), offset::integer-little-32>> = key
      prefix <> key <> <<offset + byte_size(prefix)::integer-little-32>>
    else
      prefix <> key
    end
  end

  @doc """
  Returns the start and stop keys in the form of a tuple pair representing all
  keys in the subspace that encode tuples strictly starting with the specifed
  tuple.
  """
  @spec range(t, [any], keyword) :: {binary, binary}
  def range(subspace, tuple \\ [], opts \\ [])

  def range(%__MODULE__{key: key}, [], _) do
    {key <> <<0x00>>, key <> <<0xFF>>}
  end

  def range(%__MODULE__{key: prefix}, tuple, opts) do
    key = Tuple.pack(tuple, opts)
    {prefix <> key <> <<0x00>>, prefix <> key <> <<0xFF>>}
  end

  @doc """
  Returns the tuple that encoded into the given key with the subspace prefix removed.

  Raises `ArgumentError` if the `key` is not in the subspace.

  ## Options

    * `:extension` - a module that implements the behaviour `FDBC.Tuple.Extension`
      allowing the Tuple layer to be extended with user type codes.

    * `:keyed` - if present, then the tuple is returned as a keyword list, such
      as `[{string: "flip"}, {string: "flop"}]`.
  """
  @spec unpack(t, binary) :: [any]
  def unpack(%__MODULE__{key: prefix}, key, opts \\ []) do
    case key do
      <<^prefix::binary, key::binary>> -> Tuple.unpack(key, opts)
      ^prefix -> []
      _ -> raise ArgumentError, "key is not in subspace"
    end
  end
end
