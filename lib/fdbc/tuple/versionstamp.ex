defmodule FDBC.Tuple.Versionstamp do
  @moduledoc """
  Used to represent values written by versionstamp operations within the tuple
  layer. This wraps a binary of length 12 that can be used to represent some
  global order of items within the database.

  These versions are composed of two separate components:
    1. the 10-byte `tr` version.
    2. the two-byte `user` version.

  The `tr` version is set by the database, and it is used to impose an order
  between different transactions. This order is guaranteed to be monotonically
  increasing over time for a given database. In particular, it imposes an order
  that is consistent with a serialization order of the database’s transactions.
  If the client elects to leave the `tr` version as its default value of `nil`,
  then the Versionstamp is considered 'incomplete'. This will cause the first
  10 bytes of the serialized `FDBC.Tuple.Versionstamp` to be filled in with
  dummy bytes when serialized. When used with operations
  `:set_versionstamped_key` or `:set_versionstamped_value` in
  `FDBC.Transaction.atomic_op/4`, an incomplete version can be used to ensure
  that a key gets written with the current transaction’s version which can be
  useful for maintaining append-only data structures within the database. If
  the `tr` version is set to something that is not `nil`, it should be set to a
  binary of length 10. In this case, the Versionstamp is considered 'complete'.
  This is the usual case when one reads a serialized `FDBC.Tuple.Versionstamp`
  from the database.

  The `user` version should be specified as an integer, but it must fit within
  a two-byte unsigned integer. It is set by the client, and it is used to
  impose an order between items serialized within a single transaction. If left
  unset, then final two bytes of the serialized `FDBC.Tuple.Versionstamp` are
  filled in with a default constant value.

  The example belows shows how to serialize an incomplete
  `FDBC.Tuple.Versionstamp`, write it to the database and then read it back:

  ```elixir
  defmodule Example do
    def read_versionstamp(db, subspace) do
      FDBC.transact(db, fn tr ->
        {start, stop} = FDBC.Subspace.range(subspace)
        case FDBC.Transaction.get_range(tr, start, stop, limit: 1) do
          [] -> nil
          [{k, _}] -> FDBC.Subspace.unpack(k)
        end
      end)
    end

    def write_versionstamp(db, subspace) do
      FDBC.transact(db, fn tr ->
        key = FDBC.Subspace.pack(subspace, [Versionstamp.new()])
        :ok = FDBC.Transaction.atomic_op(tr, :set_versionstamped_key, key, <<>>)
        FDBC.Transaction.async_get_versionstamp(tr)
      end) |> FDBC.Future.resolve()
    end
  end

  db = FDBC.Database.create()
  subspace = FDBC.Subspace.new(['prefix'])
  {start, stop} = FDBC.Subspace.range(subspace)
  :ok = FDBC.Transaction.clear_range(db, start, stop)
  version = Example.write_versionstamp(db, subspace)
  v = Example.read_versionstamp(db, subspace)
  assert v == FDBC.Tuple.Versionstamp(tr=version)
  ```
  """

  defstruct [:tr, :user]

  @type t :: %__MODULE__{
          tr: binary,
          user: integer
        }

  @incomplete <<0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF>>

  @doc """
  Create a versionstamp.

  Where `tr` is the transaction version that is set by the database and `user`
  is the client specified version used to impose an order between items
  serialized within a single transaction.

  If `tr` is `nil` then an incomplete versionstamp is created.
  """
  @spec new(binary | nil, integer) :: t
  def new(tr \\ nil, user \\ 0)

  def new(nil, user), do: new(@incomplete, user)

  def new(tr, user) when byte_size(tr) == 10 and user >= 0 and user <= 0xFFFF do
    %__MODULE__{tr: tr, user: user}
  end

  @doc """
  Converts a versiontamp into its binary representation.
  """
  @spec dump(t) :: binary
  def dump(%__MODULE__{tr: tr, user: user}) do
    <<tr::binary-big-size(10), user::integer-big-size(16)>>
  end

  @doc """
  Loads a versionstamp from its binary representation.
  """
  @spec load(binary) :: t
  def load(<<tr::binary-big-size(10)>>) do
    new(tr)
  end

  def load(<<tr::binary-big-size(10), user::integer-big-size(16)>>) do
    new(tr, user)
  end

  @doc """
  Checks if a versionstamp is incomplete.
  """
  @spec incomplete?(t) :: boolean
  def incomplete?(%__MODULE__{tr: tr}) do
    if tr == @incomplete, do: true, else: false
  end
end
