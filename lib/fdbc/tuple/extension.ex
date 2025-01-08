defmodule FDBC.Tuple.Extension do
  @moduledoc """
  A behaviour module for implementing user type codes.

  Using this module makes it possible to extend the `FDBC.Tuple` layer with
  [user type codes](https://github.com/apple/foundationdb/blob/main/design/tuple.md#user-type-codes).
  Only the following type codes are allowed within the implementation
  `0x40..0x4F` as defined in the specification.

  Below is an example showing how to create an extension that would enable the
  tuple layer to handle `DateTime`:

  ```elixir
  defmodule TupleExtensions.DateTime do
    use FDBC.Tuple.Extension

    @datetime_typecode 0x40

    def decode(<<@datetime_typecode, length::integer-size(8)>> <> encoded) do
      {value, encoded} =
        case encoded do
          <<value::binary-size(^length), encoded>> -> {value, encoded}
          <<value::binary-size(^length)>> -> {value, <<>>}
        end

      [timestamp] = Tuple.unpack(value)
      {:ok, datetime} = DateTime.from_unix(timestamp)
      {{:datetime, datetime}, encoded}
    end

    def encode({:datetime, data}) do
      timestamp = DateTime.to_unix(data)
      packed = Tuple.pack([timestamp])
      <<@datetime_typecode>> <> <<byte_size(packed)::integer-big-8>> <> packed
    end

    def identify(%DateTime{}) do
      :datetime
    end
  end
  ```

  When using extensions the official documentation recommends that they are
  used within labelled directories to prevent type code clashes. With that in
  mind an extended tuple could be utilised like so making use of
  `FDBC.Directory`:

  ```elixir
  FDBC.transact(db, fn tr ->
    dir = FDBC.Directory.new() |> FDBC.Directory.open!(tr, ["example"], label: "datetime")
    key = FDBC.Tuple.pack([DateTime.utc_now()], extension: TupleExtensions.DateTime)
    :ok = FDBC.Transaction.set(tr, key, <<>>)
  end)
  ```
  """

  @type keyed :: {atom, any}

  @doc """
  Takes the encoded key returning the decoded tuple pair and the tail of the
  key to still be decoded.

  """
  @callback decode(binary) :: {keyed, binary}

  @doc """
  Takes the keyed type to encode in the form of a tuple pair, returning its
  encoded representation.

  """
  @callback encode(keyed) :: binary

  @doc """
  Takes the custom type and returns its key.
  """
  @callback identify(any) :: atom

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      @behaviour FDBC.Tuple.Extension

      @doc false
      def opts() do
        unquote(Macro.escape(opts))
      end

      @before_compile FDBC.Tuple.Extension
    end
  end

  defmacro __before_compile__(_) do
    quote do
      def decode(<<typecode>> <> _) do
        raise ArgumentError, "unsupported type code: #{typecode}"
      end

      def decode(<<>>) do
        raise ArgumentError, "unexpected end of output"
      end

      def encode({key, _}) do
        raise ArgumentError, "unknown identity: #{key}"
      end

      def encode(data) do
        raise ArgumentError, "unsupported data type: #{data}"
      end

      def identify(data) do
        raise ArgumentError, "unsupported data type: #{data}"
      end
    end
  end
end
