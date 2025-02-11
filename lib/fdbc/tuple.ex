defmodule FDBC.Tuple do
  @moduledoc """
  An implementation of upstreams builtin tuple layer. This layer is used for
  encoding keys that are useable by FoundationDB. The encoded key maintains the
  same sort order as the original tuple: sorted first by the first element,
  then by the second element, etc. This makes the tuple layer ideal for
  building a variety of higher-level data models.

  Below is an example showing the usage of the tuple layer for handling
  population figures for the United States are stored using keys formed from
  the tuple of state and county:

  ```elixir
  defmodule Example do
    def get_county_populations_in_state(db, state) do
      FDBC.transact(db, fn tr ->
        {start, stop} = FDBC.Tuple.range([state])
        Transaction.get_range(tr, start, stop) |> Enum.map(fn {_, v} -> String.to_integer(v) end)
      end)
    end

    def set_county_population(db, state, county, population) do
      FDBC.transact(db, fn tr ->
        key = FDBC.Tuple.pack([state, county])
        Transaction.set(tr, key, Integer.to_string(population))
      end)
    end
  end
  ```

  ## Upstream Compatibility

  At the time of writing all [type codes](https://github.com/apple/foundationdb/blob/main/design/tuple.md) are
  implemented.

  Although fully complaint, the tuple layer implementation is lossy by default
  due to Elixir's type system. While not an issue when the key space is
  exclusively managed by this library, this is not the case if the key space is
  shared. This is because types like `t:String.t/0` map directly down to
  `t:binary/0` which is a lossy process at the time of packing.

  ```elixir
  # A string will encode to the type code of a binary and not of a unicode string.
  assert "\\x01foobar\\x00" == Tuple.pack(["foobar"])

  # This can be countered via a keyword list.
  assert "\\x02foobar\\x00" == Tuple.pack([{string: "foobar"}])
  ```

  To allow interoperability on shared key spaces the `FDBC.Tuple` module
  supports the `:keyed` option. When used this option changes the tuple from a
  list into a keyword list, where the keys are used to type the tuple.

  ```elixir
  # In this case `keyed` will ensure the tuple is a keyword list and raise an exception if not.
  assert "\\x02foobar\\x00" == Tuple.pack([{string: "foobar"}], keyed: true)

  # In this case `keyed` will ensure the tuple is returned as a keyword list.
  assert [{string: "foobar"}] == Tuple.unpack("\\x02foobar\\x00", keyed: true)
  ```

  The following atoms are used for keying tuple type codes:

  * `:null` - `0x00`
  * `:binary` - `0x01`
  * `:string` - `0x02`
  * `:integer` - `0x0B..0x1D`
  * `:float` - `0x20`
  * `:double` - `0x21`
  * `:boolean` - `0x26..0x27`
  * `:uuid` - `0x30`
  * `:versionstamp` - `0x33`

  ## User Type Codes

  It is possible to extend the tuple layer with user type codes which can be
  achieved by implementing the `FDBC.Tuple.Extension` behaviour and supplying
  it to the tuple functions via the `:extension` option. For more information
  and example refer to the documentation in `FDBC.Tuple.Extension`.

  """

  alias FDBC.Tuple.Versionstamp

  @doc """
  Returns the start and stop keys for a given tuple.

  The start and stop keys allow the retrieval of all tuples of greater length
  than the provided `prefix`.

  ## Options

    * `:extension` - a module that implements the behaviour `FDBC.Tuple.Extension`
      allowing the Tuple layer to be extended with user type codes.

    * `:keyed` - if present, then the tuple must be keyedly typed as a
      keyword list, such as `[{string: "flip"}, {string: "flop"}]`.

  ## Examples

    # Returns start & stop keys which when used in a range query would return
    # tuples like ["A", 2, x], ["A", 2, x, y], etc.
    FDBC.Tuple.range(["A", 2])
  """
  @spec range([any], keyword) :: {binary, binary}
  def range(prefix, opts \\ []) do
    key = pack(prefix, opts)
    {key <> <<0x00>>, key <> <<0xFF>>}
  end

  @doc """
  Returns a key encoding the specified tuple.

  Raises an `ArgumentError` if more than one incomplete
  `FDBC.Tuple.Versionstamp` is provided.

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
  @spec pack([any], keyword) :: binary
  def pack(tuple, opts \\ []) do
    if Keyword.get(opts, :keyed, false) do
      unless Keyword.keyword?(tuple) do
        raise ArgumentError, "tuple must be a keyword list as `:keyed` mode is enabled"
      end
    end

    extension = Keyword.get(opts, :extension)
    {key, state} = do_pack(extension, tuple, <<>>, [])

    if Keyword.get(opts, :versionstamp) do
      case Keyword.get_values(state, :versionstamp) do
        [] -> raise ArgumentError, "tuple is missing an incomplete versionstamp"
        [offset] -> key <> offset
        _ -> raise ArgumentError, "tuple contains more than one incomplete versionstamp"
      end
    else
      if Keyword.get(state, :versionstamp) do
        raise ArgumentError,
              "tuple cannot contain an incomplete versionstamp unless `:versionstamp` is enabled"
      end

      key
    end
  end

  defp do_pack(_, [], key, state), do: {key, state}

  defp do_pack(extension, [head | rest], key, state) do
    {encoded, state} = encode(head, state, extension: extension, prefix: key)
    do_pack(extension, rest, key <> encoded, state)
  end

  @doc """
  Returns the tuple that encoded into the given key.

  ## Options

    * `:extension` - a module that implements the behaviour `FDBC.Tuple.Extension`
      allowing the Tuple layer to be extended with user type codes.

    * `:keyed` - if present, then the tuple is returned as a keyword list, such
      as `[{string: "flip"}, {string: "flop"}]`.
  """
  @spec unpack(binary, keyword) :: [any]
  def unpack(key, opts \\ []) do
    extension = Keyword.get(opts, :extension)
    tuple = do_unpack(extension, key, []) |> Enum.reverse()

    if Keyword.get(opts, :keyed, false) do
      tuple
    else
      unkey(tuple)
    end
  end

  defp do_unpack(_, <<>>, decoded), do: decoded

  defp do_unpack(extension, data, decoded) do
    {head, tail} = decode(data, extension)
    do_unpack(extension, tail, [head | decoded])
  end

  defp unkey([]), do: []
  defp unkey([{_, value} | tail]), do: [unkey(value) | unkey(tail)]
  defp unkey([head | tail]), do: [unkey(head) | unkey(tail)]
  defp unkey(value), do: value

  # Typecodes

  @null_typecode 0x00
  @bytes_typecode 0x01
  @string_typecode 0x02
  @tuple_typecode 0x05
  @float_typecode 0x20
  @double_typecode 0x21
  @false_typecode 0x26
  @true_typecode 0x27
  @uuid_typecode 0x30
  @versionstamp_typecode 0x33
  @user_typecodes 0x40..0x4F
  @escape_typecode 0xFF

  @hex_chars [
    ?1,
    ?2,
    ?3,
    ?4,
    ?5,
    ?6,
    ?7,
    ?8,
    ?9,
    ?a,
    ?b,
    ?c,
    ?d,
    ?e,
    ?f,
    ?A,
    ?B,
    ?C,
    ?D,
    ?E,
    ?F
  ]
  @min_f32 then(<<0xFF7FFFFF::32>>, fn <<num::float-32>> -> num end)
  @max_f32 then(<<0x7F7FFFFF::32>>, fn <<num::float-32>> -> num end)

  # Decoders

  defp decode(<<@null_typecode>> <> encoded, _), do: {{:null, nil}, encoded}

  defp decode(<<@bytes_typecode>> <> encoded, _) do
    {data, rest} = do_bytes_decode(encoded, <<>>)
    {{:binary, data}, rest}
  end

  defp decode(<<@string_typecode>> <> encoded, _) do
    {data, rest} = do_string_decode(encoded, <<>>)
    {{:string, data}, rest}
  end

  defp decode(<<@tuple_typecode>> <> encoded, extension) do
    {tuple, encoded} = do_tuple_decode(encoded, [], extension)
    {{:list, tuple |> Enum.reverse()}, encoded}
  end

  defp decode(<<0x0B, length::integer-size(8)>> <> encoded, _) do
    <<length::integer-size(8)>> = complement(<<length::integer-size(8)>>)

    {value, encoded} =
      case encoded do
        <<value::binary-size(^length), encoded::binary>> -> {value, encoded}
        <<value::binary-size(^length)>> -> {value, <<>>}
      end

    {{:integer, -:binary.decode_unsigned(complement(value))}, encoded}
  end

  defp decode(<<0x0C, value::binary-size(8)>> <> encoded, _) do
    <<value::integer-size(64)>> = complement(value)
    {{:integer, -value}, encoded}
  end

  defp decode(<<0x0D, value::binary-size(7)>> <> encoded, _) do
    <<value::integer-size(56)>> = complement(value)
    {{:integer, -value}, encoded}
  end

  defp decode(<<0x0E, value::binary-size(6)>> <> encoded, _) do
    <<value::integer-size(48)>> = complement(value)
    {{:integer, -value}, encoded}
  end

  defp decode(<<0x0F, value::binary-size(5)>> <> encoded, _) do
    <<value::integer-size(40)>> = complement(value)
    {{:integer, -value}, encoded}
  end

  defp decode(<<0x10, value::binary-size(4)>> <> encoded, _) do
    <<value::integer-size(32)>> = complement(value)
    {{:integer, -value}, encoded}
  end

  defp decode(<<0x11, value::binary-size(3)>> <> encoded, _) do
    <<value::integer-size(24)>> = complement(value)
    {{:integer, -value}, encoded}
  end

  defp decode(<<0x12, value::binary-size(2)>> <> encoded, _) do
    <<value::integer-size(16)>> = complement(value)
    {{:integer, -value}, encoded}
  end

  defp decode(<<0x13, value::binary-size(1)>> <> encoded, _) do
    <<value::integer-size(8)>> = complement(value)
    {{:integer, -value}, encoded}
  end

  defp decode(<<0x14>> <> encoded, _) do
    {{:integer, 0}, encoded}
  end

  defp decode(<<0x15, value::binary-size(1)>> <> encoded, _) do
    <<value::integer-size(8)>> = value
    {{:integer, value}, encoded}
  end

  defp decode(<<0x16, value::binary-size(2)>> <> encoded, _) do
    <<value::integer-size(16)>> = value
    {{:integer, value}, encoded}
  end

  defp decode(<<0x17, value::binary-size(3)>> <> encoded, _) do
    <<value::integer-size(24)>> = value
    {{:integer, value}, encoded}
  end

  defp decode(<<0x18, value::binary-size(4)>> <> encoded, _) do
    <<value::integer-size(32)>> = value
    {{:integer, value}, encoded}
  end

  defp decode(<<0x19, value::binary-size(5)>> <> encoded, _) do
    <<value::integer-size(40)>> = value
    {{:integer, value}, encoded}
  end

  defp decode(<<0x1A, value::binary-size(6)>> <> encoded, _) do
    <<value::integer-size(48)>> = value
    {{:integer, value}, encoded}
  end

  defp decode(<<0x1B, value::binary-size(7)>> <> encoded, _) do
    <<value::integer-size(56)>> = value
    {{:integer, value}, encoded}
  end

  defp decode(<<0x1C, value::binary-size(8)>> <> encoded, _) do
    <<value::integer-size(64)>> = value
    {{:integer, value}, encoded}
  end

  defp decode(<<0x1D, length::integer-size(8)>> <> encoded, _) do
    {value, encoded} =
      case encoded do
        <<value::binary-size(^length), encoded::binary>> -> {value, encoded}
        <<value::binary-size(^length)>> -> {value, <<>>}
      end

    {{:integer, :binary.decode_unsigned(value)}, encoded}
  end

  defp decode(<<@float_typecode, value::binary-size(4)>> <> encoded, _) do
    value =
      case do_float_decode(value) do
        <<0::size(1), 0xFF::size(8), 0::size(23)>> -> :infinity
        <<1::size(1), 0xFF::size(8), 0::size(23)>> -> :neg_infinity
        <<0::size(1), 0xFF::size(8), _::size(23)>> -> :nan
        <<1::size(1), 0xFF::size(8), _::size(23)>> -> :neg_nan
        <<value::float-32>> -> value
      end

    {{:float, value}, encoded}
  end

  defp decode(<<@double_typecode, value::binary-size(8)>> <> encoded, _) do
    value =
      case do_float_decode(value) do
        <<0::size(1), 0x7FF::size(11), 0::size(52)>> -> :infinity
        <<1::size(1), 0x7FF::size(11), 0::size(52)>> -> :neg_infinity
        <<0::size(1), 0x7FF::size(11), _::size(52)>> -> :nan
        <<1::size(1), 0x7FF::size(11), _::size(52)>> -> :neg_nan
        <<value::float-64>> -> value
      end

    {{:double, value}, encoded}
  end

  defp decode(<<@false_typecode>> <> encoded, _) do
    {{:boolean, false}, encoded}
  end

  defp decode(<<@true_typecode>> <> encoded, _) do
    {{:boolean, true}, encoded}
  end

  defp decode(
         <<@uuid_typecode>> <>
           <<a1::4, a2::4, a3::4, a4::4, a5::4, a6::4, a7::4, a8::4, b1::4, b2::4, b3::4, b4::4,
             c1::4, c2::4, c3::4, c4::4, d1::4, d2::4, d3::4, d4::4, e1::4, e2::4, e3::4, e4::4,
             e5::4, e6::4, e7::4, e8::4, e9::4, e10::4, e11::4,
             e12::4>> <>
           encoded,
         _
       ) do
    {{:uuid,
      <<d(a1), d(a2), d(a3), d(a4), d(a5), d(a6), d(a7), d(a8), ?-, d(b1), d(b2), d(b3), d(b4),
        ?-, d(c1), d(c2), d(c3), d(c4), ?-, d(d1), d(d2), d(d3), d(d4), ?-, d(e1), d(e2), d(e3),
        d(e4), d(e5), d(e6), d(e7), d(e8), d(e9), d(e10), d(e11), d(e12)>>}, encoded}
  end

  defp decode(<<@versionstamp_typecode, value::binary-size(12)>> <> encoded, _) do
    {{:versionstamp, Versionstamp.load(value)}, encoded}
  end

  defp decode(<<typecode>> <> _ = data, extension)
       when extension != nil and typecode in @user_typecodes do
    extension.decode(data)
  end

  defp decode(<<typecode>>, _) do
    raise ArgumentError, "unsupported typecode: #{typecode}"
  end

  # Encoders

  defp encode({:null, nil}, state, _), do: {<<@null_typecode>>, state}

  defp encode({:binary, data}, state, _) do
    {<<@bytes_typecode>> <>
       :binary.replace(data, <<@null_typecode>>, <<@null_typecode, @escape_typecode>>, [:global]) <>
       <<@null_typecode>>, state}
  end

  defp encode({:string, data}, state, _) do
    {<<@string_typecode>> <>
       :binary.replace(
         to_string(data),
         <<@null_typecode>>,
         <<@null_typecode, @escape_typecode>>,
         [:global]
       ) <> <<@null_typecode>>, state}
  end

  defp encode({:list, data}, state, opts) do
    prefix = Keyword.fetch!(opts, :prefix) <> <<@tuple_typecode>>

    {encoded, state} =
      Enum.reduce(data, {<<>>, state}, fn x, {key, state} ->
        case x do
          nil ->
            {key <> <<@null_typecode, @escape_typecode>>, state}

          _ ->
            opts = Keyword.put(opts, :prefix, prefix <> key)
            {e, o} = encode(x, state, opts)
            {key <> e, o}
        end
      end)

    {<<@tuple_typecode>> <> encoded <> <<@null_typecode>>, state}
  end

  defp encode({:integer, data}, state, _) do
    encoded =
      case data do
        v when v < -0xFFFFFFFFFFFFFFFF ->
          encoded = complement(:binary.encode_unsigned(-v))

          size =
            case byte_size(encoded) do
              s when s < 256 ->
                complement(<<s::integer-big-8>>)

              _ ->
                raise ArgumentError, "arbitrary-precisions integers must be less than 256 bytes"
            end

          <<0x0B>> <> size <> encoded

        v when v in -0xFFFFFFFFFFFFFFFF..-0x0100000000000000 ->
          <<0x0C>> <> complement(<<-v::integer-big-64>>)

        v when v in -0xFFFFFFFFFFFFFF..-0x01000000000000 ->
          <<0x0D>> <> complement(<<-v::integer-big-56>>)

        v when v in -0xFFFFFFFFFFFF..-0x010000000000 ->
          <<0x0E>> <> complement(<<-v::integer-big-48>>)

        v when v in -0xFFFFFFFFFF..-0x0100000000 ->
          <<0x0F>> <> complement(<<-v::integer-big-40>>)

        v when v in -0xFFFFFFFF..-0x01000000 ->
          <<0x10>> <> complement(<<-v::integer-big-32>>)

        v when v in -0xFFFFFF..-0x010000 ->
          <<0x11>> <> complement(<<-v::integer-big-24>>)

        v when v in -0xFFFF..-0x0100 ->
          <<0x12>> <> complement(<<-v::integer-big-16>>)

        v when v in -0xFF..-0x01 ->
          <<0x13>> <> complement(<<-v::integer-big-8>>)

        0 ->
          <<0x14>>

        v when v in 0x01..0xFF ->
          <<0x15>> <> <<v::integer-big-8>>

        v when v in 0x0100..0xFFFF ->
          <<0x16>> <> <<v::integer-big-16>>

        v when v in 0x010000..0xFFFFFF ->
          <<0x17>> <> <<v::integer-big-24>>

        v when v in 0x01000000..0xFFFFFFFF ->
          <<0x18>> <> <<v::integer-big-32>>

        v when v in 0x0100000000..0xFFFFFFFFFF ->
          <<0x19>> <> <<v::integer-big-40>>

        v when v in 0x010000000000..0xFFFFFFFFFFFF ->
          <<0x1A>> <> <<v::integer-big-48>>

        v when v in 0x01000000000000..0xFFFFFFFFFFFFFF ->
          <<0x1B>> <> <<v::integer-big-56>>

        v when v in 0x0100000000000000..0xFFFFFFFFFFFFFFFF ->
          <<0x1C>> <> <<v::integer-big-64>>

        v when v > 0xFFFFFFFFFFFFFFFF ->
          encoded = :binary.encode_unsigned(v)

          size =
            case byte_size(encoded) do
              s when s < 256 ->
                <<s::integer-big-8>>

              _ ->
                raise ArgumentError, "arbitrary-precisions integers must be less than 256 bytes"
            end

          <<0x1D>> <> size <> encoded
      end

    {encoded, state}
  end

  defp encode({:float, :infinity}, state, _) do
    {<<@float_typecode>> <> do_float_encode(<<0::1, 0xFF::8, 0::1, 0::22>>), state}
  end

  defp encode({:float, :neg_infinity}, state, _) do
    {<<@float_typecode>> <> do_float_encode(<<1::1, 0xFF::8, 0::1, 0::22>>), state}
  end

  defp encode({:float, :nan}, state, _) do
    {<<@float_typecode>> <> do_float_encode(<<0::1, 0xFF::8, 1::1, 0::22>>), state}
  end

  defp encode({:float, :neg_nan}, state, _) do
    {<<@float_typecode>> <> do_float_encode(<<1::1, 0xFF::8, 1::1, 0::22>>), state}
  end

  defp encode({:float, data}, state, _) when data >= @min_f32 and data <= @max_f32 do
    {<<@float_typecode>> <> do_float_encode(<<data::float-big-32>>), state}
  end

  defp encode({:double, :infinity}, state, _) do
    {<<@double_typecode>> <> do_float_encode(<<0::1, 0x7FF::11, 0::1, 0::51>>), state}
  end

  defp encode({:double, :neg_infinity}, state, _) do
    {<<@double_typecode>> <> do_float_encode(<<1::1, 0x7FF::11, 0::1, 0::51>>), state}
  end

  defp encode({:double, :nan}, state, _) do
    {<<@double_typecode>> <> do_float_encode(<<0::1, 0x7FF::11, 1::1, 0::51>>), state}
  end

  defp encode({:double, :neg_nan}, state, _) do
    {<<@double_typecode>> <> do_float_encode(<<1::1, 0x7FF::11, 1::1, 0::51>>), state}
  end

  defp encode({:double, data}, state, _) do
    {<<@double_typecode>> <> do_float_encode(<<data::float-big-64>>), state}
  end

  defp encode({:boolean, false}, state, _), do: {<<@false_typecode>>, state}

  defp encode({:boolean, true}, state, _), do: {<<@true_typecode>>, state}

  defp encode(
         {:uuid,
          <<a1, a2, a3, a4, a5, a6, a7, a8, ?-, b1, b2, b3, b4, ?-, c1, c2, c3, c4, ?-, d1, d2,
            d3, d4, ?-, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12>>},
         state,
         _
       ) do
    {
      <<@uuid_typecode>> <>
        <<e(a1)::4, e(a2)::4, e(a3)::4, e(a4)::4, e(a5)::4, e(a6)::4, e(a7)::4, e(a8)::4,
          e(b1)::4, e(b2)::4, e(b3)::4, e(b4)::4, e(c1)::4, e(c2)::4, e(c3)::4, e(c4)::4,
          e(d1)::4, e(d2)::4, e(d3)::4, e(d4)::4, e(e1)::4, e(e2)::4, e(e3)::4, e(e4)::4,
          e(e5)::4, e(e6)::4, e(e7)::4, e(e8)::4, e(e9)::4, e(e10)::4, e(e11)::4, e(e12)::4>>,
      state
    }
  end

  defp encode({:versionstamp, data}, state, opts) do
    state =
      if Versionstamp.incomplete?(data) do
        key = Keyword.fetch!(opts, :prefix)
        [versionstamp: <<byte_size(key) + 1::integer-little-32>>]
      else
        state
      end

    {<<@versionstamp_typecode>> <> Versionstamp.dump(data), state}
  end

  defp encode({key, _} = data, _, opts) do
    case Keyword.get(opts, :extension) do
      nil -> raise ArgumentError, "unknown identity: #{key}"
      ext -> ext.encode(data)
    end
  end

  defp encode(nil, state, opts), do: encode({:null, nil}, state, opts)

  defp encode(
         <<a1, a2, a3, a4, a5, a6, a7, a8, ?-, b1, b2, b3, b4, ?-, c1, c2, c3, c4, ?-, d1, d2, d3,
           d4, ?-, e1, e2, e3, e4, e5, e6, e7, e8, e9, e10, e11, e12>> = data,
         state,
         opts
       )
       when a1 in @hex_chars and
              a2 in @hex_chars and
              a3 in @hex_chars and
              a4 in @hex_chars and
              a5 in @hex_chars and
              a6 in @hex_chars and
              a7 in @hex_chars and
              a8 in @hex_chars and
              b1 in @hex_chars and
              b2 in @hex_chars and
              b3 in @hex_chars and
              b4 in @hex_chars and
              c1 in @hex_chars and
              c2 in @hex_chars and
              c3 in @hex_chars and
              c4 in @hex_chars and
              d1 in @hex_chars and
              d2 in @hex_chars and
              d3 in @hex_chars and
              d4 in @hex_chars and
              e1 in @hex_chars and
              e2 in @hex_chars and
              e3 in @hex_chars and
              e4 in @hex_chars and
              e5 in @hex_chars and
              e6 in @hex_chars and
              e7 in @hex_chars and
              e8 in @hex_chars and
              e9 in @hex_chars and
              e10 in @hex_chars and
              e11 in @hex_chars and
              e12 in @hex_chars do
    encode({:uuid, data}, state, opts)
  end

  defp encode(data, state, opts) when is_binary(data) do
    encode({:binary, data}, state, opts)
  end

  defp encode(data, state, opts) when is_list(data) do
    encode({:list, data}, state, opts)
  end

  defp encode(data, state, opts) when is_integer(data) do
    encode({:integer, data}, state, opts)
  end

  defp encode(data, state, opts) when is_float(data) do
    type =
      if data >= @min_f32 and data <= @max_f32 do
        :float
      else
        :double
      end

    encode({type, data}, state, opts)
  end

  defp encode(data, state, opts) when is_boolean(data) do
    encode({:boolean, data}, state, opts)
  end

  defp encode(%Versionstamp{} = data, state, opts) do
    encode({:versionstamp, data}, state, opts)
  end

  defp encode(data, state, opts) do
    ext =
      case Keyword.get(opts, :extension) do
        nil -> raise ArgumentError, "unsupported data type: #{data}"
        ext -> ext
      end

    identity = ext.identify(data)
    {ext.encode({identity, data}), state}
  end

  # Helpers

  defp complement(<<>>), do: <<>>

  defp complement(<<n::integer-size(8), rest::binary>>) do
    <<:erlang.bnot(n)::integer-size(8), complement(rest)::binary>>
  end

  defp do_float_encode(<<head::integer-size(8), tail::binary>> = binary) do
    if Bitwise.band(head, 0x80) != 0x00 do
      complement(binary)
    else
      <<Bitwise.bxor(head, 0x80)>> <> tail
    end
  end

  defp do_float_decode(<<head::integer-size(8), tail::binary>> = binary) do
    if Bitwise.band(head, 0x80) == 0x00 do
      complement(binary)
    else
      <<Bitwise.bxor(head, 0x80)>> <> tail
    end
  end

  defp do_bytes_decode(<<0x00, 0xFF>> <> rest, acc),
    do: do_bytes_decode(rest, acc <> <<0x00>>)

  defp do_bytes_decode(<<0x00>> <> rest, acc), do: {acc, rest}

  defp do_bytes_decode(<<char::binary-size(1)>> <> rest, acc),
    do: do_bytes_decode(rest, acc <> <<char::binary-size(1)>>)

  defp do_string_decode(<<0x00, 0xFF>> <> rest, acc),
    do: do_string_decode(rest, acc <> <<0x00>>)

  defp do_string_decode(<<0x00>> <> rest, acc), do: {acc, rest}

  defp do_string_decode(<<char::utf8>> <> rest, acc),
    do: do_string_decode(rest, acc <> <<char::utf8>>)

  defp do_tuple_decode(<<0x00, 0xFF>> <> rest, acc, extension),
    do: do_tuple_decode(rest, [nil | acc], extension)

  defp do_tuple_decode(<<0x00>> <> rest, acc, _), do: {acc, rest}

  defp do_tuple_decode(rest, acc, extension) do
    {data, rest} = decode(rest, extension)
    do_tuple_decode(rest, [data | acc], extension)
  end

  defp d(0), do: ?0
  defp d(1), do: ?1
  defp d(2), do: ?2
  defp d(3), do: ?3
  defp d(4), do: ?4
  defp d(5), do: ?5
  defp d(6), do: ?6
  defp d(7), do: ?7
  defp d(8), do: ?8
  defp d(9), do: ?9
  defp d(10), do: ?a
  defp d(11), do: ?b
  defp d(12), do: ?c
  defp d(13), do: ?d
  defp d(14), do: ?e
  defp d(15), do: ?f

  defp e(?0), do: 0
  defp e(?1), do: 1
  defp e(?2), do: 2
  defp e(?3), do: 3
  defp e(?4), do: 4
  defp e(?5), do: 5
  defp e(?6), do: 6
  defp e(?7), do: 7
  defp e(?8), do: 8
  defp e(?9), do: 9
  defp e(?A), do: 10
  defp e(?B), do: 11
  defp e(?C), do: 12
  defp e(?D), do: 13
  defp e(?E), do: 14
  defp e(?F), do: 15
  defp e(?a), do: 10
  defp e(?b), do: 11
  defp e(?c), do: 12
  defp e(?d), do: 13
  defp e(?e), do: 14
  defp e(?f), do: 15
end
