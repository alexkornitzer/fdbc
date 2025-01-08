defmodule FDBC.Error do
  @moduledoc """
  An exception raised when an error is returned from a call to the FDBC NIF.

  `FDBC.Error` exceptions have two fields, `:message` (a `t:String.t/0`) and
  `:code` (a `t:integer`) which are public and can be accessed freely when
  reading `FDBC.Error` exceptions. These values are representative of the
  FoundationDB [error codes](https://apple.github.io/foundationdb/api-error-codes.html).
  """

  defexception [:message, :code]

  @type t :: %__MODULE__{message: binary, code: integer}
end
