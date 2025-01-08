defmodule FDBC do
  @moduledoc """
  An unofficial FoundationDB client for Elixir.

  FoundationDB is a distributed database designed to handle large volumes of
  structured data across clusters of commodity servers. It organises data as an
  ordered key-value store and employs ACID transactions for all operations. It
  is especially well-suited for read/write workloads but also has excellent
  performance for write-intensive workloads.

  This documentation makes use of and refers to the [upstream
  documentation](https://apple.github.io/foundationdb/index.html) where it
  makes sense.

  ## Foundation

  `FDBC.Foundation` is a wrapper around the database geared at easing use within applications.

  ```elixir
  defmodule Demo.Foundation do
    use FDBC.Foundation,
      otp_app: :demo,
      options: [
        transaction_retry_limit: 100,
        transaction_timeout: 60_000
      ]
  end
  ```

  Where the configuration for the Foundation must be in your application
  environment, usually defined in your `config/config.exs`:

  ```elixir
  config :demo, Demo.Foundation,
    cluster: "./fdb.cluster",
    directory: "demo"
  ```

  If your application was generated with a supervisor you will have a
  `lib/demo/application.ex` file containing the application start callback that
  defines and starts your supervisor. You just need to edit the `start/2`
  function to start the repo as a supervisor on your application's supervisor:

  ```elixir
  def start(_type, _args) do
    children = [
      Demo.Foundation,
    ]

    opts = [strategy: :one_for_one, name: Demo.Supervisor]
    Supervisor.start_link(children, opts)
  end
  ```
  """

  alias FDBC.Database
  alias FDBC.Future
  alias FDBC.NIF
  alias FDBC.Transaction

  @doc """
  Specifies the version of the API that the application uses. This allows
  future versions of FoundationDB to make API changes without breaking existing
  programs.

  If the version is not specified the latest currently supported API version is used.

  > #### Note {: .info}
  > This function must be called before any other part of this library, unless
    being used as part of an OTP application.
  """
  @spec api_version(integer | nil) :: :ok
  def api_version(version \\ nil)

  def api_version(nil) do
    version = NIF.get_max_api_version()
    :ok = NIF.select_api_version(version)
    :ok
  end

  def api_version(version) when is_integer(version) do
    :ok = NIF.select_api_version(version)
    :ok
  end

  @doc """
  Performs a transaction against the database.

  This is a convenience funtion that will create a transaction, passing it to
  the provided function, committing it and handling any retry logic based on
  the error returned.

  Furthermore a transaction can be passed to this function in which case it
  logically does nothing, this is useful to allow composition of transactional
  functions.

  The options are used for creating the transaction and therefore are the same
  as those for `FDBC.Transaction.create/2`.
  """
  @spec transact(Database.t() | Transaction.t(), (Transaction.t() -> any), keyword()) :: any
  def transact(database_or_transaction, fun, opts \\ [])

  def transact(%Database{} = database, fun, opts) do
    Transaction.create(database, opts) |> do_transact(fun)
  end

  def transact(%Transaction{} = tr, fun, _opts), do: fun.(tr)

  defp do_transact(%Transaction{} = transaction, fun) do
    result = fun.(transaction)
    :ok = Transaction.commit(transaction)
    result
  rescue
    e in FDBC.Error ->
      resource = NIF.transaction_on_error(transaction.resource, e.code)

      with nil <- %Future{resource: resource} |> Future.resolve() do
        :ok
      end

      do_transact(transaction, fun)
  end
end
