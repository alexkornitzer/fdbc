defmodule FDBC.Foundation do
  @moduledoc """
  A convenience behaviour to ease the use of a database instance within an OTP
  application while also promoting best practices.

  Firstly the configuration for the behaviour must be set and is designed in
  such a way that it can be changed for each environment:

  ```elixir
  config :example, Example.Foundation,
    cluster: "./fdb.cluster",  # Path to the cluster file
    directory: "example",      # The directory the application should operate under
  ```

  The behaviour can then be used to create and manage a database instance.

  ```elixir
  defmodule Example.Foundation do
    use FDBC.Foundation,
      otp_app: :example,
      options: [
        transaction_retry_limit: 100,
        transaction_timeout: 60_000
      ]
  end
  ```

  The module must be added to the applications supervision tree, once this is
  done it can then be used to easily transact with FoundationDB.

  ```elixir
  defmodule Example.App do
    alias FDBC.{Directory, Subspace, Transaction}
    alias Example.Foundation

    def get_value(key) do
      Foundation.transact(fn tr, dir ->
        key = Directory.subspace(dir) |> Subspace.pack([key])
        Transaction.get(tr, key)
      end)
    end

    def set_value(key, value) do
      Foundation.transact(fn tr, dir ->
        key = Directory.subspace(dir) |> Subspace.pack([key])
        Transaction.set(tr, key, value)
      end)
    end
  end
  ```
  """

  alias FDBC.Database
  alias FDBC.Directory
  alias FDBC.Transaction

  @doc """
  Returns the underlying database instance.
  """
  @callback database() :: Database.t()

  @doc """
  Performs a transaction against the database.

  The directory passed to `fun` is that which namespaces the application as
  defined in the configuration.

  The options are used for creating the transaction and therefore are the same
  as those for `FDBC.Transaction.create/2`.
  """
  @callback transact(fun :: (Transaction.t(), Directory.t() -> any), opts :: keyword) :: any

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      use GenServer

      @otp_app Keyword.fetch!(opts, :otp_app)
      @options Keyword.get(opts, :options, [])

      def start_link(_opts) do
        GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
      end

      def database() do
        {db, _} = :persistent_term.get(:db)
        db
      end

      def transact(fun, opts \\ []) when is_function(fun) do
        {db, root} = :persistent_term.get(:db)

        FDBC.transact(
          db,
          fn tr ->
            dir = FDBC.Directory.new() |> FDBC.Directory.open!(tr, root)
            fun.(tr, dir)
          end,
          opts
        )
      end

      ## Callbacks

      @impl true
      def init(:ok) do
        config = Application.get_env(@otp_app, __MODULE__, [])

        cluster_file =
          case Keyword.get(config, :cluster) do
            nil -> nil
            path -> Path.expand(path)
          end

        path =
          case Keyword.fetch!(config, :directory) do
            x when is_list(x) -> x
            x -> [x]
          end

        db = FDBC.Database.create(cluster_file, @options)

        FDBC.transact(db, fn tr ->
          _ = FDBC.Directory.new() |> FDBC.Directory.open!(tr, path, create: true)
        end)

        :ok = :persistent_term.put(:db, {db, path})
        table = :ets.new(__MODULE__, [:named_table, read_concurrency: true])
        {:ok, table}
      end
    end
  end
end
