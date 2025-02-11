defmodule FDBC.Tenant do
  @moduledoc """
  Represents a FoundationDB tenant. Tenants are optional named transaction
  domains that can be used to provide multiple disjoint key-spaces to client
  applications. A transaction created in a tenant will be limited to the keys
  contained within that tenant, and transactions operating on different tenants
  can use the same key names without interfering with each other.
  """

  alias FDBC.Database
  alias FDBC.Error
  alias FDBC.Future
  alias FDBC.NIF
  alias FDBC.Tuple
  alias FDBC.Transaction

  defstruct [:resource]

  @type t :: %__MODULE__{}

  @tenant_map_prefix "\xff\xff/management/tenant/map/"

  @doc """
  Creates a new tenant in the cluster.

  The tenant name can be either a binary or a tuple and cannot start with the
  `\\xff` byte. If a tuple is provided, the tuple will be packed using the
  tuple layer to generate the byte string tenant name.

  If a database is provided to this function for the `database_or_transaction`
  parameter, then this function will first check if the tenant already exists.
  If it does, it will fail with a `tenant_already_exists` error. Otherwise, it
  will create a transaction and attempt to create the tenant in a retry loop.
  If the tenant is created concurrently by another transaction, this function
  may still return successfully.

  If a transaction is provided to this function for the
  `database_or_transaction` parameter, then this function will not check if the
  tenant already exists. It is up to the user to perform that check if
  required. The user must also successfully commit the transaction in order for
  the creation to take effect.
  """
  @spec create(Database.t() | Transaction.t(), binary | [any]) :: :ok
  def create(database_or_transaction, name)

  def create(%Database{} = db, name) do
    key = key_for_name(name)

    Transaction.create(db)
    |> Transaction.change(special_key_space_enable_writes: true)
    |> do_create(key)
  end

  def create(%Transaction{} = tr, name) do
    key = key_for_name(name)
    tr = Transaction.change(tr, special_key_space_enable_writes: true)
    Transaction.set(tr, key, <<>>)
  end

  defp do_create(%Transaction{} = tr, key, checked \\ false) do
    try do
      unless checked do
        if Transaction.get(tr, key) do
          raise Error, code: 2132, message: "A tenant with the given name already exists"
        end
      end
    rescue
      e in FDBC.Error ->
        :ok = Transaction.on_error(tr, e)
        do_create(tr, key, false)
    end

    try do
      result = Transaction.set(tr, key, <<>>)
      :ok = Transaction.commit(tr)
      result
    rescue
      e in FDBC.Error ->
        :ok = Transaction.on_error(tr, e)
        do_create(tr, key, true)
    end
  end

  @doc """
  Delete a tenant from the cluster.

  The tenant name can be either a binary or a tuple. If a tuple is provided,
  the tuple will be packed using the tuple layer to generate the byte string
  tenant name.

  It is an error to delete a tenant that still has data. To delete a non-empty
  tenant, first clear all of the keys in the tenant.

  If a database is provided to this function for the `database_or_transaction`
  parameter, then this function will first check if the tenant already exists.
  If it does not, it will fail with a tenant_not_found error. Otherwise, it
  will create a transaction and attempt to delete the tenant in a retry loop.
  If the tenant is deleted concurrently by another transaction, this function
  may still return successfully.

  If a transaction is provided to this function for the
  `database_or_transaction` parameter, then this function will not check if the
  tenant already exists. It is up to the user to perform that check if
  required. The user must also successfully commit the transaction in order for
  the deletion to take effect.
  """
  @spec delete(Database.t() | Transaction.t(), binary | [any]) :: :ok
  def delete(database_or_transaction, name)

  def delete(%Database{} = db, name) do
    key = key_for_name(name)

    Transaction.create(db)
    |> Transaction.change(special_key_space_enable_writes: true)
    |> do_delete(key)
  end

  def delete(%Transaction{} = tr, name) do
    key = key_for_name(name)
    tr = Transaction.change(tr, special_key_space_enable_writes: true)
    Transaction.clear(tr, key)
  end

  defp do_delete(%Transaction{} = tr, key, checked \\ false) do
    try do
      unless checked do
        unless Transaction.get(tr, key) do
          raise Error, code: 2131, message: "Tenant does not exist"
        end
      end
    rescue
      e in FDBC.Error ->
        :ok = Transaction.on_error(tr, e)
        do_create(tr, key, false)
    end

    try do
      :ok = Transaction.clear(tr, key)
      :ok = Transaction.commit(tr)
      :ok
    rescue
      e in FDBC.Error ->
        :ok = Transaction.on_error(tr, e)
        do_create(tr, key, true)
    end
  end

  @doc """
  Gets the ID for a given tenant.

  The ID is the unique identifier assigned to the tenant by the cluster upon
  its creation.
  """
  @spec get_id(t) :: integer
  def get_id(tenant) do
    async_get_id(tenant) |> Future.resolve()
  end

  @doc """
  The same as `get_id/1` except it returns the unresolved future.
  """
  @spec async_get_id(t) :: Future.t(integer)
  def async_get_id(%__MODULE__{resource: resource}) do
    resource = NIF.tenant_get_id(resource)

    %Future{resource: resource}
  end

  @doc """
  Lists the tenants in the cluster.

  Tenants are fetched for the given name range and limit. Returning a list of key
  value pairs representing the tenants in the cluster where the key is the
  tenant name, while the value is a JSON blob of the tenant's metadata.

  ## Options

    * `:limit` - (integer) Indicates the maximum number of key-value tenant
      pairs to return.

  """
  @spec list(Database.t() | Transaction.t(), binary, binary, keyword) :: :ok
  def list(database_or_transaction, start, stop, opts \\ []) do
    FDBC.transact(database_or_transaction, fn tr ->
      start = key_for_name(start)
      stop = key_for_name(stop)
      limit = opts[:limit] || 0

      Transaction.get_range(tr, start, stop, limit: limit)
      |> Enum.map(fn {k, v} ->
        <<@tenant_map_prefix, name::binary>> = k
        {name, v}
      end)
    end)
  end

  @doc """
  Opens a tenant on the given database.

  All transactions created by this tenant will operate on the tenant’s
  key-space.
  """
  @spec open(Database.t(), binary) :: t
  def open(%Database{} = database, name) do
    case NIF.database_open_tenant(database.resource, name) do
      {:ok, resource} ->
        %__MODULE__{resource: resource}

      {:error, code, reason} ->
        raise FDBC.Error, message: reason, code: code
    end
  end

  ## Helpers

  defp key_for_name(name) when is_list(name), do: key_for_name(Tuple.pack(name))
  defp key_for_name(name) when is_binary(name), do: @tenant_map_prefix <> name
end
