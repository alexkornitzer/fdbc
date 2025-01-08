defmodule FDBC.Tenant do
  @moduledoc """
  Represents a FoundationDB tenant. Tenants are optional named transaction
  domains that can be used to provide multiple disjoint key-spaces to client
  applications. A transaction created in a tenant will be limited to the keys
  contained within that tenant, and transactions operating on different tenants
  can use the same key names without interfering with each other.
  """

  alias FDBC.Database
  alias FDBC.NIF

  defstruct [:resource]

  @type t :: %__MODULE__{}

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
end
