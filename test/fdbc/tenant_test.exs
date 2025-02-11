defmodule FDBC.TenantTest do
  use ExUnit.Case, async: false

  alias FDBC.Database
  alias FDBC.Tenant
  alias FDBC.Transaction

  setup do
    db = Database.create()
    tr = Transaction.create(db, special_key_space_enable_writes: true)
    Transaction.clear_starts_with(tr, "\xff\xff/management/tenant/map/")
    :ok = Transaction.commit(tr)
    [db: db]
  end

  describe "create/2" do
    test "can create tenant", context do
      :ok = Tenant.create(context.db, "foo")
    end

    test "raises error if tenant already exists", context do
      :ok = Tenant.create(context.db, "foo")

      assert_raise FDBC.Error, fn ->
        :ok = Tenant.create(context.db, "foo")
      end
    end
  end

  describe "delete/2" do
    test "can delete a tenant", context do
      :ok = Tenant.create(context.db, "foo")
      :ok = Tenant.delete(context.db, "foo")
    end

    test "raises error if tenant does not exist", context do
      assert_raise FDBC.Error, fn ->
        :ok = Tenant.delete(context.db, "foo")
      end
    end
  end

  describe "get_id/1" do
    test "can get id of tenant", context do
      :ok = Tenant.create(context.db, "foo")
      assert Tenant.open(context.db, "foo") |> Tenant.get_id()
    end

    test "raises error if tenant does not exist", context do
      assert_raise FDBC.Error, fn ->
        Tenant.open(context.db, "foo") |> Tenant.get_id()
      end
    end
  end

  describe "list/4" do
    test "can list tenants", context do
      :ok = Tenant.create(context.db, "foo")

      assert [] !=
               Tenant.list(
                 context.db,
                 "\x00",
                 "\xff"
               )
    end
  end

  describe "open/2" do
    test "database can create open tenant", context do
      assert(%Tenant{} = Tenant.open(context.db, "tenant"))
    end
  end
end
