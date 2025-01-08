defmodule FDBC.TenantTest do
  use ExUnit.Case, async: true

  alias FDBC.Database
  alias FDBC.Tenant

  setup_all do
    db = Database.create()
    [db: db]
  end

  describe "open/2" do
    test "database can create open tenant", context do
      assert(%Tenant{} = Tenant.open(context.db, "tenant"))
    end
  end
end
