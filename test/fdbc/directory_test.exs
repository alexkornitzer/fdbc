defmodule FDBC.DirectoryTest do
  use ExUnit.Case

  alias FDBC.Database
  alias FDBC.Directory
  alias FDBC.Subspace
  alias FDBC.Transaction

  setup do
    db = Database.create()
    tr = Transaction.create(db)
    Transaction.clear_range(tr, <<>>, <<0xFF>>)
    :ok = Transaction.commit(tr)
    [db: db]
  end

  describe "create/4" do
    test "successfully creates a directory", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        :ok = Directory.create(r, tr, ["foo"])

        assert Directory.exists?(r, tr, ["foo"])
      end)
    end

    test "successfully creates a partition", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        :ok = Directory.create(r, tr, ["foo"], partition: true)

        assert Directory.exists?(r, tr, ["foo"])
      end)
    end

    test "successfully creates a nested directory", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        :ok = Directory.create(r, tr, ["foo"])
        :ok = Directory.create(r, tr, ["foo", "bar"])

        assert Directory.exists?(r, tr, ["foo", "bar"])
      end)
    end

    test "successfully creates a nested directory with parents", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        :ok = Directory.create(r, tr, ["foo", "bar"], parents: true)

        assert Directory.exists?(r, tr, ["foo", "bar"])
      end)
    end

    test "fails to create a nested directory missing parents", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        {:error, _} = Directory.create(r, tr, ["foo", "bar"])

        refute Directory.exists?(r, tr, ["foo", "bar"])
      end)
    end

    test "successfully creates a directory with a manual prefix", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new(allow_manual_prefixes: true)
        :ok = Directory.create(r, tr, ["foo"], prefix: <<0xCA, 0xFE>>)

        {:ok, dir} = Directory.open(r, tr, ["foo"])
        assert Subspace.new(<<0xCA, 0xFE>>) == Directory.subspace(dir)
      end)
    end

    test "fails to create a directory with a manual prefix when disallowed", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()

        assert_raise ArgumentError, fn ->
          :ok = Directory.create(r, tr, ["foo"], prefix: <<0xCA, 0xFE>>)
        end

        refute Directory.exists?(r, tr, ["foo"])
      end)
    end

    test "successfully creates a directory with a label", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        :ok = Directory.create(r, tr, ["foo"], label: "bar")
        {:ok, dir} = Directory.open(r, tr, ["foo"])

        assert "bar" == Directory.label(dir)
      end)
    end

    test "fails to create a directory with the partition label", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()

        assert_raise ArgumentError, fn ->
          :ok = Directory.create(r, tr, ["foo"], label: "partition")
        end

        refute Directory.exists?(r, tr, ["foo"])
      end)
    end

    test "disallows manual prefixes within a partition", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        :ok = Directory.create(r, tr, ["foo"], partition: true)

        assert_raise ArgumentError, fn ->
          :ok = Directory.create(r, tr, ["foo", "bar"], prefix: <<0xCA, 0xFE>>)
        end

        refute Directory.exists?(r, tr, ["foo", "bar"])
      end)
    end

    test "successfully creates a nested partition", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        :ok = Directory.create(r, tr, ["foo"], partition: true)
        :ok = Directory.create(r, tr, ["foo", "bar"], partition: true)

        {:ok, foo} = Directory.open(r, tr, ["foo"])
        {:ok, bar} = Directory.open(r, tr, ["foo", "bar"])

        prefix = foo.file_system.partition.content.key
        assert ^prefix <> _ = bar.file_system.node.key
      end)
    end
  end

  describe "exists?/3" do
    test "returns true if directory exists", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        :ok = Directory.create(r, tr, ["foo"])

        assert Directory.exists?(r, tr, ["foo"])
      end)
    end

    test "returns false if directory does not exist", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()

        refute Directory.exists?(r, tr, ["foo"])
      end)
    end
  end

  describe "label/1" do
    test "returns `nil` if no label has been set", context do
      FDBC.transact(context.db, fn _tr ->
        refute Directory.new() |> Directory.label()
      end)
    end

    test "returns `partition` for a partition directory", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        {:ok, dir} = Directory.open(r, tr, ["foo"], create: true, partition: true)
        assert "partition" == Directory.label(dir)
      end)
    end

    test "returns label for a labelled directory", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        {:ok, dir} = Directory.open(r, tr, ["foo"], create: true, label: "bar")
        assert "bar" == Directory.label(dir)
      end)
    end
  end

  describe "list/3" do
    test "returns subdirectories", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        :ok = Directory.create(r, tr, ["dir", "a"], parents: true)
        :ok = Directory.create(r, tr, ["dir", "b"], parents: true)
        :ok = Directory.create(r, tr, ["dir", "c"], parents: true)
        :ok = Directory.create(r, tr, ["dir", "d"], parents: true)
        :ok = Directory.create(r, tr, ["dir", "e"], parents: true)
        :ok = Directory.create(r, tr, ["dir", "f", "g"], parents: true)
        {:ok, dirs} = Directory.list(r, tr, ["dir"])

        assert ["a", "b", "c", "d", "e", "f"] == dirs
      end)
    end
  end

  describe "move/4" do
    test "successfully moves directory from one to another", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()

        :ok = Directory.create(r, tr, ["flip", "flop", "fly"], parents: true)
        :ok = Directory.move(r, tr, ["flip", "flop", "fly"], ["flip", "fly"])

        refute Directory.exists?(r, tr, ["flip", "flop", "fly"])
        assert Directory.exists?(r, tr, ["flip", "fly"])
      end)
    end

    test "fails to move directory on conflict", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        :ok = Directory.create(r, tr, ["flip", "flop", "fly"], parents: true)
        {:error, _} = Directory.move(r, tr, ["flip", "flop", "fly"], ["flip", "flop"])

        assert Directory.exists?(r, tr, ["flip", "flop", "fly"])
      end)
    end

    test "fails to move from one partition to another", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        :ok = Directory.create(r, tr, ["flip", "flop"], parents: true, partition: true)
        :ok = Directory.create(r, tr, ["flip", "flop", "fly"])
        {:error, _} = Directory.move(r, tr, ["flip", "flop", "fly"], ["foo"])

        assert Directory.exists?(r, tr, ["flip", "flop", "fly"])
      end)
    end

    test "successfully renames a directory", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        :ok = Directory.create(r, tr, ["flip", "flop"], parents: true)
        :ok = Directory.move(r, tr, ["flip", "flop"], ["flip", "fly"])

        refute Directory.exists?(r, tr, ["flip", "flop"])
        assert Directory.exists?(r, tr, ["flip", "fly"])
      end)
    end
  end

  describe "open/4" do
    test "successfully opens a directory", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        :ok = Directory.create(r, tr, ["foo"])
        {:ok, _} = Directory.open(r, tr, ["foo"])

        assert Directory.exists?(r, tr, ["foo"])
      end)
    end

    test "successfully opens a directory, creating it if missing", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        {:ok, _} = Directory.open(r, tr, ["foo"], create: true)

        assert Directory.exists?(r, tr, ["foo"])
      end)
    end

    test "fails to open a non existent directory", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        {:error, _} = Directory.open(r, tr, ["foo"])

        refute Directory.exists?(r, tr, ["foo"])
      end)
    end
  end

  describe "path/1" do
    test "returns the empty list for the root directory", context do
      FDBC.transact(context.db, fn _tr ->
        path = Directory.new() |> Directory.path()

        assert [] == path
      end)
    end

    test "returns the correct path for a given directory", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        {:ok, dir} = Directory.open(r, tr, ["flip", "flop", "fly"], create: true, parents: true)

        assert ["flip", "flop", "fly"] == Directory.path(dir)
      end)
    end
  end

  describe "remove/3" do
    test "successfully removes directory", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        :ok = Directory.create(r, tr, ["flip", "flop"], parents: true)
        :ok = Directory.remove(r, tr, ["flip", "flop"])

        refute Directory.exists?(r, tr, ["flip", "flop"])
        assert length(Transaction.get_range(tr, <<0x00>>, <<0xFF>>)) == 6
      end)
    end

    test "successfully removes directory and its subdirectories", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        :ok = Directory.create(r, tr, ["flip", "flop", "fly"], parents: true)
        :ok = Directory.remove(r, tr, ["flip"])

        refute Directory.exists?(r, tr, ["flip"])
        assert length(Transaction.get_range(tr, <<0x00>>, <<0xFF>>)) == 5
      end)
    end
  end

  describe "subspace/1" do
    test "returns `nil` for the root partition" do
      refute Directory.new() |> Directory.subspace()
    end

    test "returns `nil` for the any partition", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        {:ok, dir} = Directory.open(r, tr, ["foo"], create: true, partition: true)
        refute Directory.subspace(dir)
      end)
    end

    test "returns subspace for a writeable directory", context do
      FDBC.transact(context.db, fn tr ->
        r = Directory.new()
        {:ok, dir} = Directory.open(r, tr, ["foo"], create: true)
        assert %Subspace{} = Directory.subspace(dir)
      end)
    end
  end
end
