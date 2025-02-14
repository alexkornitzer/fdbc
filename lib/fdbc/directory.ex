defmodule FDBC.Directory do
  @moduledoc """
  FoundationDB provides the directories layer as a tool for managing related
  subspaces. Directories are a recommended approach for administering
  applications. Each application should create or open at least one directory
  to manage its subspaces.

  Directories are identified by hierarchical paths analogous to the paths in a
  Unix-like file system. A path is represented as a tuple of strings. Each
  directory has an associated subspace used to store its content. The directory
  layer maps each path to a short prefix used for the corresponding subspace.
  In effect, directories provide a level of indirection for access to
  subspaces.

  This design has significant benefits: while directories are logically
  hierarchical as represented by their paths, their subspaces are not
  physically nested in a corresponding way. For example, suppose we create a
  few directories with increasing paths, such as:

  ```elixir
  iex> r = FDBC.Directory.new()
  iex> a = FDBC.Directory.create!(r, db, ['alpha'])
  iex> b = FDBC.Directory.create!(r, db, ['alpha', 'bravo'])
  iex> c = FDBC.Directory.create!(r, db, ['alpha', 'bravo', 'charlie'])
  ```

  The prefixes of `a`, `b`, and `c` are allocated independently and will
  usually not increase in length. The indirection from paths to subspaces
  allows keys to be kept short and makes it fast to move directories (i.e.,
  rename their paths).

  Paths in the directory layer are always relative. In particular, paths are
  interpreted relative to the directory in which an operation is performed. For
  example, we could have created the directories `a`, `b`, and `c` as follows:

  ```elixir
  iex> r = FDBC.Directory.new()
  iex> a = FDBC.Directory.open!(r, db, ['alpha'], create: true)
  iex> b = FDBC.Directory.open!(a, db, ['bravo'], create: true)
  iex> c = FDBC.Directory.open!(b, db, ['charlie'], create: true)
  ```

  ## Usage

  Unlike the upstream implementations this one tries to remain closer to the
  analogy of a 'Unix-like' file system. By default `new/1` will create default
  root directory which is that with a content subspace of `<<>>` and a metadata
  prefix of `<<0xFE>>`.

  > #### Info {: .info}
  > When comparing to upstream documentation what its refers to as
  `node_subspace` is called `metadata` here.

  The `create/3` function will create a new directory but not return it, to get
  the newly created directory in the same request `open/3` should be used:

  ```elixir
  iex> :ok = FDBC.Directory.new() |> FDBC.Directory.create!(db, ['users'])
  iex> users_dir = FDBC.Directory.new() |> FDBC.Directory.open!(db, ['users'], create: true)
  ```

  A directory that is not a partition exposes its `FDBC.Subspace` for usage.
  The root directory is a partition.

  ```elixir
  iex> r = FDBC.Directory.new()
  iex> nil = FDBC.Directory.subspace(r)
  iex> users_dir = FDBC.Directory.open!(r, db, ["users"], create: true)
  iex> %FDBC.Subspace{} = FDBC.Directory.subspace(users_dir)
  ```

  If the directory was created previously (e.g., in a prior session or by
  another client), you can open it via its path:

  ```elixir
  iex> {:ok, users} = FDBC.Directory.new() |> FDBC.Directory.open(db, ['users'])
  ```

  Similarly to a file system directories can be enumerated, moved and removed,
  where all paths are relative to the directory provided:

  ```elixir
  iex> subdirs = FDBC.Directory.new() |> FDBC.Directory.list(db, [])
  iex> FDBC.Directory.new() |> FDBC.Directory.move(db, ["a"], ["b"])
  iex> FDBC.Directory.new() |> FDBC.Directory.remove(db, ["a"])
  ```

  ## Partitions

  Under normal operation, a directory does not share a common key prefix with
  its subdirectories. As a result, related directories are not necessarily
  located together in key-space. This means that you cannot use a single range
  query to read all contents of a directory and its descendants simultaneously,
  for example.

  For most applications this behavior is acceptable, but in some cases it may
  be useful to have a directory tree in your hierarchy where every directory
  shares a common prefix. For this purpose, the directory layer supports
  creating partitions within a directory. A partition is a directory whose
  prefix is prepended to all of its descendant directories’ prefixes.

  A partition can be created by setting `:partition` to in options, this will
  also set the directories label to `partition` and thus trying to create a
  partition with a custom label will raise an exception.

  ```elixir
  iex> r = FDBC.Directory.new()
  iex> FDBC.Directory.create(r, db, ["p1"], partition: true)
  iex> users_dir = FDBC.Directory.open!(r, db, ["p1", "users"], create: true)
  ```

  Directory partitions have the following drawbacks, and in general they should
  not be used unless specifically needed:

  * Directories cannot be moved between different partitions.

  * Directories in a partition have longer prefixes than their counterparts
  outside of partitions, which reduces performance. Nesting partitions inside
  of other partitions results in even longer prefixes.

  * The root directory of a partition cannot be used to pack/unpack keys and
  therefore cannot be used to create subspaces. You must create at least one
  subdirectory of a partition in order to store content in it.

  """

  alias FDBC.Database
  alias FDBC.Directory.FileSystem
  alias FDBC.Subspace
  alias FDBC.Transaction

  defstruct [
    :allow_manual_prefixes,
    :file_system
  ]

  # Copied from `FDBC.Directory.FileSystem` so that the type is clear to anyone
  # that wishes to override it.
  @type partition :: %{
          content: Subspace.t(),
          metadata: Subspace.t()
        }

  @type t :: %__MODULE__{}

  @doc """
  The default instance of the root directory.

  ## Options

    * `:allow_manual_prefixes` - allows the creation of manual prefixes, if not
      enabled attempts to provide a manual prefix will result in an exception.

    * `:partition` - allows for a different partition other than the root to be
      used. By default an empty prefix is used for the partition's content
      subspace, while the value `0xFE` is used for the metadata subspace.

  """
  @spec new(keyword) :: t
  def new(opts \\ []) do
    allow_manual_prefixes = Keyword.get(opts, :allow_manual_prefixes, false)

    partition =
      case Keyword.get(opts, :partition) do
        nil ->
          content = [] |> Subspace.new()
          metadata = Subspace.new(<<0xFE>>) |> Subspace.concat([])

          %{
            content: content,
            metadata: metadata
          }

        partition ->
          partition
      end

    file_system = FileSystem.new(partition)

    %__MODULE__{
      allow_manual_prefixes: allow_manual_prefixes,
      file_system: file_system
    }
  end

  @doc """
  Creates a directory at the given path relative to the current working directory.

  ## Options

    * `:label` - the label to assign to the directory to aid in its identification.

    * `:parents` - allows the creation of parent directories if not present,
      otherwise an exception will be raised.

    * `:partition` - the directory is created as a partition whose prefix is
      prepended to all of its descendant directories’ prefixes.

    * `:prefix` - creates the directory with the provided prefix, otherwise
      the prefix is allocated automatically.
  """
  @spec create(t, Database.t() | Transaction.t(), [String.t()], keyword) :: :ok | {:error, term}
  def create(%__MODULE__{} = dir, db_or_tr, path, opts \\ []) do
    opts = Keyword.put(opts, :create, true)

    if exists?(dir, db_or_tr, path) do
      {:error, "the directory already exists"}
    else
      FDBC.transact(db_or_tr, fn tr ->
        with {:ok, _} <- open(dir, tr, path, opts) do
          :ok
        end
      end)
    end
  end

  @doc """
  Similar to `create/4` but raises an error on failure.
  """
  @spec create!(t, Database.t() | Transaction.t(), [String.t()], keyword) :: :ok
  def create!(dir, db_or_tr, path, opts \\ []) do
    case create(dir, db_or_tr, path, opts) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  @doc """
  Checks if a directory exists for the given path relative to the current
  working directory.

  If no path is provided then the current working directory is checked.
  """
  @spec exists?(t, Database.t() | Transaction.t(), [String.t()]) :: boolean
  def exists?(dir, db_or_tr, path \\ [])

  def exists?(%__MODULE__{} = dir, db_or_tr, []) do
    FDBC.transact(db_or_tr, fn tr ->
      case FileSystem.root(dir.file_system)
           |> FileSystem.find_directory(tr, dir.file_system.path) do
        {:ok, nil} -> false
        {:ok, _} -> true
        {:error, error} -> raise error
      end
    end)
  end

  def exists?(%__MODULE__{} = dir, db_or_tr, path) do
    path = path_to_tuple(path)

    FDBC.transact(db_or_tr, fn tr ->
      case FileSystem.root(dir.file_system)
           |> FileSystem.find_directory(tr, dir.file_system.path ++ path) do
        {:ok, nil} -> false
        {:ok, _} -> true
        {:error, error} -> raise error
      end
    end)
  end

  @doc """
  Returns the label of the provided directory.
  """
  @spec label(t) :: binary | nil
  def label(%__MODULE__{} = dir) do
    case dir.file_system.label do
      "" -> nil
      label -> label
    end
  end

  @doc """
  List the immediate subdirectories for the given path relative to the current
  working directory.
  """
  @spec list(t, Database.t() | Transaction.t(), [String.t()]) ::
          {:ok, [String.t()]} | {:error, term}
  def list(%__MODULE__{} = dir, db_or_tr, path \\ []) do
    path = path_to_tuple(path)

    FDBC.transact(db_or_tr, fn tr ->
      FileSystem.root(dir.file_system)
      |> FileSystem.list_directory(tr, dir.file_system.path ++ path)
    end)
  end

  @doc """
  Similar to `list/3` but raises an error on failure.
  """
  @spec list!(t, Transaction.t(), [String.t()]) :: [String.t()]
  def list!(dir, tr, path \\ []) do
    case list(dir, tr, path) do
      {:ok, subdirs} -> subdirs
      {:error, error} -> raise error
    end
  end

  @doc """
  Move the `source` directory to the `destination` directory, relative to the
  current working directory.

  ## Options

    * `:parents` - allows the creation of parent directories if not present,
      otherwise an error will occur.
  """
  @spec move(t, Database.t() | Transaction.t(), [String.t()], [String.t()], keyword) ::
          :ok | {:error, term}
  def move(%__MODULE__{} = dir, db_or_tr, source, destination, opts \\ []) do
    source = path_to_tuple(source)
    destination = path_to_tuple(destination)

    if source == [] do
      {:error, "the current working directory cannot be moved while active"}
    else
      FDBC.transact(db_or_tr, fn tr ->
        FileSystem.root(dir.file_system)
        |> FileSystem.move_directory(
          tr,
          dir.file_system.path ++ source,
          dir.file_system.path ++ destination,
          opts
        )
      end)
    end
  end

  @doc """
  Similar to `move/4` but raises an error on failure.
  """
  @spec move!(t, Transaction.t(), [String.t()], [String.t()], keyword) :: :ok
  def move!(dir, tr, source, destination, opts \\ []) do
    case move(dir, tr, source, destination, opts) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  @doc """
  Opens the directory at the given path relative to the current working directory.

  ## Options

    * `:create` - allow the directory to be created if it does not exist,
      in which case the options from `create/3` will also be honoured.

  """
  @spec open(t, Database.t() | Transaction.t(), [String.t()], keyword) ::
          {:ok, t} | {:error, term}
  def open(%__MODULE__{} = directory, db_or_tr, path, opts \\ []) do
    if path == [] && directory.file_system.path == [] do
      if directory.file_system.root == nil do
        raise ArgumentError, "cannot open the root directory"
      end
    end

    prefix = Keyword.get(opts, :prefix, nil)

    if prefix && !directory.allow_manual_prefixes do
      raise ArgumentError, "cannot specify a prefix unless manual prefixes are enabled"
    end

    path = path_to_tuple(path)

    FDBC.transact(db_or_tr, fn tr ->
      path = directory.file_system.path ++ path

      with {:ok, file_system} <-
             FileSystem.root(directory.file_system) |> FileSystem.change_directory(tr, path, opts) do
        {:ok, %__MODULE__{directory | file_system: file_system}}
      end
    end)
  end

  @doc """
  Similar to `open/4` but raises an error on failure.
  """
  @spec open!(t, Transaction.t(), [String.t()], keyword) :: t
  def open!(dir, tr, path, opts \\ []) do
    case open(dir, tr, path, opts) do
      {:ok, dir} -> dir
      {:error, error} -> raise error
    end
  end

  @doc """
  Returns the path of the provided directory.
  """
  @spec path(t) :: [String.t()]
  def path(%__MODULE__{} = dir) do
    Keyword.values(dir.file_system.path)
  end

  @doc """
  Remove the directory at the given path relative to the current working
  directory.
  """
  @spec remove(t, Database.t() | Transaction.t(), [String.t()]) :: :ok | {:error, term}
  def remove(directory, db_or_tr, path)

  def remove(%__MODULE__{}, _, []) do
    raise ArgumentError, "cannot remove current working directory"
  end

  def remove(%__MODULE__{} = dir, db_or_tr, path) do
    path = path_to_tuple(path)

    FDBC.transact(db_or_tr, fn tr ->
      FileSystem.root(dir.file_system)
      |> FileSystem.remove_directory(tr, dir.file_system.path ++ path)
    end)
  end

  @doc """
  Similar to `remove/3` but raises an error on failure.
  """
  @spec remove!(t, Transaction.t(), [String.t()]) :: :ok
  def remove!(dir, tr, path) do
    case remove(dir, tr, path) do
      :ok -> :ok
      {:error, error} -> raise error
    end
  end

  @doc """
  Returns the `Subspace.t()` of the provided directory, or `nil` if the
  directory is a partition.
  """
  @spec subspace(t) :: Subspace.t() | nil
  def subspace(%__MODULE__{} = dir) do
    root =
      dir.file_system.partition.metadata
      |> Subspace.concat([dir.file_system.partition.metadata.key])

    if root == dir.file_system.node do
      nil
    else
      [prefix] = Subspace.unpack(dir.file_system.partition.metadata, dir.file_system.node.key)
      Subspace.new(prefix)
    end
  end

  @doc """
  Similar to `subspace/1` but raises an error on failure.
  """
  @spec subspace!(t) :: Subspace.t()
  def subspace!(dir) do
    case subspace(dir) do
      nil ->
        raise ArgumentError, "a partition cannot be used as a subspace"

      subspace ->
        subspace
    end
  end

  ## Helpers

  defp path_to_tuple([]), do: []

  defp path_to_tuple(path) when is_binary(path), do: [{:string, path}]

  defp path_to_tuple([{:string, _} = head | tail]) do
    [head | path_to_tuple(tail)]
  end

  defp path_to_tuple([head | tail]) when is_binary(head) do
    [{:string, head} | path_to_tuple(tail)]
  end

  defp path_to_tuple([head | tail]) when is_list(head) do
    [path_to_tuple(head) | path_to_tuple(tail)]
  end

  defp path_to_tuple(_) do
    raise ArgumentError, "path must be a unicode string or list of unicode strings"
  end
end
