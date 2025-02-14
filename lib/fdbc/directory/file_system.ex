defmodule FDBC.Directory.FileSystem do
  @moduledoc false

  alias FDBC.Directory.HighContentionAllocator
  alias FDBC.Subspace
  alias FDBC.Transaction
  alias FDBC.Tuple

  @subdirs 0
  @version {1, 0, 0}

  @type partition :: %{
          content: Subspace.t(),
          metadata: Subspace.t()
        }

  defstruct [
    :label,
    :node,
    :partition,
    :path,
    :root
  ]

  def new(partition) do
    node = partition_node(partition)

    %__MODULE__{
      label: "",
      partition: partition,
      node: node,
      path: [],
      root: nil
    }
  end

  def change_directory(%__MODULE__{} = fs, %Transaction{} = tr, path, opts \\ []) do
    create = Keyword.get(opts, :create, false)
    parents = Keyword.get(opts, :parents, false)
    prefix = Keyword.get(opts, :prefix, nil)

    with :ok <- check_version(fs, tr, read_only: !create) do
      case find(fs, tr, path) do
        nil ->
          if create do
            label =
              if Keyword.get(opts, :partition, false) do
                "partition"
              else
                case Keyword.get(opts, :label, "") do
                  "partition" ->
                    raise ArgumentError, "the partition label is reserved"

                  label when is_binary(label) ->
                    label

                  _ ->
                    raise ArgumentError, "the label must be a string"
                end
              end

            with {:ok, parent} <- fetch_parent(fs, tr, path, parents),
                 :ok <- check_version(parent, tr),
                 {:ok, prefix} <- allocate_prefix(parent, tr, prefix) do
              :ok =
                Transaction.set(
                  tr,
                  Subspace.pack(parent.node, [@subdirs, List.last(path)]),
                  prefix
                )

              subdir = Subspace.concat(parent.partition.metadata, [prefix])
              :ok = Transaction.set(tr, Subspace.pack(subdir, ["layer"]), label)
              {:ok, select(parent, label, subdir, List.last(path))}
            end
          else
            {:error, "the directory does not exist"}
          end

        fs ->
          fs =
            case Enum.drop(path, length(fs.path)) do
              [] ->
                fs

              path ->
                change_directory(fs, tr, path, create: create, parents: parents)
            end

          {:ok, fs}
      end
    end
  end

  def find_directory(%__MODULE__{} = fs, %Transaction{} = tr, path) do
    with :ok <- check_version(fs, tr, read_only: true) do
      {:ok, find(fs, tr, path)}
    end
  end

  def list_directory(%__MODULE__{} = fs, %Transaction{} = tr, path) do
    with :ok <- check_version(fs, tr, read_only: true) do
      case find(fs, tr, path) do
        nil ->
          {:error, "the directory does not exist"}

        dir ->
          subdirs_subspace = Subspace.concat(dir.node, [@subdirs])
          {start, stop} = Subspace.range(subdirs_subspace)
          subdirs = Transaction.get_range(tr, start, stop)

          {:ok,
           Enum.map(subdirs, fn {k, _v} ->
             [name] = Subspace.unpack(subdirs_subspace, k)
             name
           end)}
      end
    end
  end

  def move_directory(
        %__MODULE__{} = fs,
        %Transaction{} = tr,
        source,
        destination,
        opts \\ []
      ) do
    parents = Keyword.get(opts, :parents, false)

    if fs.root == nil && source == [] do
      {:error, "cannot move the root directory"}
    else
      with :ok <- check_version(fs, tr, read_only: false) do
        if subpath?(source, destination) do
          {:error, "the destination cannot be a subdirectory of the source directory"}
        else
          case find(fs, tr, source) do
            nil ->
              {:error, "the source directory does not exist"}

            source_node ->
              case find(fs, tr, destination) do
                nil ->
                  parent_dest_path = Enum.drop(destination, -1)

                  dest_parent =
                    if parents do
                      change_directory(fs, tr, parent_dest_path,
                        create: true,
                        parents: true
                      )
                    else
                      {:ok, find(fs, tr, parent_dest_path)}
                    end

                  with {:ok, dest_parent} <- dest_parent do
                    if dest_parent do
                      if source_node.partition.metadata != dest_parent.partition.metadata do
                        {:error, "cannot move between layers"}
                      else
                        [prefix] =
                          Subspace.unpack(source_node.partition.metadata, source_node.node.key)

                        Transaction.set(
                          tr,
                          Subspace.pack(dest_parent.node, [@subdirs, List.last(destination)]),
                          prefix
                        )

                        source_parent = find(fs, tr, Enum.drop(source, -1))

                        Transaction.clear(
                          tr,
                          Subspace.pack(source_parent.node, [@subdirs, List.last(source)])
                        )
                      end
                    else
                      {:error, "the parent directory does not exist"}
                    end
                  end

                _ ->
                  {:error, "the destination directory already exists"}
              end
          end
        end
      end
    end
  end

  def remove_directory(%__MODULE__{} = fs, %Transaction{} = tr, path) do
    with :ok <- check_version(fs, tr, read_only: false) do
      case find(fs, tr, path) do
        nil ->
          {:error, "the directory does not exist"}

        dir ->
          do_remove(fs, tr, dir)

          parent = find(fs, tr, Enum.drop(path, -1))
          Transaction.clear(tr, Subspace.pack(parent.node, [@subdirs, List.last(path)]))
      end
    end
  end

  defp do_remove(%__MODULE__{} = fs, %Transaction{} = tr, dir) do
    key = Subspace.pack(dir.node, [@subdirs])
    subdirs = Transaction.get_range(tr, key <> <<0x00>>, key <> <<0xFF>>)
    remove_subdirs(fs, tr, subdirs)
    [prefix] = Subspace.unpack(dir.partition.metadata, dir.node.key)
    Transaction.clear_starts_with(tr, prefix)
    {start, stop} = Subspace.range(dir.node)
    Transaction.clear_range(tr, start, stop)
  end

  defp remove_subdirs(%__MODULE__{}, %Transaction{}, []), do: :ok

  defp remove_subdirs(%__MODULE__{} = fs, %Transaction{} = tr, [subdir | subdirs]) do
    {name, prefix} = subdir
    subdir = Subspace.concat(fs.partition.metadata, [prefix])
    label = Transaction.get(tr, Subspace.pack(subdir, ["layer"]))
    dir = select(fs, label, subdir, name)
    do_remove(fs, tr, dir)
    remove_subdirs(fs, tr, subdirs)
  end

  def root(%__MODULE__{} = fs), do: (fs.root || fs.partition) |> new()

  ## Helpers

  defp allocate_prefix(%__MODULE__{} = fs, %Transaction{} = tr, prefix) do
    if prefix do
      if root_node(fs).key != partition_node(fs.partition).key do
        raise ArgumentError, "cannot specify a prefix in a partition"
      end

      unless prefix_available?(fs, tr, prefix) do
        {:error, "the given prefix is already in use"}
      else
        {:ok, prefix}
      end
    else
      hca_subspace = partition_node(fs.partition) |> Subspace.concat(["hca"])
      suffix = HighContentionAllocator.allocate(hca_subspace, tr)
      prefix = fs.partition.content.key <> suffix

      hits = Transaction.get_starts_with(tr, prefix, limit: 1)

      cond do
        length(hits) != 0 ->
          {:error,
           "the database has keys stored at the prefix chosen by the automatic prefix allocator"}

        prefix_available?(fs, tr, prefix, snapshot: true) == false ->
          {:error,
           "the directory layer has manually allocated prefixes that conflict with the automatic prefix allocator"}

        true ->
          {:ok, prefix}
      end
    end
  end

  defp check_version(%__MODULE__{} = fs, %Transaction{} = tr, opts \\ []) do
    read_only = Keyword.get(opts, :read_only, false)
    key = partition_node(fs.partition) |> Subspace.pack(["version"])

    if version = Transaction.get(tr, key) do
      <<major::integer-little-size(32), minor::integer-little-size(32),
        patch::integer-little-size(32)>> = version

      case @version do
        {x, _, _} when major > x ->
          {:error,
           "cannot load directory with version #{{major, minor, patch}} using directory layer #{@version}"}

        {_, y, _} when minor > y and not read_only ->
          {:error,
           "directory with version #{{major, minor, patch}} is read-only when opened using directory layer #{@version}"}

        _ ->
          :ok
      end
    else
      if !read_only do
        {major, minor, patch} = @version

        :ok =
          Transaction.set(
            tr,
            key,
            <<major::integer-little-size(32), minor::integer-little-size(32),
              patch::integer-little-size(32)>>
          )
      end

      :ok
    end
  end

  defp fetch_parent(%__MODULE__{} = fs, %Transaction{}, [], _parents), do: {:ok, fs}

  defp fetch_parent(%__MODULE__{} = fs, %Transaction{} = tr, path, parents) do
    path = Enum.drop(path, -1)
    change_directory(fs, tr, path, create: parents, parents: parents)
  end

  defp find(%__MODULE__{} = fs, %Transaction{}, []), do: fs

  defp find(%__MODULE__{} = fs, %Transaction{} = tr, [child | rest]) do
    if prefix = Transaction.get(tr, Subspace.pack(fs.node, [@subdirs, child])) do
      subdir = Subspace.concat(fs.partition.metadata, [prefix])
      label = Transaction.get(tr, Subspace.pack(subdir, ["layer"]))
      fs = select(fs, label, subdir, child)
      find(fs, tr, rest)
    end
  end

  defp node_containing_key(%__MODULE__{} = fs, %Transaction{} = tr, key, snapshot) do
    prefix = fs.partition.metadata.key

    case key do
      <<^prefix, _>> ->
        partition_node(fs.partition)

      _ ->
        Transaction.get_range(
          tr,
          prefix <> <<0x00>>,
          Subspace.pack(fs.partition.metadata, [key]) <> <<0x00>>,
          limit: 1,
          reverse: true,
          snapshot: snapshot
        )
        |> Enum.filter(fn {k, _} ->
          [prefix | _] = Subspace.unpack(fs.partition.metadata, k)

          case key do
            <<^prefix::binary, _::binary>> -> Subspace.concat(fs.partition.metadata, [prefix])
            _ -> nil
          end
        end)
        |> List.first()
    end
  end

  defp prefix_available?(%__MODULE__{} = fs, %Transaction{} = tr, prefix, opts \\ []) do
    snapshot = Keyword.get(opts, :snapshot, false)

    prefix && !node_containing_key(fs, tr, prefix, snapshot) &&
      Transaction.get_starts_with(
        tr,
        Subspace.pack(fs.partition.metadata, [prefix]),
        limit: 1,
        snapshot: snapshot
      )
      |> Enum.empty?()
  end

  defp partition_node(partition) do
    partition.metadata |> Subspace.concat([partition.metadata.key])
  end

  defp root_node(%__MODULE__{} = fs) do
    partition_node(fs.root || fs.partition)
  end

  defp select(%__MODULE__{} = fs, "partition", subdir, name) do
    [prefix] = Subspace.unpack(fs.partition.metadata, subdir.key)

    partition = %{
      content: Subspace.new(prefix),
      metadata: Subspace.new(prefix <> <<0xFE>>)
    }

    %__MODULE__{
      label: "partition",
      partition: partition,
      node: partition_node(partition),
      path: fs.path ++ [name],
      root: fs.root || fs.partition
    }
  end

  defp select(%__MODULE__{} = fs, label, subdir, name) do
    %__MODULE__{
      label: label,
      node: subdir,
      partition: fs.partition,
      path: fs.path ++ [name],
      root: fs.root
    }
  end

  defp subpath?(source, destination) do
    source = Tuple.pack(source)
    destination = Tuple.pack(destination)

    case destination do
      <<^source::binary, _::binary>> -> true
      _ -> false
    end
  end
end
