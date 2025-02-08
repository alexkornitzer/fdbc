defmodule Mix.Tasks.Compile.Fdbc do
  use Mix.Task.Compiler

  def run(_arg) do
    mode =
      if Mix.env() == :prod do
        "--release=fast"
      else
        "--release=off"
      end

    config = Mix.Project.config()
    app_priv = Path.join(Mix.Project.app_path(config), "priv")
    {result, errcode} = System.cmd("zig", ["build", mode, "-p", app_priv], stderr_to_stdout: true)

    if match?({{:unix, :darwin}, 0}, {:os.type(), errcode}) do
      lib = Path.join(app_priv, "lib/libfdbcnif.so")
      _ = :file.make_symlink("./libfdbcnif.dylib", lib)
    end

    IO.binwrite(result)
  end
end

defmodule FDB.MixProject do
  use Mix.Project

  @source_url "https://github.com/alexkornitzer/fdbc"
  @version "0.1.0"

  def project do
    [
      app: :fdbc,
      version: @version,
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      compilers: [:fdbc] ++ Mix.compilers(),
      deps: deps(),

      # Hex
      description: "An unofficial FoundationDB client for Elixir",
      package: package(),

      # Docs
      name: "fdbc",
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {FDBC.Application, []}
    ]
  end

  defp deps do
    [
      {:ex_doc, "~> 0.36", only: :dev, runtime: false}
    ]
  end

  defp docs do
    [
      main: "FDBC",
      source_ref: "v#{@version}",
      source_url: @source_url,
      extras: []
    ]
  end

  defp package() do
    [
      name: "fdbc",
      build_tools: ~w(mix zig),
      files: ~w(.formatter.exs CHANGELOG* LICENSE* README* build.zig lib mix.exs src),
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url},
      maintainers: ["Alex Kornitzer"]
    ]
  end
end
