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

  def project do
    [
      app: :fdbc,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      compilers: [:fdbc] ++ Mix.compilers(),
      deps: deps(),

      # Docs
      name: "FDBC",
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
      extras: []
    ]
  end
end
