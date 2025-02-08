[![Hex.pm](https://img.shields.io/hexpm/v/fdbc.svg)](https://hex.pm/packages/fdbc) [![Documentation](https://img.shields.io/badge/documentation-gray)](https://hexdocs.pm/fdbc/)


An unofficial FoundationDB client for Elixir.


## Installation

Add `:fdbc` to the list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:fdbc, "~> 0.1.0"}
  ]
end
```


## Usage

This library allows Elixir applications to interact with FoundationDB. Below is a minimal example:

```elixir
# In your config/config.exs file.
config :fdbc,
  api_version: 730,
  network: true

config :demo, Demo.Foundation,
  cluster: "./fdb.cluster",
  directory: "demo"

# In your application code
defmodule Demo.Foundation do
  use FDBC.Foundation,
    otp_app: :demo,
    options: [
      transaction_retry_limit: 100,
      transaction_timeout: 60_000
    ]
end

defmodule Demo.App do
  alias FDBC.{Directory, Subspace, Transaction}
  alias Demo.Foundation

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


## Running Tests

Clone the repo and fetch its dependencies:

> NOTE: Some tests will clear the keyspace of the cluster so the tests must be run against a disposable cluster.

```bash
$ git clone https://github.com/alexkornitzer/fdbc.git
$ cd fdbc
$ mix deps.get
$ FDBC_CLUSTER_FILE=./fdb.cluster mix test
```
