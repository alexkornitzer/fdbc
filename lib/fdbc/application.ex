defmodule FDBC.Application do
  @moduledoc false
  use Application

  alias FDBC.Network
  alias FDBC.NIF

  @impl true
  def start(_type, _args) do
    if version = Application.get_env(:fdbc, :api_version) do
      :ok = NIF.select_api_version(version)
    end

    :ok =
      case Application.get_env(:fdbc, :network) do
        nil -> :ok
        false -> :ok
        true -> Network.start()
        x -> Network.start(x)
      end

    opts = [strategy: :one_for_one, name: FDBC.Supervisor]
    Supervisor.start_link([], opts)
  end

  @impl true
  def stop(_state) do
    Network.stop()
  end
end
