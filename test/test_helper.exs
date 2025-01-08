ExUnit.start()

FDBC.api_version()

FDBC.Network.start()

System.at_exit(fn _exit_code ->
  FDBC.Network.stop()
end)
