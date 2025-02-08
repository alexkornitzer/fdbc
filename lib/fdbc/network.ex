defmodule FDBC.Network do
  @moduledoc """
  A module used to start and stop the network thread which is responsible for
  running most of the clients tasks.
  """

  alias FDBC.NIF

  @doc """
  Starts the networking thread used for communication with FoundationDB.

  ## Options

    * `:callbacks_on_external_threats` - (true) If set, callbacks from external
      client libraries can be called from threads created by the FoundationDB
      client library. Otherwise, callbacks will be called from either the
      thread used to add the callback or the network thread. Setting this
      option can improve performance when connected using an external client,
      but may not be safe to use in all environments. Must be set before
      setting up the network. WARNING: This feature is considered experimental
      at this time.

    * `:client_threads_per_version` - (integer) Spawns multiple worker threads
      for each version of the client that is loaded. Setting this to a number
      greater than one implies `:disable_local_client`.

    * `:client_tmp_dir` - (binary) Sets the directory for storing temporary
      files created by FDBC client, such as temporary copies of client
      libraries. Defaults to `/tmp`.

    * `:disable_client_bypass` - (true) Prevents the multi-version client API
      from being disabled, even if no external clients are configured. This
      option is required to use GRV caching.

    * `:disable_client_statistics_logging` - (true) Disables logging of client
      statistics, such as sampled transaction activity.

    * `:disable_local_client` - (true) Prevents connections through the local
      client, allowing only connections through externally loaded client
      libraries.

    * `:disable_multi_version_client_api` - (true) Disables the multi-version
      client API and instead uses the local client directly.

    * `:distributed_client_tracer` - (atom) Set a tracer to run on the client.
      Should be set to the same value as the tracer set on the server. Valid
      values are `nil`, `:log_file` or `:network_lossy`.

    * `:enable_run_loop_profiling` - (true) Enables debugging feature to
      perform run loop profiling. Requires trace logging to be enabled.
      WARNING: this feature is not recommended for use in production.

    * `:external_client_directory` - (binary) Searches the specified path for
      dynamic libraries and adds them to the list of client libraries for use
      by the multi-version client API.

    * `:external_client_library` - (binary) Adds an external client library for
      use by the multi-version client API.

    * `:fail_incompatible_client` - (true) Fail with an error if there is no
      client matching the server version the client is connecting to.

    * `:ignore_external_client_failures` - (true) Ignore the failure to
      initialize some of the external clients.

    * `:knob` - (binary) Set internal tuning or debugging knobs.

    * `:retain_client_library_copies` - (true) Retain temporary external client
      library copies that are created for enabling multi-threading.

    * `:tls_ca_bytes` - (binary) Set the certificate authority bundle.

    * `:tls_ca_path` - (binary) Set the file from which to load the certificate
      authority bundle.

    * `:tls_cert_bytes` - (binary) Set the certificate chain.

    * `:tls_cert_path` - (binary) Set the file from which to load the
      certificate chain.

    * `:tls_disable_plaintext_connection` - (true) Prevent client from
      connecting to a non-TLS endpoint by throwing network connection failed
      error.

    * `:tls_key_bytes` - (binary) Set the private key corresponding to your own
      certificate.

    * `:tls_key_path` - (binary) Set the file from which to load the private
      key corresponding to your own certificate.

    * `:tls_password` - (binary) Set the passphrase for encrypted private key.

    * `:tls_verify_peers` - (true) Set the peer certificate field verification
      criteria.

    * `:trace_clock_source` - (atom) Select clock source for trace files. Valid
      values are `:now` or `realtime`. Defaults to `:now`.

    * `:trace_enable` - (true) Enables trace output to a file in a directory of
      the clients choosing.

    * `:trace_file_identifier` - (binary) Once provided, this string will be
      used to replace the port/PID in the log file names.

    * `:trace_format` - (atom) Select the format of the log files. Valid values
      are `:xml` and `:json`. Defaults to `:xml`.

    * `:trace_initialize_on_setup` - (true) Initialize trace files on network
      setup, determine the local IP later. Otherwise tracing is initialized when
      opening the first database.

    * `:trace_log_group` - (binary) Sets the 'LogGroup' attribute with the
      specified value for all events in the trace output files. The default log
      group is 'default'.

    * `:trace_max_logs_size` - (integer) Sets the maximum size of all the trace
      output files put together. If the value is set to 0, there is no limit on
      the total size of the files. The default is a maximum size of 104,857,600
      bytes. If the de fault roll size is used, this means that a maximum of 10
      trace files will be written at a time.

    * `:trace_partial_file_suffix` - (binary) Set file suffix for partially
      written log files.

    * `:trace_roll_size` - (integer) Sets the maximum size in bytes of a single
      trace output file. If the value is set to 0, there is no limit on
      individual file size. The default is a maximum size of 10,485,760 bytes.

    * `:trace_share_among_client_threads` - (true) Use the same base trace file
      name for all client threads as it did before version 7.2. The current
      default behavior is to use distinct trace file names for client threads
      by including their version and thread index.

  """
  @spec start(keyword) :: :ok
  def start(opts \\ []) do
    Enum.each(opts, fn {k, v} -> set_option(k, v) end)

    case NIF.setup_network() do
      :ok -> :ok
      {:error, code, reason} -> raise FDBC.Error, message: reason, code: code
    end

    case NIF.run_network() do
      :ok -> :ok
      {:error, term} -> raise ErlangError, term
      {:error, code, reason} -> raise FDBC.Error, message: reason, code: code
    end

    :ok
  end

  @doc """
  Stops the network threat.

  > #### Warning {: .warning}
  > Once the network is stopped it cannot be restarted during the lifetime of
    the running program.

  """
  @spec stop() :: :ok
  def stop() do
    case NIF.stop_network() do
      :ok -> :ok
      {:error, term} -> raise ErlangError, term
      {:error, code, reason} -> raise FDBC.Error, message: reason, code: code
    end

    :ok
  end

  defp set_option(:trace_enable, path) when is_binary(path) do
    :ok = NIF.network_set_option(30, path)
    :ok
  end

  defp set_option(:trace_enable, path) when is_nil(path) do
    :ok = NIF.network_set_option(30)
    :ok
  end

  defp set_option(:trace_roll_size, size) when is_integer(size) do
    :ok = NIF.network_set_option(31, <<size::64-signed-little-integer>>)
    :ok
  end

  defp set_option(:trace_max_logs_size, size) when is_integer(size) do
    :ok = NIF.network_set_option(32, <<size::64-signed-little-integer>>)
    :ok
  end

  defp set_option(:trace_log_group, group) when is_binary(group) do
    :ok = NIF.network_set_option(33, group)
    :ok
  end

  defp set_option(:trace_format, :json) do
    :ok = NIF.network_set_option(34, "json")
    :ok
  end

  defp set_option(:trace_format, :xml) do
    :ok = NIF.network_set_option(34, "xml")
    :ok
  end

  defp set_option(:trace_clock_source, :now) do
    :ok = NIF.network_set_option(35, "now")
    :ok
  end

  defp set_option(:trace_clock_source, :realtime) do
    :ok = NIF.network_set_option(35, "realtime")
    :ok
  end

  defp set_option(:trace_file_identifier, identifier) when is_binary(identifier) do
    :ok = NIF.network_set_option(36, identifier)
    :ok
  end

  defp set_option(:trace_share_among_client_threads, true) do
    :ok = NIF.network_set_option(37)
    :ok
  end

  defp set_option(:trace_initialize_on_setup, true) do
    :ok = NIF.network_set_option(38)
    :ok
  end

  defp set_option(:trace_partial_file_suffix, suffix) when is_binary(suffix) do
    :ok = NIF.network_set_option(39, suffix)
    :ok
  end

  defp set_option(:knob, kv) when is_binary(kv) do
    :ok = NIF.network_set_option(40, kv)
    :ok
  end

  defp set_option(:tls_cert_bytes, certificates) when is_binary(certificates) do
    :ok = NIF.network_set_option(42, certificates)
    :ok
  end

  defp set_option(:tls_cert_path, path) when is_binary(path) do
    :ok = NIF.network_set_option(43, path)
    :ok
  end

  defp set_option(:tls_key_bytes, key) when is_binary(key) do
    :ok = NIF.network_set_option(45, key)
    :ok
  end

  defp set_option(:tls_key_path, path) when is_binary(path) do
    :ok = NIF.network_set_option(46, path)
    :ok
  end

  defp set_option(:tls_verify_peers, bytes) when is_binary(bytes) do
    :ok = NIF.network_set_option(47, bytes)
    :ok
  end

  defp set_option(:buggify, enable) when is_boolean(enable) do
    :ok =
      case enable do
        true -> NIF.network_set_option(48)
        false -> NIF.network_set_option(49)
      end

    :ok
  end

  defp set_option(:buggify_section_activate_probability, percentage)
       when is_integer(percentage) and percentage >= 0 and percentage <= 100 do
    :ok = NIF.network_set_option(50, <<percentage::64-signed-little-integer>>)
    :ok
  end

  defp set_option(:buggify_section_fired_probability, percentage)
       when is_integer(percentage) and percentage >= 0 and percentage <= 100 do
    :ok = NIF.network_set_option(51, <<percentage::64-signed-little-integer>>)
    :ok
  end

  defp set_option(:tls_ca_bytes, key) when is_binary(key) do
    :ok = NIF.network_set_option(52, key)
    :ok
  end

  defp set_option(:tls_ca_path, path) when is_binary(path) do
    :ok = NIF.network_set_option(53, path)
    :ok
  end

  defp set_option(:tls_password, password) when is_binary(password) do
    :ok = NIF.network_set_option(54, password)
    :ok
  end

  defp set_option(:tls_disable_plaintext_connection, true) do
    :ok = NIF.network_set_option(55)
    :ok
  end

  defp set_option(:disable_multi_version_client_api, true) do
    :ok = NIF.network_set_option(60)
    :ok
  end

  defp set_option(:callbacks_on_external_threats, true) do
    :ok = NIF.network_set_option(61)
    :ok
  end

  defp set_option(:external_client_library, path) when is_binary(path) do
    :ok = NIF.network_set_option(62, path)
    :ok
  end

  defp set_option(:external_client_directory, path) when is_binary(path) do
    :ok = NIF.network_set_option(63, path)
    :ok
  end

  defp set_option(:disable_local_client, true) do
    :ok = NIF.network_set_option(64)
    :ok
  end

  defp set_option(:client_threads_per_version, threads) when is_integer(threads) do
    :ok = NIF.network_set_option(65, <<threads::64-signed-little-integer>>)
    :ok
  end

  defp set_option(:future_version_client_library, path) when is_binary(path) do
    :ok = NIF.network_set_option(66, path)
    :ok
  end

  defp set_option(:retain_client_library_copies, true) do
    :ok = NIF.network_set_option(67)
    :ok
  end

  defp set_option(:ignore_external_client_failures, true) do
    :ok = NIF.network_set_option(68)
    :ok
  end

  defp set_option(:fail_incompatible_client, true) do
    :ok = NIF.network_set_option(69)
    :ok
  end

  defp set_option(:disable_client_statistics_logging, true) do
    :ok = NIF.network_set_option(70)
    :ok
  end

  defp set_option(:enable_run_loop_profiling, true) do
    :ok = NIF.network_set_option(71)
    :ok
  end

  defp set_option(:disable_client_bypass, true) do
    :ok = NIF.network_set_option(72)
    :ok
  end

  defp set_option(:client_buggify, enable) when is_boolean(enable) do
    :ok =
      case enable do
        true -> NIF.network_set_option(80)
        false -> NIF.network_set_option(81)
      end

    :ok
  end

  defp set_option(:client_buggify_section_activate_probability, percentage)
       when is_integer(percentage) and percentage >= 0 and percentage <= 100 do
    :ok = NIF.network_set_option(82, <<percentage::64-signed-little-integer>>)
    :ok
  end

  defp set_option(:client_buggify_section_fired_probability, percentage)
       when is_integer(percentage) and percentage >= 0 and percentage <= 100 do
    :ok = NIF.network_set_option(83, <<percentage::64-signed-little-integer>>)
    :ok
  end

  defp set_option(:distributed_client_tracer, nil) do
    :ok = NIF.network_set_option(90, "none")
    :ok
  end

  defp set_option(:distributed_client_tracer, :log_file) do
    :ok = NIF.network_set_option(90, "log_file")
    :ok
  end

  defp set_option(:distributed_client_tracer, :network_lossy) do
    :ok = NIF.network_set_option(90, "network_lossy")
    :ok
  end

  defp set_option(:client_tmp_dir, path) when is_binary(path) do
    :ok = NIF.network_set_option(91, path)
    :ok
  end

  defp set_option(k, _) do
    raise ArgumentError, message: "Invalid network option '#{k}' provided!"
  end
end
