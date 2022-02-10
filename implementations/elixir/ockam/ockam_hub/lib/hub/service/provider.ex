defmodule Ockam.Hub.Service.Provider do
  @moduledoc """
  Behaviour module and entrypoint to start Ockam.Hub services

  Provider behaviour implementations should provide a list of service names and be able to
  start service workers given names and arguments

  Provider can start all services configured in :ockam_hub => :services application environment
  with :ockam_hub => :providers provider implementations
  """

  alias Ockam.Workers.DiscoveryClient, as: ServiceDiscoveryClient

  require Logger

  @type child_spec :: Supervisor.child_spec() | {module(), term()} | module()

  ## TODO: maybe we need more than just a name here?
  @callback services() :: [atom()]

  @callback child_spec(name :: atom(), args :: Keyword.t()) :: child_spec()

  @supervisor Ockam.Hub
  ## TODO: make this configurable/retrievable
  @discovery_service_route ["discovery_service"]

  @spec configured_child_specs() :: {:ok, [child_spec()]} | {:error, any()}
  def configured_child_specs() do
    services = get_configured_services()

    {child_specs, errors} = get_services_child_specs(services)

    case errors do
      [] ->
        {:ok, child_specs}

      errors ->
        {:error, errors}
    end
  end

  @spec get_services_child_specs(Enum.t(), nil | map()) :: {[child_spec()], [{:error, any()}]}
  def get_services_child_specs(services_config, providers \\ nil) do
    service_providers_map = get_service_providers_map(providers)

    spec_results =
      Enum.map(services_config, fn service_config ->
        get_service_child_spec(service_config, service_providers_map)
      end)

    {ok_results, errors} =
      Enum.split_with(spec_results, fn
        {:ok, _} -> true
        {:error, _} -> false
      end)

    child_specs = Enum.map(ok_results, fn {:ok, spec} -> spec end)

    {child_specs, errors}
  end

  @spec get_service_child_spec(atom() | {atom(), list()}, nil | map()) ::
          {:ok, child_spec()} | {:error, any()}
  def get_service_child_spec(service_config, providers \\ nil)

  def get_service_child_spec(service_name, providers) when is_atom(service_name) do
    get_service_child_spec({service_name, []}, providers)
  end

  def get_service_child_spec({service_name, service_args}, providers) do
    service_providers_map = get_service_providers_map(providers)

    case Map.get(service_providers_map, service_name) do
      nil ->
        {:error, {:unknown_service, service_name}}

      provider_mod ->
        child_spec =
          provider_mod.child_spec(service_name, service_args)
          |> Supervisor.child_spec(id: service_name)
          |> Map.update!(:start, fn start ->
            {__MODULE__, :start_registered_service, [service_name, start]}
          end)

        {:ok, child_spec}
    end
  end

  def start_service(service_config, providers \\ nil, supervisor \\ @supervisor) do
    case get_service_child_spec(service_config, providers) do
      {:ok, child_spec} ->
        Supervisor.start_child(supervisor, child_spec)

      {:error, reason} ->
        {:error, reason}
    end
  end

  def start_configured_service(service_name, extra_args \\ [], supervisor \\ @supervisor) do
    services = get_configured_services()

    case Keyword.get(services, service_name) do
      nil ->
        {:error, :service_not_configured}

      default_args ->
        start_service({service_name, Keyword.merge(default_args, extra_args)}, nil, supervisor)
    end
  end

  def start_registered_service(id, {m, f, a}) do
    ## TODO: only register services if service discovery is active
    case apply(m, f, a) do
      {:ok, pid} ->
        {:ok, pid}

      {:ok, pid, service_api_address} ->
        ## TODO: handle errors
        name = to_string(id)

        ServiceDiscoveryClient.register_service(@discovery_service_route, name, [
          service_api_address
        ])

        {:ok, pid, service_api_address}

      other ->
        Logger.info("Other: #{inspect(other)}")
        other
    end
  end

  def get_service_providers_map(providers) when is_list(providers) or providers == nil do
    providers
    |> get_providers()
    |> Enum.flat_map(fn provider_mod ->
      Enum.map(provider_mod.services(), fn service -> {service, provider_mod} end)
    end)
    |> Map.new()
  end

  def get_service_providers_map(providers_map) when is_map(providers_map) do
    providers_map
  end

  def get_providers(providers \\ nil)
  def get_providers(nil), do: Application.get_env(:ockam_hub, :service_providers)
  def get_providers(providers) when is_list(providers), do: providers

  def get_configured_services() do
    case Application.get_env(:ockam_hub, :services_config_source) do
      "json" ->
        parse_services_json(Application.get_env(:ockam_hub, :services_json))

      "file" ->
        parse_services_file(Application.get_env(:ockam_hub, :services_file))

      "list" ->
        parse_services_list(Application.get_env(:ockam_hub, :services_list, []))

      _other ->
        parse_services_config(Application.get_env(:ockam_hub, :services, []))
    end
  end

  def parse_services_config(services) do
    Enum.map(
      services,
      fn
        atom when is_atom(atom) -> {atom, []}
        {atom, args_map} when is_map(args_map) -> {atom, Map.to_list(args_map)}
        {_atom, _args} = config -> config
      end
    )
  end

  def parse_services_list(nil) do
    []
  end

  def parse_services_list(services) do
    services
    |> String.split(",")
    |> Enum.map(fn service_name -> service_name |> String.trim() |> String.to_atom() end)
    |> parse_services_config()
  end

  def parse_services_json(nil) do
    []
  end

  def parse_services_json("") do
    []
  end

  def parse_services_json(json) do
    case Jason.decode(json, keys: :atoms) do
      {:ok, services} ->
        ## TODO: validate services
        services
        |> Enum.map(fn {service, args} -> {service, Enum.to_list(args)} end)
        |> Enum.to_list()

      {:error, err} ->
        raise("Unable to parse json services config: #{inspect(err)}")
    end
  end

  def parse_services_file(nil) do
    raise("Services config file is not defined")
  end

  def parse_services_file(filename) do
    with true <- File.exists?(filename),
         {:ok, contents} <- File.read(filename),
         data <- String.trim(contents) do
      parse_services_json(data)
    else
      _other ->
        raise("Services file is not found: #{inspect(filename)}")
    end
  end
end
