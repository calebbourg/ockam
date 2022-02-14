defmodule Ockam.Hub.TelemetryPoller do
  def dispatch_worker_count() do
    workers_by_type()
    |> Enum.map(fn({type, workers}) ->
      type_str = format_worker_type(type)
      :telemetry.execute([:ockam, :workers, :type], %{count: Enum.count(workers)}, %{type: type_str})
    end)
  end

  defp format_worker_type(nil) do
    "Other"
  end
  defp format_worker_type(module) do
    to_string(module)
  end

  @spec workers_by_type() :: [{module(), address :: String.t()}]
  def workers_by_type() do
    Ockam.Node.list_workers()
    |> Enum.group_by(fn({_address, _pid, module}) -> module end, fn({address, _pid, _modules}) -> address end)
    |> Map.new()
  end
end
