defmodule DistrLogger.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: DistrLogger.Worker.Supervisor},
      {DynamicSupervisor, strategy: :one_for_one, name: DistrLogger.Queue.Supervisor},
      {Task.Supervisor,name: DistrLogger.TaskSupervisor},
      # {DynamicSupervisor, strategy: :one_for_one, name: DistrLogger.Queue.Supervisor},
      {DistrLogger.Hub, "C:/Txt/hub"}
      # Starts a worker by calling: DistrLogger.Worker.start_link(arg)
      # {DistrLogger.Worker, arg}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: DistrLogger.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
