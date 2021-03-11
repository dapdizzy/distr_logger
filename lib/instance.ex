defmodule DistrLogger.Instance do
  use Agent
  defstruct [:self, :next]
  @self __MODULE__

  def start_link(logger) do
    Agent.start_link(fn -> %@self{self: logger} end)
  end

  def self(this) do
    this |> Agent.get(fn %@self{self: self} -> self end)
  end

  def next(this) do
    this |> Agent.get(fn %@self{next: next} -> next end)
  end

  def set_next(this, next) do
    this |> Agent.update(fn %@self{} = state -> %{state|next: next} end)
  end
end
