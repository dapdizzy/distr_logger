defmodule DistrLogger.Helpers do
  def get_pid!(start_result)
  def get_pid!({:ok, pid}) do
    pid
  end
  def get_pid!(start_result) do
    raise "Faile7d to get pid as the result was in shape of: #{inspect start_result}"
  end
end
