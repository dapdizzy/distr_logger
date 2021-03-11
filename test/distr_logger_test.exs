defmodule DistrLoggerTest do
  use ExUnit.Case
  doctest DistrLogger

  test "greets the world" do
    assert DistrLogger.hello() == :world
  end
end
