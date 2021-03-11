defmodule DistrLogger.Queue do
  use GenServer
  defstruct [:logger, :queue, :max_queue_length, :current_queue_length, :flush_chunk_size, :flushing_chunk, :last_message_time]
  @self __MODULE__
  use Timex

  # API
  def start_link({_logger, _max_queue_length, _flush_chunk_size} = start_tuple) do
    @self |> GenServer.start_link(start_tuple)
  end

  def accept(server, item) do
    server |> GenServer.call({:accept, item})
  end

  def flushed_chunk_ack(server) do
    server |> GenServer.cast(:flushed_chunk_ack)
  end

  def get_queue_length(server) do
    server |> GenServer.call(:get_queue_length)
  end

  # Callbacks
  @impl true
  def init({logger, max_queue_length, flush_chunk_size}) do
    self() |> send(:ping)
    {:ok, %@self{logger: logger, max_queue_length: max_queue_length, queue: Qex.new(), current_queue_length: 0, flush_chunk_size: flush_chunk_size}}
  end

  @impl true
  def handle_call({:accept, item}, _from, state = %@self{logger: logger, queue: queue, max_queue_length: max_queue_length, current_queue_length: current_queue_length} = state) do
    if current_queue_length < max_queue_length do
      new_state = %{state|queue: queue |> enqueue(item), current_queue_length: current_queue_length + 1, last_message_time: Timex.now()}
      if should_flush(new_state) do
        self() |> send(:flush)
      end
      if current_queue_length === 0 do
        IO.puts "Queue started for worker"
        logger |> DistrLogger.notify_queue_started()
      end
      {:reply, :ok, new_state}
    else
      {:reply, :deny, state}
    end
  end

  @impl true
  def handle_call(:get_queue_length, _from, %@self{current_queue_length: current_queue_length} = state) do
    {:reply, current_queue_length, state}
  end

  @impl true
  def handle_cast(:flushed_chunk_ack, %@self{flushing_chunk: flushing_chunk, last_message_time: last_message_time, current_queue_length: current_queue_length, logger: _logger} = state) do
    # IO.puts "Queue flushing complete (acknowledgement)"
    unless flushing_chunk do
      IO.puts "Weird, flushed chunk ack received, but no flushing chunk in the queue state"
    end
    if should_flush(state) or (Timex.diff(Timex.now(), last_message_time, :milliseconds) >= 100 and current_queue_length > 0) do
      # IO.puts "Still need to flush the queue. Sending another signal for that."
      self() |> send(:flush)
    end
    # if current_queue_length === 0 do
    #   IO.puts "#{Timex.local()}: Queue is flushed fully. No items in the queue."
    #   logger |> DistrLogger.notify_queue_emptied()
    # end
    {:noreply, %{state|flushing_chunk: nil}}
  end

  @impl true
  def handle_info(:flush, %@self{queue: queue, logger: logger, current_queue_length: current_queue_length, flush_chunk_size: flush_chunk_size, flushing_chunk: flushing_chunk} = state) do
    # IO.puts "flushing queue"
    if flushing_chunk do
      IO.puts "Queue is currently in the process of flushing chunk: #{inspect flushing_chunk}"
      {:noreply, state}
    else
      logger |> DistrLogger.notify_max_queue(current_queue_length)
      {chunk, rem_queue} =
        if current_queue_length <= flush_chunk_size do
          {(if queue, do: (queue |> Enum.to_list())), nil}
        else
          {q1, q2} = queue |> Qex.split(flush_chunk_size)
          {q1 |> Enum.to_list(), q2}
        end
      if chunk do
        logger |> DistrLogger.flush_chunk(chunk)
      end
      updated_queue_length = current_queue_length |> update_current_queue_length(flush_chunk_size)
      if updated_queue_length === 0 do
        logger |> DistrLogger.notify_queue_emptied()
      end
      {:noreply, %{state|queue: rem_queue, flushing_chunk: chunk, current_queue_length: updated_queue_length}}
    end
  end

  @impl true
  def handle_info(:ping, %@self{last_message_time: last_message_time, current_queue_length: current_queue_length, queue: _queue} = state) do
    if last_message_time do
      now = Timex.now()
      if Timex.diff(now, last_message_time, :milliseconds) >= 100 and current_queue_length > 0 do
        # if seconds_elapsed >= 5 do
        #   IO.puts "#{seconds_elapsed} seconds elapsed since last message received for queue"
        # end
        if current_queue_length > 0 do
          self() |> send(:flush)
        end
      end
    end
    self() |> Process.send_after(:ping, 500)
    {:noreply, state}
  end

  # Private functions
  defp update_current_queue_length(current_queue_length, flush_chunk_size) when current_queue_length |> is_integer() and flush_chunk_size |> is_integer() do
    if current_queue_length <= flush_chunk_size, do: 0, else: current_queue_length - flush_chunk_size
  end

  defp enqueue(nil, item), do: Qex.new() |> enqueue(item)
  defp enqueue(queue, item) do
    queue |> Qex.push(item)
  end

  defp should_flush(%@self{current_queue_length: current_queue_length, flush_chunk_size: flush_chunk_size}) when current_queue_length >= flush_chunk_size, do: true
  defp should_flush(_), do: false

end
