defmodule DistrLogger do
  @moduledoc """
  Documentation for `DistrLogger`.
  """
  use GenServer
  defstruct [:working_directory, :iodevice, :queue, :max_queue_length, :flush_chunk_size, :state, :current_task_ref, :msg_queue, :last_write_time, :hub]
  @self __MODULE__
  use Timex

  # API
  def start_link({working_directory, max_queue_length, flush_chunk_size, hub}, name \\ nil) do
    @self |> GenServer.start_link(%@self{working_directory: working_directory, max_queue_length: max_queue_length, flush_chunk_size: flush_chunk_size, hub: hub}, name_options(name))
  end

  def log(server, message) do
    if server === nil do
      IO.puts "Logger called with server === nil"
    end
    server |> GenServer.call({:log, message})
  end

  def get_working_folder(server) do
    server |> GenServer.call(:get_working_folder)
  end

  def flush_chunk(server, chunk) do
    server |> GenServer.cast({:flush_chunk, chunk})
  end

  def notify_queue_emptied(server) do
    server |> GenServer.cast(:notify_queue_emptied)
  end

  def notify_queue_started(server) do
    server |> GenServer.cast(:notify_queue_started)
  end

  def get_queue_and_writing_info(server) do
    server |> GenServer.call(:get_queue_and_writing_info)
  end

  def notify_max_queue(server, queue_length) do
    server |> GenServer.cast({:max_queue, queue_length})
  end

  # Callbacks
  @impl true
  def init(state = %@self{working_directory: working_directory}) do
    self() |> Process.send_after(:auto_close, 1_000)
    {:ok, state |> init_from_working_directory(working_directory)}
  end

  @impl true
  def handle_call(:get_working_folder, _from, %@self{working_directory: working_directory} = state) do
    {:reply, working_directory, state}
  end

  @impl true
  def handle_call({:log, message}, _from, %@self{queue: queue, working_directory: working_directory, state: current_state} = state) do
    # IO.puts "Logging [#{message}] via logger [#{working_directory}]"
    case queue |> DistrLogger.Queue.accept(message) do
      :ok -> state |> reply(:ok)
      :deny ->
        IO.puts "Denying due to queue not accepting the message for logger [#{working_directory}]"
        state |> reply(:deny)
    end
  end

  @impl true
  def handle_call(_, _from, %@self{state: :flushing} = state) do
    IO.puts "Denying as the state is flushing"
    state |> reply(:deny)
  end

  @impl true
  def handle_call(:get_queue_and_writing_info, _from, %@self{iodevice: iodevice, queue: queue, working_directory: working_directory, last_write_time: last_write_time} = state) do
    {:reply, {working_directory, [iodevice: iodevice |> is_opened?(), queue_length: queue |> DistrLogger.Queue.get_queue_length(), last_write_time: last_write_time]}, state}
  end

  @impl true
  def handle_cast({:flush_chunk, chunk} = msg, %@self{iodevice: iodevice, working_directory: working_directory, state: current_state, current_task_ref: current_task_ref, msg_queue: msg_queue, hub: hub} = state) do
    if current_state != nil or current_task_ref != nil do
      IO.puts "Currenly busy doing [#{current_state}] with task ref: [#{inspect current_task_ref}]"
      {:noreply, %{state|msg_queue: msg_queue |> enqueue_msg(msg)}}
    else
      task = Task.Supervisor.async_nolink(DistrLogger.TaskSupervisor, fn ->
        flush_chunk(chunk, iodevice, working_directory, hub)
      end)
      {:noreply, %{state|state: :flushing, current_task_ref: task.ref}}
    end
  end

  @impl true
  def handle_cast(:notify_queue_emptied, %@self{hub: hub, working_directory: working_directory} = state) do
    hub |> DistrLogger.Hub.notify_queue_emptied(self(), working_directory)
    {:noreply, state}
  end

  @impl true
  def handle_cast(:notify_queue_started, %@self{hub: hub, working_directory: working_directory} = state) do
    hub |> DistrLogger.Hub.notify_queue_started(self(), working_directory)
    {:noreply, state}
  end

  @impl true
  def handle_cast({:max_queue, queue_length}, %@self{working_directory: working_directory, hub: hub} = state) do
    hub |> DistrLogger.Hub.notify_max_queue(working_directory, queue_length)
    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %@self{current_task_ref: current_task_ref, msg_queue: msg_queue} = state) do
    new_msg_queue =
      if msg_queue do
        {item, upd_msg_queue} =
          case msg_queue |> try_dequeue() do
            nil -> {nil, nil}
            result -> result
          end
        if item do
          self() |> GenServer.cast(item)
        end
        upd_msg_queue
      end
    new_state = %{state|msg_queue: new_msg_queue}
    if ref === current_task_ref do
      {:noreply, %{new_state|current_task_ref: nil, state: nil}}
    else
      {:noreply, new_state}
    end
  end

  @impl true
  def handle_info({ref, result}, state = %@self{working_directory: working_directory, current_task_ref: ref, state: _current_state, queue: queue, msg_queue: msg_queue}) do
    something_was_flushed =
      case result do
        :flushed_queue ->
          IO.puts "Flushed queue to disk for [#{working_directory}]"
          true
        :flushed_chunk ->
          IO.puts "Flushed chunk to disk for [#{working_directory}]"
          queue |> DistrLogger.Queue.flushed_chunk_ack()
          true
        something_else ->
          IO.puts "Finished with result #{inspect something_else}"
          false
      end
    if something_was_flushed do
      self() |> Process.send_after(:auto_close, 1_050)
    end
    Process.demonitor(ref, [:flush])
    new_msg_queue =
      if msg_queue do
        {item, upd_msg_queue} =
          case msg_queue |> try_dequeue() do
            nil -> {nil, nil}
            result -> result
          end
        if item do
          IO.puts "Relaying item[#{inspect item} to be processed. Msg queue length is #{queue_length_safe(upd_msg_queue)}"
          self() |> GenServer.cast(item)
        end
        upd_msg_queue
      end
    new_state = %{state|msg_queue: new_msg_queue}
    {:noreply, %{new_state|current_task_ref: nil, state: nil, last_write_time: Timex.now()}}
  end

  def handle_info(:auto_close, %@self{iodevice: iodevice, last_write_time: last_write_time, working_directory: working_directory, hub: hub} = state) do
    now = Timex.now()
    upd_iodevice =
      if iodevice && last_write_time && Timex.diff(now, last_write_time, :seconds) >= 1 do
        IO.puts "Last message time: #{inspect last_write_time}, now is: #{inspect now}"
        new_iodevice = iodevice |> close_iodevice()
        IO.puts "Closed iodevice for [#{working_directory}]"
        # Could send a signal to the hub to inform it that we've closed the iodevice
        if new_iodevice === nil and iodevice !== new_iodevice do
          hub |> DistrLogger.Hub.notify_iodevice_closed(working_directory)
        end
        new_iodevice
      else
        iodevice
      end
    {:noreply, %{state|iodevice: upd_iodevice}}
  end

  # @impl true
  # def handle_info(:flush_queue, %@self{queue: queue, iodevice: iodevice, working_directory: working_directory, state: current_state, current_task_ref: current_task_ref} = state) do
  #   # Submit request to flush the queue
  #   task = Task.Supervisor.async_nolink(DistrLogger.TaskSupervisor, fn ->
  #     flush_queue(queue, iodevice, working_directory)
  #   end)
  #   {:noreply, %{state|state: :flushing, current_task_ref: task.ref}}
  # end

  # Private functions
  defp init_from_working_directory(%@self{max_queue_length: max_queue_length, flush_chunk_size: flush_chunk_size, hub: hub, working_directory: working_directory} = state, working_directory) do
    effective_working_directory = working_directory |> get_effective_working_directory()
    IO.puts "Starting worker at working directory [#{effective_working_directory}]"
    iodevice = effective_working_directory |> open_iodevice()
    unless iodevice === nil do
      hub |> DistrLogger.Hub.notify_iodevice_opened(effective_working_directory)
    end
    queue_pid =
      case DynamicSupervisor.start_child(DistrLogger.Queue.Supervisor, {DistrLogger.Queue, {self(), max_queue_length, flush_chunk_size}}) do
        {:ok, pid} -> pid
        something_else -> raise "Failed to start queue due to #{inspect something_else}"
      end
    %{state|working_directory: effective_working_directory, iodevice: iodevice, queue: queue_pid}
  end

  defp get_effective_working_directory(working_directory) do
    unless working_directory |> File.exists? do
      working_directory |> File.mkdir_p!
    end
    unless working_directory |> Path.basename() == "State" do
      effective_working_directory = working_directory |> Path.join("State")
      effective_working_directory |> File.mkdir!
      effective_working_directory
    else
      working_directory
    end
  end

  defp open_iodevice(directory_name) do
    filename = directory_name |> Path.join("state")
    filename |> File.open!([:utf8, :append])
  end

  defp get_opened_iodevice(nil, working_directory), do: working_directory |> open_iodevice()
  defp get_opened_iodevice(iodevice, _working_directory), do: iodevice

  defp name_options(nil), do: []
  defp name_options(name), do: [name: name]

  defp enqueue_msg(nil, msg), do: Qex.new() |> enqueue_msg(msg)
  defp enqueue_msg(queue, msg), do: queue |> Qex.push(msg)

  defp enqueue(nil, message, current_queue_length, max_queue_length), do: Qex.new() |> enqueue(message, current_queue_length, max_queue_length)
  defp enqueue(queue, message, current_queue_length, max_queue_length) do
    if current_queue_length < max_queue_length do
      {queue |> Qex.push(message), current_queue_length + 1}
    else
      :queue_is_full
    end
  end

  defp flush_queue(queue, iodevice, working_directory) do
    opened_iodevice = iodevice |> get_opened_iodevice(working_directory)
    iodevice_was_opened_locally = opened_iodevice !== nil and opened_iodevice !== iodevice
    # if iodevice_was_opened_locally do
    #   hub |> DistrLogger.Hub.notify_iodevice_opened(working_directory)
    # end
    queue |> Enum.to_list() |> Enum.each(fn item ->
      # IO.puts "Item is #{inspect item}"
      opened_iodevice |> IO.puts(item)
    end)
    # if iodevice_was_opened_locally do
    #   hub |> DistrLogger.Hub.notify_iodevice_closed(working_directory)
    # end
    :flushed_queue
  end

  defp flush_chunk(chunk, iodevice, working_directory, hub) do
    opened_iodevice = iodevice |> get_opened_iodevice(working_directory)
    iodevice_was_opened_locally = opened_iodevice !== nil and opened_iodevice !== iodevice
    if iodevice_was_opened_locally do
      hub |> DistrLogger.Hub.notify_iodevice_opened(working_directory)
    end
    # IO.puts "chunk is: #{inspect chunk}"
    chunk |> Enum.each(fn item ->
      # IO.puts "Item is #{inspect item}"
      opened_iodevice |> IO.puts("#{item}")
    end)
    if iodevice_was_opened_locally do
      hub |> DistrLogger.Hub.notify_iodevice_closed(working_directory)
    end
    :flushed_chunk
  end

  defp try_dequeue(nil), do: nil
  defp try_dequeue(:dmpty), do: nil
  defp try_dequeue(queue) do
    case queue |> Qex.pop() do
      {{:value, item}, rem_queue} -> {item, rem_queue}
      {:empty, _rem_queue} -> nil
    end
  end

  defp close_iodevice(nil), do: nil
  defp close_iodevice(iodevice) do
    iodevice |> File.close |> case do
      :ok -> nil
      {:error, error} -> raise "Failed to close iodevice [#{inspect iodevice}] due to error: #{error}"
    end
  end

  defp is_opened?(nil), do: false
  defp is_opened?(_iodevice), do: true

  defp reply(state, reply), do: {:reply, reply, state}

  defp queue_length_safe(nil), do: 0
  defp queue_length_safe(queue), do: queue |> Enum.count()
end
