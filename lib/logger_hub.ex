defmodule DistrLogger.Hub do
  use GenServer
  use Timex
  @self __MODULE__
  defstruct [:workers, :working_directory, :current_worker_instance, :busy_worker_count, :opened_iodevices, :start_requests, :iodevice_stats, :last_counter_updated, :workers_detailed_stats, :last_log_request, :duration_tracking_timer_ref, :sync_duration]

  # API
  def start_link(working_directory) do
    @self |> GenServer.start_link(working_directory, name: @self)
  end

  def add_worker(hub \\ @self, max_queue_length \\ 20, flush_chunk_size \\ 5) do
    hub |> GenServer.cast({:add_worker, max_queue_length, flush_chunk_size})
  end

  def register(hub \\ @self, worker) do
    hub |> GenServer.cast({:register, worker})
  end

  def loop_workers(hub \\ @self) do
    hub |> GenServer.cast(:loop_workers)
  end

  def log(hub \\ @self, message) do
    hub |> GenServer.call({:log, message})
  end

  def notify_queue_emptied(hub \\ @self, logger, working_directory) do
    hub |> GenServer.cast({:queue_emptied, logger, working_directory})
  end

  def notify_queue_started(hub \\ @self, logger, working_directory) do
    hub |> GenServer.cast({:queue_started, logger, working_directory})
  end

  def get_state(hub \\ @self) do
    hub |> GenServer.call(:get_state)
  end

  def notify_iodevice_opened(hub \\ @self, working_directory) do
    hub |> GenServer.cast({:iodevice_opened, working_directory})
  end

  def notify_iodevice_closed(hub \\ @self, working_directory) do
    hub |> GenServer.cast({:iodevice_closed, working_directory})
  end

  def get_queues_and_writing_info(hub \\ @self) do
    hub |> GenServer.call(:get_queues_and_writing_info)
  end

  def notify_max_queue(hub \\ @self, working_directory, max_queue) do
    hub |> GenServer.cast({:max_queue, working_directory, max_queue})
  end

  # Callbacks
  @impl true
  def init(working_directory) do
    IO.puts "Starting hub at [#{working_directory}]"
    {:ok, %@self{workers: MapSet.new(), working_directory: ensure_directory_exists(working_directory), busy_worker_count: 0, opened_iodevices: 0}}
  end

  @impl true
  def handle_cast(:loop_workers, %@self{current_worker_instance: current_worker_instance} = state) do
    working_folders = traverse_workers(current_worker_instance, current_worker_instance, []) |> Enum.reverse()
    working_folders |> Enum.each(fn working_folder ->
      IO.puts working_folder
    end)
    {:noreply, state}
  end

  @impl true
  def handle_cast({:register, worker}, %@self{workers: workers} = state) do
    {:noreply, %{state|workers: workers |> MapSet.put(worker)}}
  end

  @impl true
  def handle_cast({:add_worker, max_queue_length, flush_chunk_size}, %@self{working_directory: working_directory, current_worker_instance: current_worker_instance} = state) do
    worker_directory = working_directory |> make_worker_directory_with_retry(5)
    new_worker = try_start_worker(worker_directory, max_queue_length, flush_chunk_size, self(), 5)
    new_current_worker_instance =
      case current_worker_instance do
        nil ->
          new_instance = DistrLogger.Instance.start_link(new_worker) |> DistrLogger.Helpers.get_pid!
          new_instance |> DistrLogger.Instance.set_next(new_instance)
          new_instance
        pid when pid |> is_pid() ->
          new_instance = DistrLogger.Instance.start_link(new_worker) |> DistrLogger.Helpers.get_pid!
          case pid |> DistrLogger.Instance.next() do
            nil ->
              pid |> DistrLogger.Instance.set_next(new_instance)
              new_instance |> DistrLogger.Instance.set_next(pid)
              pid
            next when next |> is_pid() ->
              pid |> DistrLogger.Instance.set_next(new_instance)
              new_instance |> DistrLogger.Instance.set_next(next)
              pid
          end
      end
    {:noreply, %{state|current_worker_instance: new_current_worker_instance}}
  end

  @impl true
  def handle_cast({:queue_emptied, _logge, working_directory}, %@self{busy_worker_count: busy_worker_count, workers_detailed_stats: workers_detailed_stats} = state) do
    upd_busy_worker_count = busy_worker_count - 1
    IO.puts "One queue emptied. Busy queue count: #{upd_busy_worker_count}"
    case upd_busy_worker_count do
      0 -> IO.puts "All queues are flushed now"
      negative when negative |> is_integer() and negative < 0 ->
        IO.puts "OOps: current busy_worker_count === #{upd_busy_worker_count} which is weird"
      _ -> :nothing
    end
    {
      :noreply,
      %{state|
      busy_worker_count: upd_busy_worker_count,
      last_counter_updated: Timex.now(),
      workers_detailed_stats: workers_detailed_stats |> Map.put_new(working_directory, []) |> Map.update(working_directory, nil, &(["Queue emptied at #{Timex.now()}"|&1]))
      }
    }
  end

  @impl true
  def handle_cast({:queue_started, _logger, working_directory}, %@self{busy_worker_count: busy_worker_count, workers_detailed_stats: workers_detailed_stats} = state) do
    {
      :noreply,
      %{state|
        busy_worker_count: busy_worker_count + 1,
        last_counter_updated: Timex.now(),
        workers_detailed_stats: (workers_detailed_stats || %{}) |> Map.put_new(working_directory, []) |> Map.update(working_directory, nil, &(["Queue started at #{Timex.now()}"|&1]))
        }
      }
  end

  @impl true
  def handle_cast({:iodevice_opened, working_directory}, %@self{opened_iodevices: opened_iodevices, iodevice_stats: iodevice_stats, workers_detailed_stats: workers_detailed_stats} = state) do
    IO.puts "iodevice opened"
    {
      :noreply,
      %{state|
        opened_iodevices: opened_iodevices + 1,
        iodevice_stats: (iodevice_stats || %{}) |> Map.put_new(working_directory, %{}) |> update_in([working_directory, :opened_iodevice], &((&1 || 0) + 1)),
        last_counter_updated: Timex.now(),
        workers_detailed_stats: (workers_detailed_stats || %{}) |> Map.put_new(working_directory, []) |> Map.update(working_directory, nil, &(["iodevice opened at #{Timex.now()}"|&1]))
      }
    }
  end

  @impl true
  def handle_cast({:iodevice_closed, working_directory}, %@self{opened_iodevices: opened_iodevices, busy_worker_count: busy_worker_count, start_requests: start_requests, iodevice_stats: iodevice_stats, workers_detailed_stats: workers_detailed_stats} = state) do
    if opened_iodevices === 1 and busy_worker_count === 0 do
      IO.puts "Everything is closed. Hub is now resting... start_requests is #{start_requests}"
      IO.puts "It took #{Timex.diff(Timex.now(), start_requests, :milliseconds)} milliseconds"
      self() |> Process.send_after(:clear_start_requests, 1_500)
    end
    {
      :noreply,
      %{state|
        opened_iodevices: opened_iodevices - 1,
        iodevice_stats: iodevice_stats |> Map.put_new(working_directory, %{}) |> update_in([working_directory, :closed_iodevice], &((&1 || 0) + 1)),
        last_counter_updated: Timex.now(),
        workers_detailed_stats: workers_detailed_stats |> Map.put_new(working_directory, []) |> Map.update(working_directory, nil, &(["iodevice closed at #{Timex.now()}"|&1]))
        }
      }
  end

  @impl true
  def handle_cast({:max_queue, working_directory, max_queue}, %@self{workers_detailed_stats: workers_detailed_stats} = state) do
    {
      :noreply,
      %{state|
        workers_detailed_stats: workers_detailed_stats |> Map.update(working_directory, nil, &(["max queue: #{max_queue} at #{Timex.now()}"|&1]))
      }
    }
  end

  @impl true
  def handle_call({:log, message}, _from, %@self{current_worker_instance: current_worker_instance, start_requests: start_requests, workers_detailed_stats: workers_detailed_stats, duration_tracking_timer_ref: timer_ref} = state) do
    now = Timex.now()
    case current_worker_instance |> log_message(message) do
      {:ok, next_worker_instance} ->
        # IO.puts "Setting new current_worker_instance: [#{inspect next_worker_instance}]"
        new_start_requests = start_requests |>set_start_requests()
        new_workers_detailed_stats =
          unless new_start_requests !== nil and start_requests === nil do
            # Reset worker detaile7d stats
            workers_detailed_stats
          end
        {:reply, :ok, %{state|current_worker_instance: next_worker_instance, start_requests: new_start_requests, workers_detailed_stats: new_workers_detailed_stats, last_log_request: now, duration_tracking_timer_ref: (unless valid_timer?(timer_ref), do: self() |> new_duration_tracking_timer()) || timer_ref, sync_duration: nil}}
      :deny ->
        {:reply, :deny, %{state|start_requests: start_requests |> set_start_requests(), last_log_request: now}}
    end
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call(:get_queues_and_writing_info, _from, %@self{current_worker_instance: current_worker_instance} = state) do
    info = traverse_workers_and_do_stuff(current_worker_instance, current_worker_instance, &DistrLogger.get_queue_and_writing_info/1, [])
    {:reply, info, state}
  end

  @impl true
  def handle_info(:clear_start_requests, %@self{last_counter_updated: last_counter_updated, start_requests: start_requests, workers_detailed_stats: workers_detailed_stats} = state) do
    upd_start_requests = start_requests |> reset_start_requests(last_counter_updated)
    {:noreply, %{state|start_requests: upd_start_requests, workers_detailed_stats: upd_start_requests |> reverse_workers_detailed_stats(workers_detailed_stats)}}
  end

  @impl true
  def handle_info(:track_duration, %@self{last_log_request: last_log_request, start_requests: start_requests, sync_duration: current_sync_duration} = state) do
    now = Timex.now()
    {new_timer_ref, sync_duration} =
      if last_log_request != nil and Timex.diff(now, last_log_request, :milliseods) >= 100 and current_sync_duration === nil do
        duration = Timex.diff(last_log_request, start_requests, :milliseconds)
        IO.puts "Synchronous processing took #{duration} milliseconds"
        {nil, duration}
      else
        {self() |> new_duration_tracking_timer(), current_sync_duration}
      end
    {:noreply, %{state|duration_tracking_timer_ref: new_timer_ref, sync_duration: sync_duration}}
  end

  # Helpers
  defp check_directory_exists(directory) do
    if directory |> File.exists?() do
      :exists
    else
      directory |> File.mkdir_p!
      {:ok, directory}
    end
  end

  defp ensure_directory_exists(directory) do
    directory |> File.mkdir_p!
    directory
  end

  defp make_worker_directory(base_working_directory) do
    base_working_directory |> Path.join("#{get_datetime_microsecods()}") |> check_directory_exists()
  end

  defp make_worker_directory_with_retry(base_working_directory, retry_count)
  defp make_worker_directory_with_retry(base_working_directory, retry_count) when retry_count > 0 do
    case make_worker_directory(base_working_directory) do
      :exists -> make_worker_directory_with_retry(base_working_directory, retry_count - 1)
      {:ok, directory} -> directory
    end
  end
  defp make_worker_directory_with_retry(base_worker_directory, 0), do: raise "Failed to create worker directory in the base directory [#{base_worker_directory}]"

  defp get_datetime_microsecods() do
    System.monotonic_time(:second) * 1_000_000 + :rand.uniform(999_999)
  end

  defp traverse_workers(_start_instance, nil, working_folders), do: working_folders
  defp traverse_workers(start_instance, current_instance, working_folders) do
    working_folder = current_instance |> DistrLogger.Instance.self() |> DistrLogger.get_working_folder()
    upd_working_folders = [working_folder|working_folders]
    case current_instance |> DistrLogger.Instance.next() do
      ^start_instance -> upd_working_folders
      next -> traverse_workers(start_instance, next, upd_working_folders)
    end
  end

  defp traverse_workers_and_do_stuff(_start_instance, nil, _function, accumulator), do: accumulator
  defp traverse_workers_and_do_stuff(start_instance, current_instance, function, accumulator) do
    new_accumulator = [current_instance |> DistrLogger.Instance.self() |> function.()|(accumulator || [])]
    case current_instance |> DistrLogger.Instance.next() do
      ^start_instance -> new_accumulator
      next -> traverse_workers_and_do_stuff(start_instance, next, function, new_accumulator)
    end
  end

  defp log_message(worker_instance, message) do
    log_message_loop(worker_instance, worker_instance, message)
  end

  defp log_message_loop(start_instance, current_instance, message) do
    case current_instance |> DistrLogger.Instance.self() |> DistrLogger.log(message) do
      :ok ->
        {:ok, current_instance |> DistrLogger.Instance.next()}
      :deny ->
        case current_instance |> DistrLogger.Instance.next() do
          nil -> :deny
          ^start_instance -> :deny
          next_instance -> log_message_loop(start_instance, next_instance, message)
        end
    end
  end

  defp try_start_worker(worker_directory, max_queue_length, flush_chunk_size, hub, try_count) when try_count > 0 do
    case start_worker(worker_directory, max_queue_length, flush_chunk_size, hub) do
      {:ok, worker_pid} ->
        self() |> register(worker_pid)
        worker_pid
      {:error, {%File.Error{reason: :eexist}}} ->
        try_start_worker(worker_directory, max_queue_length, flush_chunk_size, hub, try_count - 1)
      something_wrong -> raise "Failed to add worker due to #{inspect something_wrong}"
    end
  end

  defp try_start_worker(_worker_directory, _max_queue_length, _flush_chunk_size, _hub, 0), do: raise "Couldn't start worker after all!"

  defp start_worker(worker_directory, max_queue_length, flush_chunk_size, hub) do
    DynamicSupervisor.start_child(DistrLogger.Worker.Supervisor, {DistrLogger, {worker_directory, max_queue_length, flush_chunk_size, hub}})
  end

  defp set_start_requests(nil) do
    start_requests = Timex.now()
    IO.puts "Setting start_requets to #{start_requests}"
    start_requests
  end

  defp set_start_requests(start_requests), do: start_requests

  defp reset_start_requests(start_requests, last_counter_updated) do
    if last_counter_updated != nil and Timex.diff(Timex.now(), last_counter_updated, :seconds) < 1, do: start_requests
  end

  defp reverse_workers_detailed_stats(start_requests, workers_detailed_stats)
  defp reverse_workers_detailed_stats(nil, workers_detailed_stats) do
    for {working_folder, stats_list} <- workers_detailed_stats, into: %{}, do: {working_folder, stats_list |> Enum.reverse()}
  end
  defp reverse_workers_detailed_stats(_start_requests, workers_detailed_stats), do: workers_detailed_stats

  defp valid_timer?(nil), do: false
  defp valid_timer?(timer_ref), do: Process.read_timer(timer_ref) |> is_integer()

  defp new_duration_tracking_timer(pid), do: pid |> Process.send_after(:track_duration, 100)
end
