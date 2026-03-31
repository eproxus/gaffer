-module(gaffer_queue_runner).
-moduledoc false.

-behaviour(gen_statem).

% API
-ignore_xref(start_link/1).
-export([start_link/1]).
-ignore_xref(poll/1).
-export([poll/1]).
-ignore_xref(reconfigure/1).
-export([reconfigure/1]).
-ignore_xref(info/1).
-export([info/1]).
% gen_statem Callbacks
-export([callback_mode/0]).
-export([init/1]).
-export([handle_event/4]).

%--- API -----------------------------------------------------------------------

-spec start_link(gaffer:queue()) -> gen_statem:start_ret().
start_link(Name) ->
    gen_statem:start_link({local, proc_name(Name)}, ?MODULE, Name, []).

-spec poll(gaffer:queue()) -> ok.
poll(Name) -> call(Name, poll).

-spec reconfigure(gaffer:queue()) -> ok.
reconfigure(Name) -> call(Name, reconfigure).

-spec info(gaffer:queue()) -> non_neg_integer().
info(Name) -> call(Name, info).

%--- gen_statem Callbacks ------------------------------------------------------

callback_mode() -> handle_event_function.

init(Name) ->
    Conf = gaffer_queue:conf(Name),
    Data = #{name => Name, workers => #{}},
    {ok, polling, Data, initial_poll(Conf) ++ poll_timeout(Conf)}.

handle_event(internal, poll, _State, #{name := Name} = Data) ->
    Conf = gaffer_queue:conf(Name),
    {keep_state, do_poll(Conf, Data)};
handle_event(state_timeout, poll, _State, #{name := Name} = Data) ->
    Conf = gaffer_queue:conf(Name),
    {keep_state, do_poll(Conf, Data), poll_timeout(Conf)};
handle_event({call, From}, poll, _State, #{name := Name} = Data) ->
    Conf = gaffer_queue:conf(Name),
    {keep_state, do_poll(Conf, Data), [{reply, From, ok} | poll_timeout(Conf)]};
handle_event({call, From}, reconfigure, _State, #{name := Name}) ->
    {keep_state_and_data, [
        {reply, From, ok} | poll_timeout(gaffer_queue:conf(Name))
    ]};
handle_event({call, From}, info, _State, #{workers := Workers}) ->
    {keep_state_and_data, [{reply, From, map_size(Workers)}]};
handle_event(
    info,
    {'DOWN', _Ref, process, Pid, Reason},
    _State,
    #{name := Name, workers := Workers} = Data
) ->
    case maps:take(Pid, Workers) of
        {OriginalJob, Workers1} ->
            _ = handle_worker_result(Reason, OriginalJob, Name),
            {keep_state, Data#{workers := Workers1}};
        error ->
            {keep_state, Data}
    end.

%--- Internal ------------------------------------------------------------------

call(Name, Msg) -> gen_statem:call(proc_name(Name), Msg).

initial_poll(#{poll_interval := infinity}) -> [];
initial_poll(#{}) -> [{next_event, internal, poll}].

poll_timeout(#{poll_interval := infinity}) -> [];
poll_timeout(#{poll_interval := Interval}) -> [{state_timeout, Interval, poll}].

do_poll(
    #{max_workers := MaxWorkers, worker := Worker},
    #{name := Name, workers := Workers} = Data
) ->
    case poll_limit(MaxWorkers, map_size(Workers)) of
        0 ->
            Data;
        Limit ->
            Jobs = gaffer_queue:claim_jobs(Name, #{
                queue => Name, limit => Limit
            }),
            NewWorkers = spawn_workers(Worker, Jobs, Workers),
            Data#{workers := NewWorkers}
    end.

poll_limit(infinity, _Active) -> infinity;
poll_limit(Max, Active) -> max(0, Max - Active).

% elp:ignore W0048 - spawn_workers calls gaffer_job:execute which is no_return
-dialyzer({no_return, [spawn_workers/3]}).
spawn_workers(_Worker, [], Workers) ->
    Workers;
spawn_workers(Worker, [Job | Rest], Workers) ->
    {Pid, _Ref} = spawn_monitor(fun() ->
        gaffer_job:execute(Worker, Job)
    end),
    spawn_workers(Worker, Rest, Workers#{Pid => Job}).

handle_worker_result({gaffer_job, Event, Job}, _OriginalJob, Name) ->
    gaffer_queue:write_result(Name, Event, Job);
handle_worker_result(Reason, OriginalJob, Name) ->
    {Event, Job} = gaffer_job:handle_crash(OriginalJob, Reason),
    gaffer_queue:write_result(Name, Event, Job).

proc_name(Name) ->
    % elp:ignore W0023 - bounded by queue count, not user input
    binary_to_atom(
        <<"gaffer_queue_runner_", (atom_to_binary(Name))/binary>>
    ).
