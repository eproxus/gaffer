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
-ignore_xref(pause/1).
-export([pause/1]).
-ignore_xref(resume/1).
-export([resume/1]).
% gen_statem Callbacks
-export([callback_mode/0]).
-export([init/1]).
-ignore_xref(active/3).
-export([active/3]).
-ignore_xref(paused/3).
-export([paused/3]).

%--- API -----------------------------------------------------------------------

-spec start_link(gaffer:queue()) -> gen_statem:start_ret().
start_link(Name) ->
    gen_statem:start_link({local, proc_name(Name)}, ?MODULE, Name, []).

-spec poll(gaffer:queue()) -> ok.
poll(Name) -> call(Name, poll).

-spec reconfigure(gaffer:queue()) -> ok.
reconfigure(Name) -> call(Name, reconfigure).

-spec info(gaffer:queue()) ->
    #{active := non_neg_integer(), status := active | paused}.
info(Name) -> call(Name, info).

-spec pause(gaffer:queue()) -> ok | {error, already_paused}.
pause(Name) -> call(Name, pause).

-spec resume(gaffer:queue()) -> ok | {error, already_active}.
resume(Name) -> call(Name, resume).

%--- gen_statem Callbacks ------------------------------------------------------

callback_mode() -> state_functions.

init(Name) ->
    Conf = gaffer_queue:conf(Name),
    Data = #{name => Name, workers => #{}},
    {ok, active, Data, initial_poll(Conf) ++ poll_timeout(Conf)}.

active(internal, poll, #{name := Name} = Data) ->
    {keep_state, do_poll(gaffer_queue:conf(Name), Data)};
active(state_timeout, poll, #{name := Name} = Data) ->
    Conf = gaffer_queue:conf(Name),
    {keep_state, do_poll(Conf, Data), poll_timeout(Conf)};
active({call, From}, reconfigure, #{name := Name}) ->
    {keep_state_and_data, [
        {reply, From, ok} | poll_timeout(gaffer_queue:conf(Name))
    ]};
active({call, From}, pause, Data) ->
    {next_state, paused, Data, [{reply, From, ok}]};
active({call, From}, resume, _Data) ->
    {keep_state_and_data, [{reply, From, {error, already_active}}]};
active(EventType, Event, Data) ->
    common(EventType, Event, active, Data).

paused({call, From}, reconfigure, _Data) ->
    {keep_state_and_data, [{reply, From, ok}]};
paused({call, From}, pause, _Data) ->
    {keep_state_and_data, [{reply, From, {error, already_paused}}]};
paused({call, From}, resume, #{name := Name} = Data) ->
    Conf = gaffer_queue:conf(Name),
    {next_state, active, Data, [
        {reply, From, ok}, {next_event, internal, poll} | poll_timeout(Conf)
    ]};
paused(EventType, Event, Data) ->
    common(EventType, Event, paused, Data).

common({call, From}, poll, _State, #{name := Name} = Data) ->
    {keep_state, do_poll(gaffer_queue:conf(Name), Data), [{reply, From, ok}]};
common({call, From}, info, State, #{workers := Workers}) ->
    Result = #{active => map_size(Workers), status => State},
    {keep_state_and_data, [{reply, From, Result}]};
common(
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
