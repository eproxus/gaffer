-module(gaffer_queue_runner).

-behaviour(gen_statem).

% API
-ignore_xref(start_link/2).
-export([start_link/2]).
-ignore_xref(poll/1).
-export([poll/1]).
-ignore_xref(complete/2).
-export([complete/2]).
-ignore_xref(fail/3).
-export([fail/3]).
-ignore_xref(schedule/3).
-export([schedule/3]).
-ignore_xref(claim/2).
-export([claim/2]).
-ignore_xref(prune/2).
-export([prune/2]).
-ignore_xref(info/1).
-export([info/1]).
% gen_statem Callbacks
-export([callback_mode/0]).
-export([init/1]).
-export([handle_event/4]).

%--- API -----------------------------------------------------------------------

-spec start_link(gaffer:queue_name(), gaffer_queue:queue_conf()) ->
    gen_statem:start_ret().
start_link(Name, Conf) ->
    gen_statem:start_link(
        {local, proc_name(Name)},
        ?MODULE,
        {Name, Conf},
        []
    ).

-spec poll(gaffer:queue_name()) -> ok.
poll(Name) ->
    gen_statem:call(proc_name(Name), poll).

-spec complete(gaffer:queue_name(), gaffer:job_id()) ->
    {ok, gaffer:job()} | {error, term()}.
complete(Name, Id) ->
    gen_statem:call(proc_name(Name), {complete, Id}).

-spec fail(
    gaffer:queue_name(), gaffer:job_id(), term()
) ->
    {ok, gaffer:job()} | {error, term()}.
fail(Name, Id, Reason) ->
    gen_statem:call(
        proc_name(Name), {fail, Id, Reason}
    ).

-spec schedule(
    gaffer:queue_name(), gaffer:job_id(), gaffer:timestamp()
) ->
    {ok, gaffer:job()} | {error, term()}.
schedule(Name, Id, At) ->
    gen_statem:call(
        proc_name(Name), {schedule, Id, At}
    ).

-spec claim(gaffer:queue_name(), gaffer:claim_opts()) ->
    [gaffer:job()].
claim(Name, Opts) ->
    gen_statem:call(proc_name(Name), {claim, Opts}).

-spec prune(gaffer:queue_name(), gaffer:prune_opts()) ->
    non_neg_integer().
prune(Name, Opts) ->
    gen_statem:call(proc_name(Name), {prune, Opts}).

-spec info(gaffer:queue_name()) -> map().
info(Name) ->
    gen_statem:call(proc_name(Name), info).

%--- gen_statem Callbacks ------------------------------------------------------

callback_mode() -> handle_event_function.

init({Name, Conf}) ->
    Data = #{
        name => Name,
        workers => #{},
        max_workers => maps:get(max_workers, Conf, 5),
        global_max_workers => maps:get(global_max_workers, Conf, 25),
        poll_interval => maps:get(poll_interval, Conf, 1000),
        worker_mod => maps:get(worker, Conf, undefined)
    },
    {ok, polling, Data, [poll_timeout(Data)]}.

handle_event(
    {call, From}, poll, _State, #{name := Name} = Data
) ->
    Data1 = do_poll(Name, Data),
    {keep_state, Data1, [{reply, From, ok}, poll_timeout(Data1)]};
handle_event({call, From}, info, _State, Data) ->
    {keep_state, Data, [{reply, From, worker_info(Data)}]};
handle_event(
    {call, From}, Cmd, _State, #{name := Name} = Data
) ->
    Reply = dispatch(Cmd, Name),
    {keep_state, Data, [{reply, From, Reply}]};
handle_event(
    state_timeout, poll, _State, #{name := Name} = Data
) ->
    Data1 = do_poll(Name, Data),
    {keep_state, Data1, [poll_timeout(Data1)]};
handle_event(
    info,
    {'DOWN', _Ref, process, Pid, Reason},
    _State,
    #{name := Name, workers := Workers} = Data
) ->
    case maps:take(Pid, Workers) of
        {{JobId, Queue}, Workers1} ->
            _ = handle_worker_result(JobId, Queue, Reason, Name),
            {keep_state, Data#{workers := Workers1}};
        error ->
            {keep_state, Data}
    end.

%--- Internal ------------------------------------------------------------------

do_poll(
    Name,
    #{workers := Workers, max_workers := MaxWorkers, worker_mod := WorkerMod} =
        Data
) ->
    Limit = MaxWorkers - map_size(Workers),
    case Limit > 0 of
        false ->
            Data;
        true ->
            Jobs = gaffer_queue:claim_jobs(Name, #{
                queue => Name,
                limit => Limit
            }),
            NewWorkers = spawn_workers(WorkerMod, Jobs, Workers),
            Data#{workers := NewWorkers}
    end.

spawn_workers(_WorkerMod, [], Workers) ->
    Workers;
spawn_workers(
    WorkerMod, [#{id := JobId, queue := Queue} = Job | Rest], Workers
) ->
    {Pid, _Ref} = spawn_monitor(fun() ->
        Result = gaffer_worker:perform(WorkerMod, Job),
        exit({gaffer_result, Result})
    end),
    spawn_workers(WorkerMod, Rest, Workers#{Pid => {JobId, Queue}}).

handle_worker_result(JobId, Queue, {gaffer_result, Result}, Name) ->
    dispatch(worker_cmd(JobId, Queue, Result), Name);
handle_worker_result(JobId, _Queue, CrashReason, Name) ->
    dispatch(fail_cmd(JobId, CrashReason), Name).

worker_cmd(JobId, _Queue, complete) -> {complete, JobId};
worker_cmd(JobId, _Queue, {complete, _}) -> {complete, JobId};
worker_cmd(JobId, _Queue, {fail, Reason}) -> fail_cmd(JobId, Reason);
worker_cmd(JobId, Queue, {cancel, _}) -> {cancel, Queue, JobId};
worker_cmd(JobId, _Queue, {schedule, At}) -> {schedule, JobId, At}.

fail_cmd(JobId, Reason) ->
    {fail, JobId, Reason}.

poll_timeout(#{poll_interval := infinity}) ->
    {state_timeout, infinity, poll};
poll_timeout(#{poll_interval := Interval}) ->
    {state_timeout, Interval, poll}.

dispatch({complete, Id}, Name) ->
    gaffer_queue:complete_job(Name, Id);
dispatch({fail, Id, Reason}, Name) ->
    gaffer_queue:fail_job(Name, Id, Reason);
dispatch({schedule, Id, At}, Name) ->
    gaffer_queue:schedule_job(Name, Id, At);
dispatch({cancel, Queue, Id}, _Name) ->
    gaffer_queue:cancel_job(Queue, Id);
dispatch({claim, #{queue := Queue} = Opts}, _Name) ->
    gaffer_queue:claim_jobs(Queue, Opts);
dispatch({prune, Opts}, Name) ->
    gaffer_queue:prune_jobs(Name, Opts).

worker_info(#{
    workers := Workers, max_workers := MaxW, global_max_workers := GlobalMaxW
}) ->
    #{
        active => map_size(Workers),
        max => #{local => MaxW, global => GlobalMaxW}
    }.

proc_name(Name) ->
    % elp:ignore W0023 - bounded by queue count, not user input
    binary_to_atom(
        <<"gaffer_queue_runner_", (atom_to_binary(Name))/binary>>
    ).
