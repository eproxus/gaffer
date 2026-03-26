-module(gaffer_queue_runner).
-moduledoc false.

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
-ignore_xref(reconfigure/1).
-export([reconfigure/1]).
-ignore_xref(info/1).
-export([info/1]).
-export([pid/1]).
% gen_statem Callbacks
-export([callback_mode/0]).
-export([init/1]).
-export([handle_event/4]).

%--- API -----------------------------------------------------------------------

-spec start_link(gaffer:queue(), gaffer_queue:queue_conf()) ->
    gen_statem:start_ret().
start_link(Name, _Conf) ->
    gen_statem:start_link({local, proc_name(Name)}, ?MODULE, Name, []).

-spec poll(gaffer:queue()) -> ok.
poll(Name) -> call(Name, poll).

-spec complete(gaffer:queue(), gaffer:job_id()) ->
    {ok, gaffer:job()} | {error, term()}.
complete(Name, Id) -> call(Name, {complete, Id}).

-spec fail(gaffer:queue(), gaffer:job_id(), term()) ->
    {ok, gaffer:job()} | {error, term()}.
fail(Name, Id, Reason) -> call(Name, {fail, Id, Reason}).

-spec schedule(gaffer:queue(), gaffer:job_id(), gaffer:timestamp()) ->
    {ok, gaffer:job()} | {error, term()}.
schedule(Name, Id, At) -> call(Name, {schedule, Id, At}).

-spec claim(gaffer:queue(), gaffer_queue:claim_opts()) -> [gaffer:job()].
claim(Name, Opts) -> call(Name, {claim, Opts}).

-spec prune(gaffer:queue(), gaffer_queue:prune_opts()) -> non_neg_integer().
prune(Name, Opts) -> call(Name, {prune, Opts}).

-spec reconfigure(gaffer:queue()) -> ok.
reconfigure(Name) -> call(Name, reconfigure).

-spec info(gaffer:queue()) -> non_neg_integer().
info(Name) -> call(Name, info).

-spec pid(gaffer:queue()) -> pid().
pid(Name) ->
    Pid = erlang:whereis(proc_name(Name)),
    true = is_pid(Pid),
    Pid.

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
handle_event({call, From}, Cmd, _State, #{name := Name} = Data) ->
    {keep_state, Data, [{reply, From, dispatch(Cmd, Name)}]};
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
                queue => Name,
                limit => Limit
            }),
            NewWorkers = spawn_workers(Worker, Jobs, Workers),
            Data#{workers := NewWorkers}
    end.

poll_limit(infinity, _Active) -> infinity;
poll_limit(Max, Active) -> max(0, Max - Active).

spawn_workers(_Worker, [], Workers) ->
    Workers;
spawn_workers(
    Worker, [#{id := JobId, queue := Queue} = Job | Rest], Workers
) ->
    {Pid, _Ref} = spawn_monitor(fun() ->
        Result = gaffer_worker:perform(Worker, Job),
        exit({gaffer_result, Result})
    end),
    spawn_workers(Worker, Rest, Workers#{Pid => {JobId, Queue}}).

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

proc_name(Name) ->
    % elp:ignore W0023 - bounded by queue count, not user input
    binary_to_atom(
        <<"gaffer_queue_runner_", (atom_to_binary(Name))/binary>>
    ).
