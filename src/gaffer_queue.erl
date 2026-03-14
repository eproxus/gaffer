-module(gaffer_queue).

%% Functional API for managing a queue and its jobs.
%%
%% This is the only module that calls driver callbacks.

%% Queue management
-export([init/0]).
-export([teardown/0]).
-export([create/1]).
-export([delete/1]).
-export([get/1]).
-export([update/2]).
-export([list/0]).
-export([lookup/1]).

%% Job operations (queue-name based)
-export([insert_job/3]).
-export([get_job/2]).
-export([list_jobs/1]).
-export([cancel_job/2]).

%% Job operations (driver-explicit, used by gaffer_queue_runner)
-export([complete_job/2]).
-export([fail_job/3]).
-export([schedule_job/3]).
-export([claim_jobs/2]).
-export([prune_jobs/2]).

-compile({no_auto_import, [get/1]}).

-type driver() :: {module(), gaffer_driver:driver_opts()}.

%% Dialyzer over-constrains types from internal call sites, making exhaustive
%% clauses in the state machine appear unreachable.
-dialyzer({no_match, [validate/1, valid_transition/2, set_timestamp/3]}).
-export_type([driver/0]).

%--- Queue management ----------------------------------------------------------

-spec init() -> ok.
init() ->
    ok.

-spec teardown() -> ok.
teardown() ->
    _ = [
        persistent_term:erase(Key)
     || {Key, _} <:- persistent_term:get(),
        is_gaffer_key(Key)
    ],
    ok.

-spec create(gaffer:queue_conf()) -> ok | {error, already_exists}.
create(#{name := Name, driver := {Mod, DS} = Driver} = Conf) ->
    case persistent_term:get({gaffer_queue, Name}, undefined) of
        undefined ->
            Mod:queue_put(Conf, DS),
            persistent_term:put({gaffer_queue, Name}, Driver),
            {ok, _Pid} = gaffer_sup:start_queue(Name),
            ok;
        _ ->
            {error, already_exists}
    end.

-spec delete(gaffer:queue_name()) -> ok.
delete(Name) ->
    ok = gaffer_sup:stop_queue(Name),
    {Mod, DS} = lookup(Name),
    persistent_term:erase({gaffer_queue, Name}),
    Mod:queue_delete(Name, DS),
    ok.

-spec list() -> [{gaffer:queue_name(), gaffer:queue_conf()}].
list() ->
    lists:filtermap(fun queue_entry/1, persistent_term:get()).

-spec queue_entry({term(), term()}) ->
    {true, {gaffer:queue_name(), gaffer:queue_conf()}} | false.
queue_entry({{gaffer_queue, Name}, {Mod, _}}) when
    is_atom(Name), is_atom(Mod)
->
    {true, {Name, get(Name)}};
queue_entry(_) ->
    false.

-spec get(gaffer:queue_name()) -> gaffer:queue_conf().
get(Name) ->
    call(Name, queue_get, [Name]).

-spec update(gaffer:queue_name(), map()) -> ok.
update(Name, Updates) ->
    Conf = get(Name),
    Merged = maps:merge(Conf, maps:remove(name, Updates)),
    call(Name, queue_put, [Merged]).

%--- Job operations (queue-name based) -----------------------------------------

-spec insert_job(gaffer:queue_name(), term(), gaffer:job_opts()) ->
    gaffer:job().
insert_job(Queue, Payload, Opts) ->
    NewJob = build_job(Queue, Payload, Opts),
    validate(NewJob),
    Inserted = call(Queue, job_insert, [NewJob]),
    validate_id(Inserted),
    Inserted.

-spec get_job(gaffer:queue_name(), gaffer:job_id()) ->
    {ok, gaffer:job()} | {error, not_found}.
get_job(Queue, JobId) ->
    case call(Queue, job_get, [JobId]) of
        not_found -> {error, not_found};
        Job -> {ok, Job}
    end.

-spec list_jobs(gaffer:list_opts()) ->
    [gaffer:job()].
list_jobs(#{queue := Queue} = Opts) ->
    call(Queue, job_list, [Opts]).

-spec cancel_job(gaffer:queue_name(), gaffer:job_id()) ->
    {ok, gaffer:job()} | {error, term()}.
cancel_job(Queue, JobId) ->
    case call(Queue, job_get, [JobId]) of
        not_found ->
            {error, not_found};
        Job ->
            case transition(Job, cancelled) of
                {ok, Updated} ->
                    call(Queue, job_update, [Updated]),
                    {ok, Updated};
                {error, _} = Err ->
                    Err
            end
    end.

%--- Job operations (driver-explicit) ------------------------------------------

-spec complete_job(gaffer:job_id(), driver()) ->
    {ok, gaffer:job()} | {error, term()}.
complete_job(Id, {Mod, DS}) ->
    case Mod:job_get(Id, DS) of
        not_found ->
            {error, not_found};
        #{attempt := Attempt} = Job ->
            case transition(Job, completed) of
                {ok, Updated} ->
                    Result = Updated#{attempt := Attempt + 1},
                    Mod:job_update(Result, DS),
                    {ok, Result};
                {error, _} = Err ->
                    Err
            end
    end.

-spec fail_job(gaffer:job_id(), gaffer:job_error(), driver()) ->
    {ok, gaffer:job()} | {error, term()}.
fail_job(Id, Error, {Mod, DS}) ->
    case Mod:job_get(Id, DS) of
        not_found ->
            {error, not_found};
        Job ->
            Attempt = maps:get(attempt, Job) + 1,
            MaxAttempts = maps:get(max_attempts, Job, 3),
            Job1 = Job#{attempt := Attempt},
            Job2 = add_error(Job1, Error),
            {ok, Job3} = transition(Job2, failed),
            Job4 =
                case Attempt >= MaxAttempts of
                    true ->
                        {ok, J} = transition(Job3, discarded),
                        J;
                    false ->
                        Job3
                end,
            Mod:job_update(Job4, DS),
            {ok, Job4}
    end.

-spec schedule_job(gaffer:job_id(), gaffer:timestamp(), driver()) ->
    {ok, gaffer:job()} | {error, term()}.
schedule_job(Id, At, {Mod, DS}) ->
    case Mod:job_get(Id, DS) of
        not_found ->
            {error, not_found};
        Job ->
            case transition(Job, scheduled) of
                {ok, Updated} ->
                    Result = Updated#{scheduled_at => At},
                    Mod:job_update(Result, DS),
                    {ok, Result};
                {error, _} = Err ->
                    Err
            end
    end.

-spec claim_jobs(gaffer:claim_opts(), driver()) ->
    [gaffer:job()].
claim_jobs(Opts, {Mod, DS}) ->
    Now = erlang:system_time(),
    Changes = #{state => executing, attempted_at => Now},
    Mod:job_claim(Opts, Changes, DS).

-spec prune_jobs(gaffer:prune_opts(), driver()) ->
    non_neg_integer().
prune_jobs(Opts, {Mod, DS}) ->
    Mod:job_prune(Opts, DS).

%--- Job construction (private) -----------------------------------------------

-spec build_job(gaffer:queue_name(), term(), gaffer:job_opts()) ->
    gaffer:new_job().
build_job(Queue, Payload, Opts) ->
    Now = erlang:system_time(),
    ScheduledAt = maps:get(scheduled_at, Opts, undefined),
    State =
        case ScheduledAt of
            undefined -> available;
            _ -> scheduled
        end,
    Job = #{
        queue => Queue,
        payload => Payload,
        state => State,
        attempt => 0,
        max_attempts => maps:get(max_attempts, Opts, 3),
        priority => maps:get(priority, Opts, 0),
        inserted_at => Now,
        errors => []
    },
    maybe_put(scheduled_at, ScheduledAt, Job).

%--- Validation (private) -----------------------------------------------------

-spec validate(map()) -> ok.
validate(#{queue := Queue} = Job) ->
    Checks = [
        {
            fun() -> is_atom(Queue) end,
            {invalid_queue, Queue}
        },
        {
            fun() -> maps:get(max_attempts, Job, 1) >= 1 end,
            invalid_max_attempts
        },
        {
            fun() -> maps:get(priority, Job, 0) >= 0 end,
            invalid_priority
        }
    ],
    run_checks(Checks).

validate_id(#{id := _}) -> ok.

%--- State machine (private) --------------------------------------------------

-spec transition(gaffer:job(), gaffer:job_state()) ->
    {ok, gaffer:job()} | {error, {invalid_transition, term()}}.
transition(#{state := From} = Job, To) ->
    case valid_transition(From, To) of
        true ->
            Now = erlang:system_time(),
            {ok, set_timestamp(To, Now, Job#{state := To})};
        false ->
            {error, {invalid_transition, {From, To}}}
    end.

-spec add_error(gaffer:job(), gaffer:job_error()) ->
    gaffer:job().
add_error(#{errors := Errors} = Job, Error) ->
    Job#{errors := [Error | Errors]}.

-spec valid_transition(gaffer:job_state(), gaffer:job_state()) ->
    boolean().
valid_transition(available, executing) -> true;
valid_transition(available, cancelled) -> true;
valid_transition(scheduled, available) -> true;
valid_transition(scheduled, cancelled) -> true;
valid_transition(executing, completed) -> true;
valid_transition(executing, failed) -> true;
valid_transition(executing, cancelled) -> true;
valid_transition(executing, scheduled) -> true;
valid_transition(failed, discarded) -> true;
valid_transition(failed, scheduled) -> true;
valid_transition(_, _) -> false.

set_timestamp(scheduled, TS, Job) -> Job#{scheduled_at => TS};
set_timestamp(executing, TS, Job) -> Job#{attempted_at => TS};
set_timestamp(completed, TS, Job) -> Job#{completed_at => TS};
set_timestamp(cancelled, TS, Job) -> Job#{cancelled_at => TS};
set_timestamp(discarded, TS, Job) -> Job#{discarded_at => TS};
set_timestamp(_State, _TS, Job) -> Job.

maybe_put(_Key, undefined, Map) -> Map;
maybe_put(Key, Value, Map) -> Map#{Key => Value}.

run_checks([]) ->
    ok;
run_checks([{Check, Reason} | Rest]) ->
    case Check() of
        true -> run_checks(Rest);
        false -> error({invalid_job, Reason})
    end.

%--- Driver dispatch (private) ------------------------------------------------

call(Queue, Fun, Args) ->
    {Mod, DS} = lookup(Queue),
    apply(Mod, Fun, Args ++ [DS]).

-spec lookup(gaffer:queue_name()) -> driver().
lookup(Name) ->
    case persistent_term:get({gaffer_queue, Name}, undefined) of
        undefined -> error({unknown_queue, Name});
        Driver -> Driver
    end.

is_gaffer_key({gaffer_queue, _}) -> true;
is_gaffer_key(_) -> false.
