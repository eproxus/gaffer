-module(gaffer_queue).

%% Functional API for managing a queue and its jobs. No process —
%% takes a driver tuple {Mod, DriverState} on all calls.
%%
%% This is the only module that calls driver callbacks.

-export([new/2]).
-export([put_conf/2]).
-export([get_conf/2]).
-export([delete_conf/2]).
-export([insert/4]).
-export([get/2]).
-export([list/2]).
-export([cancel/2]).
-export([complete/2]).
-export([fail/3]).
-export([schedule/3]).
-export([claim/2]).
-export([prune/2]).
-export([validate/1]).
-export([transition/2]).
-export([add_error/2]).

-ignore_xref([new/2, validate/1, transition/2, add_error/2]).

-type driver() :: {module(), gaffer_driver:driver_state()}.
-export_type([driver/0]).

%--- Queue construction -------------------------------------------------------

-spec new(module(), map()) -> driver().
new(Mod, Opts) ->
    DS = Mod:start(Opts),
    {Mod, DS}.

%--- Queue config operations --------------------------------------------------

-spec put_conf(gaffer:queue_conf(), driver()) ->
    driver().
put_conf(Conf, {Mod, DS}) ->
    DS1 = Mod:queue_put(Conf, DS),
    {Mod, DS1}.

-spec get_conf(gaffer:queue_name(), driver()) ->
    {gaffer:queue_conf(), driver()}.
get_conf(Name, {Mod, DS}) ->
    {Conf, DS1} = Mod:queue_get(Name, DS),
    {Conf, {Mod, DS1}}.

-spec delete_conf(gaffer:queue_name(), driver()) ->
    driver().
delete_conf(Name, {Mod, DS}) ->
    DS1 = Mod:queue_delete(Name, DS),
    {Mod, DS1}.

%--- Job operations -----------------------------------------------------------

-spec insert(gaffer:queue_name(), term(), gaffer:job_opts(), driver()) ->
    {gaffer:job(), driver()}.
insert(Queue, Payload, Opts, {Mod, DS}) ->
    Job = build_job(Queue, Payload, Opts),
    validate(Job),
    {Inserted, DS1} = Mod:job_insert(Job, DS),
    {Inserted, {Mod, DS1}}.

-spec get(gaffer:job_id(), driver()) ->
    {ok, gaffer:job()} | {error, not_found}.
get(Id, {Mod, DS}) ->
    case Mod:job_get(Id, DS) of
        {not_found, _DS1} -> {error, not_found};
        {Job, _DS1} -> {ok, Job}
    end.

-spec list(gaffer:list_opts(), driver()) ->
    [gaffer:job()].
list(Opts, {Mod, DS}) ->
    {Jobs, _DS1} = Mod:job_list(Opts, DS),
    Jobs.

-spec cancel(gaffer:job_id(), driver()) ->
    {ok, gaffer:job(), driver()} | {error, term()}.
cancel(Id, {Mod, DS}) ->
    case Mod:job_get(Id, DS) of
        {not_found, _DS1} ->
            {error, not_found};
        {Job, DS1} ->
            case transition(Job, cancelled) of
                {ok, Updated} ->
                    DS2 = Mod:job_update(Updated, DS1),
                    {ok, Updated, {Mod, DS2}};
                {error, _} = Err ->
                    Err
            end
    end.

-spec complete(gaffer:job_id(), driver()) ->
    {ok, gaffer:job(), driver()} | {error, term()}.
complete(Id, {Mod, DS}) ->
    case Mod:job_get(Id, DS) of
        {not_found, _DS1} ->
            {error, not_found};
        {#{attempt := Attempt} = Job, DS1} ->
            case transition(Job, completed) of
                {ok, Updated} ->
                    Result = Updated#{attempt := Attempt + 1},
                    DS2 = Mod:job_update(Result, DS1),
                    {ok, Result, {Mod, DS2}};
                {error, _} = Err ->
                    Err
            end
    end.

-spec fail(gaffer:job_id(), gaffer:job_error(), driver()) ->
    {ok, gaffer:job(), driver()} | {error, term()}.
fail(Id, Error, {Mod, DS}) ->
    case Mod:job_get(Id, DS) of
        {not_found, _DS1} ->
            {error, not_found};
        {Job, DS1} ->
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
            DS2 = Mod:job_update(Job4, DS1),
            {ok, Job4, {Mod, DS2}}
    end.

-spec schedule(gaffer:job_id(), gaffer:timestamp(), driver()) ->
    {ok, gaffer:job(), driver()} | {error, term()}.
schedule(Id, At, {Mod, DS}) ->
    case Mod:job_get(Id, DS) of
        {not_found, _DS1} ->
            {error, not_found};
        {Job, DS1} ->
            case transition(Job, scheduled) of
                {ok, Updated} ->
                    Result = Updated#{scheduled_at => At},
                    DS2 = Mod:job_update(Result, DS1),
                    {ok, Result, {Mod, DS2}};
                {error, _} = Err ->
                    Err
            end
    end.

-spec claim(gaffer:claim_opts(), driver()) ->
    {[gaffer:job()], driver()}.
claim(Opts, {Mod, DS}) ->
    Now = erlang:system_time(microsecond),
    Changes = #{state => executing, attempted_at => Now},
    {Claimed, DS1} = Mod:job_claim(Opts, Changes, DS),
    {Claimed, {Mod, DS1}}.

-spec prune(gaffer:prune_opts(), driver()) ->
    {non_neg_integer(), driver()}.
prune(Opts, {Mod, DS}) ->
    {Count, DS1} = Mod:job_prune(Opts, DS),
    {Count, {Mod, DS1}}.

%--- Job construction (private) -----------------------------------------------

-spec build_job(gaffer:queue_name(), term(), gaffer:job_opts()) ->
    gaffer:job().
build_job(Queue, Payload, Opts) ->
    Now = erlang:system_time(microsecond),
    ScheduledAt = maps:get(scheduled_at, Opts, undefined),
    State =
        case ScheduledAt of
            undefined -> available;
            _ -> scheduled
        end,
    Job = #{
        id => keysmith:uuid(7),
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
validate(#{id := Id, queue := Queue} = Job) ->
    Checks = [
        {
            fun() -> is_binary(Id) andalso byte_size(Id) > 0 end,
            {invalid_id, Id}
        },
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
    run_checks(Checks);
validate(_) ->
    error({invalid_job, missing_required_fields}).

%--- State machine (private) --------------------------------------------------

-spec transition(gaffer:job(), gaffer:job_state()) ->
    {ok, gaffer:job()} | {error, {invalid_transition, term()}}.
transition(#{state := From} = Job, To) ->
    case valid_transition(From, To) of
        true ->
            Now = erlang:system_time(microsecond),
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

set_timestamp(scheduled, Now, Job) ->
    Job#{scheduled_at => Now};
set_timestamp(executing, Now, Job) ->
    Job#{attempted_at => Now};
set_timestamp(completed, Now, Job) ->
    Job#{completed_at => Now};
set_timestamp(cancelled, Now, Job) ->
    Job#{cancelled_at => Now};
set_timestamp(discarded, Now, Job) ->
    Job#{discarded_at => Now};
set_timestamp(_, _Now, Job) ->
    Job.

maybe_put(_Key, undefined, Map) -> Map;
maybe_put(Key, Value, Map) -> Map#{Key => Value}.

run_checks([]) ->
    ok;
run_checks([{Check, Reason} | Rest]) ->
    case Check() of
        true -> run_checks(Rest);
        false -> error({invalid_job, Reason})
    end.
