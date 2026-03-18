-module(gaffer_queue).

% API
% Queues
-export([init/0]).
-export([teardown/0]).
-export([create/1]).
-export([delete/1]).
-export([get/1]).
-export([update/2]).
-export([list/0]).
-export([queue_conf_template/0]).
% Jobs (user)
-export([insert_job/3]).
-export([get_job/2]).
-export([list_jobs/1]).
-export([cancel_job/2]).
-export([delete_job/2]).
% Jobs (runner)
-export([complete_job/2]).
-export([fail_job/3]).
-export([schedule_job/3]).
-export([claim_jobs/2]).
-export([prune_jobs/2]).

-compile({no_auto_import, [get/1]}).

%--- Types ---------------------------------------------------------------------

-type driver() :: {module(), gaffer_driver:driver_opts()}.

-type queue_conf() :: gaffer:queue_conf().

% elp:ignore W0048 - dialyzer over-constrains types from internal call sites
-dialyzer({no_match, [validate/1, valid_transition/2, set_timestamp/3]}).

-export_type([driver/0, queue_conf/0]).

%--- API -----------------------------------------------------------------------

% Queues

-spec init() -> ok.
init() ->
    ok.

-spec teardown() -> ok.
teardown() ->
    _ = [
        persistent_term:erase(Key)
     || {Key, _} <:- persistent_term:get(), is_gaffer_key(Key)
    ],
    ok.

-spec create(queue_conf()) -> ok | {error, already_exists}.
create(#{name := Name, driver := {Mod, DS} = Driver} = Conf) ->
    case persistent_term:get({gaffer_queue, Name}, undefined) of
        undefined ->
            Hooks = maps:get(hooks, Conf, []),
            Validated = validate_conf(strip_runtime(Conf)),
            Persisted = Validated#{name => Name},
            _ = gaffer_hooks:with_hooks(
                Hooks,
                [gaffer, queue, create],
                Conf,
                fun(C) ->
                    Mod:queue_insert(Persisted, DS),
                    persistent_term:put(
                        {gaffer_queue, Name}, {Driver, Hooks}
                    ),
                    {ok, _Pid} = gaffer_sup:start_queue(Name, Conf),
                    C
                end
            ),
            ok;
        _ ->
            {error, already_exists}
    end.

-spec delete(gaffer:queue_name()) -> ok.
delete(Name) ->
    ok = gaffer_sup:stop_queue(Name),
    _ = with_queue(Name, [gaffer, queue, delete], Name, fun(N, Mod, DS) ->
        persistent_term:erase({gaffer_queue, Name}),
        Mod:queue_delete(Name, DS),
        N
    end),
    ok.

-spec list() -> [{gaffer:queue_name(), gaffer:queue_conf()}].
list() ->
    lists:filtermap(fun queue_entry/1, persistent_term:get()).

queue_entry({{gaffer_queue, Name}, {{Mod, _}, _}}) when
    is_atom(Name), is_atom(Mod)
->
    {true, {Name, get(Name)}};
queue_entry(_) ->
    false.

-spec get(gaffer:queue_name()) -> gaffer:queue_conf().
get(Name) ->
    case driver(Name, queue_get, [Name]) of
        not_found -> error({unknown_queue, Name});
        Conf -> Conf
    end.

-spec update(gaffer:queue_name(), map()) -> ok.
update(Name, Updates) ->
    Validated = validate_updates(strip_runtime(Updates)),
    _ = with_queue(Name, [gaffer, queue, update], Updates, fun(U, Mod, DS) ->
        Mod:queue_update(Name, Validated, DS),
        U
    end),
    ok.

-spec queue_conf_template() -> map().
% erlfmt-ignore
queue_conf_template() ->
    #{
        global_max_workers => #{type => integer, default => 25},
        max_workers        => #{type => integer, default => 5},
        shutdown_timeout   => #{type => integer, default => 5000},
        max_attempts       => #{type => integer, default => 3},
        timeout            => #{type => integer, default => 30000},
        backoff            => #{type => integer, default => 1000},
        priority           => #{type => integer, default => 0},
        on_discard         => #{type => atom}
    }.

% Job (user)

-spec insert_job(gaffer:queue_name(), term(), gaffer:job_opts()) ->
    gaffer:job().
insert_job(Queue, Payload, Opts) ->
    NewJob = build_job(Queue, Payload, Opts),
    validate(NewJob),
    with_queue(Queue, [gaffer, job, insert], NewJob, fun(Job, Mod, DS) ->
        Mod:job_insert(Job, DS)
    end).

-spec get_job(gaffer:queue_name(), gaffer:job_id()) ->
    gaffer:job() | not_found.
get_job(Queue, JobId) ->
    driver(Queue, job_get, [JobId]).

-spec list_jobs(gaffer:list_opts()) ->
    [gaffer:job()].
list_jobs(#{queue := Queue} = Opts) ->
    driver(Queue, job_list, [Opts]).

-spec delete_job(gaffer:queue_name(), gaffer:job_id()) -> ok.
delete_job(Queue, JobId) ->
    case driver(Queue, job_delete, [JobId]) of
        not_found ->
            error({unknown_job, JobId});
        ok ->
            _ = with_queue(Queue, [gaffer, job, delete], JobId, fun(Id, _, _) ->
                Id
            end),
            ok
    end.

-spec cancel_job(gaffer:queue_name(), gaffer:job_id()) ->
    {ok, gaffer:job()} | {error, term()}.
cancel_job(Queue, JobId) ->
    with_job(Queue, JobId, fun(Job) ->
        case transition(Job, cancelled) of
            {ok, Updated} ->
                Result = with_queue(
                    Queue,
                    [gaffer, job, cancel],
                    Updated,
                    fun(J, Mod, DS) ->
                        Mod:job_update(J, DS),
                        J
                    end
                ),
                {ok, Result};
            {error, _} = Err ->
                Err
        end
    end).

% Job (runner)

-spec complete_job(gaffer:queue_name(), gaffer:job_id()) ->
    {ok, gaffer:job()} | {error, term()}.
complete_job(Queue, Id) ->
    with_job(Queue, Id, fun(#{attempt := Attempt} = Job) ->
        case transition(Job, completed) of
            {ok, Updated} ->
                Completed = Updated#{attempt := Attempt + 1},
                Result = with_queue(
                    Queue,
                    [gaffer, job, complete],
                    Completed,
                    fun(J, Mod, DS) ->
                        Mod:job_update(J, DS),
                        J
                    end
                ),
                {ok, Result};
            {error, _} = Err ->
                Err
        end
    end).

-spec fail_job(gaffer:queue_name(), gaffer:job_id(), gaffer:job_error()) ->
    {ok, gaffer:job()} | {error, term()}.
fail_job(Queue, Id, Error) ->
    with_job(Queue, Id, fun(Job) ->
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
        Result = with_queue(
            Queue,
            [gaffer, job, fail],
            Job4,
            fun(J, Mod, DS) ->
                Mod:job_update(J, DS),
                J
            end
        ),
        {ok, Result}
    end).

-spec schedule_job(gaffer:queue_name(), gaffer:job_id(), gaffer:timestamp()) ->
    {ok, gaffer:job()} | {error, term()}.
schedule_job(Queue, Id, At) ->
    with_job(Queue, Id, fun(Job) ->
        case transition(Job, scheduled) of
            {ok, Updated} ->
                Scheduled = Updated#{scheduled_at => At},
                Result = with_queue(
                    Queue,
                    [gaffer, job, schedule],
                    Scheduled,
                    fun(J, Mod, DS) ->
                        Mod:job_update(J, DS),
                        J
                    end
                ),
                {ok, Result};
            {error, _} = Err ->
                Err
        end
    end).

-spec claim_jobs(gaffer:queue_name(), gaffer:claim_opts()) ->
    [gaffer:job()].
claim_jobs(Queue, Opts) ->
    Now = erlang:system_time(),
    Changes = #{state => executing, attempted_at => Now},
    with_queue(Queue, [gaffer, job, claim], Opts, fun(_, Mod, DS) ->
        Mod:job_claim(Opts, Changes, DS)
    end).

-spec prune_jobs(gaffer:queue_name(), gaffer:prune_opts()) ->
    non_neg_integer().
prune_jobs(Queue, Opts) ->
    {{Mod, DS}, _} = lookup(Queue),
    Mod:job_prune(Opts, DS).

%--- Internal ------------------------------------------------------------------

is_gaffer_key({gaffer_queue, _}) -> true;
is_gaffer_key(_) -> false.

% Config validation

validate_conf(Conf) ->
    check_extra_keys(Conf),
    apply_defaults(Conf, queue_conf_template()).

validate_updates(Updates) when map_size(Updates) =:= 0 ->
    error({invalid_queue_conf, #{extra => []}});
validate_updates(Updates) ->
    check_extra_keys(Updates),
    Updates.

check_extra_keys(Map) ->
    Template = queue_conf_template(),
    case maps:keys(maps:without(maps:keys(Template), Map)) of
        [] -> ok;
        Extra -> error({invalid_queue_conf, #{extra => Extra}})
    end.

strip_runtime(Conf) ->
    maps:without([name, driver, worker, poll_interval, hooks], Conf).

apply_defaults(Conf, Template) ->
    maps:fold(
        fun(K, Spec, Acc) ->
            case {maps:is_key(K, Acc), maps:get(default, Spec, undefined)} of
                {false, undefined} -> Acc;
                {false, Default} -> Acc#{K => Default};
                {true, _} -> Acc
            end
        end,
        Conf,
        Template
    ).

% Job construction

build_job(Queue, Payload, Opts) ->
    Now = erlang:system_time(),
    ScheduledAt = maps:get(scheduled_at, Opts, undefined),
    State =
        case ScheduledAt of
            undefined -> available;
            _ -> scheduled
        end,
    Job = #{
        id => keysmith:uuid(7, binary),
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

-spec validate(gaffer:job()) -> ok.
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

run_checks([]) ->
    ok;
run_checks([{Check, Reason} | Rest]) ->
    case Check() of
        true -> run_checks(Rest);
        false -> error({invalid_job, Reason})
    end.

% Job state machine

transition(#{state := From} = Job, To) ->
    case valid_transition(From, To) of
        true ->
            Now = erlang:system_time(),
            {ok, set_timestamp(To, Now, Job#{state := To})};
        false ->
            {error, {invalid_transition, {From, To}}}
    end.

add_error(#{errors := Errors} = Job, Error) ->
    Normalized = normalize_error(Error),
    Job#{errors := [Normalized | Errors]}.

normalize_error(#{at := At, error := Err} = Error) ->
    Error#{
        at := to_microsecond(At),
        error := normalize_error_term(Err)
    }.

normalize_error_term(T) when is_atom(T); is_binary(T); is_number(T) -> T;
normalize_error_term(T) when is_map(T); is_list(T) -> T;
normalize_error_term(T) -> iolist_to_binary(io_lib:format(~"~0tp", [T])).

to_microsecond({Unit, V}) ->
    erlang:convert_time_unit(V, Unit, microsecond);
to_microsecond(Native) ->
    erlang:convert_time_unit(Native, native, microsecond).

-spec valid_transition(gaffer:job_state(), gaffer:job_state()) -> boolean().
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

-spec set_timestamp(gaffer:job_state(), integer(), gaffer:job()) ->
    gaffer:job().
set_timestamp(scheduled, TS, Job) -> Job#{scheduled_at => TS};
set_timestamp(executing, TS, Job) -> Job#{attempted_at => TS};
set_timestamp(completed, TS, Job) -> Job#{completed_at => TS};
set_timestamp(cancelled, TS, Job) -> Job#{cancelled_at => TS};
set_timestamp(discarded, TS, Job) -> Job#{discarded_at => TS};
set_timestamp(_State, _TS, Job) -> Job.

maybe_put(_Key, undefined, Map) -> Map;
maybe_put(Key, Value, Map) -> Map#{Key => Value}.

% Driver dispatch

with_job(Queue, JobId, Fun) ->
    case driver(Queue, job_get, [JobId]) of
        not_found -> {error, not_found};
        Job -> Fun(Job)
    end.

with_queue(Queue, Event, Data, Fun) ->
    {{Mod, DS}, Hooks} = lookup(Queue),
    gaffer_hooks:with_hooks(Hooks, Event, Data, fun(D) ->
        Fun(D, Mod, DS)
    end).

driver(Queue, Fun, Args) ->
    {{Mod, DS}, _} = lookup(Queue),
    apply(Mod, Fun, Args ++ [DS]).

-spec lookup(gaffer:queue_name()) -> {driver(), gaffer:hooks()}.
lookup(Name) ->
    case persistent_term:get({gaffer_queue, Name}, undefined) of
        undefined -> error({unknown_queue, Name});
        Entry -> Entry
    end.
