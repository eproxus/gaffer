-module(gaffer_queue).
-moduledoc false.

% API
% Queues
-export([init/0]).
-export([teardown/0]).
-export([create/1]).
-export([ensure/1]).
-export([delete/1]).
-export([get/1]).
-export([update/2]).
-export([list/0]).
-export([conf/1]).
% Introspection
-export([info/1]).
% Jobs (user)
-export([insert_job/3]).
-export([get_job/2]).
-export([list_jobs/1]).
-export([cancel_job/2]).
-export([delete_job/2]).
% Jobs (runner)
-export([write_result/3]).
-export([claim_jobs/2]).
-export([prune_jobs/2]).

-compile({no_auto_import, [get/1]}).

%--- Types ---------------------------------------------------------------------

-type driver() :: {module(), gaffer_driver:driver_state()}.

-type queue_conf() :: gaffer:queue_conf().

-type claim_opts() :: #{
    queue := gaffer:queue(),
    limit := pos_integer()
}.

-type prune_opts() :: #{states => [gaffer:job_state()]}.

-export_type([driver/0]).
-export_type([queue_conf/0]).
-export_type([claim_opts/0]).
-export_type([prune_opts/0]).

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
create(Conf0) ->
    #{name := Name, driver := {Mod, DS}, hooks := Hooks} =
        Conf = with_defaults(Conf0),
    case persistent_term:get({gaffer_queue, Name}, undefined) of
        undefined ->
            Validated = validate_conf(strip_runtime(Conf)),
            validate_on_discard(Conf, Mod, DS),
            _ = gaffer_hooks:with_hooks(
                Hooks,
                [gaffer, queue, create],
                Conf,
                fun(C) ->
                    Mod:queue_insert(Name, DS),
                    persistent_term:put(
                        {gaffer_queue, Name},
                        maps:merge(Conf, Validated)
                    ),
                    {ok, _Pid} = gaffer_sup:start_queue(Name, Conf),
                    C
                end
            ),
            ok;
        _ ->
            {error, already_exists}
    end.

-spec ensure(queue_conf()) -> ok.
ensure(Conf0) ->
    #{name := Name, driver := {Mod, DS}, hooks := Hooks} =
        Conf = with_defaults(Conf0),
    Validated = validate_conf(strip_runtime(Conf)),
    validate_on_discard(Conf, Mod, DS),
    Mod:queue_insert(Name, DS),
    persistent_term:put(
        {gaffer_queue, Name},
        maps:merge(Conf, Validated)
    ),
    case ensure_runner(Name, Conf) of
        created ->
            _ = gaffer_hooks:with_hooks(
                Hooks, [gaffer, queue, create], Conf, fun(C) -> C end
            ),
            ok;
        updated ->
            _ = gaffer_hooks:with_hooks(
                Hooks, [gaffer, queue, update], Conf, fun(C) -> C end
            ),
            ok
    end.

-spec delete(gaffer:queue()) -> ok.
delete(Name) ->
    #{driver := {Mod, DS}, hooks := Hooks} = conf(Name),
    _ = gaffer_hooks:with_hooks(Hooks, [gaffer, queue, delete], Name, fun(N) ->
        case Mod:queue_delete(Name, DS) of
            ok ->
                ok = gaffer_sup:stop_queue(gaffer_queue_runner:pid(Name)),
                persistent_term:erase({gaffer_queue, Name}),
                N;
            {error, has_jobs} ->
                error({queue_has_jobs, Name})
        end
    end),
    ok.

-spec list() -> [{gaffer:queue(), gaffer:queue_conf()}].
list() -> lists:filtermap(fun queue_entry/1, persistent_term:get()).

queue_entry({{gaffer_queue, Name}, #{driver := {Mod, _}}}) when
    is_atom(Name), is_atom(Mod)
->
    {true, {Name, get(Name)}};
queue_entry(_) ->
    false.

-spec get(gaffer:queue()) -> gaffer:queue_conf().
get(Name) ->
    conf(Name).

-spec update(gaffer:queue(), map()) -> ok.
update(Name, Updates) ->
    Validated = validate_updates(strip_runtime(Updates)),
    #{driver := {Mod, DS}, hooks := Hooks} = Conf = conf(Name),
    MergedConf = maps:merge(Conf, Validated),
    _ = validate_conf(strip_runtime(MergedConf)),
    validate_on_discard(MergedConf, Mod, DS),
    _ = gaffer_hooks:with_hooks(Hooks, [gaffer, queue, update], Updates, fun(U) ->
        persistent_term:put({gaffer_queue, Name}, MergedConf),
        gaffer_queue_runner:reconfigure(Name),
        U
    end),
    ok.

% Introspection

-spec info(gaffer:queue()) -> gaffer:queue_info().
info(Queue) ->
    #{driver := {Mod, DS}} = Conf = conf(Queue),
    StorageInfo = Mod:info(Queue, DS),
    Active = gaffer_queue_runner:info(Queue),
    WorkerInfo = #{
        active => Active,
        max => #{
            local => maps:get(max_workers, Conf),
            global => maps:get(global_max_workers, Conf)
        }
    },
    StorageInfo#{workers => WorkerInfo}.

% Job (user)

-spec insert_job(gaffer:queue(), term(), gaffer:job_opts()) ->
    gaffer:job().
insert_job(Queue, Payload, Opts) ->
    #{driver := {Mod, DS}, hooks := Hooks} = Conf = conf(Queue),
    NewJob = gaffer_job:create(Conf, Payload, Opts),
    gaffer_hooks:with_hooks(
        Hooks,
        [gaffer, job, insert],
        NewJob,
        fun(Job) -> hd(Mod:job_write([Job], DS)) end
    ).

-spec get_job(gaffer:queue(), gaffer:job_id()) ->
    gaffer:job() | not_found.
get_job(Queue, JobId) ->
    #{driver := {Mod, DS}} = conf(Queue),
    Mod:job_get(JobId, DS).

-spec list_jobs(#{queue := gaffer:queue(), _ => _}) ->
    [gaffer:job()].
list_jobs(#{queue := Queue} = Opts) ->
    #{driver := {Mod, DS}} = conf(Queue),
    Mod:job_list(Opts, DS).

-spec delete_job(gaffer:queue(), gaffer:job_id()) -> ok.
delete_job(Queue, JobId) ->
    #{driver := {Mod, DS}, hooks := Hooks} = conf(Queue),
    _ = gaffer_hooks:with_hooks(
        Hooks,
        [gaffer, job, delete],
        JobId,
        fun(Id) ->
            case Mod:job_delete(Id, DS) of
                not_found -> error({unknown_job, Id});
                ok -> Id
            end
        end
    ),
    ok.

-spec cancel_job(gaffer:queue(), gaffer:job_id()) ->
    {ok, gaffer:job()} | {error, term()}.
cancel_job(Queue, JobId) ->
    #{driver := {Mod, DS}, hooks := Hooks} = conf(Queue),
    case Mod:job_get(JobId, DS) of
        not_found ->
            {error, not_found};
        Job ->
            case gaffer_job:transition(Job, cancelled) of
                {ok, Cancelled} ->
                    Written = gaffer_hooks:with_hooks(
                        Hooks,
                        [gaffer, job, cancel],
                        Cancelled,
                        fun(J) -> hd(Mod:job_write([J], DS)) end
                    ),
                    {ok, Written};
                {error, _} = Err ->
                    Err
            end
    end.

% Job (runner)

-spec write_result(gaffer:queue(), gaffer_hooks:event(), gaffer:job()) ->
    {ok, gaffer:job()}.
write_result(Queue, Event, Job) ->
    #{hooks := Hooks} = Conf = conf(Queue),
    Written = gaffer_hooks:with_hooks(Hooks, Event, Job, fun(J) ->
        write_result_jobs(Conf, J),
        J
    end),
    run_forward_hooks(Conf, Written),
    {ok, Written}.

-spec claim_jobs(gaffer:queue(), claim_opts()) ->
    [gaffer:job()].
claim_jobs(Queue, Opts) ->
    Changes = gaffer_job:claim_changes(),
    #{driver := {Mod, DS}, hooks := Hooks} = Conf = conf(Queue),
    GlobalMax = maps:get(global_max_workers, Conf),
    Limit = clamp_limit(maps:get(limit, Opts), GlobalMax),
    ClaimOpts = Opts#{limit := Limit, global_max_workers => GlobalMax},
    gaffer_hooks:with_hooks(
        Hooks,
        [gaffer, job, claim],
        ClaimOpts,
        fun(_) -> Mod:job_claim(ClaimOpts, Changes, DS) end
    ).

-spec prune_jobs(gaffer:queue(), prune_opts()) -> non_neg_integer().
prune_jobs(Queue, Opts) ->
    #{driver := {Mod, DS}} = conf(Queue),
    Defaults = #{states => [completed, discarded]},
    Mod:job_prune(maps:merge(Defaults, Opts), DS).

%--- Internal ------------------------------------------------------------------

clamp_limit(infinity, infinity) -> infinity;
clamp_limit(infinity, GlobalMax) -> GlobalMax;
clamp_limit(Limit, _) -> Limit.

is_gaffer_key({gaffer_queue, _}) -> true;
is_gaffer_key(_) -> false.

% Config validation

% erlfmt-ignore
queue_conf_defaults() ->
    #{
        global_max_workers => infinity,
        max_workers        => 1,
        shutdown_timeout   => 5000,
        max_attempts       => 3,
        timeout            => 30000,
        backoff            => [1000],
        priority           => 0
    }.

validate_conf(Conf) ->
    check_extra_keys(Conf),
    maps:merge(queue_conf_defaults(), Conf).

validate_updates(Updates) when map_size(Updates) =:= 0 ->
    error({invalid_queue_conf, #{extra => []}});
validate_updates(Updates) ->
    check_extra_keys(Updates),
    Updates.

check_extra_keys(Map) ->
    Allowed = maps:keys(queue_conf_defaults()) ++ [on_discard],
    case maps:keys(maps:without(Allowed, Map)) of
        [] -> ok;
        Extra -> error({invalid_queue_conf, #{extra => Extra}})
    end.

strip_runtime(Conf) ->
    maps:without([name, driver, worker, poll_interval, hooks], Conf).

validate_on_discard(#{on_discard := Target}, Mod, DS) ->
    case Mod:queue_exists(Target, DS) of
        true -> ok;
        false -> error({on_discard_queue_not_found, Target})
    end;
validate_on_discard(_, _, _) ->
    ok.

write_result_jobs(#{driver := Driver} = Conf, #{state := discarded} = Job) ->
    case Conf of
        #{on_discard := Target} ->
            #{driver := TargetDriver} = conf(Target),
            Forwarded = gaffer_job:create(
                conf(Target), gaffer_job:forward_payload(Job), #{}
            ),
            write_atomic(Driver, [Job], TargetDriver, [Forwarded]);
        _ ->
            write(Driver, [Job])
    end;
write_result_jobs(#{driver := Driver}, Job) ->
    write(Driver, [Job]).

% Same driver: single atomic write
write_atomic(Same, Jobs1, Same, Jobs2) ->
    write(Same, Jobs2 ++ Jobs1);
% Cross-driver: target first for at-least-once delivery
write_atomic(Source, Jobs1, Target, Jobs2) ->
    write(Target, Jobs2),
    write(Source, Jobs1).

write({Mod, DS}, Jobs) -> Mod:job_write(Jobs, DS).

run_forward_hooks(#{on_discard := Target}, #{state := discarded} = Job) ->
    #{hooks := Hooks} = conf(Target),
    Forwarded = gaffer_job:create(
        conf(Target), gaffer_job:forward_payload(Job), #{}
    ),
    gaffer_hooks:with_hooks(Hooks, [gaffer, job, insert], Forwarded, fun(J) ->
        J
    end);
run_forward_hooks(_, _) ->
    ok.

with_defaults(Conf) ->
    resolve_driver(maps:merge(#{hooks => [], poll_interval => 1000}, Conf)).

resolve_driver(#{driver := {_Mod, _DS}} = Conf) ->
    Conf;
resolve_driver(#{driver := Name} = Conf) when is_atom(Name) ->
    Conf#{driver := gaffer_driver:lookup(Name)}.

ensure_runner(Name, Conf) ->
    case gaffer_sup:start_queue(Name, Conf) of
        {ok, _Pid} ->
            created;
        {error, {already_started, _Pid}} ->
            gaffer_queue_runner:reconfigure(Name),
            updated
    end.

-spec conf(gaffer:queue()) -> queue_conf().
conf(Name) ->
    case persistent_term:get({gaffer_queue, Name}, undefined) of
        undefined -> error({unknown_queue, Name});
        Conf -> Conf
    end.
