-module(gaffer_queue).
-moduledoc false.

% API
% Queues
-export([init/0]).
-export([teardown/0]).
-export([create/1]).
-export([ensure/1]).
-export([delete/1]).
-export([delete/2]).
-export([orphaned/1]).
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

-type prune_opts() :: gaffer:max_age().

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
create(#{name := Name} = Conf0) ->
    case persistent_term:get({gaffer_queue, Name}, undefined) of
        undefined -> ensure(Conf0);
        _ -> {error, already_exists}
    end.

-spec ensure(queue_conf()) -> ok.
ensure(Conf0) ->
    #{name := Name, driver := {Mod, DS}, hooks := Hooks} =
        Conf = with_defaults(Conf0),
    Validated = validate_conf(strip_runtime(Conf)),
    validate_on_discard(Conf),
    Mod:queue_insert(Name, DS),
    persistent_term:put(
        {gaffer_queue, Name},
        maps:merge(Conf, Validated)
    ),
    case gaffer_queue_sup:ensure(Name) of
        created -> gaffer_hooks:notify(Hooks, [gaffer, queue, create], Conf);
        updated -> gaffer_hooks:notify(Hooks, [gaffer, queue, update], Conf)
    end.

-spec delete(gaffer:queue()) -> ok.
delete(Name) ->
    #{driver := {Mod, DS}} = conf(Name),
    case Mod:queue_delete(Name, DS) of
        ok -> teardown_queue(Name);
        {error, has_jobs} -> error({queue_has_jobs, Name})
    end.

-spec delete(gaffer:queue(), gaffer_driver:driver()) -> ok.
delete(Name, {Mod, DS}) ->
    teardown_queue(Name),
    prune(Name, #{'_' => 0}, {Mod, DS}),
    case Mod:queue_delete(Name, DS) of
        ok -> ok;
        {error, not_found} -> ok
    end.

-spec orphaned(gaffer_driver:driver()) -> [gaffer:queue()].
orphaned({Mod, DS}) ->
    ActiveQueues = [Name || {Name, _} <:- list()],
    Mod:queue_list(DS) -- ActiveQueues.

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
    #{hooks := Hooks} = Conf = conf(Name),
    MergedConf = maps:merge(Conf, Validated),
    _ = validate_conf(strip_runtime(MergedConf)),
    validate_on_discard(MergedConf),
    persistent_term:put({gaffer_queue, Name}, MergedConf),
    gaffer_queue_runner:reconfigure(Name),
    gaffer_hooks:notify(Hooks, [gaffer, queue, update], Updates).

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
    [Written] = Mod:job_write([NewJob], DS),
    gaffer_hooks:notify(Hooks, [gaffer, job, insert], Written),
    Written.

-spec get_job(gaffer:queue(), gaffer:job_id()) ->
    gaffer:job() | not_found.
get_job(Queue, ID) ->
    #{driver := {Mod, DS}} = conf(Queue),
    Mod:job_get(ID, DS).

-spec list_jobs(#{queue := gaffer:queue(), _ => _}) ->
    [gaffer:job()].
list_jobs(#{queue := Queue} = Opts) ->
    #{driver := {Mod, DS}} = conf(Queue),
    Mod:job_list(Opts, DS).

-spec delete_job(gaffer:queue(), gaffer:job_id()) -> ok.
delete_job(Queue, ID) ->
    #{driver := {Mod, DS}, hooks := Hooks} = conf(Queue),
    case Mod:job_delete(ID, DS) of
        not_found -> error({unknown_job, ID});
        ok -> gaffer_hooks:notify(Hooks, [gaffer, job, delete], ID)
    end.

-spec cancel_job(gaffer:queue(), gaffer:job_id()) ->
    {ok, gaffer:job()} | {error, term()}.
cancel_job(Queue, ID) ->
    #{driver := {Mod, DS}, hooks := Hooks} = conf(Queue),
    case Mod:job_get(ID, DS) of
        not_found ->
            {error, not_found};
        Job ->
            case gaffer_job:transition(Job, cancelled) of
                {ok, Cancelled} ->
                    [Written] = Mod:job_write([Cancelled], DS),
                    gaffer_hooks:notify(Hooks, [gaffer, job, cancel], Written),
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
    write_result_jobs(Conf, Job),
    gaffer_hooks:notify(Hooks, Event, Job),
    run_forward_hooks(Conf, Job),
    {ok, Job}.

-spec claim_jobs(gaffer:queue(), claim_opts()) ->
    [gaffer:job()].
claim_jobs(Queue, Opts) ->
    Changes = gaffer_job:claim_changes(),
    #{driver := {Mod, DS}, hooks := Hooks} = Conf = conf(Queue),
    GlobalMax = maps:get(global_max_workers, Conf),
    Limit = clamp_limit(maps:get(limit, Opts), GlobalMax),
    ClaimOpts = Opts#{limit := Limit, global_max_workers => GlobalMax},
    Claimed = Mod:job_claim(ClaimOpts, Changes, DS),
    gaffer_hooks:notify(Hooks, [gaffer, job, claim], Claimed),
    Claimed.

-spec prune_jobs(gaffer:queue(), prune_opts()) -> [gaffer:job_id()].
prune_jobs(Queue, MaxAge) ->
    #{driver := Driver, hooks := Hooks} = conf(Queue),
    IDs = prune(Queue, MaxAge, Driver),
    Event = [gaffer, job, delete],
    _ = [gaffer_hooks:notify(Hooks, Event, ID) || ID <:- IDs],
    IDs.

%--- Internal ------------------------------------------------------------------

teardown_queue(Name) ->
    teardown_queue(Name, persistent_term:get({gaffer_queue, Name}, undefined)).

teardown_queue(_Name, undefined) ->
    ok;
teardown_queue(Name, #{hooks := Hooks}) ->
    ok = gaffer_sup:stop_queue(gaffer_queue_sup:pid(Name)),
    persistent_term:erase({gaffer_queue, Name}),
    gaffer_hooks:notify(Hooks, [gaffer, queue, delete], Name).

prune(Queue, MaxAge, Driver) ->
    do_prune(Queue, normalize_max_age(MaxAge), Driver).

do_prune(_Queue, Cutoffs, _Driver) when map_size(Cutoffs) =:= 0 -> [];
do_prune(Queue, Cutoffs, {Mod, DS}) -> Mod:job_prune(Queue, Cutoffs, DS).

normalize_max_age(MaxAge) ->
    Default = maps:get('_', MaxAge, infinity),
    Now = erlang:system_time(),
    #{
        S => cutoff(Age, Now)
     || S <:- gaffer_job:all_states(),
        Age <:- [maps:get(S, MaxAge, Default)],
        Age =/= infinity
    }.

cutoff(0, _Now) ->
    all;
cutoff(AgeMs, Now) ->
    Now - erlang:convert_time_unit(AgeMs, millisecond, native).

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
    maps:without([name, driver, worker, poll_interval, hooks, prune], Conf).

validate_on_discard(#{on_discard := Target}) ->
    case persistent_term:get({gaffer_queue, Target}, undefined) of
        undefined -> error({on_discard_queue_not_found, Target});
        _ -> ok
    end;
validate_on_discard(_) ->
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
    gaffer_hooks:notify(Hooks, [gaffer, job, insert], Forwarded);
run_forward_hooks(_, _) ->
    ok.

with_defaults(Conf) ->
    resolve_driver(
        mapz:deep_merge(
            #{
                hooks => [],
                poll_interval => 10,
                prune => #{
                    interval => 100,
                    max_age => #{
                        '_' => infinity,
                        completed => 0,
                        discarded => 0,
                        cancelled => 0
                    }
                }
            },
            Conf
        )
    ).

resolve_driver(#{driver := {_Mod, _DS}} = Conf) ->
    Conf;
resolve_driver(#{driver := Name} = Conf) when is_atom(Name) ->
    Conf#{driver := gaffer_driver:lookup(Name)}.

-spec conf(gaffer:queue()) -> queue_conf().
conf(Name) ->
    case persistent_term:get({gaffer_queue, Name}, undefined) of
        undefined -> error({unknown_queue, Name});
        Conf -> Conf
    end.
