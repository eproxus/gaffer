-module(gaffer_driver_ets).

-behaviour(gaffer_driver).

-export([start/1]).
-export([stop/1]).
-export([queue_insert/2]).
-export([queue_update/3]).
-export([queue_get/2]).
-export([queue_delete/2]).
-export([job_insert/2]).
-export([job_get/2]).
-export([job_list/2]).
-export([job_delete/2]).
-export([job_claim/3]).
-export([job_update/2]).
-export([job_prune/2]).

-export_type([state/0]).

-type state() :: #{
    queued := ets:table(),
    locked := ets:table(),
    queues := ets:table()
}.

%--- Lifecycle ----------------------------------------------------------------

-spec start(map()) -> state().
start(_Opts) ->
    Queued = ets:new(gaffer_driver_ets_queued, [public, set]),
    Locked = ets:new(gaffer_driver_ets_locked, [public, set]),
    Queues = ets:new(gaffer_driver_ets_queues, [public, set]),
    #{queued => Queued, locked => Locked, queues => Queues}.

-spec stop(state()) -> ok.
stop(#{queued := Queued, locked := Locked, queues := Queues}) ->
    ets:delete(Queued),
    ets:delete(Locked),
    ets:delete(Queues),
    ok.

%--- Queue config -------------------------------------------------------------

-spec queue_insert(gaffer:queue_conf(), state()) ->
    ok.
queue_insert(#{name := Name} = Conf, #{queues := Tab}) ->
    validate_on_discard(Conf, Tab),
    case ets:lookup(Tab, Name) of
        [] ->
            true = ets:insert(Tab, {Name, Conf}),
            ok;
        [{_, Conf}] ->
            ok;
        [{_, Existing}] ->
            error(
                {queue_config_mismatch, Name, #{
                    expected => Conf, stored => Existing
                }}
            )
    end.

-spec queue_update(gaffer:queue_name(), map(), state()) ->
    ok.
queue_update(Name, Updates, #{queues := Tab}) ->
    [{_, Conf}] = ets:lookup(Tab, Name),
    Merged = maps:merge(Conf, Updates),
    validate_on_discard(Merged, Tab),
    true = ets:insert(Tab, {Name, Merged}),
    ok.

-spec queue_get(gaffer:queue_name(), state()) ->
    gaffer:queue_conf() | not_found.
queue_get(Name, #{queues := Tab}) ->
    case ets:lookup(Tab, Name) of
        [{_, Conf}] -> Conf;
        [] -> not_found
    end.

-spec queue_delete(gaffer:queue_name(), state()) ->
    ok.
queue_delete(Name, #{queues := Tab}) ->
    true = ets:delete(Tab, Name),
    ok.

%--- Jobs ---------------------------------------------------------------------

-spec job_insert(gaffer:new_job(), state()) ->
    gaffer:job().
job_insert(Job, #{queued := Tab}) ->
    Id = make_ref(),
    Job1 = Job#{id => Id},
    true = ets:insert(Tab, {Id, Job1}),
    Job1.

-spec job_get(gaffer:job_id(), state()) ->
    gaffer:job() | not_found.
job_get(Id, #{queued := Queued, locked := Locked}) ->
    case ets:lookup(Locked, Id) of
        [{_, Job}] ->
            Job;
        [] ->
            case ets:lookup(Queued, Id) of
                [{_, Job}] -> Job;
                [] -> not_found
            end
    end.

-spec job_list(gaffer:list_opts(), state()) ->
    [gaffer:job()].
job_list(Opts, #{queued := Queued, locked := Locked}) ->
    Pattern = {'_', Opts},
    [Job || {_, Job} <:- ets:match_object(Queued, Pattern)] ++
        [
            Job
         || {_, Job} <:- ets:match_object(Locked, Pattern)
        ].

-spec job_delete(gaffer:job_id(), state()) ->
    ok | not_found.
job_delete(Id, #{queued := Queued, locked := Locked}) ->
    case {ets:member(Queued, Id), ets:member(Locked, Id)} of
        {false, false} ->
            not_found;
        _ ->
            ets:delete(Queued, Id),
            ets:delete(Locked, Id),
            ok
    end.

-spec job_claim(
    gaffer:claim_opts(), gaffer:job_changes(), state()
) ->
    [gaffer:job()].
job_claim(
    Opts,
    Changes,
    #{queued := Queued, locked := Locked, queues := Queues}
) ->
    Queue = maps:get(queue, Opts, undefined),
    Limit0 = maps:get(limit, Opts, 1),
    Limit = apply_global_max(Queue, Limit0, Locked, Queues),
    Now = erlang:system_time(),
    All = [Job || {_, Job} <:- ets:tab2list(Queued)],
    Available = [
        Job
     || #{state := St} = Job <:- All,
        St =:= available,
        matches_queue(Queue, Job),
        not is_scheduled_future(Job, Now)
    ],
    Sorted = lists:sort(fun compare_priority/2, Available),
    ToFetch = lists:sublist(Sorted, max(0, Limit)),
    claim_jobs(ToFetch, Changes, Queued, Locked, []).

-spec job_update(gaffer:job(), state()) ->
    ok.
job_update(
    #{id := Id, state := JobState} = Job,
    #{queued := Queued, locked := Locked}
) ->
    % Move job to appropriate table based on state
    case JobState of
        executing ->
            ets:delete(Queued, Id),
            true = ets:insert(Locked, {Id, Job});
        _ ->
            ets:delete(Locked, Id),
            true = ets:insert(Queued, {Id, Job})
    end,
    ok.

-spec job_prune(gaffer:prune_opts(), state()) ->
    non_neg_integer().
job_prune(Opts, #{queued := Queued, locked := Locked}) ->
    States = maps:get(states, Opts, [completed, discarded]),
    AllQ = [
        {Id, Job}
     || {Id, #{state := St} = Job} <:- ets:tab2list(Queued),
        lists:member(St, States)
    ],
    AllL = [
        {Id, Job}
     || {Id, #{state := St} = Job} <:- ets:tab2list(Locked),
        lists:member(St, States)
    ],
    Count = length(AllQ) + length(AllL),
    _ = [ets:delete(Queued, Id) || {Id, _} <:- AllQ],
    _ = [ets:delete(Locked, Id) || {Id, _} <:- AllL],
    Count.

%--- Internal -----------------------------------------------------------------

validate_on_discard(#{on_discard := Target}, Tab) ->
    case ets:member(Tab, Target) of
        true -> ok;
        false -> error({on_discard_queue_not_found, Target})
    end;
validate_on_discard(_, _Tab) ->
    ok.

apply_global_max(undefined, Limit, _Locked, _Queues) ->
    Limit;
apply_global_max(Queue, Limit, Locked, Queues) ->
    case ets:lookup(Queues, Queue) of
        [{_, #{global_max_workers := Max}}] ->
            Executing = length([
                J
             || {_, #{queue := Q} = J} <:-
                    ets:tab2list(Locked),
                Q =:= Queue
            ]),
            min(Limit, Max - Executing);
        _ ->
            Limit
    end.

matches_queue(undefined, _Job) -> true;
matches_queue(Q, #{queue := Q}) -> true;
matches_queue(_, _) -> false.

is_scheduled_future(#{scheduled_at := At}, Now) -> At > Now;
is_scheduled_future(_, _Now) -> false.

compare_priority(#{priority := P1}, #{priority := P2}) when
    P1 =/= P2
->
    P1 < P2;
compare_priority(A, B) ->
    maps:get(inserted_at, A, undefined) =<
        maps:get(inserted_at, B, undefined).

claim_jobs([], _Changes, _Queued, _Locked, Acc) ->
    lists:reverse(Acc);
claim_jobs(
    [#{id := Id} | Rest], Changes, Queued, Locked, Acc
) ->
    case ets:take(Queued, Id) of
        [{Id, Job}] ->
            Updated = maps:merge(Job, Changes),
            true = ets:insert(Locked, {Id, Updated}),
            claim_jobs(
                Rest, Changes, Queued, Locked, [Updated | Acc]
            );
        [] ->
            claim_jobs(Rest, Changes, Queued, Locked, Acc)
    end.
