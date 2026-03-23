-module(gaffer_driver_ets).
-moduledoc "In-memory ETS driver for gaffer.".

-behaviour(gaffer_driver).

% Lifecycle
-export([start/1]).
-export([stop/1]).
% Queues
-export([queue_insert/2]).
-export([queue_update/3]).
-export([queue_get/2]).
-export([queue_delete/2]).
% Jobs
-export([job_insert/2]).
-export([job_get/2]).
-export([job_list/2]).
-export([job_delete/2]).
-export([job_claim/3]).
-export([job_update/2]).
-export([job_prune/2]).
% Introspection
-export([info/2]).

-export_type([driver_state/0]).

-doc "ETS driver state.".
-opaque driver_state() :: #{
    queued := ets:table(),
    locked := ets:table(),
    queues := ets:table()
}.

%--- gaffer_driver Callbacks ---------------------------------------------------

% Lifecycle

-doc "Starts the driver.".
-spec start(map()) -> driver_state().
start(#{}) ->
    Queued = ets:new(gaffer_driver_ets_queued, [public, set]),
    Locked = ets:new(gaffer_driver_ets_locked, [public, set]),
    Queues = ets:new(gaffer_driver_ets_queues, [public, set]),
    #{queued => Queued, locked => Locked, queues => Queues}.

-doc "Stops the driver.".
-spec stop(driver_state()) -> ok.
stop(#{queued := Queued, locked := Locked, queues := Queues}) ->
    ets:delete(Queued),
    ets:delete(Locked),
    ets:delete(Queues),
    ok.

% Queues

-doc false.
-spec queue_insert(gaffer:queue_conf(), driver_state()) ->
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

-doc false.
-spec queue_update(gaffer:queue(), map(), driver_state()) ->
    ok.
queue_update(Name, Updates, #{queues := Tab}) ->
    [{_, Conf}] = ets:lookup(Tab, Name),
    Merged = maps:merge(Conf, Updates),
    validate_on_discard(Merged, Tab),
    true = ets:insert(Tab, {Name, Merged}),
    ok.

-doc false.
-spec queue_get(gaffer:queue(), driver_state()) ->
    gaffer:queue_conf() | not_found.
queue_get(Name, #{queues := Tab}) ->
    ets:lookup_element(Tab, Name, 2, not_found).

-doc false.
-spec queue_delete(gaffer:queue(), driver_state()) ->
    ok.
queue_delete(Name, #{queues := Tab}) ->
    true = ets:delete(Tab, Name),
    ok.

% Jobs

-doc false.
-spec job_insert(gaffer:job(), driver_state()) ->
    gaffer:job().
job_insert(#{id := Id} = Job, #{queued := Tab}) ->
    true = ets:insert(Tab, {Id, Job}),
    Job.

-doc false.
-spec job_get(gaffer:job_id(), driver_state()) ->
    gaffer:job() | not_found.
job_get(Id, #{queued := Queued, locked := Locked}) ->
    case ets:lookup(Locked, Id) of
        [{_, Job}] ->
            Job;
        [] ->
            ets:lookup_element(Queued, Id, 2, not_found)
    end.

-doc false.
-spec job_list(gaffer:job_filter(), driver_state()) ->
    [gaffer:job()].
job_list(Opts, #{queued := Queued, locked := Locked}) ->
    Pattern = {'_', Opts},
    [Job || {_, Job} <:- ets:match_object(Queued, Pattern)] ++
        [
            Job
         || {_, Job} <:- ets:match_object(Locked, Pattern)
        ].

-doc false.
-spec job_delete(gaffer:job_id(), driver_state()) ->
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

-doc false.
-spec job_claim(
    gaffer_driver:claim_opts(), gaffer_driver:job_changes(), driver_state()
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

-doc false.
-spec job_update(gaffer:job(), driver_state()) ->
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

-doc false.
-spec job_prune(gaffer_driver:prune_opts(), driver_state()) ->
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

% Introspection

-doc false.
-spec info(gaffer:queue(), driver_state()) ->
    #{jobs := #{gaffer:job_state() => gaffer:state_info()}}.
info(Queue, #{queued := Queued, locked := Locked}) ->
    Empty = #{
        available => #{count => 0},
        executing => #{count => 0},
        completed => #{count => 0},
        failed => #{count => 0},
        cancelled => #{count => 0},
        discarded => #{count => 0}
    },
    AllJobs =
        [Job || {_, #{queue := Q} = Job} <:- ets:tab2list(Queued), Q =:= Queue] ++
            [
                Job
             || {_, #{queue := Q} = Job} <:- ets:tab2list(Locked), Q =:= Queue
            ],
    Jobs = lists:foldl(fun accumulate_info/2, Empty, AllJobs),
    #{jobs => Jobs}.

accumulate_info(#{state := State} = Job, Acc) ->
    TS = info_timestamp(State, Job),
    Entry = maps:get(State, Acc),
    #{count := Count} = Entry,
    Entry1 = Entry#{count := Count + 1},
    Entry2 =
        case {TS, maps:find(oldest, Entry1)} of
            {undefined, _} ->
                Entry1;
            {T, error} ->
                Entry1#{oldest => T, newest => T};
            {T, {ok, O}} ->
                Entry1#{
                    oldest => min(T, O),
                    newest => max(T, maps:get(newest, Entry1))
                }
        end,
    Acc#{State := Entry2}.

info_timestamp(available, #{inserted_at := T}) -> T;
info_timestamp(executing, #{attempted_at := T}) -> T;
info_timestamp(completed, #{completed_at := T}) -> T;
info_timestamp(failed, #{attempted_at := T}) -> T;
info_timestamp(cancelled, #{cancelled_at := T}) -> T;
info_timestamp(discarded, #{discarded_at := T}) -> T;
info_timestamp(_, _) -> undefined.

%--- Internal ------------------------------------------------------------------

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
