-module(gaffer_driver_ets).
-moduledoc "In-memory ETS driver for gaffer.".

-behaviour(gaffer_driver).

% Lifecycle
-export([start/1]).
-export([stop/1]).
% Queues
-export([queue_insert/2]).
-export([queue_exists/2]).
-export([queue_delete/2]).
% Jobs
-export([job_write/2]).
-export([job_get/2]).
-export([job_list/2]).
-export([job_delete/2]).
-export([job_claim/3]).
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
start(#{}) ->
    Queued = ets:new(gaffer_driver_ets_queued, [public, set]),
    Locked = ets:new(gaffer_driver_ets_locked, [public, set]),
    Queues = ets:new(gaffer_driver_ets_queues, [public, set]),
    State = #{queued => Queued, locked => Locked, queues => Queues},
    gaffer_driver:register(ets, {?MODULE, State}),
    State.

-doc "Stops the driver.".
stop(#{queued := Queued, locked := Locked, queues := Queues}) ->
    gaffer_driver:unregister(ets),
    ets:delete(Queued),
    ets:delete(Locked),
    ets:delete(Queues),
    ok.

% Queues

-doc false.
queue_insert(Name, #{queues := Tab}) ->
    _ = ets:insert_new(Tab, {Name, true}),
    ok.

-doc false.
queue_exists(Name, #{queues := Tab}) ->
    ets:member(Tab, Name).

-doc false.
queue_delete(Name, #{queues := Tab, queued := Queued, locked := Locked}) ->
    case ets:member(Tab, Name) of
        false ->
            {error, not_found};
        true ->
            HasJobs = lists:any(
                fun({_, #{queue := Q}}) -> Q =:= Name end,
                ets:tab2list(Queued) ++ ets:tab2list(Locked)
            ),
            case HasJobs of
                true ->
                    {error, has_jobs};
                false ->
                    ets:delete(Tab, Name),
                    ok
            end
    end.

% Jobs

-doc false.
job_write(Jobs, #{queued := Queued, locked := Locked}) ->
    {QueuedJobs, LockedJobs} = lists:partition(
        fun(#{state := S}) -> S =/= executing end, Jobs
    ),
    ets:insert(Queued, [{Id, J} || #{id := Id} = J <:- QueuedJobs]),
    ets:insert(Locked, [{Id, J} || #{id := Id} = J <:- LockedJobs]),
    [ets:delete(Locked, Id) || #{id := Id} <:- QueuedJobs],
    [ets:delete(Queued, Id) || #{id := Id} <:- LockedJobs],
    Jobs.

-doc false.
job_get(Id, #{queued := Queued, locked := Locked}) ->
    case ets:lookup(Locked, Id) of
        [{_, Job}] ->
            Job;
        [] ->
            ets:lookup_element(Queued, Id, 2, not_found)
    end.

-doc false.
job_list(Opts, #{queued := Queued, locked := Locked}) ->
    Pattern = {'_', Opts},
    [Job || {_, Job} <:- ets:match_object(Queued, Pattern)] ++
        [
            Job
         || {_, Job} <:- ets:match_object(Locked, Pattern)
        ].

-doc false.
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
job_claim(
    #{queue := Queue, limit := Limit0, global_max_workers := GlobalMax},
    Changes,
    #{queued := Queued, locked := Locked}
) ->
    Limit = apply_global_max(Queue, Limit0, GlobalMax, Locked),
    Now = erlang:system_time(),
    All = [Job || {_, Job} <:- ets:tab2list(Queued)],
    Available = [
        Job
     || #{state := St, queue := Q} = Job <:- All,
        St =:= available,
        Q =:= Queue,
        not is_scheduled_future(Job, Now)
    ],
    Sorted = lists:sort(fun compare_priority/2, Available),
    ToFetch = take(Sorted, Limit),
    claim_jobs(ToFetch, Changes, Queued, Locked, []).

-doc false.
job_prune(#{states := States}, #{queued := Queued, locked := Locked}) ->
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
info(Queue, #{queued := Queued, locked := Locked}) ->
    Empty = #{
        available => #{count => 0},
        executing => #{count => 0},
        completed => #{count => 0},
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
info_timestamp(cancelled, #{cancelled_at := T}) -> T;
info_timestamp(discarded, #{discarded_at := T}) -> T;
info_timestamp(_, _) -> undefined.

%--- Internal ------------------------------------------------------------------

apply_global_max(_Queue, Limit, infinity, _Locked) ->
    Limit;
apply_global_max(Queue, Limit, Max, Locked) ->
    Executing = length([
        J
     || {_, #{queue := Q} = J} <:- ets:tab2list(Locked),
        Q =:= Queue
    ]),
    min(Limit, Max - Executing).

take(List, infinity) -> List;
take(List, N) -> lists:sublist(List, max(0, N)).

is_scheduled_future(#{scheduled_at := At}, Now) -> At > Now;
is_scheduled_future(_, _Now) -> false.

compare_priority(#{priority := P1}, #{priority := P2}) when P1 =/= P2 ->
    P1 > P2;
compare_priority(A, B) ->
    maps:get(inserted_at, A, undefined) =< maps:get(inserted_at, B, undefined).

claim_jobs([], _Changes, _Queued, _Locked, Acc) ->
    lists:reverse(Acc);
claim_jobs([#{id := Id} | Rest], Changes, Queued, Locked, Acc) ->
    case ets:take(Queued, Id) of
        [{Id, Job}] ->
            Updated = maps:merge(Job, Changes),
            true = ets:insert(Locked, {Id, Updated}),
            claim_jobs(Rest, Changes, Queued, Locked, [Updated | Acc]);
        [] ->
            claim_jobs(Rest, Changes, Queued, Locked, Acc)
    end.
