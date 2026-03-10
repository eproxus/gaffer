-module(gaffer_driver_ets).

-behaviour(gaffer_driver).

-export([start/1]).
-export([stop/1]).
-export([queue_put/2]).
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

-spec queue_put(gaffer:queue_conf(), state()) ->
    state().
queue_put(#{name := Name} = Conf, #{queues := Tab} = State) ->
    true = ets:insert(Tab, {Name, Conf}),
    State.

-spec queue_get(gaffer:queue_name(), state()) ->
    {gaffer:queue_conf(), state()}.
queue_get(Name, #{queues := Tab} = State) ->
    [{_, Conf}] = ets:lookup(Tab, Name),
    {Conf, State}.

-spec queue_delete(gaffer:queue_name(), state()) ->
    state().
queue_delete(Name, #{queues := Tab} = State) ->
    true = ets:delete(Tab, Name),
    State.

%--- Jobs ---------------------------------------------------------------------

-spec job_insert(gaffer:new_job(), state()) ->
    {gaffer:job(), state()}.
job_insert(Job, #{queued := Tab} = State) ->
    Id = make_ref(),
    Job1 = Job#{id => Id},
    true = ets:insert(Tab, {Id, Job1}),
    {Job1, State}.

-spec job_get(gaffer:job_id(), state()) ->
    {gaffer:job() | not_found, state()}.
job_get(Id, #{queued := Queued, locked := Locked} = State) ->
    case ets:lookup(Locked, Id) of
        [{_, Job}] ->
            {Job, State};
        [] ->
            case ets:lookup(Queued, Id) of
                [{_, Job}] -> {Job, State};
                [] -> {not_found, State}
            end
    end.

-spec job_list(gaffer:list_opts(), state()) ->
    {[gaffer:job()], state()}.
job_list(Opts, #{queued := Queued, locked := Locked} = State) ->
    Pattern = {'_', Opts},
    Jobs =
        [Job || {_, Job} <:- ets:match_object(Queued, Pattern)] ++
            [
                Job
             || {_, Job} <:- ets:match_object(Locked, Pattern)
            ],
    {Jobs, State}.

-spec job_delete(gaffer:job_id(), state()) ->
    state().
job_delete(Id, #{queued := Queued, locked := Locked} = State) ->
    ets:delete(Queued, Id),
    ets:delete(Locked, Id),
    State.

-spec job_claim(
    gaffer:claim_opts(), gaffer:job_changes(), state()
) ->
    {[gaffer:job()], state()}.
job_claim(
    Opts,
    Changes,
    #{queued := Queued, locked := Locked, queues := Queues} = State
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
    Claimed = claim_jobs(ToFetch, Changes, Queued, Locked, []),
    {Claimed, State}.

-spec job_update(gaffer:job(), state()) ->
    state().
job_update(
    #{id := Id, state := JobState} = Job,
    #{queued := Queued, locked := Locked} = State
) ->
    %% Move job to appropriate table based on state
    case JobState of
        executing ->
            ets:delete(Queued, Id),
            true = ets:insert(Locked, {Id, Job});
        _ ->
            ets:delete(Locked, Id),
            true = ets:insert(Queued, {Id, Job})
    end,
    State.

-spec job_prune(gaffer:prune_opts(), state()) ->
    {non_neg_integer(), state()}.
job_prune(Opts, #{queued := Queued, locked := Locked} = State) ->
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
    {Count, State}.

%--- Internal -----------------------------------------------------------------

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
