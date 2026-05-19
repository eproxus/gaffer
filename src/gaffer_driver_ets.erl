-module(gaffer_driver_ets).
-moduledoc "In-memory ETS driver for gaffer.".

-behaviour(gaffer_driver).

% Lifecycle
-export([start/1]).
-export([stop/1]).
% Queues
-export([queue_insert/2]).
-export([queue_exists/2]).
-export([queue_list/1]).
-export([queue_delete/2]).
% Jobs
-export([job_write/2]).
-export([job_get/2]).
-export([job_list/2]).
-export([job_delete/2]).
-export([job_claim/3]).
-export([job_prune/3]).

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
queue_exists(Name, #{queues := Tab}) -> ets:member(Tab, Name).

-doc false.
queue_list(#{queues := Tab}) -> ets:select(Tab, [{{'$1', '_'}, [], ['$1']}]).

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
    % We insert in all tables before cross-deleting, so if a row is being moved
    % it can be briefly visible in both tables. When claiming jobs in
    % job_claim/3 the claim algorithm will always see an active job for a chain,
    % preventing subsequent jobs in a chain from executing prematurely.
    ets:insert(Queued, [{ID, J} || #{id := ID} = J <:- QueuedJobs]),
    ets:insert(Locked, [{ID, J} || #{id := ID} = J <:- LockedJobs]),
    [ets:delete(Locked, ID) || #{id := ID} <:- QueuedJobs],
    [ets:delete(Queued, ID) || #{id := ID} <:- LockedJobs],
    Jobs.

-doc false.
job_get(ID, #{queued := Queued, locked := Locked}) ->
    case ets:lookup(Locked, ID) of
        [{_, Job}] ->
            Job;
        [] ->
            ets:lookup_element(Queued, ID, 2, not_found)
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
job_delete(ID, #{queued := Queued, locked := Locked}) ->
    case {ets:member(Queued, ID), ets:member(Locked, ID)} of
        {false, false} ->
            not_found;
        _ ->
            ets:delete(Queued, ID),
            ets:delete(Locked, ID),
            ok
    end.

-doc false.
job_claim(
    #{queue := Queue, limit := Limit, global_max_workers := GlobalMax},
    Changes,
    #{queued := Queued, locked := Locked} = State
) ->
    Now = erlang:system_time(),
    Active = active_jobs(Queue, State),
    Heads = chain_heads(Active),
    Eligible = [
        J
     || #{state := S} = J <:- Active,
        S =:= available,
        not is_scheduled_future(J, Now),
        is_chain_head(J, Heads)
    ],
    Sorted = lists:sort(fun earlier/2, Eligible),
    Max = available_slots(Limit, GlobalMax, Active),
    claim_jobs(take(Sorted, Max), Changes, Queued, Locked, []).

-doc false.
job_prune(Queue, Opts, #{queued := Queued, locked := Locked}) ->
    MS = prune_match_spec(Queue, Opts),
    QueuedIDs = ets:select(Queued, MS),
    LockedIDs = ets:select(Locked, MS),
    [ets:delete(Queued, ID) || ID <:- QueuedIDs],
    [ets:delete(Locked, ID) || ID <:- LockedIDs],
    QueuedIDs ++ LockedIDs.

%--- Internal ------------------------------------------------------------------

prune_match_spec(Queue, Opts) ->
    {_, MS} = maps:fold(fun prune_clause/3, {Queue, []}, Opts),
    MS.

prune_clause(State, all, {Q, Clauses}) ->
    {Q, [{{'$1', #{queue => Q, state => State}}, [], ['$1']} | Clauses]};
prune_clause(State, Cutoff, {Q, Clauses}) ->
    TSKey = state_timestamp_key(State),
    Clause = {
        {'$1', #{queue => Q, state => State, TSKey => '$2'}},
        [{'<', '$2', Cutoff}],
        ['$1']
    },
    {Q, [Clause | Clauses]}.

state_timestamp_key(available) -> created_at;
state_timestamp_key(executing) -> attempted_at;
state_timestamp_key(completed) -> completed_at;
state_timestamp_key(cancelled) -> cancelled_at;
state_timestamp_key(failed) -> failed_at.

% Jobs in this queue that participate in claim and chain decisions.
active_jobs(Queue, #{queued := Queued, locked := Locked}) ->
    [
        J
     || Tab <:- [Queued, Locked],
        {_, #{queue := Q, state := S} = J} <:- ets:tab2list(Tab),
        Q =:= Queue,
        S =:= available orelse S =:= executing
    ].

% Heads = #{Chain => earliest active job in that chain}. Only the head of a
% chain is claimable; later same-chain jobs must wait for it to terminate.
chain_heads(Active) -> lists:foldl(fun update_head/2, #{}, Active).

update_head(#{chain := C} = J, Heads) when is_binary(C), is_map_key(C, Heads) ->
    #{C := Prev} = Heads,
    Heads#{C := earliest(J, Prev)};
update_head(#{chain := C} = J, Heads) when is_binary(C) ->
    Heads#{C => J};
update_head(_J, Heads) ->
    Heads.

earliest(J1, J2) ->
    case earlier(J1, J2) of
        true -> J1;
        false -> J2
    end.

is_chain_head(#{chain := C, id := ID}, Heads) when is_binary(C) ->
    case Heads of
        #{C := #{id := ID}} -> true;
        _ -> false
    end;
is_chain_head(_J, _Heads) ->
    true.

available_slots(Limit, infinity, _Active) ->
    Limit;
available_slots(Limit, Max, Active) ->
    Executing = length([J || #{state := S} = J <:- Active, S =:= executing]),
    min(Limit, Max - Executing).

take(List, infinity) -> List;
take(List, N) -> lists:sublist(List, max(0, N)).

is_scheduled_future(#{scheduled_at := At}, Now) -> At > Now;
is_scheduled_future(_, _Now) -> false.

earlier(#{priority := P1}, #{priority := P2}) when P1 =/= P2 -> P1 > P2;
earlier(#{created_at := C1}, #{created_at := C2}) when C1 =/= C2 -> C1 < C2;
earlier(#{id := I1}, #{id := I2}) -> I1 =< I2.

claim_jobs([], _Changes, _Queued, _Locked, Acc) ->
    lists:reverse(Acc);
claim_jobs([#{id := ID} | Rest], Changes, Queued, Locked, Acc) ->
    case ets:take(Queued, ID) of
        [{ID, Job}] ->
            Updated = maps:merge(Job, Changes),
            true = ets:insert(Locked, {ID, Updated}),
            claim_jobs(Rest, Changes, Queued, Locked, [Updated | Acc]);
        [] ->
            claim_jobs(Rest, Changes, Queued, Locked, Acc)
    end.
