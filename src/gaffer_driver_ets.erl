-module(gaffer_driver_ets).

-behaviour(gaffer_driver).

-export([init/1]).
-export([stop/1]).
-export([queue_put/2]).
-export([queue_get/2]).
-export([queue_list/1]).
-export([queue_delete/2]).
-export([job_insert/2]).
-export([job_get/2]).
-export([job_list/2]).
-export([job_fetch/2]).
-export([job_complete/2]).
-export([job_fail/3]).
-export([job_cancel/2]).
-export([job_retry/3]).
-export([job_snooze/3]).
-export([job_prune/2]).

-export_type([state/0]).

-type state() :: #{
    queued := ets:table(),
    locked := ets:table(),
    queues := ets:table()
}.

%--- Lifecycle ----------------------------------------------------------------

-spec init(map()) -> {ok, state()}.
init(_Opts) ->
    Queued = ets:new(gaffer_queued, [public, set]),
    Locked = ets:new(gaffer_locked, [public, set]),
    Queues = ets:new(gaffer_queues, [public, set]),
    {ok, #{queued => Queued, locked => Locked, queues => Queues}}.

-spec stop(state()) -> ok.
stop(#{queued := Queued, locked := Locked, queues := Queues}) ->
    ets:delete(Queued),
    ets:delete(Locked),
    ets:delete(Queues),
    ok.

%--- Queue config -------------------------------------------------------------

-spec queue_put(gaffer:queue_conf(), state()) -> {ok, state()}.
queue_put(#{name := Name} = Conf, #{queues := Tab} = S) ->
    true = ets:insert(Tab, {Name, Conf}),
    {ok, S}.

-spec queue_get(gaffer:queue_name(), state()) ->
    {ok, gaffer:queue_conf()} | {error, not_found}.
queue_get(Name, #{queues := Tab}) ->
    case ets:lookup(Tab, Name) of
        [{_, Conf}] -> {ok, Conf};
        [] -> {error, not_found}
    end.

-spec queue_list(state()) -> {ok, [gaffer:queue_conf()]}.
queue_list(#{queues := Tab}) ->
    Confs = [Conf || {_, Conf} <:- ets:tab2list(Tab)],
    {ok, Confs}.

-spec queue_delete(gaffer:queue_name(), state()) ->
    {ok, state()}.
queue_delete(Name, #{queues := Tab} = S) ->
    true = ets:delete(Tab, Name),
    {ok, S}.

%--- Jobs ---------------------------------------------------------------------

-spec job_insert(gaffer:job(), state()) ->
    {ok, gaffer:job(), state()}.
job_insert(#{id := Id} = Job, #{queued := Tab} = S) ->
    true = ets:insert(Tab, {Id, Job}),
    {ok, Job, S}.

-spec job_get(gaffer:job_id(), state()) ->
    {ok, gaffer:job()} | {error, not_found}.
job_get(Id, #{queued := Queued, locked := Locked}) ->
    case ets:lookup(Locked, Id) of
        [{_, Job}] ->
            {ok, Job};
        [] ->
            case ets:lookup(Queued, Id) of
                [{_, Job}] -> {ok, Job};
                [] -> {error, not_found}
            end
    end.

-spec job_list(gaffer:list_opts(), state()) ->
    {ok, [gaffer:job()]}.
job_list(Opts, #{queued := Queued, locked := Locked}) ->
    All =
        [Job || {_, Job} <:- ets:tab2list(Queued)] ++
            [Job || {_, Job} <:- ets:tab2list(Locked)],
    Filtered = filter_jobs(Opts, All),
    {ok, Filtered}.

-spec job_fetch(gaffer:fetch_opts(), state()) ->
    {ok, [gaffer:job()], state()}.
job_fetch(Opts, #{queued := Queued, locked := Locked} = S) ->
    Queue = maps:get(queue, Opts, undefined),
    Limit = maps:get(limit, Opts, 1),
    Now = calendar:universal_time(),
    All = [Job || {_, Job} <:- ets:tab2list(Queued)],
    Available = [
        Job
     || #{state := St} = Job <:- All,
        St =:= available,
        matches_queue(Queue, Job),
        not is_scheduled_future(Job, Now)
    ],
    Sorted = lists:sort(fun compare_priority/2, Available),
    ToFetch = lists:sublist(Sorted, Limit),
    Claimed = claim_jobs(ToFetch, Queued, Locked, []),
    {ok, Claimed, S}.

-spec job_complete(gaffer:job_id(), state()) ->
    {ok, gaffer:job(), state()}.
job_complete(Id, #{locked := Locked} = S) ->
    case ets:lookup(Locked, Id) of
        [{_, Job0}] ->
            {ok, Job} = gaffer_job:transition(Job0, completed),
            Job1 = Job#{attempt := maps:get(attempt, Job) + 1},
            true = ets:insert(Locked, {Id, Job1}),
            {ok, Job1, S};
        [] ->
            error({not_executing, Id})
    end.

-spec job_fail(gaffer:job_id(), gaffer:job_error(), state()) ->
    {ok, gaffer:job(), state()}.
job_fail(Id, JobError, S) ->
    #{locked := Locked, queued := Queued} = S,
    case ets:lookup(Locked, Id) of
        [{_, Job0}] ->
            Attempt = maps:get(attempt, Job0) + 1,
            MaxAttempts = maps:get(max_attempts, Job0, 3),
            Job1 = Job0#{attempt := Attempt},
            Job2 = gaffer_job:add_error(Job1, JobError),
            {ok, Job3} = gaffer_job:transition(Job2, failed),
            Job4 =
                case Attempt >= MaxAttempts of
                    true ->
                        {ok, J} = gaffer_job:transition(
                            Job3, discarded
                        ),
                        J;
                    false ->
                        Job3
                end,
            true = ets:delete(Locked, Id),
            true = ets:insert(Queued, {Id, Job4}),
            {ok, Job4, S};
        [] ->
            error({not_executing, Id})
    end.

-spec job_cancel(gaffer:job_id(), state()) ->
    {ok, gaffer:job(), state()}.
job_cancel(Id, #{locked := Locked} = S) ->
    case ets:lookup(Locked, Id) of
        [{_, Job0}] ->
            {ok, Job} = gaffer_job:transition(Job0, cancelled),
            true = ets:insert(Locked, {Id, Job}),
            {ok, Job, S};
        [] ->
            error({not_executing, Id})
    end.

-spec job_retry(
    gaffer:job_id(), calendar:datetime(), state()
) ->
    {ok, gaffer:job(), state()}.
job_retry(Id, ScheduledAt, S) ->
    #{queued := Queued, locked := Locked} = S,
    {Job0, Source} = lookup_any(Id, Queued, Locked),
    {ok, Job1} = gaffer_job:transition(Job0, scheduled),
    Job2 = Job1#{scheduled_at => ScheduledAt},
    move_job(Id, Job2, Source, Queued),
    {ok, Job2, S}.

-spec job_snooze(gaffer:job_id(), pos_integer(), state()) ->
    {ok, gaffer:job(), state()}.
job_snooze(Id, Seconds, #{locked := Locked, queued := Queued} = S) ->
    case ets:lookup(Locked, Id) of
        [{_, Job0}] ->
            {ok, Job1} = gaffer_job:transition(
                Job0, scheduled
            ),
            ScheduledAt = add_seconds(
                calendar:universal_time(), Seconds
            ),
            Job2 = Job1#{scheduled_at => ScheduledAt},
            true = ets:delete(Locked, Id),
            true = ets:insert(Queued, {Id, Job2}),
            {ok, Job2, S};
        [] ->
            error({not_executing, Id})
    end.

-spec job_prune(gaffer:prune_opts(), state()) ->
    {ok, non_neg_integer(), state()}.
job_prune(Opts, #{queued := Queued, locked := Locked} = S) ->
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
    {ok, Count, S}.

%--- Internal -----------------------------------------------------------------

filter_jobs(Opts, Jobs) ->
    Queue = maps:get(queue, Opts, undefined),
    State = maps:get(state, Opts, undefined),
    [
        Job
     || Job <:- Jobs,
        matches_queue(Queue, Job),
        matches_state(State, Job)
    ].

matches_queue(undefined, _Job) -> true;
matches_queue(Q, #{queue := Q}) -> true;
matches_queue(_, _) -> false.

matches_state(undefined, _Job) -> true;
matches_state(St, #{state := St}) -> true;
matches_state(_, _) -> false.

is_scheduled_future(#{scheduled_at := At}, Now) -> At > Now;
is_scheduled_future(_, _Now) -> false.

compare_priority(#{priority := P1}, #{priority := P2}) when
    P1 =/= P2
->
    P1 < P2;
compare_priority(A, B) ->
    maps:get(inserted_at, A, undefined) =<
        maps:get(inserted_at, B, undefined).

claim_jobs([], _Queued, _Locked, Acc) ->
    lists:reverse(Acc);
claim_jobs([#{id := Id} | Rest], Queued, Locked, Acc) ->
    case ets:take(Queued, Id) of
        [{Id, Job}] ->
            {ok, Executing} = gaffer_job:transition(
                Job, executing
            ),
            true = ets:insert(Locked, {Id, Executing}),
            claim_jobs(Rest, Queued, Locked, [Executing | Acc]);
        [] ->
            claim_jobs(Rest, Queued, Locked, Acc)
    end.

lookup_any(Id, Queued, Locked) ->
    case ets:lookup(Queued, Id) of
        [{_, Job}] ->
            {Job, Queued};
        [] ->
            case ets:lookup(Locked, Id) of
                [{_, Job}] -> {Job, Locked};
                [] -> error({not_found, Id})
            end
    end.

move_job(Id, Job, Source, Dest) when Source =:= Dest ->
    true = ets:insert(Source, {Id, Job}),
    ok;
move_job(Id, Job, Source, Dest) ->
    true = ets:delete(Source, Id),
    true = ets:insert(Dest, {Id, Job}),
    ok.

add_seconds(DateTime, Seconds) ->
    GregSec = calendar:datetime_to_gregorian_seconds(DateTime),
    calendar:gregorian_seconds_to_datetime(GregSec + Seconds).
