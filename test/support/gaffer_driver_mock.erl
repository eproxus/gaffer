-module(gaffer_driver_mock).

%% Minimal map-based mock driver for gaffer_queue unit tests.
%% No ETS, no processes — just a plain map as state.

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

-type state() :: #{jobs := #{gaffer:job_id() => gaffer:job()}}.

%--- Lifecycle ----------------------------------------------------------------

-spec start(map()) -> state().
start(_Opts) ->
    #{jobs => #{}}.

-spec stop(state()) -> ok.
stop(_State) ->
    ok.

%--- Queue config (no-op for unit tests) --------------------------------------

-spec queue_put(gaffer:queue_conf(), state()) ->
    state().
queue_put(_Conf, State) -> State.

-spec queue_get(gaffer:queue_name(), state()) ->
    {gaffer:queue_conf(), state()}.
queue_get(Name, State) -> {#{name => Name}, State}.

-spec queue_delete(gaffer:queue_name(), state()) ->
    state().
queue_delete(_Name, State) -> State.

%--- Jobs ---------------------------------------------------------------------

-spec job_insert(gaffer:job(), state()) ->
    {gaffer:job(), state()}.
job_insert(#{id := Id} = Job, #{jobs := Jobs} = State) ->
    {Job, State#{jobs := Jobs#{Id => Job}}}.

-spec job_get(gaffer:job_id(), state()) ->
    {gaffer:job() | not_found, state()}.
job_get(Id, #{jobs := Jobs} = State) ->
    case maps:find(Id, Jobs) of
        {ok, Job} -> {Job, State};
        error -> {not_found, State}
    end.

-spec job_list(gaffer:list_opts(), state()) ->
    {[gaffer:job()], state()}.
job_list(Opts, #{jobs := Jobs} = State) ->
    All = maps:values(Jobs),
    Filtered = lists:filter(
        fun(Job) -> matches(Opts, Job) end, All
    ),
    {Filtered, State}.

-spec job_delete(gaffer:job_id(), state()) ->
    state().
job_delete(Id, #{jobs := Jobs} = State) ->
    State#{jobs := maps:remove(Id, Jobs)}.

-spec job_claim(
    gaffer:claim_opts(), gaffer:job_changes(), state()
) ->
    {[gaffer:job()], state()}.
job_claim(Opts, Changes, #{jobs := Jobs} = State) ->
    Limit = maps:get(limit, Opts, 1),
    Queue = maps:get(queue, Opts, undefined),
    Available = [
        Job
     || #{state := St} = Job <:- maps:values(Jobs),
        St =:= available,
        Queue =:= undefined orelse maps:get(queue, Job) =:= Queue
    ],
    ToTake = lists:sublist(Available, Limit),
    Updated = lists:map(
        fun(Job) -> maps:merge(Job, Changes) end, ToTake
    ),
    NewJobs = lists:foldl(
        fun(#{id := Id} = Job, Acc) -> Acc#{Id := Job} end,
        Jobs,
        Updated
    ),
    {Updated, State#{jobs := NewJobs}}.

-spec job_update(gaffer:job(), state()) ->
    state().
job_update(#{id := Id} = Job, #{jobs := Jobs} = State) ->
    State#{jobs := Jobs#{Id := Job}}.

-spec job_prune(gaffer:prune_opts(), state()) ->
    {non_neg_integer(), state()}.
job_prune(Opts, #{jobs := Jobs} = State) ->
    States = maps:get(states, Opts, [completed, discarded]),
    {Keep, Count} = maps:fold(
        fun(Id, #{state := St} = _Job, {Acc, N}) ->
            case lists:member(St, States) of
                true -> {maps:remove(Id, Acc), N + 1};
                false -> {Acc, N}
            end
        end,
        {Jobs, 0},
        Jobs
    ),
    {Count, State#{jobs := Keep}}.

%--- Internal -----------------------------------------------------------------

matches(Opts, Job) ->
    maps:fold(
        fun(Key, Val, Acc) ->
            Acc andalso maps:get(Key, Job, undefined) =:= Val
        end,
        true,
        Opts
    ).
