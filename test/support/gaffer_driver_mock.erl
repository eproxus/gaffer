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

-spec start(map()) -> {ok, state()}.
start(_Opts) ->
    {ok, #{jobs => #{}}}.

-spec stop(state()) -> ok.
stop(_State) ->
    ok.

%--- Queue config (no-op for unit tests) --------------------------------------

-spec queue_put(gaffer:queue_conf(), state()) ->
    {ok, state()}.
queue_put(_Conf, State) -> {ok, State}.

-spec queue_get(gaffer:queue_name(), state()) ->
    {ok, gaffer:queue_conf()}.
queue_get(Name, _State) -> {ok, #{name => Name}}.

-spec queue_delete(gaffer:queue_name(), state()) ->
    {ok, state()}.
queue_delete(_Name, State) -> {ok, State}.

%--- Jobs ---------------------------------------------------------------------

-spec job_insert(gaffer:job(), state()) ->
    {ok, gaffer:job(), state()}.
job_insert(#{id := Id} = Job, #{jobs := Jobs} = State) ->
    {ok, Job, State#{jobs := Jobs#{Id => Job}}}.

-spec job_get(gaffer:job_id(), state()) ->
    {ok, gaffer:job()} | {error, not_found}.
job_get(Id, #{jobs := Jobs}) ->
    case maps:find(Id, Jobs) of
        {ok, Job} -> {ok, Job};
        error -> {error, not_found}
    end.

-spec job_list(gaffer:list_opts(), state()) ->
    {ok, [gaffer:job()]}.
job_list(Opts, #{jobs := Jobs}) ->
    All = maps:values(Jobs),
    Filtered = lists:filter(
        fun(Job) -> matches(Opts, Job) end, All
    ),
    {ok, Filtered}.

-spec job_delete(gaffer:job_id(), state()) ->
    {ok, state()}.
job_delete(Id, #{jobs := Jobs} = State) ->
    {ok, State#{jobs := maps:remove(Id, Jobs)}}.

-spec job_claim(
    gaffer:claim_opts(), gaffer:job_changes(), state()
) ->
    {ok, [gaffer:job()], state()}.
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
    {ok, Updated, State#{jobs := NewJobs}}.

-spec job_update(gaffer:job(), state()) ->
    {ok, gaffer:job(), state()}.
job_update(#{id := Id} = Job, #{jobs := Jobs} = State) ->
    {ok, Job, State#{jobs := Jobs#{Id := Job}}}.

-spec job_prune(gaffer:prune_opts(), state()) ->
    {ok, non_neg_integer(), state()}.
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
    {ok, Count, State#{jobs := Keep}}.

%--- Internal -----------------------------------------------------------------

matches(Opts, Job) ->
    maps:fold(
        fun(Key, Val, Acc) ->
            Acc andalso maps:get(Key, Job, undefined) =:= Val
        end,
        true,
        Opts
    ).
