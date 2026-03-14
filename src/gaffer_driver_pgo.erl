-module(gaffer_driver_pgo).

-behaviour(gaffer_driver).

%% Lifecycle
-export([start/1]).
-export([stop/1]).
-export([rollback/2]).

%% Queue config
-export([queue_put/2]).
-export([queue_get/2]).
-export([queue_delete/2]).

%% Job CRUD
-export([job_insert/2]).
-export([job_get/2]).
-export([job_list/2]).
-export([job_delete/2]).
-export([job_claim/3]).
-export([job_update/2]).
-export([job_prune/2]).

%% rollback/2 is an operational tool (shell use), not called from app code
-ignore_xref([rollback/2]).

%% Stubs — remove ignore_xref as each callback gets implemented
-ignore_xref([
    queue_put/2,
    queue_get/2,
    queue_delete/2,
    job_insert/2,
    job_get/2,
    job_list/2,
    job_delete/2,
    job_claim/3,
    job_update/2,
    job_prune/2
]).

-type state() :: #{
    pool := atom(),
    owns_pool := boolean()
}.

-export_type([state/0]).

%--- Lifecycle ----------------------------------------------------------------

-spec start(map()) -> state().
start(Opts) ->
    {Pool, OwnsPool} = resolve_pool(Opts),
    State = #{pool => Pool, owns_pool => OwnsPool},
    MigrationOpts = maps:with([uuid_format], Opts),
    ensure_migrations_table(Pool),
    Current = applied_version(Pool),
    Migrations = gaffer_postgres:migrations(MigrationOpts),
    Pending = [M || {V, _, _} = M <:- Migrations, V > Current],
    run_migrations(Pool, fun gaffer_postgres:migrate_up/1, Pending),
    State.

-spec stop(state()) -> ok.
stop(#{owns_pool := false}) ->
    ok;
stop(#{pool := Pool, owns_pool := true}) ->
    stop_pool(Pool),
    ok.

-spec rollback(TargetVersion :: non_neg_integer(), state()) -> ok.
rollback(TargetVersion, #{pool := Pool}) ->
    Current = applied_version(Pool),
    AllMigrations = gaffer_postgres:migrations(#{}),
    ToRollback = [
        lists:keyfind(V, 1, AllMigrations)
     || V <:- lists:seq(Current, TargetVersion + 1, -1)
    ],
    run_migrations(Pool, fun gaffer_postgres:migrate_down/1, ToRollback),
    ok.

%--- Queue config (stubs) ----------------------------------------------------

%% TODO: Catch FK violation on on_discard and raise
%%       error({on_discard_queue_not_found, Name}).
-spec queue_put(gaffer:queue_conf(), state()) -> no_return().
queue_put(_Conf, _State) -> error(not_implemented).

-spec queue_get(gaffer:queue_name(), state()) -> no_return().
queue_get(_Name, _State) -> error(not_implemented).

-spec queue_delete(gaffer:queue_name(), state()) -> no_return().
queue_delete(_Name, _State) -> error(not_implemented).

%--- Job CRUD (stubs) --------------------------------------------------------

-spec job_insert(gaffer:new_job(), state()) -> no_return().
job_insert(_Job, _State) -> error(not_implemented).

-spec job_get(gaffer:job_id(), state()) -> no_return().
job_get(_Id, _State) -> error(not_implemented).

-spec job_list(gaffer:list_opts(), state()) -> no_return().
job_list(_Opts, _State) -> error(not_implemented).

-spec job_delete(gaffer:job_id(), state()) -> no_return().
job_delete(_Id, _State) -> error(not_implemented).

-spec job_claim(
    gaffer:claim_opts(), gaffer:job_changes(), state()
) -> no_return().
job_claim(_Opts, _Changes, _State) -> error(not_implemented).

-spec job_update(gaffer:job(), state()) -> no_return().
job_update(_Job, _State) -> error(not_implemented).

-spec job_prune(gaffer:prune_opts(), state()) -> no_return().
job_prune(_Opts, _State) -> error(not_implemented).

%--- Internal -----------------------------------------------------------------

%% pgo does not expose a public stop_pool API, so we reach into its
%% internal supervisor. If pgo changes its supervision tree, update here.
stop_pool(Pool) ->
    case whereis(Pool) of
        undefined -> ok;
        Pid -> supervisor:terminate_child(pgo_sup, Pid)
    end.

resolve_pool(#{pool := Pool, start := PgoConfig}) ->
    {ok, _} = pgo:start_pool(Pool, PgoConfig),
    {Pool, true};
resolve_pool(#{pool := Pool}) ->
    {Pool, false}.

ensure_migrations_table(Pool) ->
    lists:foreach(
        fun({SQL, Params}) ->
            pgo:query(SQL, Params, #{pool => Pool})
        end,
        gaffer_postgres:ensure_migrations_table()
    ).

run_migrations(Pool, ToQueries, Migrations) ->
    lists:foreach(
        fun(Migration) -> transaction(Pool, ToQueries(Migration)) end,
        Migrations
    ).

applied_version(Pool) ->
    [{SQL, Params}] = gaffer_postgres:applied_version(),
    #{rows := [{Version}]} = pgo:query(SQL, Params, #{pool => Pool}),
    Version.

%% Runs a list of queries in a single transaction.
%% pgo:query/2 (without pool opt) uses the implicit transaction
%% connection from the process dictionary, set by pgo:transaction/2.
transaction(Pool, Queries) ->
    pgo:transaction(
        fun() ->
            lists:foreach(
                fun({SQL, Params}) -> pgo:query(SQL, Params) end,
                Queries
            )
        end,
        #{pool => Pool}
    ).
