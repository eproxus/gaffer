-module(gaffer_driver_pgo_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

-export([
    migration_idempotent_test/1,
    migration_rollback_test/1,
    start_with_new_pool_test/1
]).

all() ->
    [
        migration_idempotent_test,
        migration_rollback_test,
        start_with_new_pool_test
    ].

%--- Suite setup --------------------------------------------------------------

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(pgo),
    PgConfig = ct:get_config(postgres),
    PoolConfig = pool_config(PgConfig),
    [{pool_config, PoolConfig} | Config].

end_per_suite(_Config) ->
    ok.

%--- Test case setup ----------------------------------------------------------

init_per_testcase(_TestCase, Config) ->
    PoolConfig = ?config(pool_config, Config),
    stop_pool(test_pool),
    {ok, _} = pgo:start_pool(test_pool, PoolConfig),
    reset_database(test_pool),
    Config.

end_per_testcase(_TestCase, _Config) ->
    try
        reset_database(test_pool)
    catch
        _:_ -> ok
    end,
    stop_pool(test_pool),
    ok.

%--- Tests --------------------------------------------------------------------

migration_idempotent_test(_Config) ->
    State = gaffer_driver_pgo:start(#{pool => test_pool}),
    %% Second start should be a no-op
    State2 = gaffer_driver_pgo:start(#{pool => test_pool}),
    ?assertMatch(#{pool := test_pool}, State),
    ?assertMatch(#{pool := test_pool}, State2),
    %% Verify tables exist
    ?assert(table_exists(test_pool, ~"gaffer_queues")),
    ?assert(table_exists(test_pool, ~"gaffer_jobs")),
    ok.

migration_rollback_test(_Config) ->
    State = gaffer_driver_pgo:start(#{pool => test_pool}),
    ?assert(table_exists(test_pool, ~"gaffer_queues")),
    ?assert(table_exists(test_pool, ~"gaffer_jobs")),
    %% Rollback everything
    ok = gaffer_driver_pgo:rollback(0, State),
    ?assertNot(table_exists(test_pool, ~"gaffer_queues")),
    ?assertNot(table_exists(test_pool, ~"gaffer_jobs")),
    %% Verify version is back to 0
    #{rows := Rows} = pgo:query(
        ~"SELECT version FROM gaffer_schema_version",
        [],
        #{pool => test_pool}
    ),
    ?assertEqual([{0}], Rows),
    ok.

start_with_new_pool_test(Config) ->
    PoolConfig = ?config(pool_config, Config),
    State = gaffer_driver_pgo:start(#{
        pool => my_started_pool, start => PoolConfig
    }),
    try
        ?assertMatch(#{pool := my_started_pool, owns_pool := true}, State),
        ?assert(table_exists(my_started_pool, ~"gaffer_queues")),
        ?assert(table_exists(my_started_pool, ~"gaffer_jobs"))
    after
        gaffer_driver_pgo:stop(State)
    end.

%--- Helpers ------------------------------------------------------------------

table_exists(Pool, TableName) ->
    #{rows := Rows} = pgo:query(
        ~"""
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = $1
        """,
        [TableName],
        #{pool => Pool}
    ),
    Rows =/= [].

reset_database(Pool) ->
    Opts = #{pool => Pool},
    pgo:query(~"DROP SCHEMA public CASCADE", [], Opts),
    pgo:query(~"CREATE SCHEMA public", [], Opts),
    ok.

%% See gaffer_driver_pgo:stop_pool/1
stop_pool(Pool) ->
    case whereis(Pool) of
        undefined -> ok;
        Pid -> supervisor:terminate_child(pgo_sup, Pid)
    end.

pool_config(PgConfig) ->
    Config = maps:with(
        [host, port, database, user, password],
        maps:from_list(PgConfig)
    ),
    Config#{pool_size => 2}.
