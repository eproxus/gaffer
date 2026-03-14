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
    start_with_new_pool_test/1,
    queue_on_discard_test/1,
    queue_idempotent_create_test/1,
    queue_config_mismatch_test/1,
    queue_update_on_discard_fk_test/1,
    insert_scheduled_test/1,
    get_not_found_test/1,
    list_filter_state_test/1,
    job_delete_test/1,
    job_delete_not_found_test/1
]).

all() ->
    [
        migration_idempotent_test,
        migration_rollback_test,
        start_with_new_pool_test,
        queue_on_discard_test,
        queue_idempotent_create_test,
        queue_config_mismatch_test,
        queue_update_on_discard_fk_test,
        insert_scheduled_test,
        get_not_found_test,
        list_filter_state_test,
        job_delete_test,
        job_delete_not_found_test
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
    {ok, _} = application:ensure_all_started(gaffer),
    Config.

end_per_testcase(_TestCase, _Config) ->
    application:stop(gaffer),
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
        ?assert(table_exists(my_started_pool, ~"gaffer_queues")),
        ?assert(table_exists(my_started_pool, ~"gaffer_jobs"))
    after
        gaffer_driver_pgo:stop(State)
    end.

queue_on_discard_test(_Config) ->
    Driver = driver(),
    %% Create a dead_letter queue first
    ok = gaffer:create_queue(#{
        name => dead_letter, driver => Driver
    }),
    %% Create a queue referencing dead_letter
    ok = gaffer:create_queue(#{
        name => on_discard_source, driver => Driver, on_discard => dead_letter
    }),
    ?assertMatch(
        #{on_discard := dead_letter},
        gaffer:get_queue(on_discard_source)
    ),
    %% FK violation: referencing nonexistent queue
    ?assertError(
        {on_discard_queue_not_found, nonexistent},
        gaffer:create_queue(#{
            name => bad_queue, driver => Driver, on_discard => nonexistent
        })
    ).

queue_idempotent_create_test(_Config) ->
    Driver = driver(),
    Conf = #{name => idempotent_q, driver => Driver, max_workers => 3},
    ok = gaffer:create_queue(Conf),
    %% Second create from a "different node" — bypass persistent_term check
    %% by calling the driver directly with the same conf that create would produce
    {_Mod, DS} = Driver,
    Persisted = gaffer:get_queue(idempotent_q),
    ?assertEqual(ok, gaffer_driver_pgo:queue_insert(Persisted, DS)).

queue_config_mismatch_test(_Config) ->
    Driver = driver(),
    ok = gaffer:create_queue(#{
        name => mismatch_q, driver => Driver, max_workers => 3
    }),
    %% Insert with different config directly via driver
    {_Mod, DS} = Driver,
    Conf = #{
        name => mismatch_q,
        max_workers => 99,
        global_max_workers => 25,
        poll_interval => 1000,
        shutdown_timeout => 5000,
        max_attempts => 3,
        timeout => 30000,
        backoff => 1000,
        priority => 0
    },
    ?assertError(
        {queue_config_mismatch, mismatch_q, _},
        gaffer_driver_pgo:queue_insert(Conf, DS)
    ).

queue_update_on_discard_fk_test(_Config) ->
    Driver = driver(),
    ok = gaffer:create_queue(#{name => fk_update_q, driver => Driver}),
    ?assertError(
        {on_discard_queue_not_found, nonexistent},
        gaffer:update_queue(fk_update_q, #{on_discard => nonexistent})
    ).

%--- Job CRUD tests -----------------------------------------------------------

insert_scheduled_test(_Config) ->
    Driver = driver(),
    Queue = job_sched_q,
    ok = gaffer:create_queue(#{name => Queue, driver => Driver}),
    ScheduledAt = {microsecond, erlang:system_time(microsecond) + 60_000_000},
    Job = gaffer:insert(Queue, #{}, #{scheduled_at => ScheduledAt}),
    ?assertEqual(scheduled, maps:get(state, Job)),
    Got = gaffer:get(Queue, maps:get(id, Job)),
    ?assertEqual(scheduled, maps:get(state, Got)),
    ?assert(is_map_key(scheduled_at, Got)),
    {microsecond, GotUs} = maps:get(scheduled_at, Got),
    {microsecond, ExpUs} = ScheduledAt,
    ?assert(abs(GotUs - ExpUs) < 2).

get_not_found_test(_Config) ->
    Driver = driver(),
    Queue = job_nf_q,
    ok = gaffer:create_queue(#{name => Queue, driver => Driver}),
    ?assertError({unknown_job, _}, gaffer:get(Queue, <<0:128>>)).

list_filter_state_test(_Config) ->
    Driver = driver(),
    Queue = job_filter_q,
    ok = gaffer:create_queue(#{name => Queue, driver => Driver}),
    gaffer:insert(Queue, #{}),
    Available = gaffer:list(#{queue => Queue, state => available}),
    ?assertEqual(1, length(Available)),
    Completed = gaffer:list(#{queue => Queue, state => completed}),
    ?assertEqual(0, length(Completed)).

job_delete_test(_Config) ->
    Driver = driver(),
    Queue = job_del_q,
    ok = gaffer:create_queue(#{name => Queue, driver => Driver}),
    Job = gaffer:insert(Queue, #{}),
    Id = maps:get(id, Job),
    ok = gaffer:delete(Queue, Id),
    ?assertError({unknown_job, _}, gaffer:get(Queue, Id)).

job_delete_not_found_test(_Config) ->
    Driver = driver(),
    Queue = job_del_nf_q,
    ok = gaffer:create_queue(#{name => Queue, driver => Driver}),
    ?assertError({unknown_job, _}, gaffer:delete(Queue, <<0:128>>)).

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

driver() ->
    State = gaffer_driver_pgo:start(#{pool => test_pool}),
    {gaffer_driver_pgo, State}.

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
