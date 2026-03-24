-module(gaffer_pgo_tests).

-hank([
    {unnecessary_function_arguments, [pgo_start_with_new_pool]}
]).

-include_lib("eunit/include/eunit.hrl").

-define(Q, ?FUNCTION_NAME).
-define(CONF(Driver), #{name => ?Q, driver => Driver, poll_interval => infinity}).
-define(CONF(Driver, Extra), maps:merge(?CONF(Driver), Extra)).

%--- Fixtures -----------------------------------------------------------------

gaffer_pgo_test_() ->
    gaffer_test_helpers:harness(
        gaffer_driver_pgo,
        [
            % Calls driver directly to test upsert behavior
            fun pgo_idempotent_create/1
        ],
        [
            % Driver migration/startup internals (mutate shared schema)
            fun pgo_migration_idempotent/1,
            fun pgo_migration_rollback/1,
            fun pgo_start_with_new_pool/1
        ]
    ).

%--- PGO-specific tests -------------------------------------------------------

pgo_migration_idempotent({gaffer_driver_pgo, #{pool := Pool}}) ->
    State = gaffer_driver_pgo:start(#{pool => Pool}),
    State2 = gaffer_driver_pgo:start(#{pool => Pool}),
    ?assertMatch(#{pool := _}, State),
    ?assertMatch(#{pool := _}, State2),
    ?assert(table_exists(Pool, ~"gaffer_queues")),
    ?assert(table_exists(Pool, ~"gaffer_jobs")).

pgo_migration_rollback({gaffer_driver_pgo, #{pool := Pool}}) ->
    State = gaffer_driver_pgo:start(#{pool => Pool}),
    ?assert(table_exists(Pool, ~"gaffer_queues")),
    ?assert(table_exists(Pool, ~"gaffer_jobs")),
    ok = gaffer_driver_pgo:rollback(0, State),
    ?assertNot(table_exists(Pool, ~"gaffer_queues")),
    ?assertNot(table_exists(Pool, ~"gaffer_jobs")),
    #{rows := Rows} = pgo:query(
        ~"SELECT version FROM gaffer_schema_version",
        [],
        #{pool => Pool}
    ),
    ?assertEqual([{0}], Rows).

pgo_start_with_new_pool(_Driver) ->
    PoolConfig = gaffer_test_helpers:pgo_pool_config(),
    gaffer_test_helpers:stop_pool(my_started_pool),
    State = gaffer_driver_pgo:start(#{
        pool => my_started_pool, start => PoolConfig
    }),
    try
        ?assert(table_exists(my_started_pool, ~"gaffer_queues")),
        ?assert(table_exists(my_started_pool, ~"gaffer_jobs"))
    after
        gaffer_driver_pgo:stop(State)
    end.

pgo_idempotent_create({gaffer_driver_pgo, DS} = Driver) ->
    ok = gaffer:create_queue(?CONF(Driver, #{max_workers => 3})),
    % Bypass persistent_term and insert the same config via driver directly
    Persisted = gaffer:get_queue(?Q),
    ?assertEqual(ok, gaffer_driver_pgo:queue_insert(Persisted, DS)).

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
