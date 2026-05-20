-module(gaffer_pgo_tests).

-hank([
    {unnecessary_function_arguments, [
        pgo_advisory_lock_query_emitted,
        pgo_start_with_new_pool,
        pgo_multi_node_distribution,
        pgo_multi_node_ensure_queue,
        pgo_multi_node_ensure_queue_rolling_upgrade
    ]}
]).

-include_lib("eunit/include/eunit.hrl").

-define(Q, ?FUNCTION_NAME).
-define(CONF(Driver), #{
    name => ?Q,
    driver => Driver,
    worker => gaffer_test_worker,
    poll_interval => infinity
}).
-define(CONF(Driver, Extra), maps:merge(?CONF(Driver), Extra)).

%--- Fixtures -----------------------------------------------------------------

gaffer_pgo_test_() ->
    gaffer_test_helpers:harness(
        gaffer_driver_pgo,
        [
            % Calls driver directly to test idempotent insert
            fun pgo_idempotent_create/1
        ],
        [
            % Driver migration/startup internals (mutate shared schema)
            fun pgo_migration_idempotent/1,
            fun pgo_migration_up_sql_idempotent/1,
            fun pgo_migration_down_sql_idempotent/1,
            fun pgo_migration_rollback/1,
            fun pgo_migrations_listing/1,
            fun pgo_migrations_rollback_round_trip/1,
            fun pgo_rollback_unknown_migration_version/1,
            fun pgo_migration_checksum_mismatch_detected/1,
            fun pgo_advisory_lock_query_emitted/1,
            fun pgo_jobs_constraint_violations/1,
            fun pgo_jobs_column_defaults/1,
            fun pgo_concurrent_prune_disjoint/1,
            fun pgo_class_40_returns_transient/1,
            fun pgo_start_with_new_pool/1,
            fun pgo_multi_node_distribution/1,
            fun pgo_multi_node_ensure_queue/1,
            fun pgo_multi_node_ensure_queue_rolling_upgrade/1
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

pgo_migration_up_sql_idempotent({gaffer_driver_pgo, #{pool := Pool} = State}) ->
    ok = gaffer_driver_pgo:rollback(0, State),
    % Pre-create one of the schema objects outside the migration system
    pgo:query(
        ~"CREATE TABLE gaffer_queues (name TEXT PRIMARY KEY)",
        [],
        #{pool => Pool}
    ),
    % start/1 sees version 1 unapplied and runs up SQL.
    % Without IF NOT EXISTS, CREATE TABLE gaffer_queues fails here.
    _ = gaffer_driver_pgo:start(#{pool => Pool}),
    ?assert(table_exists(Pool, ~"gaffer_queues")),
    ?assert(table_exists(Pool, ~"gaffer_jobs")).

pgo_migration_down_sql_idempotent(
    {gaffer_driver_pgo, #{pool := Pool} = State}
) ->
    ?assert(table_exists(Pool, ~"gaffer_jobs")),
    pgo:query(~"DROP TABLE gaffer_jobs", [], #{pool => Pool}),
    pgo:query(~"DROP TABLE gaffer_queues", [], #{pool => Pool}),
    % Migration log still records version 1 as applied.
    % Without IF EXISTS, DROP TABLE gaffer_jobs fails here.
    ok = gaffer_driver_pgo:rollback(0, State),
    ?assertNot(table_exists(Pool, ~"gaffer_queues")),
    ?assertNot(table_exists(Pool, ~"gaffer_jobs")).

pgo_migration_rollback({gaffer_driver_pgo, #{pool := Pool} = State}) ->
    ?assert(table_exists(Pool, ~"gaffer_queues")),
    ?assert(table_exists(Pool, ~"gaffer_jobs")),
    ok = gaffer_driver_pgo:rollback(0, State),
    ?assertNot(table_exists(Pool, ~"gaffer_queues")),
    ?assertNot(table_exists(Pool, ~"gaffer_jobs")),
    #{applied := Applied} = gaffer_driver_pgo:migrations(State),
    ?assertEqual([], Applied).

pgo_migrations_listing({gaffer_driver_pgo, State}) ->
    Static = [V || {V, _, _} <:- gaffer_postgres:migrations(#{})],
    #{all := All, applied := Applied} = gaffer_driver_pgo:migrations(State),
    ?assertEqual(Static, All),
    ?assertEqual(lists:sort(All), lists:sort(Applied)).

pgo_migrations_rollback_round_trip({gaffer_driver_pgo, State}) ->
    #{applied := Applied} = gaffer_driver_pgo:migrations(State),
    Target = lists:max(Applied) - 1,
    ok = gaffer_driver_pgo:rollback(Target, State),
    #{applied := Applied2} = gaffer_driver_pgo:migrations(State),
    ?assertEqual(lists:droplast(Applied), Applied2).

pgo_rollback_unknown_migration_version(
    {gaffer_driver_pgo, #{pool := Pool} = State}
) ->
    pgo:query(
        ~"INSERT INTO gaffer_schema_migrations (version, sql_checksum) VALUES (9999, $1)",
        [<<0:256>>],
        #{pool => Pool}
    ),
    ?assertError(
        {unknown_migration_version, 9999},
        gaffer_driver_pgo:rollback(0, State)
    ).

pgo_migration_checksum_mismatch_detected({gaffer_driver_pgo, #{pool := Pool}}) ->
    pgo:query(
        ~"UPDATE gaffer_schema_migrations SET sql_checksum = $1 WHERE version = 1",
        [<<0:256>>],
        #{pool => Pool}
    ),
    ?assertError(
        {migration_checksum_mismatch, 1, _, _},
        gaffer_driver_pgo:start(#{pool => Pool})
    ).

pgo_advisory_lock_query_emitted(_Driver) ->
    ?assertMatch(
        [{~"SELECT pg_advisory_xact_lock($1)::text", [_]}],
        gaffer_postgres:advisory_lock()
    ).

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
    % Registering the same queue name again is idempotent
    ?assertEqual(ok, gaffer_driver_pgo:queue_insert(?Q, DS)).

pgo_jobs_constraint_violations({gaffer_driver_pgo, #{pool := Pool}}) ->
    Q = ensure_queue(~"constraints_q", Pool),
    %% INSERTs that violate a CHECK constraint are rejected
    [
        ?assertMatch(
            {error, {pgsql_error, #{code := ~"23514"}}},
            insert_job(Q, Pool, A)
        )
     || A <:- [
            #{attempt => -1},
            #{attempt => 2, max_attempts => 1},
            #{max_attempts => 0}
        ]
    ],
    %% attempt = max_attempts is the terminal-write boundary
    ?assertMatch(
        #{command := insert},
        insert_job(Q, Pool, #{attempt => 3, max_attempts => 3})
    ),
    %% UPDATE that pushes attempt past max_attempts is rejected
    Id = uuid(),
    insert_job(Q, Pool, #{id => Id}),
    ?assertMatch(
        {error, {pgsql_error, #{code := ~"23514"}}},
        pgo:query(
            ~"UPDATE gaffer_jobs SET attempt = 2 WHERE id = $1",
            [Id],
            #{pool => Pool}
        )
    ).

pgo_jobs_column_defaults({gaffer_driver_pgo, #{pool := Pool}}) ->
    Q = ensure_queue(~"defaults_q", Pool),
    Id = uuid(),
    insert_job(Q, Pool, #{id => Id}),
    ?assertMatch(
        #{rows := [{0, 1, 0}]},
        pgo:query(
            ~"""
            SELECT attempt, max_attempts, priority
            FROM gaffer_jobs WHERE id = $1
            """,
            [Id],
            #{pool => Pool}
        )
    ).

%% Drives a real Postgres 40P01 deadlock via reentrant pgo:transaction calls
%% and asserts that the driver catches it as a transient error rather than
%% letting a pgsql_error exception escape. Each process locks one row, then
%% calls into the driver to write the OTHER row — the driver's pgo:transaction
%% reuses the outer connection, so the second lock attempt completes the
%% deadlock cycle.
pgo_class_40_returns_transient(
    {gaffer_driver_pgo, #{pool := Pool} = DS} = Driver
) ->
    QName = ?FUNCTION_NAME,
    ok = gaffer:create_queue(
        ?CONF(Driver, #{name => QName, prune => #{interval => infinity}})
    ),
    JobA = gaffer:insert(QName, #{n => 1}),
    JobB = gaffer:insert(QName, #{n => 2}),
    #{id := IdA} = JobA,
    #{id := IdB} = JobB,
    deadlock_race(Pool, DS, IdA, JobB, IdB, JobA).

deadlock_race(Pool, DS, IdA, JobB, IdB, JobA) ->
    Parent = self(),
    spawn_locker(Parent, Pool, DS, IdA, JobB, p1),
    spawn_locker(Parent, Pool, DS, IdB, JobA, p2),
    P1Worker = await_locked(p1),
    P2Worker = await_locked(p2),
    P1Worker ! go,
    P2Worker ! go,
    R1 = await_done(p1),
    R2 = await_done(p2),
    ?assert(
        lists:any(fun is_transient_deadlock/1, [R1, R2]),
        lists:flatten(io_lib:format("R1=~p R2=~p", [R1, R2]))
    ).

spawn_locker(Parent, Pool, DS, LockId, OtherJob, MyTag) ->
    spawn(fun() ->
        Result =
            try
                pgo:transaction(
                    fun() ->
                        locker_tx(Parent, DS, LockId, OtherJob, MyTag)
                    end,
                    #{pool => Pool}
                )
            catch
                Class:Reason -> {Class, Reason}
            end,
        Parent ! {done, MyTag, Result}
    end).

locker_tx(Parent, DS, LockId, OtherJob, MyTag) ->
    pgo:query(
        ~"SELECT id FROM gaffer_jobs WHERE id = $1 FOR UPDATE",
        [LockId]
    ),
    Parent ! {locked, MyTag, self()},
    receive
        go -> ok
    after 10000 -> error({go_timeout, MyTag})
    end,
    gaffer_driver_pgo:job_write([OtherJob], DS).

await_locked(Tag) ->
    receive
        {locked, Tag, W} -> W
    after 5000 -> error({lock_timeout, Tag})
    end.

await_done(Tag) ->
    receive
        {done, Tag, X} -> X
    after 10000 -> error({done_timeout, Tag})
    end.

is_transient_deadlock(
    {error, {transient, {pgsql_error, #{code := <<"40", _/binary>>}}}}
) ->
    true;
is_transient_deadlock(_) ->
    false.

%% Verifies the new SKIP LOCKED prune CTE: with multiple concurrent prunes
%% against the same eligible rows, every row is deleted exactly once and
%% no transaction errors out. The pool size is small but each transaction
%% gets its own connection.
pgo_concurrent_prune_disjoint({gaffer_driver_pgo, _DS} = Driver) ->
    QName = ?FUNCTION_NAME,
    ok = gaffer:create_queue(
        ?CONF(Driver, #{name => QName, prune => #{interval => infinity}})
    ),
    IDs = seed_cancelled_jobs(QName, 30),
    Returned = race_prunes(QName, 2),
    %% No transient errors expected with the new CTE
    [?assert(is_list(R)) || R <:- Returned],
    All = lists:append(Returned),
    ?assertEqual(lists:sort(IDs), lists:sort(All)),
    ?assertEqual(length(All), length(lists:usort(All))).

seed_cancelled_jobs(QName, Total) ->
    lists:sort([seed_cancelled(QName, N) || N <:- lists:seq(1, Total)]).

seed_cancelled(QName, N) ->
    #{id := Id} = gaffer:insert(QName, #{n => N}),
    {ok, _} = gaffer:cancel(QName, Id),
    Id.

race_prunes(QName, Workers) ->
    Parent = self(),
    [
        spawn_link(fun() ->
            R = gaffer_queue:prune_jobs(QName, #{cancelled => 0}, user),
            Parent ! {prune_result, self(), R}
        end)
     || _ <:- lists:seq(1, Workers)
    ],
    [
        receive
            {prune_result, _, R} -> R
        after 10000 -> error(prune_timeout)
        end
     || _ <:- lists:seq(1, Workers)
    ].

%--- Multi-node tests ---------------------------------------------------------

pgo_multi_node_distribution(_Driver) ->
    ensure_distributed(),
    PoolConfig = gaffer_test_helpers:pgo_pool_config(),
    Peers = [
        start_peer(Name, PoolConfig)
     || Name <:- [gaffer_peer_1, gaffer_peer_2]
    ],
    PeerNodes = [N || {_, N} <:- Peers],
    try
        QueueConf = queue_conf(pgo_multi_node_distribution),
        ok = gaffer:create_queue(QueueConf),
        [
            ok = erpc:call(N, fun() -> gaffer:create_queue(QueueConf) end)
         || N <:- PeerNodes
        ],
        Nodes = insert_and_collect(pgo_multi_node_distribution, 12),
        UniqueNodes = lists:usort(Nodes),
        ExpectedNodes = lists:sort([node() | PeerNodes]),
        ?assertEqual(ExpectedNodes, UniqueNodes)
    after
        [peer:stop(P) || {P, _} <:- Peers],
        try
            gaffer:delete_queue(pgo_multi_node_distribution)
        catch
            _:_ -> ok
        end
    end.

pgo_multi_node_ensure_queue(_Driver) ->
    ensure_distributed(),
    PoolConfig = gaffer_test_helpers:pgo_pool_config(),
    Peers = [
        start_peer(Name, PoolConfig)
     || Name <:- [gaffer_peer_1, gaffer_peer_2]
    ],
    PeerNodes = [N || {_, N} <:- Peers],
    QName = pgo_multi_node_ensure_queue,
    try
        % Node A creates the queue with max_workers=1
        ok = gaffer:create_queue(queue_conf(QName, #{max_workers => 1})),
        % Node B ensures with different local config (max_workers=2)
        ok = erpc:call(hd(PeerNodes), fun() ->
            gaffer:ensure_queue(queue_conf(QName, #{max_workers => 2}))
        end),
        % Node C also ensures with max_workers=2
        ok = erpc:call(lists:last(PeerNodes), fun() ->
            gaffer:ensure_queue(queue_conf(QName, #{max_workers => 2}))
        end),
        % Local config is node-local, not shared
        #{max_workers := 1} = gaffer:get_queue(QName),
        % All nodes process jobs
        Nodes = insert_and_collect(QName, 12),
        UniqueNodes = lists:usort(Nodes),
        ExpectedNodes = lists:sort([node() | PeerNodes]),
        ?assertEqual(ExpectedNodes, UniqueNodes)
    after
        [peer:stop(P) || {P, _} <:- Peers],
        try
            gaffer:delete_queue(QName)
        catch
            _:_ -> ok
        end
    end.

pgo_multi_node_ensure_queue_rolling_upgrade(_Driver) ->
    ensure_distributed(),
    PoolConfig = gaffer_test_helpers:pgo_pool_config(),
    QName = pgo_multi_node_ensure_queue_rolling_upgrade,
    try
        % "Old" node starts with max_workers=1
        ok = gaffer:ensure_queue(queue_conf(QName, #{max_workers => 1})),
        ?assertMatch(#{max_workers := 1}, gaffer:get_queue(QName)),
        % "New" nodes start with max_workers=3 (rolling upgrade)
        {Peer1, Node1} = start_peer(gaffer_peer_1, PoolConfig),
        ensure_queue_on(Node1, QName, #{max_workers => 3}),
        % Local config stays at old value on this node
        ?assertMatch(#{max_workers := 1}, gaffer:get_queue(QName)),
        % Second "new" node also starts with max_workers=3
        {Peer2, Node2} = start_peer(gaffer_peer_2, PoolConfig),
        ensure_queue_on(Node2, QName, #{max_workers => 3}),
        % All three nodes participate in processing
        Nodes = insert_and_collect(QName, 12),
        UniqueNodes = lists:usort(Nodes),
        ExpectedNodes = lists:sort([node(), Node1, Node2]),
        ?assertEqual(ExpectedNodes, UniqueNodes),
        peer:stop(Peer1),
        peer:stop(Peer2)
    after
        try
            gaffer:delete_queue(QName)
        catch
            _:_ -> ok
        end
    end.

%--- Helpers ------------------------------------------------------------------

ensure_distributed() ->
    case is_alive() of
        true ->
            ok;
        false ->
            _ = os:cmd("epmd -daemon"),
            {ok, _} = net_kernel:start(
                gaffer_test, #{name_domain => shortnames}
            )
    end.

start_peer(Name, PoolConfig) ->
    {ok, Peer, Node} = peer:start_link(#{name => Name}),
    erpc:call(Node, code, add_paths, [code:get_path()]),
    erpc:call(Node, fun() ->
        {ok, _} = application:ensure_all_started(pgo),
        {ok, _} = pgo:start_pool(test_pool, PoolConfig),
        {ok, _} = application:ensure_all_started(gaffer)
    end),
    {Peer, Node}.

ensure_queue_on(Node, QName, Extra) ->
    ok = erpc:call(Node, fun() ->
        gaffer:ensure_queue(queue_conf(QName, Extra))
    end).

queue_conf(Name) ->
    queue_conf(Name, #{}).

queue_conf(Name, Extra) ->
    maps:merge(
        #{
            name => Name,
            driver => {gaffer_driver_pgo, #{pool => test_pool}},
            worker => gaffer_test_worker,
            max_workers => 1,
            poll_interval => 1
        },
        Extra
    ).

insert_and_collect(QueueName, JobCount) ->
    PidBin = gaffer_test_worker:encode_pid(self()),
    [
        gaffer:insert(QueueName, #{
            ~"action" => ~"complete",
            ~"test_pid" => PidBin
        })
     || _ <:- lists:seq(1, JobCount)
    ],
    [
        receive
            {job_executed, #{node := N}} -> N
        after 10000 ->
            error(timeout)
        end
     || _ <:- lists:seq(1, JobCount)
    ].

uuid() ->
    keysmith:uuid(7, binary).

ensure_queue(Name, Pool) ->
    pgo:query(
        ~"INSERT INTO gaffer_queues (name) VALUES ($1)",
        [Name],
        #{pool => Pool}
    ),
    Name.

insert_job(QueueName, Pool, Attrs) ->
    Defaults = #{
        id => uuid(),
        queue => QueueName,
        state => ~"available",
        payload => ~"{}",
        errors => ~"[]",
        created_at => {{2026, 1, 1}, {0, 0, 0}}
    },
    Pairs = maps:to_list(maps:merge(Defaults, Attrs)),
    Cols = [atom_to_binary(K) || {K, _} <:- Pairs],
    Vals = [V || {_, V} <:- Pairs],
    Phs = [[~"$", integer_to_binary(I)] || I <:- lists:seq(1, length(Cols))],
    SQL = iolist_to_binary([
        ~"INSERT INTO gaffer_jobs (",
        lists:join(~", ", Cols),
        ~") VALUES (",
        lists:join(~", ", Phs),
        ~")"
    ]),
    pgo:query(SQL, Vals, #{pool => Pool}).

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
