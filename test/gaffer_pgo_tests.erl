-module(gaffer_pgo_tests).

-hank([
    {unnecessary_function_arguments, [
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
            fun pgo_migration_rollback/1,
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
    % Registering the same queue name again is idempotent
    ?assertEqual(ok, gaffer_driver_pgo:queue_insert(?Q, DS)).

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
