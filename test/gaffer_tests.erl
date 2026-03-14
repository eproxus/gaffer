-module(gaffer_tests).

-include_lib("eunit/include/eunit.hrl").

%% Tests intentionally pass invalid conf to verify validation
-eqwalizer({nowarn_function, create_queue_extra_key/1}).

%--- Fixtures -----------------------------------------------------------------

% Tests that run against all drivers (ETS + Postgres)
gaffer_test_() ->
    Tests = [
        %% Queue management
        fun create_queue/1,
        fun get_queue/1,
        fun update_queue/1,
        fun delete_queue/1,
        fun list_queues/1,
        %% Insert
        fun insert/1,
        fun insert_with_opts/1,
        fun insert_scheduled/1,
        %% Get / list
        fun get_job/1,
        fun list_jobs/1,
        %% Insert validation
        fun insert_invalid_max_attempts/1,
        %% Validation
        fun create_queue_extra_key/1,
        fun update_queue_extra_key/1,
        fun update_queue_empty/1
    ],
    [
        harness(gaffer_driver_ets, Tests),
        harness(gaffer_driver_pgo, Tests)
    ].

% Tests that run against ETS only (to be migrated into gaffer_test_/0)
gaffer_ets_test_() ->
    {setup, fun() -> setup(gaffer_driver_ets) end, fun teardown/1, fun(
        #{driver := Driver}
    ) ->
        {inparallel, [
            {with, Driver, [T]}
         || T <:- [
                %% Get / list (make_ref IDs not supported in PGO)
                fun get_not_found/1,
                %% Cancel
                fun cancel/1,
                fun cancel_not_found/1,
                fun cancel_scheduled/1,
                fun cancel_executing/1,
                fun cancel_completed_error/1,
                fun cancel_discarded_error/1,
                %% Complete
                fun complete/1,
                fun complete_not_found/1,
                fun complete_available_error/1,
                %% Fail
                fun fail_retryable/1,
                fun fail_discarded/1,
                fun fail_not_found/1,
                fun fail_error_normalization/1,
                %% Schedule
                fun schedule/1,
                fun schedule_from_failed/1,
                fun schedule_not_found/1,
                fun schedule_available_error/1,
                %% Claim
                fun claim/1,
                fun claim_empty/1,
                %% Prune
                fun prune/1
            ]
        ]}
    end}.

harness(DriverMod, Tests) ->
    {setup, fun() -> setup(DriverMod) end, fun teardown/1, fun(
        #{driver := Driver}
    ) ->
        {inparallel, [{with, Driver, [T]} || T <:- Tests]}
    end}.

%--- Setup / teardown ---------------------------------------------------------

setup(DriverMod) ->
    {Driver, Apps0} = setup_driver(DriverMod),
    {ok, Apps1} = application:ensure_all_started(gaffer),
    #{driver => Driver, gaffer_apps => Apps1, driver_apps => Apps0}.

teardown(#{
    driver := Driver, gaffer_apps := GafferApps, driver_apps := DriverApps
}) ->
    [application:stop(A) || A <:- lists:reverse(GafferApps)],
    teardown_driver(Driver),
    [application:stop(A) || A <:- lists:reverse(DriverApps)].

setup_driver(gaffer_driver_ets) ->
    DS = gaffer_driver_ets:start(#{}),
    {{gaffer_driver_ets, DS}, []};
setup_driver(gaffer_driver_pgo) ->
    {ok, Apps} = application:ensure_all_started(pgo),
    stop_pool(test_pool),
    {ok, _} = pgo:start_pool(test_pool, pgo_pool_config()),
    reset_database(test_pool),
    DS = gaffer_driver_pgo:start(#{pool => test_pool}),
    {{gaffer_driver_pgo, DS}, Apps}.

teardown_driver({gaffer_driver_ets, DS}) ->
    gaffer_driver_ets:stop(DS);
teardown_driver({gaffer_driver_pgo, DS}) ->
    gaffer_driver_pgo:stop(DS),
    reset_database(test_pool),
    stop_pool(test_pool).

%--- PGO helpers --------------------------------------------------------------

pgo_pool_config() ->
    {ok, [PgConfig]} = file:consult("config/test.config"),
    {postgres, Props} = PgConfig,
    Config = maps:with(
        [host, port, database, user, password],
        maps:from_list(Props)
    ),
    Config#{pool_size => 2}.

reset_database(Pool) ->
    Opts = #{pool => Pool},
    pgo:query(~"DROP SCHEMA public CASCADE", [], Opts),
    pgo:query(~"CREATE SCHEMA public", [], Opts),
    ok.

stop_pool(Pool) ->
    case whereis(Pool) of
        undefined -> ok;
        Pid -> supervisor:terminate_child(pgo_sup, Pid)
    end.

%--- Queue management tests ---------------------------------------------------

create_queue(Driver) ->
    Conf = #{name => ?FUNCTION_NAME, driver => Driver},
    ?assertEqual(ok, gaffer:create_queue(Conf)),
    ?assertMatch(
        #{name := create_queue},
        gaffer:get_queue(?FUNCTION_NAME)
    ),
    ?assertEqual(
        {error, already_exists}, gaffer:create_queue(Conf)
    ).

get_queue(Driver) ->
    Conf = #{name => ?FUNCTION_NAME, driver => Driver},
    ok = gaffer:create_queue(Conf),
    ?assertMatch(
        #{
            name := get_queue,
            global_max_workers := 25,
            max_workers := 5,
            priority := 0
        },
        gaffer:get_queue(?FUNCTION_NAME)
    ).

update_queue(Driver) ->
    ok = gaffer:create_queue(#{
        name => ?FUNCTION_NAME,
        driver => Driver,
        global_max_workers => 5
    }),
    ok = gaffer:update_queue(?FUNCTION_NAME, #{global_max_workers => 10}),
    Updated = gaffer:get_queue(?FUNCTION_NAME),
    ?assertEqual(10, maps:get(global_max_workers, Updated)).

delete_queue(Driver) ->
    ok = gaffer:create_queue(
        #{name => ?FUNCTION_NAME, driver => Driver}
    ),
    ?assertEqual(ok, gaffer:delete_queue(?FUNCTION_NAME)),
    ?assertError(
        {unknown_queue, delete_queue},
        gaffer:get_queue(?FUNCTION_NAME)
    ),
    ?assertError(
        {unknown_queue, delete_queue},
        gaffer:delete_queue(?FUNCTION_NAME)
    ).

list_queues(Driver) ->
    ok = gaffer:create_queue(
        #{name => list_queues_1, driver => Driver}
    ),
    ok = gaffer:create_queue(
        #{name => list_queues_2, driver => Driver}
    ),
    Queues = gaffer:list_queues(),
    Names = [Name || {Name, _} <:- Queues],
    ?assert(lists:member(list_queues_1, Names)),
    ?assert(lists:member(list_queues_2, Names)).

%--- Insert tests -------------------------------------------------------------

insert(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    Job = gaffer:insert(Q, #{task => 1}),
    ?assertMatch(
        #{
            queue := insert,
            payload := #{task := 1},
            state := available,
            priority := 0,
            max_attempts := 3,
            attempt := 0
        },
        atomize_keys(Job)
    ).

insert_with_opts(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    Opts = #{priority => 5, max_attempts => 10},
    Job = gaffer:insert(Q, #{task => 1}, Opts),
    ?assertMatch(
        #{
            queue := insert_with_opts,
            payload := #{task := 1},
            priority := 5,
            max_attempts := 10
        },
        atomize_keys(Job)
    ).

insert_scheduled(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    At = erlang:system_time() + erlang:convert_time_unit(3600, second, native),
    Job = gaffer:insert(Q, #{task => 1}, #{scheduled_at => At}),
    ?assertMatch(#{state := scheduled, scheduled_at := _}, Job).

%--- Get / list tests ---------------------------------------------------------

get_job(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    #{id := Id} = gaffer:insert(Q, #{task => 1}),
    Job = gaffer:get(Q, Id),
    ?assertMatch(
        #{id := Id, queue := get_job, payload := #{task := 1}},
        atomize_keys(Job)
    ).

get_not_found(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    ?assertError(
        {unknown_job, _},
        gaffer:get(Q, make_ref())
    ).

list_jobs(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    _ = gaffer:insert(Q, #{task => 1}),
    _ = gaffer:insert(Q, #{task => 2}),
    Jobs = gaffer:list(#{queue => Q}),
    ?assertEqual(2, length(Jobs)).

%--- Cancel tests -------------------------------------------------------------

cancel(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    #{id := Id} = gaffer:insert(Q, #{task => 1}),
    {ok, Job} = gaffer:cancel(Q, Id),
    ?assertMatch(#{state := cancelled, cancelled_at := _, id := Id}, Job).

cancel_not_found(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    ?assertError(
        {unknown_job, _},
        gaffer:cancel(Q, make_ref())
    ).

cancel_scheduled(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    At = erlang:system_time() + erlang:convert_time_unit(3600, second, native),
    #{id := Id} = gaffer:insert(Q, #{task => 1}, #{scheduled_at => At}),
    {ok, Cancelled} = gaffer:cancel(Q, Id),
    ?assertMatch(#{state := cancelled, cancelled_at := _}, Cancelled).

cancel_executing(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    #{id := Id} = gaffer:insert(Q, #{task => 1}),
    [_] = gaffer_queue_runner:claim(Q, #{queue => Q, limit => 1}),
    {ok, Cancelled} = gaffer:cancel(Q, Id),
    ?assertMatch(#{state := cancelled, cancelled_at := _}, Cancelled).

cancel_completed_error(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    #{id := Id} = gaffer:insert(Q, #{task => 1}),
    [_] = gaffer_queue_runner:claim(Q, #{queue => Q, limit => 1}),
    {ok, _} = gaffer_queue_runner:complete(Q, Id),
    ?assertMatch(
        {error, {invalid_transition, {completed, cancelled}}},
        gaffer:cancel(Q, Id)
    ).

cancel_discarded_error(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    #{id := Id} = gaffer:insert(Q, #{task => 1}, #{max_attempts => 1}),
    [_] = gaffer_queue_runner:claim(Q, #{queue => Q, limit => 1}),
    E = #{attempt => 1, error => boom, at => erlang:system_time()},
    {ok, _} = gaffer_queue_runner:fail(Q, Id, E),
    ?assertMatch(
        {error, {invalid_transition, {discarded, cancelled}}},
        gaffer:cancel(Q, Id)
    ).

%--- Complete tests -----------------------------------------------------------

complete(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    #{id := Id} = gaffer:insert(Q, #{task => 1}),
    [_] = gaffer_queue_runner:claim(Q, #{queue => Q, limit => 1}),
    {ok, Completed} = gaffer_queue_runner:complete(Q, Id),
    ?assertMatch(
        #{state := completed, attempt := 1, completed_at := _}, Completed
    ).

complete_not_found(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    ?assertEqual(
        {error, not_found},
        gaffer_queue_runner:complete(Q, make_ref())
    ).

complete_available_error(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    #{id := Id} = gaffer:insert(Q, #{task => 1}),
    ?assertMatch(
        {error, {invalid_transition, {available, completed}}},
        gaffer_queue_runner:complete(Q, Id)
    ).

%--- Fail tests ---------------------------------------------------------------

fail_retryable(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    #{id := Id} = gaffer:insert(Q, #{task => 1}, #{max_attempts => 3}),
    [_] = gaffer_queue_runner:claim(Q, #{queue => Q, limit => 1}),
    E = #{attempt => 1, error => timeout, at => erlang:system_time()},
    {ok, Failed} = gaffer_queue_runner:fail(Q, Id, E),
    ?assertMatch(
        #{
            state := failed,
            attempt := 1,
            errors := [#{attempt := 1, error := timeout}]
        },
        Failed
    ).

fail_discarded(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    #{id := Id} = gaffer:insert(Q, #{task => 1}, #{max_attempts => 1}),
    [_] = gaffer_queue_runner:claim(Q, #{queue => Q, limit => 1}),
    E = #{attempt => 1, error => boom, at => erlang:system_time()},
    {ok, Discarded} = gaffer_queue_runner:fail(Q, Id, E),
    ?assertMatch(#{state := discarded, discarded_at := _}, Discarded).

fail_not_found(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    E = #{attempt => 1, error => boom, at => erlang:system_time()},
    ?assertEqual(
        {error, not_found},
        gaffer_queue_runner:fail(Q, make_ref(), E)
    ).

fail_error_normalization(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    #{id := Id} = gaffer:insert(Q, #{task => 1}, #{max_attempts => 3}),
    [_] = gaffer_queue_runner:claim(Q, #{queue => Q, limit => 1}),
    AtUs = erlang:system_time(microsecond),
    ErrorInfo = [#{reason => {badrpc, nodedown}}],
    E = #{attempt => 1, error => ErrorInfo, at => {microsecond, AtUs}},
    {ok, Failed} = gaffer_queue_runner:fail(Q, Id, E),
    ?assertMatch(
        #{errors := [#{attempt := 1, error := ErrorInfo, at := AtUs}]},
        Failed
    ).

%--- Schedule tests -----------------------------------------------------------

schedule(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    #{id := Id} = gaffer:insert(Q, #{task => 1}),
    [_] = gaffer_queue_runner:claim(Q, #{queue => Q, limit => 1}),
    FutureAt =
        erlang:system_time() + erlang:convert_time_unit(60, second, native),
    {ok, Scheduled} = gaffer_queue_runner:schedule(Q, Id, FutureAt),
    ?assertMatch(#{state := scheduled, scheduled_at := FutureAt}, Scheduled).

schedule_from_failed(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    #{id := Id} = gaffer:insert(Q, #{task => 1}, #{max_attempts => 3}),
    [_] = gaffer_queue_runner:claim(Q, #{queue => Q, limit => 1}),
    E = #{attempt => 1, error => boom, at => erlang:system_time()},
    {ok, _} = gaffer_queue_runner:fail(Q, Id, E),
    FutureAt =
        erlang:system_time() + erlang:convert_time_unit(60, second, native),
    {ok, Scheduled} = gaffer_queue_runner:schedule(Q, Id, FutureAt),
    ?assertMatch(#{state := scheduled, scheduled_at := FutureAt}, Scheduled).

schedule_not_found(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    FutureAt =
        erlang:system_time() + erlang:convert_time_unit(60, second, native),
    ?assertEqual(
        {error, not_found},
        gaffer_queue_runner:schedule(Q, make_ref(), FutureAt)
    ).

schedule_available_error(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    #{id := Id} = gaffer:insert(Q, #{task => 1}),
    FutureAt =
        erlang:system_time() + erlang:convert_time_unit(60, second, native),
    ?assertMatch(
        {error, {invalid_transition, {available, scheduled}}},
        gaffer_queue_runner:schedule(Q, Id, FutureAt)
    ).

%--- Insert validation tests --------------------------------------------------

insert_invalid_max_attempts(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    ?assertError(
        {invalid_job, invalid_max_attempts},
        gaffer:insert(Q, #{task => 1}, #{max_attempts => 0})
    ).

%--- Validation tests ---------------------------------------------------------

create_queue_extra_key(Driver) ->
    ?assertError(
        {invalid_queue_conf, #{extra := [bogus]}},
        gaffer:create_queue(#{
            name => ?FUNCTION_NAME, driver => Driver, bogus => 42
        })
    ).

update_queue_extra_key(Driver) ->
    ok = gaffer:create_queue(#{name => ?FUNCTION_NAME, driver => Driver}),
    ?assertError(
        {invalid_queue_conf, _},
        gaffer:update_queue(?FUNCTION_NAME, #{bogus => 42})
    ).

update_queue_empty(Driver) ->
    ok = gaffer:create_queue(#{name => ?FUNCTION_NAME, driver => Driver}),
    ?assertError(
        {invalid_queue_conf, _},
        gaffer:update_queue(?FUNCTION_NAME, #{})
    ).

%--- Claim tests --------------------------------------------------------------

claim(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    _ = gaffer:insert(Q, #{task => 1}),
    _ = gaffer:insert(Q, #{task => 2}),
    ?assertMatch(
        [#{state := executing, attempted_at := _}],
        gaffer_queue_runner:claim(Q, #{queue => Q, limit => 1})
    ).

claim_empty(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    ?assertEqual(
        [],
        gaffer_queue_runner:claim(Q, #{queue => Q, limit => 5})
    ).

%--- Prune tests --------------------------------------------------------------

prune(Driver) ->
    Q = ?FUNCTION_NAME,
    ok = gaffer:create_queue(#{name => Q, driver => Driver}),
    #{id := Id} = gaffer:insert(Q, #{task => 1}),
    {ok, _} = gaffer:cancel(Q, Id),
    Count = gaffer_queue_runner:prune(Q, #{states => [cancelled]}),
    ?assert(Count >= 1).

%--- Helpers ------------------------------------------------------------------

atomize_keys(Map) when is_map(Map) ->
    maps:fold(
        fun
            (K, V, Acc) when is_binary(K) ->
                Acc#{binary_to_existing_atom(K) => atomize_keys(V)};
            (K, V, Acc) ->
                Acc#{K => atomize_keys(V)}
        end,
        #{},
        Map
    );
atomize_keys(Other) ->
    Other.
