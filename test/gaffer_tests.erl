-module(gaffer_tests).

-behaviour(gaffer_hooks).

-hank([
    {unnecessary_function_arguments, [
        pgo_migration_idempotent,
        pgo_migration_rollback,
        pgo_start_with_new_pool
    ]}
]).

-export([gaffer_hook/3]).

-include_lib("eunit/include/eunit.hrl").

-define(Q, ?FUNCTION_NAME).
-define(CONF(Driver), #{name => ?Q, driver => Driver, poll_interval => infinity}).
-define(CONF(Driver, Extra), maps:merge(?CONF(Driver), Extra)).

%--- Fixtures -----------------------------------------------------------------

% Tests that run against all drivers (ETS + Postgres)
gaffer_test_() ->
    Parallel = [
        % --- CRUD ---
        % Queue management
        fun create_queue/1,
        fun get_queue/1,
        fun update_queue/1,
        fun delete_queue/1,
        fun list_queues/1,
        fun create_queue_on_discard/1,
        fun update_queue_on_discard_not_found/1,
        % Insert
        fun insert/1,
        fun insert_with_opts/1,
        fun insert_scheduled/1,
        fun insert_scheduled_microsecond/1,
        % Get / list
        fun get_job/1,
        fun get_not_found/1,
        fun list_jobs/1,
        % Delete
        fun delete_job/1,
        fun delete_not_found/1,
        % List filtering
        fun list_filter_state/1,
        % Validation
        fun insert_invalid_max_attempts/1,
        fun create_queue_extra_key/1,
        fun update_queue_extra_key/1,
        fun update_queue_empty/1,
        % --- Lifecycle ---
        % Cancel
        fun cancel/1,
        fun cancel_not_found/1,
        fun cancel_scheduled/1,
        fun cancel_executing/1,
        fun cancel_completed_error/1,
        fun cancel_discarded_error/1,
        % Complete
        fun complete/1,
        fun complete_not_found/1,
        % Fail
        fun fail_retryable/1,
        fun fail_discarded/1,
        fun fail_not_found/1,
        fun fail_error_normalization/1,
        % Schedule
        fun schedule/1,
        fun schedule_from_failed/1,
        fun schedule_not_found/1,
        % Claim
        fun claim/1,
        fun claim_empty/1,
        % Prune
        fun prune/1,
        % Polling
        fun poll_claims_and_spawns/1,
        fun poll_worker_completes_job/1,
        fun poll_worker_crash_fails_job/1,
        fun poll_max_workers_limits/1,
        fun poll_auto_executes/1,
        % --- Hooks ---
        fun hook_cancel/1,
        fun hook_complete/1,
        fun hook_fail/1,
        fun hook_schedule/1,
        fun hook_insert_pre_before_persist/1,
        fun hook_delete/1,
        fun hook_delete_pre_sees_job/1,
        fun hook_order/1,
        fun hook_data_passthrough/1,
        % --- Defaults ---
        fun job_inherits_queue_defaults/1,
        fun forwarded_job_inherits_target_defaults/1,
        fun job_overrides_timeout_backoff_shutdown/1,
        fun backoff_is_array/1,
        % --- Forwarding ---
        fun forward_on_discard/1,
        fun forward_on_discard_chain/1,
        fun forward_on_discard_retryable/1,
        fun forward_on_discard_fresh/1
    ],
    Sequential = [
        % Tests that restart gaffer
        fun create_queue_config_mismatch/1,
        % Hook tests that affect global state
        fun hook_module/1,
        fun hook_global_queue/1
    ],
    [
        harness(gaffer_driver_ets, Parallel, Sequential),
        harness(gaffer_driver_pgo, Parallel, Sequential)
    ].

% PGO-specific tests (driver internals, UUID IDs)
gaffer_pgo_test_() ->
    harness(
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

harness(DriverMod, Parallel, Sequential) ->
    {setup, fun() -> setup(DriverMod) end, fun teardown/1, fun(
        #{driver := Driver}
    ) ->
        {inorder, [
            {inparallel, [{with, Driver, [T]} || T <:- Parallel]},
            [{with, Driver, [T]} || T <:- Sequential]
        ]}
    end}.

%--- Setup / teardown ---------------------------------------------------------

setup(DriverMod) ->
    error_logger:tty(false),
    {Driver, Apps0} = setup_driver(DriverMod),
    {ok, Apps1} = application:ensure_all_started(gaffer),
    #{driver => Driver, gaffer_apps => Apps1, driver_apps => Apps0}.

teardown(#{
    driver := Driver, gaffer_apps := GafferApps, driver_apps := DriverApps
}) ->
    [application:stop(A) || A <:- lists:reverse(GafferApps)],
    teardown_driver(Driver),
    [application:stop(A) || A <:- lists:reverse(DriverApps)],
    error_logger:tty(true).

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
    {ok, Props} = application:get_env(gaffer, postgres),
    Config = maps:with(
        [host, port, database, user, password], maps:from_list(Props)
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
    Conf = ?CONF(Driver),
    ?assertEqual(ok, gaffer:create_queue(Conf)),
    ?assertMatch(#{name := create_queue}, gaffer:get_queue(?Q)),
    ?assertEqual({error, already_exists}, gaffer:create_queue(Conf)).

get_queue(Driver) ->
    Conf = ?CONF(Driver),
    ok = gaffer:create_queue(Conf),
    ?assertMatch(
        #{
            name := get_queue,
            global_max_workers := 25,
            max_workers := 5,
            priority := 0
        },
        gaffer:get_queue(?Q)
    ).

update_queue(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver, #{global_max_workers => 5})),
    ok = gaffer:update_queue(?Q, #{global_max_workers => 10}),
    Updated = gaffer:get_queue(?Q),
    ?assertEqual(10, maps:get(global_max_workers, Updated)).

delete_queue(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    ?assertEqual(ok, gaffer:delete_queue(?Q)),
    ?assertError({unknown_queue, delete_queue}, gaffer:get_queue(?Q)),
    ?assertError({unknown_queue, delete_queue}, gaffer:delete_queue(?Q)).

list_queues(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver, #{name => list_queues_1})),
    ok = gaffer:create_queue(?CONF(Driver, #{name => list_queues_2})),
    Queues = gaffer:list_queues(),
    Names = [Name || {Name, _} <:- Queues],
    ?assert(lists:member(list_queues_1, Names)),
    ?assert(lists:member(list_queues_2, Names)).

create_queue_on_discard(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver, #{name => dead_letter})),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{name => on_discard_source, on_discard => dead_letter})
    ),
    ?assertMatch(
        #{on_discard := dead_letter}, gaffer:get_queue(on_discard_source)
    ),
    ?assertError(
        {on_discard_queue_not_found, nonexistent},
        gaffer:create_queue(
            ?CONF(Driver, #{name => bad_queue, on_discard => nonexistent})
        )
    ).

update_queue_on_discard_not_found(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    ?assertError(
        {on_discard_queue_not_found, nonexistent},
        gaffer:update_queue(?Q, #{on_discard => nonexistent})
    ).

create_queue_config_mismatch(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver, #{max_workers => 3})),
    % Simulate a second node: restart gaffer to clear persistent_term
    % while keeping driver state intact
    application:stop(gaffer),
    {ok, _} = application:ensure_all_started(gaffer),
    ?assertError(
        {queue_config_mismatch, create_queue_config_mismatch, _},
        gaffer:create_queue(?CONF(Driver, #{max_workers => 99}))
    ).

%--- Insert tests -------------------------------------------------------------

insert(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    Job = gaffer:insert(?Q, #{task => 1}),
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
    ok = gaffer:create_queue(?CONF(Driver)),
    Opts = #{priority => 5, max_attempts => 10},
    Job = gaffer:insert(?Q, #{task => 1}, Opts),
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
    ok = gaffer:create_queue(?CONF(Driver)),
    At = erlang:system_time() + erlang:convert_time_unit(3600, second, native),
    Job = gaffer:insert(?Q, #{task => 1}, #{scheduled_at => At}),
    ?assertMatch(#{state := scheduled, scheduled_at := _}, Job).

insert_scheduled_microsecond(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    At = {microsecond, erlang:system_time(microsecond) + 60_000_000},
    Job = gaffer:insert(?Q, #{task => 1}, #{scheduled_at => At}),
    ?assertMatch(#{state := scheduled, scheduled_at := _}, Job).

%--- Get / list tests ---------------------------------------------------------

get_job(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}),
    Job = gaffer:get(?Q, Id),
    ?assertMatch(
        #{id := Id, queue := get_job, payload := #{task := 1}},
        atomize_keys(Job)
    ).

get_not_found(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    ?assertError({unknown_job, _}, gaffer:get(?Q, keysmith:uuid(nil, binary))).

list_jobs(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    _ = gaffer:insert(?Q, #{task => 1}),
    _ = gaffer:insert(?Q, #{task => 2}),
    Jobs = gaffer:list(#{queue => ?Q}),
    ?assertEqual(2, length(Jobs)).

list_filter_state(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    _ = gaffer:insert(?Q, #{task => 1}),
    ?assertEqual(1, length(gaffer:list(#{queue => ?Q, state => available}))),
    ?assertEqual(0, length(gaffer:list(#{queue => ?Q, state => completed}))).

%--- Delete tests -------------------------------------------------------------

delete_job(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}),
    ok = gaffer:delete(?Q, Id),
    ?assertError({unknown_job, _}, gaffer:get(?Q, Id)).

delete_not_found(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    ?assertError(
        {unknown_job, _}, gaffer:delete(?Q, keysmith:uuid(nil, binary))
    ).

%--- Cancel tests -------------------------------------------------------------

cancel(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}),
    {ok, Job} = gaffer:cancel(?Q, Id),
    ?assertMatch(#{state := cancelled, cancelled_at := _, id := Id}, Job).

cancel_not_found(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    ?assertError(
        {unknown_job, _}, gaffer:cancel(?Q, keysmith:uuid(nil, binary))
    ).

cancel_scheduled(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    At = erlang:system_time() + erlang:convert_time_unit(3600, second, native),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}, #{scheduled_at => At}),
    {ok, Cancelled} = gaffer:cancel(?Q, Id),
    ?assertMatch(#{state := cancelled, cancelled_at := _}, Cancelled).

cancel_executing(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    {ok, Cancelled} = gaffer:cancel(?Q, Id),
    ?assertMatch(#{state := cancelled, cancelled_at := _}, Cancelled).

cancel_completed_error(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    {ok, _} = gaffer_queue_runner:complete(?Q, Id),
    ?assertMatch(
        {error, {invalid_transition, {completed, cancelled}}},
        gaffer:cancel(?Q, Id)
    ).

cancel_discarded_error(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}, #{max_attempts => 1}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    E = #{attempt => 1, error => boom, at => erlang:system_time()},
    {ok, _} = gaffer_queue_runner:fail(?Q, Id, E),
    ?assertMatch(
        {error, {invalid_transition, {discarded, cancelled}}},
        gaffer:cancel(?Q, Id)
    ).

%--- Complete tests -----------------------------------------------------------

complete(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    {ok, Completed} = gaffer_queue_runner:complete(?Q, Id),
    ?assertMatch(
        #{state := completed, attempt := 1, completed_at := _}, Completed
    ).

complete_not_found(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    ?assertEqual(
        {error, not_found},
        gaffer_queue_runner:complete(?Q, keysmith:uuid(nil, binary))
    ).

%--- Fail tests ---------------------------------------------------------------

fail_retryable(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}, #{max_attempts => 3}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    E = #{attempt => 1, error => timeout, at => erlang:system_time()},
    {ok, Failed} = gaffer_queue_runner:fail(?Q, Id, E),
    ?assertMatch(
        #{
            state := failed,
            attempt := 1,
            errors := [#{attempt := 1, error := timeout}]
        },
        Failed
    ).

fail_discarded(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}, #{max_attempts => 1}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    E = #{attempt => 1, error => boom, at => erlang:system_time()},
    {ok, Discarded} = gaffer_queue_runner:fail(?Q, Id, E),
    ?assertMatch(#{state := discarded, discarded_at := _}, Discarded).

fail_not_found(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    E = #{attempt => 1, error => boom, at => erlang:system_time()},
    ?assertEqual(
        {error, not_found},
        gaffer_queue_runner:fail(?Q, keysmith:uuid(nil, binary), E)
    ).

fail_error_normalization(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}, #{max_attempts => 3}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    AtUs = erlang:system_time(microsecond),
    ErrorInfo = [#{reason => {badrpc, nodedown}}],
    E = #{attempt => 1, error => ErrorInfo, at => {microsecond, AtUs}},
    {ok, Failed} = gaffer_queue_runner:fail(?Q, Id, E),
    ?assertMatch(
        #{errors := [#{attempt := 1, error := ErrorInfo, at := AtUs}]}, Failed
    ).

%--- Schedule tests -----------------------------------------------------------

schedule(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    FutureAt =
        erlang:system_time() + erlang:convert_time_unit(60, second, native),
    {ok, Scheduled} = gaffer_queue_runner:schedule(?Q, Id, FutureAt),
    ?assertMatch(#{state := scheduled, scheduled_at := FutureAt}, Scheduled).

schedule_from_failed(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}, #{max_attempts => 3}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    E = #{attempt => 1, error => boom, at => erlang:system_time()},
    {ok, _} = gaffer_queue_runner:fail(?Q, Id, E),
    FutureAt =
        erlang:system_time() + erlang:convert_time_unit(60, second, native),
    {ok, Scheduled} = gaffer_queue_runner:schedule(?Q, Id, FutureAt),
    ?assertMatch(#{state := scheduled, scheduled_at := FutureAt}, Scheduled).

schedule_not_found(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    FutureAt =
        erlang:system_time() + erlang:convert_time_unit(60, second, native),
    ?assertEqual(
        {error, not_found},
        gaffer_queue_runner:schedule(?Q, keysmith:uuid(nil, binary), FutureAt)
    ).

%--- Validation tests ---------------------------------------------------------

insert_invalid_max_attempts(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    ?assertError(
        {invalid_job, invalid_max_attempts},
        gaffer:insert(?Q, #{task => 1}, #{max_attempts => 0})
    ).

create_queue_extra_key(Driver) ->
    ?assertError(
        {invalid_queue_conf, #{extra := [bogus]}},
        gaffer:create_queue(?CONF(Driver, #{bogus => 42}))
    ).

update_queue_extra_key(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    ?assertError(
        {invalid_queue_conf, _}, gaffer:update_queue(?Q, #{bogus => 42})
    ).

update_queue_empty(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    ?assertError({invalid_queue_conf, _}, gaffer:update_queue(?Q, #{})).

%--- Claim tests --------------------------------------------------------------

claim(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    _ = gaffer:insert(?Q, #{task => 1}),
    _ = gaffer:insert(?Q, #{task => 2}),
    ?assertMatch(
        [#{state := executing, attempted_at := _}],
        gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1})
    ).

claim_empty(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    ?assertEqual(
        [],
        gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 5})
    ).

%--- Prune tests --------------------------------------------------------------

prune(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}),
    {ok, _} = gaffer:cancel(?Q, Id),
    Count = gaffer_queue_runner:prune(?Q, #{states => [cancelled]}),
    ?assert(Count >= 1).

%--- Polling tests ------------------------------------------------------------

poll_claims_and_spawns(Driver) ->
    ok = gaffer:create_queue(
        ?CONF(Driver, #{worker => gaffer_test_worker})
    ),
    _ = gaffer:insert(?Q, #{
        ~"action" => ~"block",
        ~"test_pid" => gaffer_test_worker:encode_pid(self())
    }),
    ok = gaffer_queue_runner:poll(?Q),
    receive
        {job_started, _} -> ok
    after 5000 -> error(timeout)
    end,
    [Job] = gaffer:list(#{queue => ?Q}),
    ?assertMatch(#{state := executing}, Job).

poll_worker_completes_job(Driver) ->
    ok = gaffer:create_queue(
        ?CONF(Driver, #{worker => gaffer_test_worker})
    ),
    #{id := Id} = gaffer:insert(
        ?Q, #{
            ~"action" => ~"complete",
            ~"test_pid" => gaffer_test_worker:encode_pid(self())
        }
    ),
    ok = gaffer_queue_runner:poll(?Q),
    receive
        {job_executed, Id} -> ok
    after 5000 -> error(timeout)
    end,
    % Give the DOWN handler time to process
    timer:sleep(50),
    Job = gaffer:get(?Q, Id),
    ?assertMatch(#{state := completed}, Job).

poll_worker_crash_fails_job(Driver) ->
    ok = gaffer:create_queue(
        ?CONF(Driver, #{worker => gaffer_test_worker})
    ),
    #{id := Id} = gaffer:insert(?Q, #{~"action" => ~"crash"}),
    ok = gaffer_queue_runner:poll(?Q),
    timer:sleep(50),
    Job = gaffer:get(?Q, Id),
    ?assertMatch(#{state := failed}, Job).

poll_max_workers_limits(Driver) ->
    ok = gaffer:create_queue(
        ?CONF(Driver, #{worker => gaffer_test_worker, max_workers => 2})
    ),
    _ = gaffer:insert(?Q, #{
        ~"action" => ~"block",
        ~"test_pid" => gaffer_test_worker:encode_pid(self())
    }),
    _ = gaffer:insert(?Q, #{
        ~"action" => ~"block",
        ~"test_pid" => gaffer_test_worker:encode_pid(self())
    }),
    _ = gaffer:insert(?Q, #{
        ~"action" => ~"block",
        ~"test_pid" => gaffer_test_worker:encode_pid(self())
    }),
    ok = gaffer_queue_runner:poll(?Q),
    % Only 2 workers should have started
    receive
        {job_started, _} -> ok
    after 5000 -> error(timeout)
    end,
    receive
        {job_started, _} -> ok
    after 5000 -> error(timeout)
    end,
    % Third job should still be available
    Available = gaffer:list(#{queue => ?Q, state => available}),
    ?assertEqual(1, length(Available)).

poll_auto_executes(Driver) ->
    ok = gaffer:create_queue(
        ?CONF(Driver, #{worker => gaffer_test_worker, poll_interval => 50})
    ),
    #{id := Id} = gaffer:insert(
        ?Q, #{
            ~"action" => ~"complete",
            ~"test_pid" => gaffer_test_worker:encode_pid(self())
        }
    ),
    % No manual poll — the timer should trigger it
    receive
        {job_executed, Id} -> ok
    after 5000 -> error(timeout)
    end,
    timer:sleep(50),
    Job = gaffer:get(?Q, Id),
    ?assertMatch(#{state := completed}, Job).

%--- Defaults tests -----------------------------------------------------------

job_inherits_queue_defaults(Driver) ->
    ok = gaffer:create_queue(
        ?CONF(Driver, #{max_attempts => 7, priority => 5})
    ),
    Job = gaffer:insert(?Q, #{task => 1}),
    ?assertMatch(#{max_attempts := 7, priority := 5}, Job).

forwarded_job_inherits_target_defaults(Driver) ->
    ok = gaffer:create_queue(
        ?CONF(Driver, #{
            name => fwd_target_defaults,
            max_attempts => 10,
            priority => 3
        })
    ),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{on_discard => fwd_target_defaults, max_attempts => 1})
    ),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    E = #{attempt => 1, error => boom, at => erlang:system_time()},
    {ok, _} = gaffer_queue_runner:fail(?Q, Id, E),
    [Forwarded] = gaffer:list(#{queue => fwd_target_defaults}),
    ?assertMatch(#{max_attempts := 10, priority := 3}, Forwarded).

job_overrides_timeout_backoff_shutdown(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    Opts = #{
        timeout => 60000,
        backoff => [1000, 5000],
        shutdown_timeout => 10000
    },
    Job = gaffer:insert(?Q, #{task => 1}, Opts),
    ?assertMatch(
        #{
            timeout := 60000,
            backoff := [1000, 5000],
            shutdown_timeout := 10000
        },
        Job
    ).

backoff_is_array(Driver) ->
    ok = gaffer:create_queue(
        ?CONF(Driver, #{backoff => [1000, 2000, 4000]})
    ),
    Job = gaffer:insert(?Q, #{task => 1}),
    ?assertMatch(#{backoff := [1000, 2000, 4000]}, Job).

%--- Forwarding tests ---------------------------------------------------------

% FIXME: Merge forward_on_discard, forward_on_discard_retryable, and
% forward_on_discard_fresh into a single multi-attempt test once the
% scheduled state is removed (scheduled is just available with a future
% scheduled_at). Currently we can't chain claim-fail cycles because
% failed→scheduled jobs have no path back to available for re-claiming.

forward_on_discard(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver, #{name => fwd_dlq})),
    ok = gaffer:create_queue(?CONF(Driver, #{on_discard => fwd_dlq})),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}, #{max_attempts => 1}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    E = #{attempt => 1, error => boom, at => erlang:system_time()},
    {ok, Discarded} = gaffer_queue_runner:fail(?Q, Id, E),
    ?assertMatch(#{state := discarded}, Discarded),
    Wrapped = atomize_keys(
        maps:get(payload, hd(gaffer:list(#{queue => fwd_dlq})))
    ),
    ?assertMatch(
        #{
            payload := #{task := 1},
            attempt := 1,
            errors := [_],
            discarded_at := _
        },
        Wrapped
    ),
    ?assertEqual(forward_on_discard, to_atom(maps:get(queue, Wrapped))).

forward_on_discard_chain(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver, #{name => fwd_chain_q3})),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{
            name => fwd_chain_q2, on_discard => fwd_chain_q3, max_attempts => 1
        })
    ),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{on_discard => fwd_chain_q2})
    ),
    #{id := Id1} = gaffer:insert(?Q, #{task => original}, #{max_attempts => 1}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    E1 = #{attempt => 1, error => boom, at => erlang:system_time()},
    {ok, _} = gaffer_queue_runner:fail(?Q, Id1, E1),
    % Job should now be in Q2 with max_attempts=1 from Q2's queue config
    [Q2Job] = gaffer:list(#{queue => fwd_chain_q2}),
    #{id := Id2} = Q2Job,
    [_] = gaffer_queue_runner:claim(fwd_chain_q2, #{
        queue => fwd_chain_q2, limit => 1
    }),
    E2 = #{attempt => 1, error => crash, at => erlang:system_time()},
    {ok, _} = gaffer_queue_runner:fail(fwd_chain_q2, Id2, E2),
    % Job should now be in Q3 with nested wrapping
    Outer = atomize_keys(
        maps:get(payload, hd(gaffer:list(#{queue => fwd_chain_q3})))
    ),
    ?assertMatch(
        #{
            attempt := 1,
            errors := [_],
            discarded_at := _
        },
        Outer
    ),
    ?assertNot(is_map_key(task, Outer), "Wrapped payload has no task"),
    ?assertEqual(fwd_chain_q2, to_atom(maps:get(queue, Outer))),
    Inner = maps:get(payload, Outer),
    ?assertMatch(
        #{
            attempt := 1,
            errors := [_],
            discarded_at := _,
            payload := #{task := _}
        },
        Inner
    ),
    ?assertEqual(forward_on_discard_chain, to_atom(maps:get(queue, Inner))).

forward_on_discard_retryable(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver, #{name => fwd_retry_dlq})),
    ok = gaffer:create_queue(?CONF(Driver, #{on_discard => fwd_retry_dlq})),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}, #{max_attempts => 3}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    E = #{attempt => 1, error => boom, at => erlang:system_time()},
    {ok, Failed} = gaffer_queue_runner:fail(?Q, Id, E),
    ?assertMatch(#{state := failed}, Failed),
    ?assertEqual([], gaffer:list(#{queue => fwd_retry_dlq})).

forward_on_discard_fresh(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver, #{name => fwd_fresh_dlq})),
    ok = gaffer:create_queue(?CONF(Driver, #{on_discard => fwd_fresh_dlq})),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}, #{max_attempts => 1}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    E = #{attempt => 1, error => boom, at => erlang:system_time()},
    {ok, _} = gaffer_queue_runner:fail(?Q, Id, E),
    Forwarded = atomize_keys(hd(gaffer:list(#{queue => fwd_fresh_dlq}))),
    ?assertMatch(
        #{state := available, attempt := 0, errors := []},
        Forwarded
    ),
    ?assertEqual(#{task => 1}, mapz:deep_get([payload, payload], Forwarded)).

%--- PGO-specific tests -------------------------------------------------------

pgo_migration_idempotent(_Driver) ->
    State = gaffer_driver_pgo:start(#{pool => test_pool}),
    State2 = gaffer_driver_pgo:start(#{pool => test_pool}),
    ?assertMatch(#{pool := test_pool}, State),
    ?assertMatch(#{pool := test_pool}, State2),
    ?assert(table_exists(test_pool, ~"gaffer_queues")),
    ?assert(table_exists(test_pool, ~"gaffer_jobs")).

pgo_migration_rollback(_Driver) ->
    State = gaffer_driver_pgo:start(#{pool => test_pool}),
    ?assert(table_exists(test_pool, ~"gaffer_queues")),
    ?assert(table_exists(test_pool, ~"gaffer_jobs")),
    ok = gaffer_driver_pgo:rollback(0, State),
    ?assertNot(table_exists(test_pool, ~"gaffer_queues")),
    ?assertNot(table_exists(test_pool, ~"gaffer_jobs")),
    #{rows := Rows} = pgo:query(
        ~"SELECT version FROM gaffer_schema_version",
        [],
        #{pool => test_pool}
    ),
    ?assertEqual([{0}], Rows).

pgo_start_with_new_pool(_Driver) ->
    PoolConfig = pgo_pool_config(),
    stop_pool(my_started_pool),
    State = gaffer_driver_pgo:start(#{
        pool => my_started_pool, start => PoolConfig
    }),
    try
        ?assert(table_exists(my_started_pool, ~"gaffer_queues")),
        ?assert(table_exists(my_started_pool, ~"gaffer_jobs"))
    after
        gaffer_driver_pgo:stop(State)
    end.

pgo_idempotent_create(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver, #{max_workers => 3})),
    % Bypass persistent_term and insert the same config via driver directly
    {gaffer_driver_pgo, DS} = Driver,
    Persisted = gaffer:get_queue(?Q),
    ?assertEqual(ok, gaffer_driver_pgo:queue_insert(Persisted, DS)).

%--- Hook tests ---------------------------------------------------------------

hook_cancel(Driver) ->
    CrashHook = fun(_Phase, _Event, _Data) -> error(boom) end,
    Hook = make_hook(),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [CrashHook, Hook]})),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}),
    {ok, _} = gaffer:cancel(?Q, Id),
    ?assertEqual(
        [
            {hook, pre, [gaffer, queue, create]},
            {hook, post, [gaffer, queue, create]},
            {hook, pre, [gaffer, job, insert]},
            {hook, post, [gaffer, job, insert]},
            {hook, pre, [gaffer, job, cancel]},
            {hook, post, [gaffer, job, cancel]}
        ],
        flush_events()
    ).

hook_complete(Driver) ->
    Hook = make_hook(),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    {ok, _} = gaffer_queue_runner:complete(?Q, Id),
    ?assertEqual(
        [
            {hook, pre, [gaffer, queue, create]},
            {hook, post, [gaffer, queue, create]},
            {hook, pre, [gaffer, job, insert]},
            {hook, post, [gaffer, job, insert]},
            {hook, pre, [gaffer, job, claim]},
            {hook, post, [gaffer, job, claim]},
            {hook, pre, [gaffer, job, complete]},
            {hook, post, [gaffer, job, complete]}
        ],
        flush_events()
    ).

hook_fail(Driver) ->
    Hook = make_hook(),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    E = #{attempt => 1, error => boom, at => erlang:system_time()},
    {ok, _} = gaffer_queue_runner:fail(?Q, Id, E),
    ?assertEqual(
        [
            {hook, pre, [gaffer, queue, create]},
            {hook, post, [gaffer, queue, create]},
            {hook, pre, [gaffer, job, insert]},
            {hook, post, [gaffer, job, insert]},
            {hook, pre, [gaffer, job, claim]},
            {hook, post, [gaffer, job, claim]},
            {hook, pre, [gaffer, job, fail]},
            {hook, post, [gaffer, job, fail]}
        ],
        flush_events()
    ).

hook_schedule(Driver) ->
    Hook = make_hook(),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}),
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    FutureAt =
        erlang:system_time() + erlang:convert_time_unit(60, second, native),
    {ok, _} = gaffer_queue_runner:schedule(?Q, Id, FutureAt),
    ?assertEqual(
        [
            {hook, pre, [gaffer, queue, create]},
            {hook, post, [gaffer, queue, create]},
            {hook, pre, [gaffer, job, insert]},
            {hook, post, [gaffer, job, insert]},
            {hook, pre, [gaffer, job, claim]},
            {hook, post, [gaffer, job, claim]},
            {hook, pre, [gaffer, job, schedule]},
            {hook, post, [gaffer, job, schedule]}
        ],
        flush_events()
    ).

hook_delete(Driver) ->
    Hook = make_hook(),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}),
    ok = gaffer:delete(?Q, Id),
    ?assertEqual(
        [
            {hook, pre, [gaffer, queue, create]},
            {hook, post, [gaffer, queue, create]},
            {hook, pre, [gaffer, job, insert]},
            {hook, post, [gaffer, job, insert]},
            {hook, pre, [gaffer, job, delete]},
            {hook, post, [gaffer, job, delete]}
        ],
        flush_events()
    ).

hook_insert_pre_before_persist(Driver) ->
    Self = self(),
    Hook = fun
        (pre, [gaffer, job, insert], #{id := JobId} = Job) ->
            try gaffer:get(?Q, JobId) of
                _ -> Self ! {pre_job, found}
            catch
                _:_ -> Self ! {pre_job, missing}
            end,
            Job;
        (_Phase, _Event, Data) ->
            Data
    end,
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    _ = gaffer:insert(?Q, #{task => 1}),
    receive
        {pre_job, missing} -> ok;
        {pre_job, found} -> error(job_persisted_before_pre_hook)
    after 1000 ->
        error(timeout)
    end.

hook_delete_pre_sees_job(Driver) ->
    Self = self(),
    Hook = fun
        (pre, [gaffer, job, delete], JobId) ->
            try gaffer:get(?Q, JobId) of
                _ -> Self ! {pre_job, found}
            catch
                _:_ -> Self ! {pre_job, missing}
            end,
            JobId;
        (_Phase, _Event, Data) ->
            Data
    end,
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}),
    ok = gaffer:delete(?Q, Id),
    receive
        {pre_job, found} -> ok;
        {pre_job, missing} -> error(job_missing_in_pre_hook)
    after 1000 ->
        error(timeout)
    end.

hook_order(Driver) ->
    Hook1 = make_hook(first),
    Hook2 = make_hook(second),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook1, Hook2]})),
    _ = gaffer:insert(?Q, #{task => 1}),
    ?assertEqual(
        [
            {first, pre, [gaffer, queue, create]},
            {second, pre, [gaffer, queue, create]},
            {first, post, [gaffer, queue, create]},
            {second, post, [gaffer, queue, create]},
            {first, pre, [gaffer, job, insert]},
            {second, pre, [gaffer, job, insert]},
            {first, post, [gaffer, job, insert]},
            {second, post, [gaffer, job, insert]}
        ],
        flush_events()
    ).

hook_module(Driver) ->
    register(hook_test_proc, self()),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [gaffer_tests]})),
    _ = gaffer:insert(?Q, #{task => 1}),
    unregister(hook_test_proc),
    ?assertEqual(
        [
            {hook, pre, [gaffer, queue, create]},
            {hook, post, [gaffer, queue, create]},
            {hook, pre, [gaffer, job, insert]},
            {hook, post, [gaffer, job, insert]}
        ],
        flush_events()
    ).

hook_data_passthrough(Driver) ->
    Path = [payload, ~"hook_log"],
    Annotate = fun
        (Phase, Event, #{payload := _} = Job) ->
            Entry = [atom_to_binary(A) || A <:- [Phase | Event]],
            Prepend = fun(Log) -> [Entry | Log] end,
            mapz:deep_update_with(Path, Prepend, [Entry], Job);
        (_Phase, _Event, Data) ->
            Data
    end,
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Annotate]})),
    Inserted = gaffer:insert(?Q, #{task => 1}, #{max_attempts => 3}),
    ?assertEqual(
        [
            [~"pre", ~"gaffer", ~"job", ~"insert"],
            [~"post", ~"gaffer", ~"job", ~"insert"]
        ],
        % eqwalizer:ignore - we know mapz:deep_get returns a list
        lists:reverse(mapz:deep_get(Path, Inserted)),
        "Return job should have all hook transformation"
    ),
    #{id := Id} = Inserted,
    [_] = gaffer_queue_runner:claim(?Q, #{queue => ?Q, limit => 1}),
    E = #{attempt => 1, error => boom, at => erlang:system_time()},
    {ok, Failed} = gaffer_queue_runner:fail(?Q, Id, E),
    ?assertEqual(
        [
            [~"pre", ~"gaffer", ~"job", ~"insert"],
            [~"pre", ~"gaffer", ~"job", ~"fail"],
            [~"post", ~"gaffer", ~"job", ~"fail"]
        ],
        % eqwalizer:ignore - we know mapz:deep_get returns a list
        lists:reverse(mapz:deep_get(Path, Failed)),
        "Persisted job has only the pre-modified data"
    ),
    Stored = gaffer:get(?Q, Id),
    ?assertEqual(
        [
            [~"pre", ~"gaffer", ~"job", ~"insert"],
            [~"pre", ~"gaffer", ~"job", ~"fail"]
        ],
        % eqwalizer:ignore - we know mapz:deep_get returns a list
        lists:reverse(mapz:deep_get(Path, Stored)),
        "Stored job has pre-events only (post events are ephemeral)"
    ).

hook_global_queue(Driver) ->
    GlobalHook = make_hook(global),
    QueueHook = make_hook(queue),
    application:set_env(gaffer, hooks, [GlobalHook]),
    try
        ok = gaffer:create_queue(?CONF(Driver, #{hooks => [QueueHook]})),
        _ = gaffer:insert(?Q, #{task => 1}),
        ?assertEqual(
            [
                {global, pre, [gaffer, queue, create]},
                {queue, pre, [gaffer, queue, create]},
                {global, post, [gaffer, queue, create]},
                {queue, post, [gaffer, queue, create]},
                {global, pre, [gaffer, job, insert]},
                {queue, pre, [gaffer, job, insert]},
                {global, post, [gaffer, job, insert]},
                {queue, post, [gaffer, job, insert]}
            ],
            flush_events()
        )
    after
        application:unset_env(gaffer, hooks)
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

atomize_keys(Map) when is_map(Map) ->
    maps:fold(
        fun(K, V, Acc) -> Acc#{to_atom(K) => atomize_keys(V)} end,
        #{},
        Map
    );
atomize_keys(Other) ->
    Other.

to_atom(A) when is_atom(A) -> A;
to_atom(B) when is_binary(B) -> binary_to_existing_atom(B).

make_hook() -> make_hook(hook).
make_hook(Tag) ->
    Self = self(),
    fun(Phase, Event, Data) ->
        Self ! {Tag, Phase, Event, Data},
        Data
    end.

flush() -> flush([]).
flush(Acc) ->
    receive
        {Tag, Phase, Event, Data} when Phase =:= pre; Phase =:= post ->
            flush([{Tag, Phase, Event, Data} | Acc])
    after 0 ->
        lists:reverse(Acc)
    end.

flush_events() ->
    [{Tag, Phase, Event} || {Tag, Phase, Event, _} <:- flush()].

% gaffer_hooks behaviour callback
gaffer_hook(Phase, Event, Data) ->
    case whereis(hook_test_proc) of
        undefined -> ok;
        Pid -> Pid ! {hook, Phase, Event, Data}
    end,
    Data.
