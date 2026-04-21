-module(gaffer_tests).

-behaviour(gaffer_hooks).
-hank([{unnecessary_function_arguments, [driver_shorthand]}]).

-import(gaffer_test_helpers, [normalize/1]).

-export([gaffer_hook/2]).

-include_lib("eunit/include/eunit.hrl").
-include("gaffer_test_helpers.hrl").

-define(Q, ?FUNCTION_NAME).
-define(CONF(Driver), #{
    name => ?Q,
    driver => Driver,
    worker => gaffer_test_worker,
    poll_interval => infinity,
    prune => #{interval => infinity}
}).
-define(CONF(Driver, Extra), maps:merge(?CONF(Driver), Extra)).

%--- Fixtures -----------------------------------------------------------------

% Tests that run against all drivers (ETS + Postgres)
gaffer_test_() ->
    Parallel = [
        % --- CRUD ---
        % Queue management
        fun create_queue/1,
        fun ensure_queue/1,
        fun ensure_queue_update/1,
        fun ensure_queue_idempotent/1,
        fun ensure_queue_starts_runner/1,
        fun update_queue_propagates/1,
        fun get_queue/1,
        fun update_queue/1,
        fun delete_queue/1,
        fun delete_queue_has_jobs/1,
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
        fun complete_without_result/1,
        % Fail
        fun fail_retryable/1,
        fun fail_discarded/1,
        fun fail_error_normalization/1,
        % Retries
        fun retries_backoff/1,
        fun retries_only_one_value/1,
        % Schedule
        fun schedule/1,
        % Claim
        fun claim_global_max/1,
        fun claim_max_workers_infinity/1,
        % Priority
        fun claim_priority_order/1,
        % Prune
        fun prune_max_age/1,
        fun prune_per_state_cutoffs/1,
        fun prune_infinity/1,
        fun prune_zero/1,
        fun prune_wildcard/1,
        fun prune_wildcard_infinity_with_override/1,
        fun prune_wildcard_zero_with_override/1,
        fun pruner_process/1,
        fun pruner_manual_trigger/1,
        % Polling
        fun poll_worker_lifecycle/1,
        fun poll_worker_crash_fails_job/1,
        fun poll_worker_killed_fails_job/1,
        fun poll_worker_complete_result/1,
        fun poll_auto_executes/1,
        fun worker_fun/1,
        fun driver_shorthand/1,
        % --- Hooks ---
        fun hook_cancel/1,
        fun hook_complete/1,
        fun hook_fail/1,
        fun hook_schedule/1,
        fun hook_delete/1,
        fun hook_order/1,
        % --- Defaults ---
        fun job_inherits_queue_defaults/1,
        fun forwarded_job_inherits_target_defaults/1,
        fun job_overrides_timeout_backoff_shutdown/1,
        fun backoff_is_array/1,
        % --- Forwarding ---
        fun forward_on_discard/1,
        fun forward_on_discard_chain/1,
        fun forward_on_discard_retryable/1,
        fun forward_on_discard_fresh/1,
        % --- Info ---
        fun info_empty_queue/1,
        fun info_after_inserts/1,
        fun info_mixed_states/1,
        fun info_timestamps_per_state/1,
        fun info_workers/1,
        % --- Pause / Resume ---
        fun pause_stops_claiming/1,
        fun pause_lets_inflight_finish/1,
        fun pause_pruner_timer_suspended/1,
        fun pause_manual_prune_still_works/1,
        fun pause_resume_transitions/1,
        fun pause_reconfigure_works/1,
        fun info_reports_status/1
    ],
    Sequential = [
        % Hook tests that affect global state
        fun hook_module/1,
        fun hook_global_queue/1,
        % Orphaned queue tests (restart app)
        fun orphaned_queues/1
    ],
    [
        gaffer_test_helpers:harness(gaffer_driver_ets, Parallel, Sequential),
        gaffer_test_helpers:harness(gaffer_driver_pgo, Parallel, Sequential)
    ].

%--- Queue management tests ---------------------------------------------------

create_queue(Driver) ->
    Conf = ?CONF(Driver),
    ?assertEqual(ok, gaffer:create_queue(Conf)),
    ?assertMatch(#{name := create_queue}, gaffer:get_queue(?Q)),
    ?assertEqual({error, already_exists}, gaffer:create_queue(Conf)).

ensure_queue(Driver) ->
    Conf = ?CONF(Driver),
    ?assertEqual(ok, gaffer:ensure_queue(Conf)),
    ?assertMatch(#{name := ensure_queue}, gaffer:get_queue(?Q)).

ensure_queue_update(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver, #{max_workers => 5})),
    ok = gaffer:ensure_queue(?CONF(Driver, #{max_workers => 20})),
    Updated = gaffer:get_queue(?Q),
    ?assertEqual(20, maps:get(max_workers, Updated)).

ensure_queue_idempotent(Driver) ->
    Conf = ?CONF(Driver, #{max_workers => 3}),
    ok = gaffer:ensure_queue(Conf),
    ok = gaffer:ensure_queue(Conf),
    ?assertMatch(#{name := ensure_queue_idempotent}, gaffer:get_queue(?Q)).

ensure_queue_starts_runner(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:ensure_queue(
        ?CONF(Driver, #{poll_interval => 50, hooks => [Hook]})
    ),
    #{id := ID} = gaffer:insert(
        ?Q, #{
            ~"action" => ~"complete",
            ~"test_pid" => gaffer_test_worker:encode_pid(self())
        }
    ),
    ?assertHook([gaffer, job, complete], #{id := ID}),
    ?assertMatch(#{state := completed}, gaffer:get(?Q, ID)).

update_queue_propagates(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{global_max_workers => 2, hooks => [Hook]})
    ),
    TestPid = gaffer_test_worker:encode_pid(self()),
    % Update max_workers to 2
    ok = gaffer:update_queue(?Q, #{max_workers => 2}),
    % Insert 2 blocking jobs
    #{id := ID1} = gaffer:insert(?Q, #{
        ~"action" => ~"block", ~"test_pid" => TestPid
    }),
    #{id := ID2} = gaffer:insert(?Q, #{
        ~"action" => ~"block", ~"test_pid" => TestPid
    }),
    ok = gaffer_queue_runner:poll(?Q),
    % Both should start since max_workers is now 2
    Pid1 =
        receive
            {job_started, #{id := ID1, worker := P1}} -> P1
        after 5000 -> error(timeout)
        end,
    Pid2 =
        receive
            {job_started, #{id := ID2, worker := P2}} -> P2
        after 5000 -> error(timeout)
        end,
    Pid1 ! continue,
    Pid2 ! continue,
    ?assertHook([gaffer, job, complete], #{id := ID1}),
    ?assertHook([gaffer, job, complete], #{id := ID2}),
    ?assertEqual(2, length(gaffer:list(?Q, #{state => completed}))).

get_queue(Driver) ->
    Conf = ?CONF(Driver),
    ok = gaffer:create_queue(Conf),
    ?assertMatch(
        #{
            name := get_queue,
            driver := _,
            worker := gaffer_test_worker,
            global_max_workers := infinity,
            max_workers := 1,
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

delete_queue_has_jobs(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    _ = gaffer:insert(?Q, #{task => 1}),
    ?assertError(
        {queue_has_jobs, delete_queue_has_jobs}, gaffer:delete_queue(?Q)
    ).

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
        normalize(Job)
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
        normalize(Job)
    ).

insert_scheduled(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    At = erlang:system_time() + erlang:convert_time_unit(3600, second, native),
    Job = gaffer:insert(?Q, #{task => 1}, #{scheduled_at => At}),
    ?assertMatch(#{state := available, scheduled_at := _}, Job).

insert_scheduled_microsecond(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    At = {microsecond, erlang:system_time(microsecond) + 60_000_000},
    Job = gaffer:insert(?Q, #{task => 1}, #{scheduled_at => At}),
    ?assertMatch(#{state := available, scheduled_at := _}, Job).

%--- Get / list tests ---------------------------------------------------------

get_job(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := ID} = gaffer:insert(?Q, #{task => 1}),
    Job = gaffer:get(?Q, ID),
    ?assertMatch(
        #{id := ID, queue := get_job, payload := #{task := 1}},
        normalize(Job)
    ).

get_not_found(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    ?assertError({unknown_job, _}, gaffer:get(?Q, keysmith:uuid(nil, binary))).

list_jobs(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    _ = gaffer:insert(?Q, #{task => 1}),
    _ = gaffer:insert(?Q, #{task => 2}),
    Jobs = gaffer:list(?Q),
    ?assertEqual(2, length(Jobs)).

list_filter_state(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    _ = gaffer:insert(?Q, #{task => 1}),
    ?assertEqual(1, length(gaffer:list(?Q, #{state => available}))),
    ?assertEqual(0, length(gaffer:list(?Q, #{state => completed}))).

%--- Delete tests -------------------------------------------------------------

delete_job(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := ID} = gaffer:insert(?Q, #{task => 1}),
    ok = gaffer:delete(?Q, ID),
    ?assertError({unknown_job, _}, gaffer:get(?Q, ID)).

delete_not_found(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    ?assertError(
        {unknown_job, _}, gaffer:delete(?Q, keysmith:uuid(nil, binary))
    ).

%--- Cancel tests -------------------------------------------------------------

cancel(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := ID} = gaffer:insert(?Q, #{task => 1}),
    {ok, Job} = gaffer:cancel(?Q, ID),
    ?assertMatch(#{state := cancelled, cancelled_at := _, id := ID}, Job).

cancel_not_found(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    ?assertError(
        {unknown_job, _}, gaffer:cancel(?Q, keysmith:uuid(nil, binary))
    ).

cancel_scheduled(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    At = erlang:system_time() + erlang:convert_time_unit(3600, second, native),
    #{id := ID} = gaffer:insert(?Q, #{task => 1}, #{scheduled_at => At}),
    {ok, Cancelled} = gaffer:cancel(?Q, ID),
    ?assertMatch(#{state := cancelled, cancelled_at := _}, Cancelled).

cancel_executing(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := ID} = gaffer:insert(?Q, #{
        ~"action" => ~"block", ~"test_pid" => TestPid
    }),
    ok = gaffer_queue_runner:poll(?Q),
    WorkerPid =
        receive
            {job_started, #{id := ID, worker := P}} -> P
        after 5000 -> error(timeout)
        end,
    {ok, Cancelled} = gaffer:cancel(?Q, ID),
    ?assertMatch(#{state := cancelled, cancelled_at := _}, Cancelled),
    WorkerPid ! continue.

cancel_completed_error(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := ID} = gaffer:insert(?Q, #{
        ~"action" => ~"complete", ~"test_pid" => TestPid
    }),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, complete], #{id := ID}),
    ?assertMatch(
        {error, {invalid_transition, {completed, cancelled}}},
        gaffer:cancel(?Q, ID)
    ).

cancel_discarded_error(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    #{id := ID} = gaffer:insert(?Q, #{~"action" => ~"crash"}, #{
        max_attempts => 1
    }),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, fail], #{id := ID, state := discarded}),
    ?assertMatch(
        {error, {invalid_transition, {discarded, cancelled}}},
        gaffer:cancel(?Q, ID)
    ).

%--- Complete tests -----------------------------------------------------------

complete_without_result(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := ID} = gaffer:insert(?Q, #{
        ~"action" => ~"complete", ~"test_pid" => TestPid
    }),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, complete], #{id := ID, result := undefined}),
    ?assertMatch(
        #{state := completed, result := undefined}, gaffer:get(?Q, ID)
    ),
    % Verify result key absent before completion
    #{id := ID2} = gaffer:insert(?Q, #{task => 2}),
    ?assertNot(maps:is_key(result, gaffer:get(?Q, ID2))).

%--- Fail tests ---------------------------------------------------------------

fail_retryable(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    #{id := ID} = gaffer:insert(?Q, #{~"action" => ~"crash"}, #{
        max_attempts => 3
    }),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, fail], #{
        id := ID, state := available, attempt := 1
    }),
    Failed = gaffer:get(?Q, ID),
    ?assertMatch(
        #{state := available, attempt := 1, errors := [#{attempt := 1}]}, Failed
    ).

fail_discarded(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    #{id := ID} = gaffer:insert(?Q, #{~"action" => ~"crash"}, #{
        max_attempts => 1
    }),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, fail], #{
        id := ID, state := discarded, discarded_at := _
    }),
    ?assertMatch(#{state := discarded, discarded_at := _}, gaffer:get(?Q, ID)).

fail_error_normalization(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    #{id := ID} = gaffer:insert(?Q, #{~"action" => ~"fail"}, #{
        max_attempts => 3
    }),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, fail], #{id := ID}),
    % The worker returns {fail, [#{reason => {badrpc, nodedown}}]}
    % which exercises all normalize_error_term clauses (list, map, atom, tuple)
    ?assertMatch(
        #{
            state := available,
            errors := [
                #{
                    attempt := 1,
                    error := [#{reason := ~"{badrpc,nodedown}"}],
                    at := At
                }
            ]
        } when is_integer(At),
        normalize(gaffer:get(?Q, ID))
    ).

%--- Retries tests ------------------------------------------------------------

retries_backoff(Driver) ->
    MaxAttempts = 5,
    Backoff = [10, 20, 30],
    [B1, B2, B3] = [
        erlang:convert_time_unit(T, millisecond, native)
     || T <- Backoff
    ],
    ok = gaffer:create_queue(
        ?CONF(Driver, #{
            poll_interval => 10,
            max_attempts => MaxAttempts,
            backoff => Backoff
        })
    ),
    #{id := ID, inserted_at := InsertedAt} = gaffer:insert(
        ?Q, #{
            ~"action" => ~"crash",
            ~"test_pid" => gaffer_test_worker:encode_pid(self())
        }
    ),
    #{state := State, errors := [E5, E4, E3, E2, E1]} =
        await_errors(?Q, ID, 5),
    ?assertEqual(discarded, State),
    ?assertMatch(#{at := At} when At > InsertedAt, E1),
    ?assertMatch(#{at := At} when At > (InsertedAt + B1), E2),
    ?assertMatch(#{at := At} when At > (InsertedAt + B1 + B2), E3),
    ?assertMatch(#{at := At} when At > (InsertedAt + B1 + B2 + B3), E4),
    ?assertMatch(#{at := At} when At > (InsertedAt + B1 + B2 + B3 + B3), E5).

retries_only_one_value(Driver) ->
    MaxAttempts = 4,
    Backoff = 10,
    BackoffNative = erlang:convert_time_unit(10, native, millisecond),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{
            poll_interval => 10,
            max_attempts => MaxAttempts,
            backoff => Backoff
        })
    ),
    #{id := ID, inserted_at := InsertedAt} = gaffer:insert(
        ?Q, #{
            ~"action" => ~"crash",
            ~"test_pid" => gaffer_test_worker:encode_pid(self())
        }
    ),
    #{state := State, errors := [E4, E3, E2, E1]} =
        await_errors(?Q, ID, 4),
    ?assertEqual(discarded, State),
    ?assertMatch(#{at := At} when At > InsertedAt, E1),
    ?assertMatch(#{at := At} when At > (InsertedAt + BackoffNative), E2),
    ?assertMatch(#{at := At} when At > (InsertedAt + BackoffNative * 2), E3),
    ?assertMatch(#{at := At} when At > (InsertedAt + BackoffNative * 3), E4).

%--- Schedule tests -----------------------------------------------------------

schedule(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, schedule]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := ID} = gaffer:insert(?Q, #{
        ~"action" => ~"schedule",
        ~"test_pid" => TestPid,
        ~"offset_seconds" => 60
    }),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, schedule], #{id := ID, scheduled_at := _}),
    Scheduled = gaffer:get(?Q, ID),
    ?assertMatch(#{state := available, scheduled_at := _}, Scheduled),
    ?assert(maps:get(scheduled_at, Scheduled) > erlang:system_time()).

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

%--- Concurrency tests --------------------------------------------------------

claim_global_max(Driver) ->
    ok = gaffer:create_queue(
        ?CONF(Driver, #{global_max_workers => 2, max_workers => 10})
    ),
    TestPid = gaffer_test_worker:encode_pid(self()),
    [
        gaffer:insert(?Q, #{~"action" => ~"block", ~"test_pid" => TestPid})
     || _ <:- lists:seq(1, 3)
    ],
    ok = gaffer_queue_runner:poll(?Q),
    % Only 2 workers should start due to global_max_workers
    P1 =
        receive
            {job_started, #{worker := W1}} -> W1
        after 5000 -> error(timeout)
        end,
    P2 =
        receive
            {job_started, #{worker := W2}} -> W2
        after 5000 -> error(timeout)
        end,
    ?assertEqual(1, length(gaffer:list(?Q, #{state => available}))),
    ?assertEqual(2, length(gaffer:list(?Q, #{state => executing}))),
    P1 ! continue,
    P2 ! continue.

claim_max_workers_infinity(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver, #{max_workers => infinity})),
    TestPid = gaffer_test_worker:encode_pid(self()),
    [
        gaffer:insert(?Q, #{~"action" => ~"block", ~"test_pid" => TestPid})
     || _ <:- lists:seq(1, 5)
    ],
    ok = gaffer_queue_runner:poll(?Q),
    Pids = [
        receive
            {job_started, #{worker := P}} -> P
        after 5000 -> error(timeout)
        end
     || _ <:- lists:seq(1, 5)
    ],
    ?assertEqual(5, length(gaffer:list(?Q, #{state => executing}))),
    [P ! continue || P <:- Pids].

%--- Priority tests -----------------------------------------------------------

claim_priority_order(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{max_workers => 1, hooks => [Hook]})
    ),
    TestPid = gaffer_test_worker:encode_pid(self()),
    Payload = #{~"action" => ~"block", ~"test_pid" => TestPid},
    #{id := ID1} = gaffer:insert(?Q, Payload, #{priority => 5}),
    #{id := ID2} = gaffer:insert(?Q, Payload, #{priority => -1}),
    #{id := ID3} = gaffer:insert(?Q, Payload, #{priority => 5}),
    #{id := ID4} = gaffer:insert(?Q, Payload, #{priority => 0}),
    % Claim and verify order: 5 (first), 5 (second), 0, -1
    Claim = fun() ->
        ok = gaffer_queue_runner:poll(?Q),
        receive
            {job_started, #{id := ID, worker := W}} ->
                W ! continue,
                ?assertHook([gaffer, job, complete], #{id := ID}),
                ID
        after 5000 -> error(timeout)
        end
    end,
    ?assertEqual([ID1, ID3, ID4, ID2], [Claim(), Claim(), Claim(), Claim()]).

%--- Prune tests --------------------------------------------------------------

prune_max_age(Driver) ->
    PruneConf = #{interval => infinity, max_age => #{cancelled => 5_000}},
    ok = gaffer:create_queue(?CONF(Driver, #{prune => PruneConf})),
    #{id := ID1} = gaffer:insert(?Q, #{task => 1}),
    #{id := ID2} = gaffer:insert(?Q, #{task => 2}),
    {ok, _} = gaffer:cancel(?Q, ID1),
    {ok, _} = gaffer:cancel(?Q, ID2),
    % Jobs are fresh — 5s max_age should not prune them
    ?assertEqual([], gaffer:prune(?Q)),
    ?assertEqual(
        lists:sort([ID1, ID2]),
        lists:sort([ID || #{id := ID} <:- gaffer:list(?Q)])
    ),
    timer:sleep(10),
    % After sleeping, re-configure to 1ms max_age — now they're old enough
    PruneConf2 = #{interval => infinity, max_age => #{cancelled => 1}},
    ok = gaffer:ensure_queue(?CONF(Driver, #{prune => PruneConf2})),
    ?assertEqual(lists:sort([ID1, ID2]), lists:sort(gaffer:prune(?Q))),
    ?assertEqual([], gaffer:list(?Q)).

prune_per_state_cutoffs(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    % Create a cancelled job
    #{id := ID1} = gaffer:insert(?Q, #{task => 1}),
    {ok, _} = gaffer:cancel(?Q, ID1),
    % Create a completed job (need to execute it)
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := ID2} = gaffer:insert(?Q, #{
        ~"action" => ~"complete", ~"test_pid" => TestPid
    }),
    gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, complete], #{id := ID2}),
    % Prune only completed (age 0 = all)
    ?assertMatch([_], gaffer_queue:prune_jobs(?Q, #{completed => 0})),
    % The cancelled job should still exist
    ?assertMatch([#{id := ID1}], gaffer:list(?Q)).

pruner_process(Driver) ->
    % Short interval so the pruner fires quickly
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, delete]]),
    PruneConf = #{interval => 10, max_age => #{cancelled => 0}},
    ok = gaffer:create_queue(
        ?CONF(Driver, #{prune => PruneConf, hooks => [Hook]})
    ),
    #{id := ID1} = gaffer:insert(?Q, #{task => 1}),
    {ok, _} = gaffer:cancel(?Q, ID1),
    % Wait for the first prune cycle to delete the job
    ?assertHook([gaffer, job, delete], ID1),
    ?assertEqual([], gaffer:list(?Q)),
    % Insert and cancel another job, verify second prune cycle picks it up
    #{id := ID2} = gaffer:insert(?Q, #{task => 2}),
    {ok, _} = gaffer:cancel(?Q, ID2),
    ?assertHook([gaffer, job, delete], ID2),
    ?assertEqual([], gaffer:list(?Q)).

pruner_manual_trigger(Driver) ->
    PruneConf = #{interval => infinity, max_age => #{cancelled => 0}},
    ok = gaffer:create_queue(?CONF(Driver, #{prune => PruneConf})),
    #{id := ID} = gaffer:insert(?Q, #{task => 1}),
    {ok, _} = gaffer:cancel(?Q, ID),
    ?assertMatch([#{id := ID}], gaffer:list(?Q)),
    % Manual trigger via public API
    gaffer:prune(?Q),
    ?assertEqual([], gaffer:list(?Q)).

prune_infinity(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := ID1} = gaffer:insert(?Q, #{task => 1}),
    #{id := ID2} = gaffer:insert(?Q, #{task => 2}),
    {ok, _} = gaffer:cancel(?Q, ID1),
    {ok, _} = gaffer:cancel(?Q, ID2),
    % infinity = delete nothing (technically "jobs older than infinity")
    ?assertEqual([], gaffer_queue:prune_jobs(?Q, #{cancelled => infinity})),
    ?assertEqual(
        lists:sort([ID1, ID2]),
        lists:sort([ID || #{id := ID} <:- gaffer:list(?Q)])
    ).

prune_zero(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := ID1} = gaffer:insert(?Q, #{task => 1}),
    #{id := ID2} = gaffer:insert(?Q, #{task => 2}),
    {ok, _} = gaffer:cancel(?Q, ID1),
    {ok, _} = gaffer:cancel(?Q, ID2),
    % 0 = delete all cancelled older than 0
    ?assertEqual(
        lists:sort([ID1, ID2]),
        lists:sort(gaffer_queue:prune_jobs(?Q, #{cancelled => 0}))
    ),
    ?assertEqual([], gaffer:list(?Q)).

prune_wildcard(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    % Create a cancelled job
    #{id := ID1} = gaffer:insert(?Q, #{task => 1}),
    {ok, _} = gaffer:cancel(?Q, ID1),
    % Create a completed job
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := ID2} = gaffer:insert(?Q, #{
        ~"action" => ~"complete", ~"test_pid" => TestPid
    }),
    gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, complete], #{id := ID2}),
    % Also an available job still in the queue
    #{id := ID3} = gaffer:insert(?Q, #{task => 3}),
    % '_' => infinity deletes nothing
    ?assertEqual([], gaffer_queue:prune_jobs(?Q, #{'_' => infinity})),
    ?assertEqual(
        lists:sort([ID1, ID2, ID3]),
        lists:sort([ID || #{id := ID} <:- gaffer:list(?Q)])
    ),
    % '_' => 0 deletes everything
    ?assertEqual(
        lists:sort([ID1, ID2, ID3]),
        lists:sort(gaffer_queue:prune_jobs(?Q, #{'_' => 0}))
    ),
    ?assertEqual([], gaffer:list(?Q)).

prune_wildcard_infinity_with_override(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := ID1} = gaffer:insert(?Q, #{task => 1}),
    #{id := ID2} = gaffer:insert(?Q, #{task => 2}),
    {ok, _} = gaffer:cancel(?Q, ID1),
    {ok, _} = gaffer:cancel(?Q, ID2),
    % Also an available job
    #{id := ID3} = gaffer:insert(?Q, #{task => 3}),
    % '_' => infinity (keep everything) except cancelled => 0 (override)
    ?assertEqual(
        lists:sort([ID1, ID2]),
        lists:sort(
            gaffer_queue:prune_jobs(?Q, #{'_' => infinity, cancelled => 0})
        )
    ),
    % The available job should still exist
    ?assertMatch([#{id := ID3}], gaffer:list(?Q)).

prune_wildcard_zero_with_override(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := ID1} = gaffer:insert(?Q, #{task => 1}),
    #{id := ID2} = gaffer:insert(?Q, #{task => 2}),
    {ok, _} = gaffer:cancel(?Q, ID1),
    {ok, _} = gaffer:cancel(?Q, ID2),
    % Also an available job
    #{id := _ID3} = gaffer:insert(?Q, #{task => 3}),
    % '_' => 0 (drop everything) except cancelled => infinity (override)
    ?assertMatch(
        [_], gaffer_queue:prune_jobs(?Q, #{'_' => 0, cancelled => infinity})
    ),
    % The cancelled jobs should still exist
    ?assertEqual(
        lists:sort([ID1, ID2]),
        lists:sort([ID || #{id := ID} <:- gaffer:list(?Q)])
    ).

%--- Polling tests ------------------------------------------------------------

poll_worker_lifecycle(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    Conf = ?CONF(Driver, #{
        max_workers => 2, global_max_workers => 2, hooks => [Hook]
    }),
    ok = gaffer:create_queue(Conf),
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := ID1} = gaffer:insert(?Q, #{
        ~"action" => ~"block", ~"test_pid" => TestPid
    }),
    #{id := ID2} = gaffer:insert(?Q, #{
        ~"action" => ~"block", ~"test_pid" => TestPid
    }),
    _ = gaffer:insert(?Q, #{~"action" => ~"block", ~"test_pid" => TestPid}),
    ok = gaffer_queue_runner:poll(?Q),
    % max_workers caps concurrent workers at 2
    Pid1 =
        receive
            {job_started, #{id := ID1, worker := P1}} -> P1
        after 5000 -> error(timeout)
        end,
    Pid2 =
        receive
            {job_started, #{id := ID2, worker := P2}} -> P2
        after 5000 -> error(timeout)
        end,
    % Third job stays available while workers are busy
    ?assertEqual(1, length(gaffer:list(?Q, #{state => available}))),
    % Polling claims jobs and moves them to executing
    ?assertEqual(2, length(gaffer:list(?Q, #{state => executing}))),
    % Completing workers transitions jobs to completed
    Pid1 ! continue,
    Pid2 ! continue,
    ?assertHook([gaffer, job, complete], #{id := ID1}),
    ?assertHook([gaffer, job, complete], #{id := ID2}),
    ?assertEqual(2, length(gaffer:list(?Q, #{state => completed}))).

poll_worker_crash_fails_job(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    #{id := ID} = gaffer:insert(?Q, #{~"action" => ~"crash"}),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, fail], #{id := ID}),
    ?assertMatch(#{state := available}, gaffer:get(?Q, ID)).

poll_worker_killed_fails_job(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := ID} = gaffer:insert(?Q, #{
        ~"action" => ~"block", ~"test_pid" => TestPid
    }),
    ok = gaffer_queue_runner:poll(?Q),
    WorkerPid =
        receive
            {job_started, #{id := ID, worker := P}} -> P
        after 5000 -> error(timeout)
        end,
    exit(WorkerPid, kill),
    ?assertHook([gaffer, job, fail], #{
        id := ID, state := available, attempt := 1
    }),
    ?assertMatch(
        #{
            state := available,
            attempt := 1,
            errors := [#{attempt := 1, error := killed, at := _}]
        },
        normalize(gaffer:get(?Q, ID))
    ).

poll_worker_complete_result(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    ExpectedResult = #{~"computed" => ~"value"},
    #{id := ID} = gaffer:insert(?Q, #{
        ~"action" => ~"complete_result",
        ~"test_pid" => gaffer_test_worker:encode_pid(self()),
        ~"result" => ExpectedResult
    }),
    ok = gaffer_queue_runner:poll(?Q),
    receive
        {job_executed, _} -> ok
    after 5000 -> error(timeout)
    end,
    ?assertHook([gaffer, job, complete], #{id := ID, result := ExpectedResult}),
    ?assertMatch(
        #{state := completed, result := ExpectedResult}, gaffer:get(?Q, ID)
    ).

poll_auto_executes(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{poll_interval => 50, hooks => [Hook]})
    ),
    #{id := ID} = gaffer:insert(
        ?Q, #{
            ~"action" => ~"complete",
            ~"test_pid" => gaffer_test_worker:encode_pid(self())
        }
    ),
    % No manual poll — the timer should trigger it
    ?assertHook([gaffer, job, complete], #{id := ID}),
    ?assertMatch(#{state := completed}, gaffer:get(?Q, ID)).

worker_fun(Driver) ->
    TestPid = self(),
    Worker = fun(#{payload := Payload}) ->
        TestPid ! {worker_fun_executed, Payload},
        complete
    end,
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{worker => Worker, hooks => [Hook]})
    ),
    #{id := ID} = gaffer:insert(?Q, #{~"hello" => ~"world"}),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, complete], #{id := ID}),
    ?assertMatch(#{state := completed}, gaffer:get(?Q, ID)),
    receive
        {worker_fun_executed, Payload} ->
            ?assertEqual(#{~"hello" => ~"world"}, Payload)
    after 1000 -> error(timeout)
    end.

driver_shorthand(_) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(#{
        name => ?Q,
        driver => ets,
        worker => gaffer_test_worker,
        poll_interval => infinity,
        hooks => [Hook]
    }),
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := ID} = gaffer:insert(?Q, #{
        ~"action" => ~"complete", ~"test_pid" => TestPid
    }),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, complete], #{id := ID}),
    ?assertMatch(#{state := completed}, gaffer:get(?Q, ID)).

%--- Defaults tests -----------------------------------------------------------

job_inherits_queue_defaults(Driver) ->
    ok = gaffer:create_queue(
        ?CONF(Driver, #{max_attempts => 7, priority => 5})
    ),
    Job = gaffer:insert(?Q, #{task => 1}),
    ?assertMatch(#{max_attempts := 7, priority := 5}, Job).

forwarded_job_inherits_target_defaults(Driver) ->
    DlqHook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, insert]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{
            name => fwd_target_defaults,
            max_attempts => 10,
            priority => 3,
            hooks => [DlqHook]
        })
    ),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{on_discard => fwd_target_defaults, max_attempts => 1})
    ),
    _ = gaffer:insert(?Q, #{~"action" => ~"crash"}),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, insert], #{queue := fwd_target_defaults}),
    [Forwarded] = gaffer:list(fwd_target_defaults),
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
    ok = gaffer:create_queue(?CONF(Driver, #{backoff => [1000, 2000, 4000]})),
    Job = gaffer:insert(?Q, #{task => 1}),
    ?assertMatch(#{backoff := [1000, 2000, 4000]}, Job).

%--- Forwarding tests ---------------------------------------------------------

forward_on_discard(Driver) ->
    DlqHook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, insert]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{name => fwd_dlq, hooks => [DlqHook]})
    ),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{on_discard => fwd_dlq, max_attempts => 1})
    ),
    #{id := ID} = gaffer:insert(?Q, #{~"action" => ~"crash"}),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, insert], #{queue := fwd_dlq}),
    ?assertMatch(#{state := discarded}, gaffer:get(?Q, ID)),
    Wrapped = normalize(maps:get(payload, hd(gaffer:list(fwd_dlq)))),
    ?assertMatch(
        #{
            payload := #{action := crash},
            attempt := 1,
            errors := [_],
            discarded_at := _
        },
        Wrapped
    ),
    ?assertEqual(forward_on_discard, maps:get(queue, Wrapped)).

forward_on_discard_chain(Driver) ->
    Q3Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, insert]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{name => fwd_chain_q3, hooks => [Q3Hook]})
    ),
    Q2Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, insert]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{
            name => fwd_chain_q2,
            on_discard => fwd_chain_q3,
            max_attempts => 1,
            hooks => [Q2Hook]
        })
    ),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{on_discard => fwd_chain_q2, max_attempts => 1})
    ),
    _ = gaffer:insert(?Q, #{~"action" => ~"crash"}),
    ok = gaffer_queue_runner:poll(?Q),
    % Wait for Q2 insert hook (fired by Q1's maybe_forward)
    ?assertHook([gaffer, job, insert], #{queue := fwd_chain_q2}),
    % Q2 worker gets wrapped payload → no matching action → discard → forward to Q3
    ok = gaffer_queue_runner:poll(fwd_chain_q2),
    % Wait for Q3 insert hook (fired by Q2's maybe_forward)
    ?assertHook([gaffer, job, insert], #{queue := fwd_chain_q3}),
    % Job should now be in Q3 with nested wrapping
    Outer = normalize(maps:get(payload, hd(gaffer:list(fwd_chain_q3)))),
    ?assertMatch(#{attempt := 1, errors := [_], discarded_at := _}, Outer),
    ?assertNot(is_map_key(action, Outer), "Wrapped payload has no action"),
    ?assertEqual(fwd_chain_q2, maps:get(queue, Outer)),
    Inner = maps:get(payload, Outer),
    ?assertMatch(
        #{
            attempt := 1,
            errors := [_],
            discarded_at := _,
            payload := #{action := _}
        },
        Inner
    ),
    ?assertEqual(forward_on_discard_chain, maps:get(queue, Inner)).

forward_on_discard_retryable(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver, #{name => fwd_retry_dlq})),
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{on_discard => fwd_retry_dlq, hooks => [Hook]})
    ),
    #{id := ID} = gaffer:insert(?Q, #{~"action" => ~"crash"}, #{
        max_attempts => 3
    }),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, fail], #{id := ID, state := available}),
    ?assertMatch(#{state := available}, gaffer:get(?Q, ID)),
    ?assertEqual([], gaffer:list(fwd_retry_dlq)).

forward_on_discard_fresh(Driver) ->
    DlqHook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, insert]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{name => fwd_fresh_dlq, hooks => [DlqHook]})
    ),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{on_discard => fwd_fresh_dlq, max_attempts => 1})
    ),
    _ = gaffer:insert(?Q, #{~"action" => ~"crash"}, #{max_attempts => 1}),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, insert], #{queue := fwd_fresh_dlq}),
    Forwarded = normalize(hd(gaffer:list(fwd_fresh_dlq))),
    ?assertMatch(
        #{state := available, attempt := 0, errors := []},
        Forwarded
    ),
    ?assertEqual(
        #{action => crash},
        mapz:deep_get([payload, payload], Forwarded)
    ).

%--- Info tests ---------------------------------------------------------------

info_empty_queue(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    Info = gaffer:info(?Q),
    #{jobs := Jobs, workers := Workers} = Info,
    ?assertMatch(
        #{
            available := #{count := 0},
            executing := #{count := 0},
            completed := #{count := 0},
            cancelled := #{count := 0},
            discarded := #{count := 0}
        },
        Jobs
    ),
    % No oldest/newest when count is 0
    ?assertNot(maps:is_key(oldest, maps:get(available, Jobs))),
    ?assertNot(maps:is_key(newest, maps:get(available, Jobs))),
    ?assertMatch(
        #{active := 0, max := #{local := 1, global := infinity}},
        Workers
    ).

info_after_inserts(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    _ = gaffer:insert(?Q, #{task => 1}),
    _ = gaffer:insert(?Q, #{task => 2}),
    _ = gaffer:insert(?Q, #{task => 3}),
    #{jobs := #{available := Available}} = gaffer:info(?Q),
    ?assertMatch(#{count := 3, oldest := _, newest := _}, Available).

info_mixed_states(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := ID1} = gaffer:insert(?Q, #{task => 1}),
    _ = gaffer:insert(?Q, #{task => 2}),
    _ = gaffer:insert(?Q, #{task => 3}),
    {ok, _} = gaffer:cancel(?Q, ID1),
    #{jobs := Jobs} = gaffer:info(?Q),
    ?assertMatch(#{count := 2}, maps:get(available, Jobs)),
    ?assertMatch(#{count := 1}, maps:get(cancelled, Jobs)).

info_timestamps_per_state(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := ID} = gaffer:insert(?Q, #{
        ~"action" => ~"complete", ~"test_pid" => TestPid
    }),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, complete], #{id := ID}),
    #{jobs := Jobs} = gaffer:info(?Q),
    % completed uses completed_at
    #{completed := #{count := 1, oldest := Oldest, newest := Newest}} = Jobs,
    ?assert(is_integer(Oldest)),
    ?assert(is_integer(Newest)),
    ?assertEqual(Oldest, Newest).

info_workers(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{max_workers => 2, hooks => [Hook]})
    ),
    #{id := ID} = gaffer:insert(?Q, #{
        ~"action" => ~"block",
        ~"test_pid" => gaffer_test_worker:encode_pid(self())
    }),
    ok = gaffer_queue_runner:poll(?Q),
    WorkerPid =
        receive
            {job_started, #{id := ID, worker := Pid}} -> Pid
        after 5000 -> error(timeout)
        end,
    #{workers := #{active := Active1}} = gaffer:info(?Q),
    ?assertEqual(1, Active1),
    % Unblock the worker and verify active drops to 0
    WorkerPid ! continue,
    ?assertHook([gaffer, job, complete], #{id := ID}),
    #{workers := #{active := 0}} = gaffer:info(?Q).

%--- Pause / Resume tests -----------------------------------------------------

pause_stops_claiming(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, claim]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{poll_interval => 50, hooks => [Hook]})
    ),
    ok = gaffer:pause(?Q),
    % Drain any claim hook that may have fired before the pause took effect
    drain_gaffer_hooks([gaffer, job, claim], 100),
    _ = gaffer:insert(?Q, #{task => 1}),
    % 4x poll_interval without a claim hook proves state_timeout is suppressed
    receive
        {gaffer_hook, [gaffer, job, claim], _} -> error(claim_while_paused)
    after 200 -> ok
    end,
    ?assertEqual(1, length(gaffer:list(?Q, #{state => available}))),
    % Manual poll still works while paused
    ok = gaffer_queue_runner:poll(?Q).

pause_lets_inflight_finish(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := ID} = gaffer:insert(?Q, #{
        ~"action" => ~"block", ~"test_pid" => TestPid
    }),
    ok = gaffer_queue_runner:poll(?Q),
    WorkerPid =
        receive
            {job_started, #{id := ID, worker := P}} -> P
        after 5000 -> error(timeout)
        end,
    ok = gaffer:pause(?Q),
    WorkerPid ! continue,
    ?assertHook([gaffer, job, complete], #{id := ID}),
    ?assertMatch(#{state := completed}, gaffer:get(?Q, ID)).

pause_pruner_timer_suspended(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, delete]]),
    PruneConf = #{interval => 50, max_age => #{cancelled => 0}},
    ok = gaffer:create_queue(
        ?CONF(Driver, #{prune => PruneConf, hooks => [Hook]})
    ),
    ok = gaffer:pause(?Q),
    drain_gaffer_hooks([gaffer, job, delete], 100),
    #{id := ID} = gaffer:insert(?Q, #{task => 1}),
    {ok, _} = gaffer:cancel(?Q, ID),
    % 4x prune interval without a delete hook proves pruner timer is suppressed
    receive
        {gaffer_hook, [gaffer, job, delete], _} -> error(prune_while_paused)
    after 200 -> ok
    end,
    ?assertEqual([ID], [I || #{id := I} <:- gaffer:list(?Q)]).

pause_manual_prune_still_works(Driver) ->
    PruneConf = #{interval => infinity, max_age => #{cancelled => 0}},
    ok = gaffer:create_queue(?CONF(Driver, #{prune => PruneConf})),
    #{id := ID} = gaffer:insert(?Q, #{task => 1}),
    {ok, _} = gaffer:cancel(?Q, ID),
    ok = gaffer:pause(?Q),
    ?assertEqual([ID], gaffer:prune(?Q)),
    ?assertEqual([], gaffer:list(?Q)).

pause_resume_transitions(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(
        self(), [[gaffer, queue, pause], [gaffer, queue, resume]]
    ),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    ?assertEqual(ok, gaffer:pause(?Q)),
    ?assertHook([gaffer, queue, pause], pause_resume_transitions),
    ?assertEqual({error, already_paused}, gaffer:pause(?Q)),
    assert_no_gaffer_hook(100),
    ?assertEqual(ok, gaffer:resume(?Q)),
    ?assertHook([gaffer, queue, resume], pause_resume_transitions),
    ?assertEqual({error, already_active}, gaffer:resume(?Q)),
    assert_no_gaffer_hook(100),
    ?assertEqual(ok, gaffer:pause(?Q)),
    ?assertHook([gaffer, queue, pause], pause_resume_transitions).

pause_reconfigure_works(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    ok = gaffer:pause(?Q),
    % ensure_queue on an existing queue reconfigures runner and pruner
    ok = gaffer:ensure_queue(?CONF(Driver, #{max_workers => 3})),
    ?assertMatch(#{status := paused}, gaffer:info(?Q)),
    ?assertEqual(3, maps:get(max_workers, gaffer:get_queue(?Q))).

info_reports_status(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    ?assertMatch(#{status := active}, gaffer:info(?Q)),
    ok = gaffer:pause(?Q),
    ?assertMatch(#{status := paused}, gaffer:info(?Q)),
    ok = gaffer:resume(?Q),
    ?assertMatch(#{status := active}, gaffer:info(?Q)).

%--- Hook tests ---------------------------------------------------------------

hook_cancel(Driver) ->
    CrashHook = fun(_Event, _Data) -> error(boom) end,
    Hook = make_hook(),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [CrashHook, Hook]})),
    #{id := ID} = gaffer:insert(?Q, #{task => 1}),
    {ok, _} = gaffer:cancel(?Q, ID),
    ?assertEqual(
        [
            {hook, [gaffer, queue, create]},
            {hook, [gaffer, job, insert]},
            {hook, [gaffer, job, cancel]}
        ],
        flush_events()
    ).

hook_complete(Driver) ->
    Notify = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    Hook = make_hook(),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook, Notify]})),
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := ID} = gaffer:insert(?Q, #{
        ~"action" => ~"complete", ~"test_pid" => TestPid
    }),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, complete], #{id := ID}),
    ?assertEqual(
        [
            {hook, [gaffer, queue, create]},
            {hook, [gaffer, job, insert]},
            {hook, [gaffer, job, claim]},
            {hook, [gaffer, job, complete]}
        ],
        flush_events()
    ).

hook_fail(Driver) ->
    Notify = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    Hook = make_hook(),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook, Notify]})),
    #{id := ID} = gaffer:insert(?Q, #{~"action" => ~"crash"}),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, fail], #{id := ID}),
    ?assertEqual(
        [
            {hook, [gaffer, queue, create]},
            {hook, [gaffer, job, insert]},
            {hook, [gaffer, job, claim]},
            {hook, [gaffer, job, fail]}
        ],
        flush_events()
    ).

hook_schedule(Driver) ->
    Notify = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, schedule]]),
    Hook = make_hook(),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook, Notify]})),
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := ID} = gaffer:insert(?Q, #{
        ~"action" => ~"schedule",
        ~"test_pid" => TestPid,
        ~"offset_seconds" => 60
    }),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, schedule], #{id := ID}),
    ?assertEqual(
        [
            {hook, [gaffer, queue, create]},
            {hook, [gaffer, job, insert]},
            {hook, [gaffer, job, claim]},
            {hook, [gaffer, job, schedule]}
        ],
        flush_events()
    ).

hook_delete(Driver) ->
    Hook = make_hook(),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    #{id := ID} = gaffer:insert(?Q, #{task => 1}),
    ok = gaffer:delete(?Q, ID),
    ?assertEqual(
        [
            {hook, [gaffer, queue, create]},
            {hook, [gaffer, job, insert]},
            {hook, [gaffer, job, delete]}
        ],
        flush_events()
    ).

hook_order(Driver) ->
    Hook1 = make_hook(first),
    Hook2 = make_hook(second),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook1, Hook2]})),
    _ = gaffer:insert(?Q, #{task => 1}),
    ?assertEqual(
        [
            {first, [gaffer, queue, create]},
            {second, [gaffer, queue, create]},
            {first, [gaffer, job, insert]},
            {second, [gaffer, job, insert]}
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
            {hook, [gaffer, queue, create]},
            {hook, [gaffer, job, insert]}
        ],
        flush_events()
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
                {global, [gaffer, queue, create]},
                {queue, [gaffer, queue, create]},
                {global, [gaffer, job, insert]},
                {queue, [gaffer, job, insert]}
            ],
            flush_events()
        )
    after
        application:unset_env(gaffer, hooks)
    end.

%--- Orphaned queue tests (sequential) ----------------------------------------

orphaned_queues(Driver) ->
    % Create two queues with jobs
    QueueA = orphaned_queues_a,
    QueueB = orphaned_queues_b,
    ConfA = #{
        name => QueueA,
        driver => Driver,
        worker => gaffer_test_worker,
        poll_interval => infinity
    },
    ConfB = #{
        name => QueueB,
        driver => Driver,
        worker => gaffer_test_worker,
        poll_interval => infinity
    },
    ok = gaffer:create_queue(ConfA),
    ok = gaffer:create_queue(ConfB),
    _ = gaffer:insert(QueueA, #{task => 1}),
    _ = gaffer:insert(QueueB, #{task => 2}),

    % Simulate restart: stop gaffer, re-start, only ensure queue_a
    ok = application:stop(gaffer),
    {ok, _} = application:ensure_all_started(gaffer),
    ok = gaffer:ensure_queue(ConfA),

    % Verify orphaned
    Orphaned = gaffer:orphaned_queues(Driver),
    ?assert(lists:member(QueueB, Orphaned)),
    ?assertNot(lists:member(QueueA, Orphaned)),

    % Delete the orphaned queue
    ok = gaffer:delete_queue(QueueB, Driver),

    % Verify gone
    ?assertNot(lists:member(QueueB, gaffer:orphaned_queues(Driver))).

%--- Helpers ------------------------------------------------------------------

make_hook() -> make_hook(hook).
make_hook(Tag) ->
    Self = self(),
    fun(Event, Data) -> Self ! {Tag, Event, Data} end.

flush() -> flush([]).
flush(Acc) ->
    receive
        {Tag, Event, Data} when is_list(Event) ->
            flush([{Tag, Event, Data} | Acc])
    after 0 ->
        lists:reverse(Acc)
    end.

flush_events() ->
    [{Tag, Event} || {Tag, Event, _} <:- flush()].

drain_gaffer_hooks(Event, Timeout) ->
    receive
        {gaffer_hook, Event, _} -> drain_gaffer_hooks(Event, Timeout)
    after Timeout -> ok
    end.

assert_no_gaffer_hook(Timeout) ->
    receive
        {gaffer_hook, _, _} = Msg -> error({unexpected_hook, Msg})
    after Timeout -> ok
    end.

await_errors(Queue, ID, ErrorCount) ->
    gaffer_test_helpers:wait_for(
        fun(_) ->
            case gaffer:get(Queue, ID) of
                #{errors := Errors} = Job when length(Errors) >= ErrorCount ->
                    {result, Job};
                _ ->
                    {wait, undefined}
            end
        end,
        undefined
    ).

% gaffer_hooks behaviour callback
gaffer_hook(Event, Data) ->
    case whereis(hook_test_proc) of
        undefined -> ok;
        Pid -> Pid ! {hook, Event, Data}
    end.
