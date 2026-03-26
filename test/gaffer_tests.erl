-module(gaffer_tests).

-behaviour(gaffer_hooks).
-hank([{unnecessary_function_arguments, [driver_shorthand]}]).

-export([gaffer_hook/3]).

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
        % Schedule
        fun schedule/1,
        % Claim
        fun claim_global_max/1,
        fun claim_max_workers_infinity/1,
        % Prune
        fun prune/1,
        % Polling
        fun poll_worker_lifecycle/1,
        fun poll_worker_crash_fails_job/1,
        fun poll_worker_complete_result/1,
        fun poll_auto_executes/1,
        fun worker_fun/1,
        fun driver_shorthand/1,
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
        fun forward_on_discard_fresh/1,
        % --- Info ---
        fun info_empty_queue/1,
        fun info_after_inserts/1,
        fun info_mixed_states/1,
        fun info_timestamps_per_state/1,
        fun info_workers/1
    ],
    Sequential = [
        % Hook tests that affect global state
        fun hook_module/1,
        fun hook_global_queue/1
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
    #{id := Id} = gaffer:insert(
        ?Q, #{
            ~"action" => ~"complete",
            ~"test_pid" => gaffer_test_worker:encode_pid(self())
        }
    ),
    gaffer_test_helpers:await_hook(),
    ?assertMatch(#{state := completed}, gaffer:get(?Q, Id)).

update_queue_propagates(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{global_max_workers => 2, hooks => [Hook]})
    ),
    TestPid = gaffer_test_worker:encode_pid(self()),
    % Update max_workers to 2
    ok = gaffer:update_queue(?Q, #{max_workers => 2}),
    % Insert 2 blocking jobs
    #{id := Id1} = gaffer:insert(?Q, #{
        ~"action" => ~"block", ~"test_pid" => TestPid
    }),
    #{id := Id2} = gaffer:insert(?Q, #{
        ~"action" => ~"block", ~"test_pid" => TestPid
    }),
    ok = gaffer_queue_runner:poll(?Q),
    % Both should start since max_workers is now 2
    Pid1 =
        receive
            {job_started, #{id := Id1, worker := P1}} -> P1
        after 5000 -> error(timeout)
        end,
    Pid2 =
        receive
            {job_started, #{id := Id2, worker := P2}} -> P2
        after 5000 -> error(timeout)
        end,
    Pid1 ! continue,
    Pid2 ! continue,
    gaffer_test_helpers:await_hooks(2),
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
    ?assertMatch(#{state := available, scheduled_at := _}, Job).

insert_scheduled_microsecond(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    At = {microsecond, erlang:system_time(microsecond) + 60_000_000},
    Job = gaffer:insert(?Q, #{task => 1}, #{scheduled_at => At}),
    ?assertMatch(#{state := available, scheduled_at := _}, Job).

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
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := Id} = gaffer:insert(?Q, #{
        ~"action" => ~"block", ~"test_pid" => TestPid
    }),
    ok = gaffer_queue_runner:poll(?Q),
    WorkerPid =
        receive
            {job_started, #{id := Id, worker := P}} -> P
        after 5000 -> error(timeout)
        end,
    {ok, Cancelled} = gaffer:cancel(?Q, Id),
    ?assertMatch(#{state := cancelled, cancelled_at := _}, Cancelled),
    WorkerPid ! continue.

cancel_completed_error(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := Id} = gaffer:insert(?Q, #{
        ~"action" => ~"complete", ~"test_pid" => TestPid
    }),
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
    ?assertMatch(
        {error, {invalid_transition, {completed, cancelled}}},
        gaffer:cancel(?Q, Id)
    ).

cancel_discarded_error(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    #{id := Id} = gaffer:insert(?Q, #{~"action" => ~"crash"}, #{
        max_attempts => 1
    }),
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
    ?assertMatch(
        {error, {invalid_transition, {discarded, cancelled}}},
        gaffer:cancel(?Q, Id)
    ).

%--- Complete tests -----------------------------------------------------------

complete_without_result(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := Id} = gaffer:insert(?Q, #{
        ~"action" => ~"complete", ~"test_pid" => TestPid
    }),
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
    ?assertMatch(
        #{state := completed, result := undefined}, gaffer:get(?Q, Id)
    ),
    % Verify result key absent before completion
    #{id := Id2} = gaffer:insert(?Q, #{task => 2}),
    ?assertNot(maps:is_key(result, gaffer:get(?Q, Id2))).

%--- Fail tests ---------------------------------------------------------------

fail_retryable(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    #{id := Id} = gaffer:insert(?Q, #{~"action" => ~"crash"}, #{
        max_attempts => 3
    }),
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
    Failed = gaffer:get(?Q, Id),
    ?assertMatch(
        #{state := failed, attempt := 1, errors := [#{attempt := 1}]}, Failed
    ).

fail_discarded(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    #{id := Id} = gaffer:insert(?Q, #{~"action" => ~"crash"}, #{
        max_attempts => 1
    }),
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
    ?assertMatch(#{state := discarded, discarded_at := _}, gaffer:get(?Q, Id)).

fail_error_normalization(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    #{id := Id} = gaffer:insert(?Q, #{~"action" => ~"fail"}, #{
        max_attempts => 3
    }),
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
    % The worker returns {fail, [#{reason => {badrpc, nodedown}}]}
    % which exercises all normalize_error_term clauses (list, map, atom, tuple)
    ?assertMatch(
        #{
            errors := [
                #{
                    attempt := 1,
                    error := [#{reason := ~"{badrpc,nodedown}"}],
                    at := At
                }
            ]
        } when is_integer(At),
        atomize_keys(gaffer:get(?Q, Id))
    ).

%--- Schedule tests -----------------------------------------------------------

schedule(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, schedule]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := Id} = gaffer:insert(?Q, #{
        ~"action" => ~"schedule",
        ~"test_pid" => TestPid,
        ~"offset_seconds" => 60
    }),
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
    Scheduled = gaffer:get(?Q, Id),
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

%--- Prune tests --------------------------------------------------------------

prune(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    #{id := Id} = gaffer:insert(?Q, #{task => 1}),
    {ok, _} = gaffer:cancel(?Q, Id),
    Count = gaffer_queue_runner:prune(?Q, #{states => [cancelled]}),
    ?assert(Count >= 1).

%--- Polling tests ------------------------------------------------------------

poll_worker_lifecycle(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    Conf = ?CONF(Driver, #{
        max_workers => 2, global_max_workers => 2, hooks => [Hook]
    }),
    ok = gaffer:create_queue(Conf),
    TestPid = gaffer_test_worker:encode_pid(self()),
    #{id := Id1} = gaffer:insert(?Q, #{
        ~"action" => ~"block", ~"test_pid" => TestPid
    }),
    #{id := Id2} = gaffer:insert(?Q, #{
        ~"action" => ~"block", ~"test_pid" => TestPid
    }),
    _ = gaffer:insert(?Q, #{~"action" => ~"block", ~"test_pid" => TestPid}),
    ok = gaffer_queue_runner:poll(?Q),
    % max_workers caps concurrent workers at 2
    Pid1 =
        receive
            {job_started, #{id := Id1, worker := P1}} -> P1
        after 5000 -> error(timeout)
        end,
    Pid2 =
        receive
            {job_started, #{id := Id2, worker := P2}} -> P2
        after 5000 -> error(timeout)
        end,
    % Third job stays available while workers are busy
    ?assertEqual(1, length(gaffer:list(?Q, #{state => available}))),
    % Polling claims jobs and moves them to executing
    ?assertEqual(2, length(gaffer:list(?Q, #{state => executing}))),
    % Completing workers transitions jobs to completed
    Pid1 ! continue,
    Pid2 ! continue,
    gaffer_test_helpers:await_hooks(2),
    ?assertEqual(2, length(gaffer:list(?Q, #{state => completed}))).

poll_worker_crash_fails_job(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    #{id := Id} = gaffer:insert(?Q, #{~"action" => ~"crash"}),
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
    ?assertMatch(#{state := failed}, gaffer:get(?Q, Id)).

poll_worker_complete_result(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    ExpectedResult = #{~"computed" => ~"value"},
    #{id := Id} = gaffer:insert(?Q, #{
        ~"action" => ~"complete_result",
        ~"test_pid" => gaffer_test_worker:encode_pid(self()),
        ~"result" => ExpectedResult
    }),
    ok = gaffer_queue_runner:poll(?Q),
    receive
        {job_executed, _} -> ok
    after 5000 -> error(timeout)
    end,
    gaffer_test_helpers:await_hook(),
    ?assertMatch(
        #{state := completed, result := ExpectedResult}, gaffer:get(?Q, Id)
    ).

poll_auto_executes(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{poll_interval => 50, hooks => [Hook]})
    ),
    #{id := Id} = gaffer:insert(
        ?Q, #{
            ~"action" => ~"complete",
            ~"test_pid" => gaffer_test_worker:encode_pid(self())
        }
    ),
    % No manual poll — the timer should trigger it
    gaffer_test_helpers:await_hook(),
    ?assertMatch(#{state := completed}, gaffer:get(?Q, Id)).

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
    #{id := Id} = gaffer:insert(?Q, #{~"hello" => ~"world"}),
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
    ?assertMatch(#{state := completed}, gaffer:get(?Q, Id)),
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
    #{id := Id} = gaffer:insert(?Q, #{
        ~"action" => ~"complete", ~"test_pid" => TestPid
    }),
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
    ?assertMatch(#{state := completed}, gaffer:get(?Q, Id)).

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
    gaffer_test_helpers:await_hook(),
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
    #{id := Id} = gaffer:insert(?Q, #{~"action" => ~"crash"}),
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
    ?assertMatch(#{state := discarded}, gaffer:get(?Q, Id)),
    Wrapped = atomize_keys(maps:get(payload, hd(gaffer:list(fwd_dlq)))),
    ?assertMatch(
        #{
            payload := #{action := ~"crash"},
            attempt := 1,
            errors := [_],
            discarded_at := _
        },
        Wrapped
    ),
    ?assertEqual(forward_on_discard, to_atom(maps:get(queue, Wrapped))).

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
    gaffer_test_helpers:await_hook(),
    % Q2 worker gets wrapped payload → no matching action → discard → forward to Q3
    ok = gaffer_queue_runner:poll(fwd_chain_q2),
    % Wait for Q3 insert hook (fired by Q2's maybe_forward)
    gaffer_test_helpers:await_hook(),
    % Job should now be in Q3 with nested wrapping
    Outer = atomize_keys(maps:get(payload, hd(gaffer:list(fwd_chain_q3)))),
    ?assertMatch(#{attempt := 1, errors := [_], discarded_at := _}, Outer),
    ?assertNot(is_map_key(action, Outer), "Wrapped payload has no action"),
    ?assertEqual(fwd_chain_q2, to_atom(maps:get(queue, Outer))),
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
    ?assertEqual(forward_on_discard_chain, to_atom(maps:get(queue, Inner))).

forward_on_discard_retryable(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver, #{name => fwd_retry_dlq})),
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{on_discard => fwd_retry_dlq, hooks => [Hook]})
    ),
    #{id := Id} = gaffer:insert(?Q, #{~"action" => ~"crash"}, #{
        max_attempts => 3
    }),
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
    ?assertMatch(#{state := failed}, gaffer:get(?Q, Id)),
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
    gaffer_test_helpers:await_hook(),
    Forwarded = atomize_keys(hd(gaffer:list(fwd_fresh_dlq))),
    ?assertMatch(
        #{state := available, attempt := 0, errors := []},
        Forwarded
    ),
    ?assertEqual(
        #{action => ~"crash"},
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
            failed := #{count := 0},
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
    #{id := Id1} = gaffer:insert(?Q, #{task => 1}),
    _ = gaffer:insert(?Q, #{task => 2}),
    _ = gaffer:insert(?Q, #{task => 3}),
    {ok, _} = gaffer:cancel(?Q, Id1),
    #{jobs := Jobs} = gaffer:info(?Q),
    ?assertMatch(#{count := 2}, maps:get(available, Jobs)),
    ?assertMatch(#{count := 1}, maps:get(cancelled, Jobs)).

info_timestamps_per_state(Driver) ->
    Hook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Hook]})),
    TestPid = gaffer_test_worker:encode_pid(self()),
    _ = gaffer:insert(?Q, #{~"action" => ~"complete", ~"test_pid" => TestPid}),
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
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
    #{id := Id} = gaffer:insert(?Q, #{
        ~"action" => ~"block",
        ~"test_pid" => gaffer_test_worker:encode_pid(self())
    }),
    ok = gaffer_queue_runner:poll(?Q),
    WorkerPid =
        receive
            {job_started, #{id := Id, worker := Pid}} -> Pid
        after 5000 -> error(timeout)
        end,
    #{workers := #{active := Active1}} = gaffer:info(?Q),
    ?assertEqual(1, Active1),
    % Unblock the worker and verify active drops to 0
    WorkerPid ! continue,
    gaffer_test_helpers:await_hook(),
    #{workers := #{active := 0}} = gaffer:info(?Q).

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
    Notify = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, complete]]),
    Hook = make_hook(),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Notify, Hook]})),
    TestPid = gaffer_test_worker:encode_pid(self()),
    _ = gaffer:insert(?Q, #{~"action" => ~"complete", ~"test_pid" => TestPid}),
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
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
    Notify = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    Hook = make_hook(),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Notify, Hook]})),
    _ = gaffer:insert(?Q, #{~"action" => ~"crash"}),
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
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
    Notify = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, schedule]]),
    Hook = make_hook(),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => [Notify, Hook]})),
    TestPid = gaffer_test_worker:encode_pid(self()),
    _ = gaffer:insert(?Q, #{
        ~"action" => ~"schedule",
        ~"test_pid" => TestPid,
        ~"offset_seconds" => 60
    }),
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
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
    Notify = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{hooks => [Notify, Annotate]})
    ),
    Inserted = gaffer:insert(?Q, #{~"action" => ~"crash"}, #{max_attempts => 3}),
    ?assertEqual(
        [
            [~"pre", ~"gaffer", ~"job", ~"insert"],
            [~"post", ~"gaffer", ~"job", ~"insert"]
        ],
        % eqwalizer:ignore - we know mapz:deep_get returns a list
        lists:reverse(mapz:deep_get(Path, Inserted)),
        "Return job should have all hook transformations"
    ),
    #{id := Id} = Inserted,
    ok = gaffer_queue_runner:poll(?Q),
    gaffer_test_helpers:await_hook(),
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

atomize_keys(Map) when is_map(Map) ->
    maps:fold(
        fun(K, V, Acc) -> Acc#{to_atom(K) => atomize_keys(V)} end,
        #{},
        Map
    );
atomize_keys(List) when is_list(List) ->
    [atomize_keys(E) || E <:- List];
atomize_keys(Other) ->
    Other.

to_atom(A) when is_atom(A) -> A;
to_atom(B) when is_binary(B) -> binary_to_atom(B).

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
