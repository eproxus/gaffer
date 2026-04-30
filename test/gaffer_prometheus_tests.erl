-module(gaffer_prometheus_tests).

-hank([{unnecessary_function_arguments, [idempotent_start, unknown_event]}]).

-include_lib("eunit/include/eunit.hrl").
-include("gaffer_test_helpers.hrl").

-define(Q, ?FUNCTION_NAME).
-define(CONF(Driver), #{
    name => ?Q,
    driver => Driver,
    worker => gaffer_test_worker,
    poll_interval => infinity,
    prune => #{interval => infinity},
    hooks => [gaffer_prometheus]
}).
-define(CONF(Driver, Extra), maps:merge(?CONF(Driver), Extra)).

%--- Fixtures -----------------------------------------------------------------

gaffer_prometheus_test_() ->
    Parallel = [
        fun insert_claim_complete/1,
        fun fail_retry_then_terminal/1,
        fun timeout_runner_fail/1,
        fun user_cancel_available/1,
        fun worker_cancel_executing/1,
        fun schedule/1,
        fun claim_batch_size/1,
        fun queue_lifecycle/1,
        fun delete_job/1,
        fun idempotent_start/1,
        fun unknown_event/1
    ],
    Sequential = [
        fun forward_target_hook/1
    ],
    {setup, fun setup_prometheus/0, fun teardown_prometheus/1, [
        gaffer_test_helpers:harness(gaffer_driver_ets, Parallel, Sequential),
        gaffer_test_helpers:harness(gaffer_driver_pgo, Parallel, Sequential)
    ]}.

%--- Scenarios ----------------------------------------------------------------

insert_claim_complete(Driver) ->
    NotifyHook = gaffer_test_helpers:notify_hook(
        self(), [[gaffer, job, complete]]
    ),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => hooks(NotifyHook)})),
    Pid = gaffer_test_worker:encode_pid(self()),
    Pre = snapshot(insert_claim_complete_keys()),
    #{id := ID} = gaffer:insert(?Q, #{
        ~"action" => ~"complete", ~"test_pid" => Pid
    }),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, complete], #{job := #{id := ID}, actor := worker}),
    Post = snapshot(insert_claim_complete_keys()),

    ?assertEqual(
        1, delta(counter, gaffer_jobs_inserted_total, [?Q, user], Pre, Post)
    ),
    ?assertEqual(
        1, delta(counter, gaffer_jobs_claimed_total, [?Q, runner], Pre, Post)
    ),
    ?assertEqual(
        1, delta(counter, gaffer_jobs_completed_total, [?Q, worker], Pre, Post)
    ),
    ?assertEqual(
        0, delta(counter, gaffer_jobs_failed_total, [?Q, worker], Pre, Post)
    ),
    ?assertEqual(
        1,
        delta(
            histogram,
            gaffer_job_execution_duration_seconds,
            [?Q, worker, completed],
            Pre,
            Post
        )
    ),
    ?assertEqual(
        1,
        delta(
            histogram, gaffer_job_attempts, [?Q, worker, completed], Pre, Post
        )
    ).

insert_claim_complete_keys() ->
    Q = insert_claim_complete,
    [
        {counter, gaffer_jobs_inserted_total, [Q, user]},
        {counter, gaffer_jobs_claimed_total, [Q, runner]},
        {counter, gaffer_jobs_completed_total, [Q, worker]},
        {counter, gaffer_jobs_failed_total, [Q, worker]},
        {histogram, gaffer_job_execution_duration_seconds, [
            Q, worker, completed
        ]},
        {histogram, gaffer_job_attempts, [Q, worker, completed]}
    ].

fail_retry_then_terminal(Driver) ->
    NotifyHook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{
            hooks => hooks(NotifyHook), max_attempts => 2, backoff => 0
        })
    ),
    Pre = snapshot(fail_retry_then_terminal_keys()),
    #{id := ID} = gaffer:insert(?Q, #{~"action" => ~"crash"}),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook(
        [gaffer, job, fail],
        #{job := #{id := ID, state := available, attempt := 1}, actor := worker}
    ),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook(
        [gaffer, job, fail],
        #{job := #{id := ID, state := failed, attempt := 2}, actor := worker}
    ),
    Post = snapshot(fail_retry_then_terminal_keys()),

    ?assertEqual(
        1, delta(counter, gaffer_jobs_retries_total, [?Q, worker], Pre, Post)
    ),
    ?assertEqual(
        1, delta(counter, gaffer_jobs_failed_total, [?Q, worker], Pre, Post)
    ),
    ?assertEqual(
        1,
        delta(
            histogram,
            gaffer_job_execution_duration_seconds,
            [?Q, worker, available],
            Pre,
            Post
        )
    ),
    ?assertEqual(
        1,
        delta(
            histogram,
            gaffer_job_execution_duration_seconds,
            [?Q, worker, failed],
            Pre,
            Post
        )
    ),
    ?assertEqual(
        1,
        delta(histogram, gaffer_job_attempts, [?Q, worker, failed], Pre, Post)
    ),
    ?assertEqual(
        0,
        delta(
            histogram, gaffer_job_attempts, [?Q, worker, available], Pre, Post
        )
    ).

fail_retry_then_terminal_keys() ->
    Q = fail_retry_then_terminal,
    [
        {counter, gaffer_jobs_retries_total, [Q, worker]},
        {counter, gaffer_jobs_failed_total, [Q, worker]},
        {histogram, gaffer_job_execution_duration_seconds, [
            Q, worker, available
        ]},
        {histogram, gaffer_job_execution_duration_seconds, [Q, worker, failed]},
        {histogram, gaffer_job_attempts, [Q, worker, failed]},
        {histogram, gaffer_job_attempts, [Q, worker, available]}
    ].

timeout_runner_fail(Driver) ->
    Timeout = 10,
    NotifyHook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, fail]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{
            hooks => hooks(NotifyHook),
            timeout => Timeout,
            max_attempts => 1
        })
    ),
    Pid = gaffer_test_worker:encode_pid(self()),
    Keys = [{counter, gaffer_jobs_failed_total, [?Q, worker]}],
    Pre = snapshot(Keys),
    #{id := ID} = gaffer:insert(?Q, #{
        ~"action" => ~"block", ~"test_pid" => Pid
    }),
    ok = gaffer_queue_runner:poll(?Q),
    receive
        {job_started, #{id := ID}} -> ok
    after 5000 -> error(timeout)
    end,
    % The runner kills the worker via exit(_, kill); the DOWN reason is
    % `killed`, which falls through to the worker-actor branch in the
    % runner. Actor is `worker`, not `runner`, despite this being the
    % runner-driven kill path.
    ?assertHook(
        [gaffer, job, fail],
        #{job := #{id := ID, state := failed}, actor := worker}
    ),
    Post = snapshot(Keys),
    ?assertEqual(
        1, delta(counter, gaffer_jobs_failed_total, [?Q, worker], Pre, Post)
    ).

user_cancel_available(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    Keys = [
        {counter, gaffer_jobs_cancelled_total, [?Q, user]},
        {histogram, gaffer_job_execution_duration_seconds, [
            ?Q, user, cancelled
        ]},
        {histogram, gaffer_job_attempts, [?Q, user, cancelled]}
    ],
    Pre = snapshot(Keys),
    #{id := ID} = gaffer:insert(?Q, #{task => 1}),
    {ok, _} = gaffer:cancel(?Q, ID),
    Post = snapshot(Keys),
    ?assertEqual(
        1, delta(counter, gaffer_jobs_cancelled_total, [?Q, user], Pre, Post)
    ),
    ?assertEqual(
        0,
        delta(
            histogram,
            gaffer_job_execution_duration_seconds,
            [?Q, user, cancelled],
            Pre,
            Post
        )
    ),
    ?assertEqual(
        0,
        delta(histogram, gaffer_job_attempts, [?Q, user, cancelled], Pre, Post)
    ).

worker_cancel_executing(Driver) ->
    NotifyHook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, cancel]]),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => hooks(NotifyHook)})),
    Keys = [
        {counter, gaffer_jobs_cancelled_total, [?Q, worker]},
        {histogram, gaffer_job_execution_duration_seconds, [
            ?Q, worker, cancelled
        ]},
        {histogram, gaffer_job_attempts, [?Q, worker, cancelled]}
    ],
    Pre = snapshot(Keys),
    #{id := ID} = gaffer:insert(?Q, #{~"action" => ~"cancel"}),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, cancel], #{job := #{id := ID}, actor := worker}),
    Post = snapshot(Keys),
    ?assertEqual(
        1, delta(counter, gaffer_jobs_cancelled_total, [?Q, worker], Pre, Post)
    ),
    ?assertEqual(
        1,
        delta(
            histogram,
            gaffer_job_execution_duration_seconds,
            [?Q, worker, cancelled],
            Pre,
            Post
        )
    ),
    ?assertEqual(
        1,
        delta(
            histogram, gaffer_job_attempts, [?Q, worker, cancelled], Pre, Post
        )
    ).

schedule(Driver) ->
    NotifyHook = gaffer_test_helpers:notify_hook(
        self(), [[gaffer, job, schedule]]
    ),
    ok = gaffer:create_queue(?CONF(Driver, #{hooks => hooks(NotifyHook)})),
    Pid = gaffer_test_worker:encode_pid(self()),
    Keys = [
        {counter, gaffer_jobs_scheduled_total, [?Q, worker]},
        {counter, gaffer_jobs_completed_total, [?Q, worker]},
        {histogram, gaffer_job_execution_duration_seconds, [
            ?Q, worker, completed
        ]}
    ],
    Pre = snapshot(Keys),
    #{id := ID} = gaffer:insert(?Q, #{
        ~"action" => ~"schedule",
        ~"test_pid" => Pid,
        ~"offset_seconds" => 60
    }),
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook([gaffer, job, schedule], #{job := #{id := ID}, actor := worker}),
    Post = snapshot(Keys),
    ?assertEqual(
        1, delta(counter, gaffer_jobs_scheduled_total, [?Q, worker], Pre, Post)
    ),
    ?assertEqual(
        0, delta(counter, gaffer_jobs_completed_total, [?Q, worker], Pre, Post)
    ),
    ?assertEqual(
        0,
        delta(
            histogram,
            gaffer_job_execution_duration_seconds,
            [?Q, worker, completed],
            Pre,
            Post
        )
    ).

claim_batch_size(Driver) ->
    NotifyHook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, claim]]),
    ok = gaffer:create_queue(
        ?CONF(Driver, #{hooks => hooks(NotifyHook), max_workers => 5})
    ),
    Keys = [
        {counter, gaffer_jobs_claimed_total, [?Q, runner]},
        {histogram, gaffer_job_claim_batch_size, [?Q, runner]}
    ],
    Pre = snapshot(Keys),
    [gaffer:insert(?Q, #{n => N}) || N <:- lists:seq(1, 4)],
    ok = gaffer_queue_runner:poll(?Q),
    ?assertHook(
        [gaffer, job, claim], #{
            queue := ?Q, jobs := [_, _, _, _], actor := runner
        }
    ),
    Post = snapshot(Keys),
    ?assertEqual(
        4, delta(counter, gaffer_jobs_claimed_total, [?Q, runner], Pre, Post)
    ),
    ?assertEqual(
        1,
        delta(histogram, gaffer_job_claim_batch_size, [?Q, runner], Pre, Post)
    ),
    % batch buckets [1, 2, 5, 10, 25, 50, 100, 250, 1000]: a value of 4
    % lands in bucket index 2 (bound 5).
    ?assertEqual(
        2,
        top_bucket(gaffer_job_claim_batch_size, [?Q, runner], Pre, Post)
    ).

queue_lifecycle(Driver) ->
    Keys = [
        {counter, gaffer_queues_created_total, [?Q, user]},
        {counter, gaffer_queues_updated_total, [?Q, user, update]},
        {counter, gaffer_queues_paused_total, [?Q, user]},
        {counter, gaffer_queues_resumed_total, [?Q, user]},
        {counter, gaffer_queues_deleted_total, [?Q, user]}
    ],
    Pre = snapshot(Keys),
    ok = gaffer:create_queue(?CONF(Driver)),
    ok = gaffer:update_queue(?Q, #{max_workers => 3}),
    ok = gaffer:pause(?Q),
    ok = gaffer:resume(?Q),
    ok = gaffer:delete_queue(?Q),
    Post = snapshot(Keys),
    ?assertEqual(
        1, delta(counter, gaffer_queues_created_total, [?Q, user], Pre, Post)
    ),
    ?assertEqual(
        1,
        delta(
            counter, gaffer_queues_updated_total, [?Q, user, update], Pre, Post
        )
    ),
    ?assertEqual(
        1, delta(counter, gaffer_queues_paused_total, [?Q, user], Pre, Post)
    ),
    ?assertEqual(
        1, delta(counter, gaffer_queues_resumed_total, [?Q, user], Pre, Post)
    ),
    ?assertEqual(
        1, delta(counter, gaffer_queues_deleted_total, [?Q, user], Pre, Post)
    ).

delete_job(Driver) ->
    ok = gaffer:create_queue(?CONF(Driver)),
    Keys = [{counter, gaffer_jobs_deleted_total, [?Q, user]}],
    Pre = snapshot(Keys),
    #{id := ID} = gaffer:insert(?Q, #{task => 1}),
    ok = gaffer:delete(?Q, ID),
    Post = snapshot(Keys),
    ?assertEqual(
        1, delta(counter, gaffer_jobs_deleted_total, [?Q, user], Pre, Post)
    ).

idempotent_start(_Driver) ->
    ?assertEqual(ok, gaffer_prometheus:start()),
    ?assertEqual(ok, gaffer_prometheus:start()),
    ?assertEqual(ok, gaffer_prometheus:start()).

unknown_event(_Driver) ->
    Data = #{actor => user, queue => unknown_event_q},
    ?assertEqual(ok, gaffer_prometheus:gaffer_hook([gaffer, unknown], Data)).

%--- Sequential scenarios -----------------------------------------------------

forward_target_hook(Driver) ->
    application:set_env(gaffer, hooks, [gaffer_prometheus]),
    Notify = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, insert]]),
    try
        ok = gaffer:create_queue(
            ?CONF(Driver, #{name => fwd_target, hooks => [Notify]})
        ),
        ok = gaffer:create_queue(
            ?CONF(Driver, #{
                name => fwd_source,
                forward => #{failed => fwd_target},
                max_attempts => 1,
                hooks => []
            })
        ),
        Keys = [{counter, gaffer_jobs_inserted_total, [fwd_target, worker]}],
        Pre = snapshot(Keys),
        _ = gaffer:insert(fwd_source, #{~"action" => ~"crash"}),
        ok = gaffer_queue_runner:poll(fwd_source),
        ?assertHook(
            [gaffer, job, insert],
            #{job := #{queue := fwd_target}, actor := worker}
        ),
        Post = snapshot(Keys),
        ?assertEqual(
            1,
            delta(
                counter,
                gaffer_jobs_inserted_total,
                [fwd_target, worker],
                Pre,
                Post
            )
        )
    after
        application:unset_env(gaffer, hooks),
        try_delete_queue(fwd_source),
        try_delete_queue(fwd_target)
    end.

try_delete_queue(Name) ->
    try
        gaffer:delete_queue(Name)
    catch
        _:_ -> ok
    end.

%--- Helpers ------------------------------------------------------------------

setup_prometheus() ->
    {ok, Apps} = application:ensure_all_started(prometheus),
    ok = gaffer_prometheus:start(),
    Apps.

teardown_prometheus(Apps) ->
    [application:stop(A) || A <:- lists:reverse(Apps)],
    ok.

hooks(Notify) -> [gaffer_prometheus, Notify].

snapshot(Keys) ->
    maps:from_list([{K, sample(K)} || K <:- Keys]).

sample({counter, Name, Labels}) ->
    case prometheus_counter:value(Name, Labels) of
        undefined -> 0;
        N -> N
    end;
sample({histogram, Name, Labels}) ->
    case prometheus_histogram:value(Name, Labels) of
        undefined -> [];
        {Buckets, _Sum} -> Buckets
    end.

delta(counter, Name, Labels, Pre, Post) ->
    maps:get({counter, Name, Labels}, Post) -
        maps:get({counter, Name, Labels}, Pre);
delta(histogram, Name, Labels, Pre, Post) ->
    PostBuckets = maps:get({histogram, Name, Labels}, Post),
    PreBuckets = maps:get({histogram, Name, Labels}, Pre),
    lists:sum(PostBuckets) - lists:sum(PreBuckets).

% Bucket index (0-based) of the highest bucket whose count grew between the
% pre and post snapshots. Errors out if nothing changed.
top_bucket(Name, Labels, Pre, Post) ->
    PostBuckets = maps:get({histogram, Name, Labels}, Post),
    PreBuckets = pad(
        maps:get({histogram, Name, Labels}, Pre), length(PostBuckets)
    ),
    Diffs = lists:zip3(
        lists:seq(0, length(PostBuckets) - 1), PreBuckets, PostBuckets
    ),
    Changed = [I || {I, B0, B1} <:- Diffs, B1 - B0 > 0],
    case Changed of
        [] -> error(no_observations);
        _ -> lists:last(Changed)
    end.

pad([], N) -> lists:duplicate(N, 0);
pad(L, _) -> L.
