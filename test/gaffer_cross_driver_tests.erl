-module(gaffer_cross_driver_tests).

-import(gaffer_test_helpers, [normalize/1]).

-include_lib("eunit/include/eunit.hrl").

%--- Fixtures -----------------------------------------------------------------

cross_driver_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Drivers) ->
        [
            {with, Drivers, [fun forward_ets_to_pgo/1]},
            {with, Drivers, [fun forward_pgo_to_ets/1]},
            {with, Drivers, [fun forward_survives_source_failure/1]}
        ]
    end}.

%--- Tests --------------------------------------------------------------------

forward_ets_to_pgo(#{ets := Ets, pgo := Pgo}) ->
    forward(Ets, Pgo, forward_ets_to_pgo, forward_ets_to_pgo_dlq).

forward_pgo_to_ets(#{ets := Ets, pgo := Pgo}) ->
    forward(Pgo, Ets, forward_pgo_to_ets, forward_pgo_to_ets_dlq).

forward_survives_source_failure(#{ets := Ets, pgo := Pgo}) ->
    FailingSource = gaffer_test_driver:wrap(Ets, #{
        job_write => fun
            (_Inner, [#{state := discarded} | _]) -> error(simulated_crash);
            (Inner, Jobs) -> Inner(Jobs)
        end
    }),
    ok = gaffer:create_queue(#{
        name => survive_dlq,
        driver => Pgo,
        worker => gaffer_test_worker,
        poll_interval => infinity
    }),
    ok = gaffer:create_queue(#{
        name => survive_src,
        driver => FailingSource,
        worker => gaffer_test_worker,
        poll_interval => infinity,
        max_attempts => 1,
        on_discard => survive_dlq
    }),
    _ = gaffer:insert(survive_src, #{~"action" => ~"crash"}),
    ok = gaffer_queue_runner:poll(survive_src),
    % Target queue has the forwarded job despite source write crashing
    Forwarded = gaffer_test_helpers:wait_for(
        fun(S) ->
            case gaffer:list(survive_dlq) of
                [] -> {wait, S};
                Jobs -> {result, Jobs}
            end
        end,
        []
    ),
    [Job] = normalize(Forwarded),
    ?assertMatch(
        #{
            state := available,
            payload := #{
                payload := #{action := crash},
                queue := survive_src,
                attempt := 1,
                errors := [_]
            }
        },
        Job
    ).

%--- Internal -----------------------------------------------------------------

forward(SrcDriver, DlqDriver, SrcQueue, DlqQueue) ->
    DlqHook = gaffer_test_helpers:notify_hook(self(), [[gaffer, job, insert]]),
    ok = gaffer:create_queue(#{
        name => DlqQueue,
        driver => DlqDriver,
        worker => gaffer_test_worker,
        poll_interval => infinity,
        hooks => [DlqHook]
    }),
    ok = gaffer:create_queue(#{
        name => SrcQueue,
        driver => SrcDriver,
        worker => gaffer_test_worker,
        poll_interval => infinity,
        max_attempts => 1,
        on_discard => DlqQueue
    }),
    #{id := ID} = gaffer:insert(SrcQueue, #{~"action" => ~"crash"}),
    ok = gaffer_queue_runner:poll(SrcQueue),
    gaffer_test_helpers:await_hook(),
    ?assertMatch(#{state := discarded}, gaffer:get(SrcQueue, ID)),
    Forwarded = normalize(
        maps:get(payload, hd(gaffer:list(DlqQueue)))
    ),
    ?assertMatch(
        #{payload := #{action := crash}, attempt := 1, errors := [_]},
        Forwarded
    ),
    ?assertEqual(SrcQueue, maps:get(queue, Forwarded)).

setup() ->
    error_logger:tty(false),
    EtsDS = gaffer_driver_ets:start(#{}),
    {ok, PgoApps} = application:ensure_all_started(pgo),
    gaffer_test_helpers:stop_pool(cross_pool),
    {ok, _} = pgo:start_pool(cross_pool, gaffer_test_helpers:pgo_pool_config()),
    gaffer_test_helpers:reset_database(cross_pool),
    PgoDS = gaffer_driver_pgo:start(#{pool => cross_pool}),
    {ok, GafferApps} = application:ensure_all_started(gaffer),
    #{
        ets => {gaffer_driver_ets, EtsDS},
        pgo => {gaffer_driver_pgo, PgoDS},
        gaffer_apps => GafferApps,
        pgo_apps => PgoApps
    }.

teardown(#{
    ets := {_, EtsDS},
    pgo := {_, PgoDS},
    gaffer_apps := GafferApps,
    pgo_apps := PgoApps
}) ->
    [application:stop(A) || A <:- lists:reverse(GafferApps)],
    gaffer_driver_ets:stop(EtsDS),
    gaffer_driver_pgo:stop(PgoDS),
    gaffer_test_helpers:reset_database(cross_pool),
    gaffer_test_helpers:stop_pool(cross_pool),
    [application:stop(A) || A <:- lists:reverse(PgoApps)],
    error_logger:tty(true).
