-module(gaffer_cross_driver_tests).

-import(gaffer_test_helpers, [normalize/1]).

-include_lib("eunit/include/eunit.hrl").

%--- Fixtures -----------------------------------------------------------------

cross_driver_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Drivers) ->
        [
            {with, Drivers, [fun forward_ets_to_pgo/1]},
            {with, Drivers, [fun forward_pgo_to_ets/1]}
        ]
    end}.

%--- Tests --------------------------------------------------------------------

forward_ets_to_pgo(#{ets := Ets, pgo := Pgo}) ->
    forward(Ets, Pgo, forward_ets_to_pgo, forward_ets_to_pgo_dlq).

forward_pgo_to_ets(#{ets := Ets, pgo := Pgo}) ->
    forward(Pgo, Ets, forward_pgo_to_ets, forward_pgo_to_ets_dlq).

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
    % Register DLQ name in source driver so on_discard validation passes
    gaffer_test_helpers:register_queue(DlqQueue, SrcDriver),
    ok = gaffer:create_queue(#{
        name => SrcQueue,
        driver => SrcDriver,
        worker => gaffer_test_worker,
        poll_interval => infinity,
        max_attempts => 1,
        on_discard => DlqQueue
    }),
    #{id := Id} = gaffer:insert(SrcQueue, #{~"action" => ~"crash"}),
    ok = gaffer_queue_runner:poll(SrcQueue),
    gaffer_test_helpers:await_hook(),
    ?assertMatch(#{state := discarded}, gaffer:get(SrcQueue, Id)),
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
