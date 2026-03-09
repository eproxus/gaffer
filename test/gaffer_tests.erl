-module(gaffer_tests).

-include_lib("eunit/include/eunit.hrl").

%--- Application tests --------------------------------------------------------

start_stop_test() ->
    {ok, Apps} = application:ensure_all_started(gaffer),
    [
        ?assertEqual(ok, application:stop(A))
     || A <:- Apps
    ].

%--- Job construction tests ---------------------------------------------------

new_default_test() ->
    Job = gaffer_job:new(my_queue, #{task => 1}, #{}),
    ?assertMatch(
        #{
            queue := my_queue,
            args := #{task := 1},
            state := available,
            attempt := 0,
            max_attempts := 3,
            priority := 0,
            errors := [],
            tags := [],
            meta := #{}
        },
        Job
    ),
    ?assert(is_binary(maps:get(id, Job))),
    ?assertMatch({_, _}, maps:get(inserted_at, Job)).

new_scheduled_test() ->
    At = {{2026, 1, 1}, {12, 0, 0}},
    Job = gaffer_job:new(q, #{}, #{scheduled_at => At}),
    ?assertEqual(scheduled, maps:get(state, Job)),
    ?assertEqual(At, maps:get(scheduled_at, Job)).

new_with_opts_test() ->
    Opts = #{
        max_attempts => 5,
        priority => 10,
        tags => [<<"urgent">>],
        meta => #{source => <<"api">>}
    },
    Job = gaffer_job:new(q, #{}, Opts),
    ?assertEqual(5, maps:get(max_attempts, Job)),
    ?assertEqual(10, maps:get(priority, Job)),
    ?assertEqual([<<"urgent">>], maps:get(tags, Job)),
    ?assertEqual(#{source => <<"api">>}, maps:get(meta, Job)).

%--- Validation tests ---------------------------------------------------------

validate_ok_test() ->
    Job = gaffer_job:new(q, #{}, #{}),
    ?assertEqual(ok, gaffer_job:validate(Job)).

validate_bad_queue_test() ->
    Job = (gaffer_job:new(q, #{}, #{}))#{queue := "nope"},
    ?assertMatch({error, _}, gaffer_job:validate(Job)).

validate_missing_fields_test() ->
    ?assertMatch(
        {error, missing_required_fields},
        gaffer_job:validate(#{})
    ).

%--- State transition tests ---------------------------------------------------

transition_available_to_executing_test() ->
    Job = gaffer_job:new(q, #{}, #{}),
    {ok, Job1} = gaffer_job:transition(Job, executing),
    ?assertEqual(executing, maps:get(state, Job1)),
    ?assert(maps:is_key(attempted_at, Job1)).

transition_executing_to_completed_test() ->
    Job0 = gaffer_job:new(q, #{}, #{}),
    {ok, Job1} = gaffer_job:transition(Job0, executing),
    {ok, Job2} = gaffer_job:transition(Job1, completed),
    ?assertEqual(completed, maps:get(state, Job2)),
    ?assert(maps:is_key(completed_at, Job2)).

transition_executing_to_failed_test() ->
    Job0 = gaffer_job:new(q, #{}, #{}),
    {ok, Job1} = gaffer_job:transition(Job0, executing),
    {ok, Job2} = gaffer_job:transition(Job1, failed),
    ?assertEqual(failed, maps:get(state, Job2)).

transition_executing_to_cancelled_test() ->
    Job0 = gaffer_job:new(q, #{}, #{}),
    {ok, Job1} = gaffer_job:transition(Job0, executing),
    {ok, Job2} = gaffer_job:transition(Job1, cancelled),
    ?assertEqual(cancelled, maps:get(state, Job2)),
    ?assert(maps:is_key(cancelled_at, Job2)).

transition_failed_to_discarded_test() ->
    Job0 = gaffer_job:new(q, #{}, #{}),
    {ok, Job1} = gaffer_job:transition(Job0, executing),
    {ok, Job2} = gaffer_job:transition(Job1, failed),
    {ok, Job3} = gaffer_job:transition(Job2, discarded),
    ?assertEqual(discarded, maps:get(state, Job3)),
    ?assert(maps:is_key(discarded_at, Job3)).

transition_invalid_test() ->
    Job = gaffer_job:new(q, #{}, #{}),
    ?assertMatch(
        {error, {invalid_transition, {available, completed}}},
        gaffer_job:transition(Job, completed)
    ).

transition_retry_from_discarded_test() ->
    Job0 = gaffer_job:new(q, #{}, #{}),
    {ok, Job1} = gaffer_job:transition(Job0, executing),
    {ok, Job2} = gaffer_job:transition(Job1, failed),
    {ok, Job3} = gaffer_job:transition(Job2, discarded),
    {ok, Job4} = gaffer_job:transition(Job3, scheduled),
    ?assertEqual(scheduled, maps:get(state, Job4)).

transition_retry_from_cancelled_test() ->
    Job0 = gaffer_job:new(q, #{}, #{}),
    {ok, Job1} = gaffer_job:transition(Job0, executing),
    {ok, Job2} = gaffer_job:transition(Job1, cancelled),
    {ok, Job3} = gaffer_job:transition(Job2, scheduled),
    ?assertEqual(scheduled, maps:get(state, Job3)).

transition_snooze_test() ->
    Job0 = gaffer_job:new(q, #{}, #{}),
    {ok, Job1} = gaffer_job:transition(Job0, executing),
    {ok, Job2} = gaffer_job:transition(Job1, scheduled),
    ?assertEqual(scheduled, maps:get(state, Job2)).

%--- Error list tests ---------------------------------------------------------

add_error_test() ->
    Job0 = gaffer_job:new(q, #{}, #{}),
    E = #{
        attempt => 1,
        error => timeout,
        at => calendar:universal_time()
    },
    Job1 = gaffer_job:add_error(Job0, E),
    ?assertEqual([E], maps:get(errors, Job1)).

%--- ETS driver tests ---------------------------------------------------------

ets_init_stop_test() ->
    {ok, S} = gaffer_driver_ets:init(#{}),
    ?assertEqual(ok, gaffer_driver_ets:stop(S)).

ets_queue_crud_test() ->
    {ok, S0} = gaffer_driver_ets:init(#{}),
    Conf = #{name => test_q, concurrency => 5},
    {ok, S1} = gaffer_driver_ets:queue_put(Conf, S0),
    ?assertMatch(
        {ok, #{name := test_q}},
        gaffer_driver_ets:queue_get(test_q, S1)
    ),
    {ok, List} = gaffer_driver_ets:queue_list(S1),
    ?assertEqual(1, length(List)),
    {ok, S2} = gaffer_driver_ets:queue_delete(test_q, S1),
    ?assertEqual(
        {error, not_found},
        gaffer_driver_ets:queue_get(test_q, S2)
    ),
    ok = gaffer_driver_ets:stop(S2).

ets_insert_get_test() ->
    {ok, S0} = gaffer_driver_ets:init(#{}),
    Job = gaffer_job:new(q, #{x => 1}, #{}),
    Id = maps:get(id, Job),
    {ok, _, S1} = gaffer_driver_ets:job_insert(Job, S0),
    ?assertMatch(
        {ok, #{id := Id}},
        gaffer_driver_ets:job_get(Id, S1)
    ),
    ?assertEqual(
        {error, not_found},
        gaffer_driver_ets:job_get(<<"nope">>, S1)
    ),
    ok = gaffer_driver_ets:stop(S1).

ets_fetch_and_complete_test() ->
    {ok, S0} = gaffer_driver_ets:init(#{}),
    J1 = gaffer_job:new(q, #{n => 1}, #{priority => 10}),
    J2 = gaffer_job:new(q, #{n => 2}, #{priority => 1}),
    {ok, _, S1} = gaffer_driver_ets:job_insert(J1, S0),
    {ok, _, S2} = gaffer_driver_ets:job_insert(J2, S1),
    {ok, Fetched, S3} = gaffer_driver_ets:job_fetch(
        #{queue => q, limit => 1}, S2
    ),
    ?assertEqual(1, length(Fetched)),
    [Executing] = Fetched,
    ?assertEqual(executing, maps:get(state, Executing)),
    %% Lower priority number = higher priority
    ?assertEqual(
        maps:get(id, J2), maps:get(id, Executing)
    ),
    {ok, Done, _S4} = complete_job(Executing, S3),
    ?assertEqual(completed, maps:get(state, Done)),
    ?assertEqual(1, maps:get(attempt, Done)),
    ok = gaffer_driver_ets:stop(S3).

ets_fail_under_max_test() ->
    {Executing, S} = insert_and_fetch(#{max_attempts => 3}),
    {ok, Failed, _} = fail_job(Executing, S),
    ?assertEqual(failed, maps:get(state, Failed)),
    ?assertEqual(1, maps:get(attempt, Failed)),
    ok = gaffer_driver_ets:stop(S).

ets_fail_at_max_discards_test() ->
    {Executing, S} = insert_and_fetch(#{max_attempts => 1}),
    {ok, Discarded, _} = fail_job(Executing, S),
    ?assertEqual(discarded, maps:get(state, Discarded)),
    ok = gaffer_driver_ets:stop(S).

ets_cancel_test() ->
    {Executing, S} = insert_and_fetch(#{}),
    {ok, Cancelled, _} = gaffer_driver_ets:job_cancel(
        maps:get(id, Executing), S
    ),
    ?assertEqual(cancelled, maps:get(state, Cancelled)),
    ok = gaffer_driver_ets:stop(S).

ets_retry_test() ->
    {Executing, S0} = insert_and_fetch(#{max_attempts => 1}),
    {ok, Disc, S1} = fail_job(Executing, S0),
    ?assertEqual(discarded, maps:get(state, Disc)),
    At = {{2026, 6, 1}, {0, 0, 0}},
    {ok, Retried, _} = gaffer_driver_ets:job_retry(
        maps:get(id, Disc), At, S1
    ),
    ?assertEqual(scheduled, maps:get(state, Retried)),
    ?assertEqual(At, maps:get(scheduled_at, Retried)),
    ok = gaffer_driver_ets:stop(S1).

ets_snooze_test() ->
    {Executing, S} = insert_and_fetch(#{}),
    {ok, Snoozed, _} = gaffer_driver_ets:job_snooze(
        maps:get(id, Executing), 60, S
    ),
    ?assertEqual(scheduled, maps:get(state, Snoozed)),
    ?assert(maps:is_key(scheduled_at, Snoozed)),
    ok = gaffer_driver_ets:stop(S).

ets_prune_test() ->
    {Executing, S0} = insert_and_fetch(#{}),
    {ok, _, S1} = complete_job(Executing, S0),
    {ok, Count, _} = gaffer_driver_ets:job_prune(#{}, S1),
    ?assertEqual(1, Count),
    ok = gaffer_driver_ets:stop(S1).

ets_list_filter_test() ->
    {ok, S0} = gaffer_driver_ets:init(#{}),
    J1 = gaffer_job:new(q1, #{}, #{}),
    J2 = gaffer_job:new(q2, #{}, #{}),
    {ok, _, S1} = gaffer_driver_ets:job_insert(J1, S0),
    {ok, _, S2} = gaffer_driver_ets:job_insert(J2, S1),
    {ok, All} = gaffer_driver_ets:job_list(#{}, S2),
    ?assertEqual(2, length(All)),
    {ok, Q1Only} = gaffer_driver_ets:job_list(
        #{queue => q1}, S2
    ),
    ?assertEqual(1, length(Q1Only)),
    ok = gaffer_driver_ets:stop(S2).

%--- Helpers ------------------------------------------------------------------

insert_and_fetch(Opts) ->
    {ok, S0} = gaffer_driver_ets:init(#{}),
    Job = gaffer_job:new(q, #{}, Opts),
    {ok, _, S1} = gaffer_driver_ets:job_insert(Job, S0),
    {ok, [Executing], S2} = gaffer_driver_ets:job_fetch(
        #{queue => q}, S1
    ),
    {Executing, S2}.

fail_job(Executing, S) ->
    gaffer_driver_ets:job_fail(
        maps:get(id, Executing), make_error(), S
    ).

complete_job(Executing, S) ->
    gaffer_driver_ets:job_complete(
        maps:get(id, Executing), S
    ).

make_error() ->
    #{
        attempt => 1,
        error => boom,
        at => calendar:universal_time()
    }.
