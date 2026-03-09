-module(gaffer_job_tests).

-include_lib("eunit/include/eunit.hrl").

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
    ?assert(is_integer(maps:get(inserted_at, Job))).

new_scheduled_test() ->
    At = 1767261600000000,
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

transition_executing_to_scheduled_test() ->
    Job0 = gaffer_job:new(q, #{}, #{}),
    {ok, Job1} = gaffer_job:transition(Job0, executing),
    {ok, Job2} = gaffer_job:transition(Job1, scheduled),
    ?assertEqual(scheduled, maps:get(state, Job2)),
    ?assert(maps:is_key(scheduled_at, Job2)).

transition_invalid_test() ->
    Job = gaffer_job:new(q, #{}, #{}),
    ?assertMatch(
        {error, {invalid_transition, {available, completed}}},
        gaffer_job:transition(Job, completed)
    ).

%--- Error list tests ---------------------------------------------------------

add_error_test() ->
    Job0 = gaffer_job:new(q, #{}, #{}),
    E = #{
        attempt => 1,
        error => timeout,
        at => erlang:system_time(microsecond)
    },
    Job1 = gaffer_job:add_error(Job0, E),
    ?assertEqual([E], maps:get(errors, Job1)).
