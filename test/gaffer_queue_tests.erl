-module(gaffer_queue_tests).

-include_lib("eunit/include/eunit.hrl").

%--- Helpers ------------------------------------------------------------------

%% Force a job's state in the mock driver (for simulating scheduled→available).
set_job_state(Id, State, {Mod, #{jobs := Jobs} = DS}) ->
    #{Id := Job} = Jobs,
    {ok, Updated} = gaffer_queue:transition(Job, State),
    {Mod, DS#{jobs := Jobs#{Id := Updated}}}.

%--- Insert tests -------------------------------------------------------------

insert_defaults_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {Job, _D1} = gaffer_queue:insert(test_queue, #{task => 1}, #{}, D0),
    ?assertMatch(
        #{
            queue := test_queue,
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

insert_scheduled_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    At = 1767261600000000,
    {Job, _D1} =
        gaffer_queue:insert(
            test_queue, #{task => 1}, #{scheduled_at => At}, D0
        ),
    ?assertMatch(#{state := scheduled, scheduled_at := At}, Job).

insert_with_opts_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    Opts = #{
        max_attempts => 5,
        priority => 10,
        tags => [~"urgent"],
        meta => #{source => ~"api"}
    },
    {Job, _D1} = gaffer_queue:insert(test_queue, #{task => 1}, Opts, D0),
    ?assertMatch(
        #{
            max_attempts := 5,
            priority := 10,
            tags := [~"urgent"],
            meta := #{source := ~"api"}
        },
        Job
    ).

%--- Get / list tests ---------------------------------------------------------

get_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {#{id := Id}, D1} = gaffer_queue:insert(test_queue, #{task => 1}, #{}, D0),
    {ok, Found} = gaffer_queue:get(Id, D1),
    ?assertMatch(#{id := Id}, Found).

get_not_found_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    ?assertEqual(
        {error, not_found},
        gaffer_queue:get(~"nope", D0)
    ).

list_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {_, D1} = gaffer_queue:insert(test_queue, #{task => 1}, #{}, D0),
    {_, D2} = gaffer_queue:insert(test_queue, #{task => 2}, #{}, D1),
    ?assertMatch(
        [#{args := #{task := _}}, #{args := #{task := _}}],
        gaffer_queue:list(#{queue => test_queue}, D2)
    ).

%--- Cancel tests -------------------------------------------------------------

cancel_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {#{id := Id}, D1} = gaffer_queue:insert(test_queue, #{task => 1}, #{}, D0),
    {ok, Cancelled, _D2} = gaffer_queue:cancel(Id, D1),
    ?assertMatch(#{state := cancelled, cancelled_at := _}, Cancelled).

cancel_not_found_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    ?assertEqual(
        {error, not_found},
        gaffer_queue:cancel(~"nope", D0)
    ).

cancel_scheduled_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    At = 1767261600000000,
    {#{id := Id}, D1} = gaffer_queue:insert(
        test_queue, #{task => 1}, #{scheduled_at => At}, D0
    ),
    {ok, Cancelled, _D2} = gaffer_queue:cancel(Id, D1),
    ?assertMatch(#{state := cancelled, cancelled_at := _}, Cancelled).

cancel_executing_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {#{id := Id}, D1} = gaffer_queue:insert(test_queue, #{task => 1}, #{}, D0),
    {[_], D2} = gaffer_queue:claim(#{queue => test_queue, limit => 1}, D1),
    {ok, Cancelled, _D3} = gaffer_queue:cancel(Id, D2),
    ?assertMatch(#{state := cancelled, cancelled_at := _}, Cancelled).

cancel_completed_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {#{id := Id}, D1} = gaffer_queue:insert(test_queue, #{task => 1}, #{}, D0),
    {[_], D2} = gaffer_queue:claim(#{queue => test_queue, limit => 1}, D1),
    {ok, _, D3} = gaffer_queue:complete(Id, D2),
    ?assertMatch(
        {error, {invalid_transition, {completed, cancelled}}},
        gaffer_queue:cancel(Id, D3)
    ).

cancel_discarded_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {#{id := Id}, D1} = gaffer_queue:insert(
        test_queue, #{task => 1}, #{max_attempts => 1}, D0
    ),
    {[_], D2} = gaffer_queue:claim(#{queue => test_queue, limit => 1}, D1),
    E = #{attempt => 1, error => boom, at => erlang:system_time(microsecond)},
    {ok, _, D3} = gaffer_queue:fail(Id, E, D2),
    ?assertMatch(
        {error, {invalid_transition, {discarded, cancelled}}},
        gaffer_queue:cancel(Id, D3)
    ).

%--- Complete tests -----------------------------------------------------------

complete_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {#{id := Id}, D1} = gaffer_queue:insert(test_queue, #{task => 1}, #{}, D0),
    {[_], D2} = gaffer_queue:claim(#{queue => test_queue, limit => 1}, D1),
    {ok, Completed, _D3} = gaffer_queue:complete(Id, D2),
    ?assertMatch(
        #{state := completed, attempt := 1, completed_at := _}, Completed
    ).

complete_available_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {#{id := Id}, D1} = gaffer_queue:insert(test_queue, #{task => 1}, #{}, D0),
    ?assertMatch(
        {error, {invalid_transition, {available, completed}}},
        gaffer_queue:complete(Id, D1)
    ).

%--- Fail tests ---------------------------------------------------------------

fail_retryable_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {#{id := Id}, D1} = gaffer_queue:insert(
        test_queue, #{task => 1}, #{max_attempts => 3}, D0
    ),
    {[_], D2} = gaffer_queue:claim(#{queue => test_queue, limit => 1}, D1),
    JobError = #{
        attempt => 1,
        error => timeout,
        at => erlang:system_time(microsecond)
    },
    {ok, Failed, _D3} = gaffer_queue:fail(Id, JobError, D2),
    ?assertMatch(
        #{state := failed, attempt := 1, errors := [JobError]}, Failed
    ).

fail_discarded_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {#{id := Id}, D1} = gaffer_queue:insert(
        test_queue, #{task => 1}, #{max_attempts => 1}, D0
    ),
    {[_], D2} = gaffer_queue:claim(#{queue => test_queue, limit => 1}, D1),
    JobError = #{
        attempt => 1,
        error => boom,
        at => erlang:system_time(microsecond)
    },
    {ok, Discarded, _D3} = gaffer_queue:fail(
        Id, JobError, D2
    ),
    ?assertMatch(#{state := discarded, discarded_at := _}, Discarded).

%--- Full lifecycle tests -----------------------------------------------------

full_retry_cycle_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {#{id := Id}, D1} = gaffer_queue:insert(
        test_queue, #{task => 1}, #{max_attempts => 3}, D0
    ),
    %% Attempt 1: claim → fail (retryable)
    {[_], D2} = gaffer_queue:claim(#{queue => test_queue, limit => 1}, D1),
    E1 = #{attempt => 1, error => boom, at => erlang:system_time(microsecond)},
    {ok, F1, D3} = gaffer_queue:fail(Id, E1, D2),
    ?assertMatch(#{state := failed, attempt := 1}, F1),
    %% Schedule retry → schedule → claim again
    RetryAt = erlang:system_time(microsecond),
    {ok, _, D4} = gaffer_queue:schedule(Id, RetryAt, D3),
    %% Transition scheduled→available via the mock
    D5 = set_job_state(Id, available, D4),
    %% Attempt 2: claim → fail (retryable)
    {[_], D6} = gaffer_queue:claim(#{queue => test_queue, limit => 1}, D5),
    E2 = #{attempt => 2, error => boom, at => erlang:system_time(microsecond)},
    {ok, F2, D7} = gaffer_queue:fail(Id, E2, D6),
    ?assertMatch(#{state := failed, attempt := 2, errors := [E2, E1]}, F2),
    %% Schedule retry again
    {ok, _, D8} = gaffer_queue:schedule(Id, RetryAt, D7),
    D9 = set_job_state(Id, available, D8),
    %% Attempt 3: claim → fail (discarded — max attempts reached)
    {[_], D10} = gaffer_queue:claim(#{queue => test_queue, limit => 1}, D9),
    E3 = #{attempt => 3, error => boom, at => erlang:system_time(microsecond)},
    {ok, Discarded, _D11} = gaffer_queue:fail(Id, E3, D10),
    ?assertMatch(
        #{state := discarded, attempt := 3, errors := [E3, E2, E1]},
        Discarded
    ).

%--- Schedule tests -----------------------------------------------------------

schedule_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {#{id := Id}, D1} = gaffer_queue:insert(test_queue, #{task => 1}, #{}, D0),
    {[_], D2} = gaffer_queue:claim(#{queue => test_queue, limit => 1}, D1),
    FutureAt = erlang:system_time(microsecond) + 60_000_000,
    {ok, Scheduled, _D3} = gaffer_queue:schedule(
        Id, FutureAt, D2
    ),
    ?assertMatch(#{state := scheduled, scheduled_at := FutureAt}, Scheduled).

schedule_from_failed_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {#{id := Id}, D1} = gaffer_queue:insert(
        test_queue, #{task => 1}, #{max_attempts => 3}, D0
    ),
    {[_], D2} = gaffer_queue:claim(#{queue => test_queue, limit => 1}, D1),
    E = #{attempt => 1, error => boom, at => erlang:system_time(microsecond)},
    {ok, _, D3} = gaffer_queue:fail(Id, E, D2),
    FutureAt = erlang:system_time(microsecond) + 60_000_000,
    {ok, Scheduled, _D4} = gaffer_queue:schedule(
        Id, FutureAt, D3
    ),
    ?assertMatch(#{state := scheduled, scheduled_at := FutureAt}, Scheduled).

schedule_available_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {#{id := Id}, D1} = gaffer_queue:insert(test_queue, #{task => 1}, #{}, D0),
    FutureAt = erlang:system_time(microsecond) + 60_000_000,
    ?assertMatch(
        {error, {invalid_transition, {available, scheduled}}},
        gaffer_queue:schedule(Id, FutureAt, D1)
    ).

%--- Claim tests --------------------------------------------------------------

claim_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {_, D1} = gaffer_queue:insert(test_queue, #{task => 1}, #{}, D0),
    {_, D2} = gaffer_queue:insert(test_queue, #{task => 2}, #{}, D1),
    ?assertMatch(
        {[#{state := executing, attempted_at := _}], _},
        gaffer_queue:claim(#{queue => test_queue, limit => 1}, D2)
    ).

claim_empty_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {Claimed, _D1} = gaffer_queue:claim(
        #{queue => test_queue, limit => 5}, D0
    ),
    ?assertEqual([], Claimed).

%--- Prune tests --------------------------------------------------------------

prune_test() ->
    D0 = gaffer_queue:new(gaffer_driver_mock, #{}),
    {#{id := Id}, D1} = gaffer_queue:insert(test_queue, #{task => 1}, #{}, D0),
    {ok, _, D2} = gaffer_queue:cancel(Id, D1),
    {Count, _D3} = gaffer_queue:prune(
        #{states => [cancelled]}, D2
    ),
    ?assertEqual(1, Count).
