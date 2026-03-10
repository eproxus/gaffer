-module(gaffer_queue_tests).

-include_lib("eunit/include/eunit.hrl").

%--- Helpers ------------------------------------------------------------------

new_driver() ->
    gaffer_queue:new(gaffer_driver_mock, #{}).

insert_job(D) -> insert_job(D, #{}).
insert_job(D, Opts) ->
    gaffer_queue:insert(test_queue, #{task => 1}, Opts, D).

%--- Insert tests -------------------------------------------------------------

insert_defaults_test() ->
    D0 = new_driver(),
    {Job, _D1} = insert_job(D0),
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
    D0 = new_driver(),
    At = 1767261600000000,
    {Job, _D1} =
        insert_job(D0, #{scheduled_at => At}),
    ?assertMatch(#{state := scheduled, scheduled_at := At}, Job).

insert_with_opts_test() ->
    D0 = new_driver(),
    Opts = #{
        max_attempts => 5,
        priority => 10,
        tags => [~"urgent"],
        meta => #{source => ~"api"}
    },
    {Job, _D1} = insert_job(D0, Opts),
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
    D0 = new_driver(),
    {#{id := Id}, D1} = insert_job(D0),
    {ok, Found} = gaffer_queue:get(Id, D1),
    ?assertMatch(#{id := Id}, Found).

get_not_found_test() ->
    D0 = new_driver(),
    ?assertEqual(
        {error, not_found},
        gaffer_queue:get(~"nope", D0)
    ).

list_test() ->
    D0 = new_driver(),
    {_, D1} = insert_job(D0),
    {_, D2} = insert_job(D1),
    Jobs = gaffer_queue:list(
        #{queue => test_queue}, D2
    ),
    ?assertEqual(2, length(Jobs)).

%--- Cancel tests -------------------------------------------------------------

cancel_test() ->
    D0 = new_driver(),
    {#{id := Id}, D1} = insert_job(D0),
    {ok, Cancelled, _D2} = gaffer_queue:cancel(Id, D1),
    ?assertMatch(#{state := cancelled, cancelled_at := _}, Cancelled).

cancel_not_found_test() ->
    D0 = new_driver(),
    ?assertEqual(
        {error, not_found},
        gaffer_queue:cancel(~"nope", D0)
    ).

%--- Complete tests -----------------------------------------------------------

complete_test() ->
    D0 = new_driver(),
    {#{id := Id}, D1} = insert_job(D0),
    {[_], D2} = gaffer_queue:claim(
        #{queue => test_queue, limit => 1}, D1
    ),
    {ok, Completed, _D3} = gaffer_queue:complete(Id, D2),
    ?assertMatch(
        #{state := completed, attempt := 1, completed_at := _}, Completed
    ).

%--- Fail tests ---------------------------------------------------------------

fail_retryable_test() ->
    D0 = new_driver(),
    {#{id := Id}, D1} = insert_job(D0, #{max_attempts => 3}),
    {[_], D2} = gaffer_queue:claim(
        #{queue => test_queue, limit => 1}, D1
    ),
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
    D0 = new_driver(),
    {#{id := Id}, D1} = insert_job(D0, #{max_attempts => 1}),
    {[_], D2} = gaffer_queue:claim(
        #{queue => test_queue, limit => 1}, D1
    ),
    JobError = #{
        attempt => 1,
        error => boom,
        at => erlang:system_time(microsecond)
    },
    {ok, Discarded, _D3} = gaffer_queue:fail(
        Id, JobError, D2
    ),
    ?assertMatch(#{state := discarded, discarded_at := _}, Discarded).

%--- Schedule tests -----------------------------------------------------------

schedule_test() ->
    D0 = new_driver(),
    {#{id := Id}, D1} = insert_job(D0),
    {[_], D2} = gaffer_queue:claim(
        #{queue => test_queue, limit => 1}, D1
    ),
    FutureAt = erlang:system_time(microsecond) + 60_000_000,
    {ok, Scheduled, _D3} = gaffer_queue:schedule(
        Id, FutureAt, D2
    ),
    ?assertMatch(#{state := scheduled, scheduled_at := FutureAt}, Scheduled).

%--- Claim tests --------------------------------------------------------------

claim_test() ->
    D0 = new_driver(),
    {_, D1} = insert_job(D0),
    {_, D2} = insert_job(D1),
    {Claimed, _D3} = gaffer_queue:claim(
        #{queue => test_queue, limit => 1}, D2
    ),
    ?assertEqual(1, length(Claimed)),
    [Job] = Claimed,
    ?assertMatch(#{state := executing, attempted_at := _}, Job).

claim_empty_test() ->
    D0 = new_driver(),
    {Claimed, _D1} = gaffer_queue:claim(
        #{queue => test_queue, limit => 5}, D0
    ),
    ?assertEqual([], Claimed).

%--- Prune tests --------------------------------------------------------------

prune_test() ->
    D0 = new_driver(),
    {#{id := Id}, D1} = insert_job(D0),
    {ok, _, D2} = gaffer_queue:cancel(Id, D1),
    {Count, _D3} = gaffer_queue:prune(
        #{states => [cancelled]}, D2
    ),
    ?assertEqual(1, Count).
