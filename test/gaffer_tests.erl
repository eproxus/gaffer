-module(gaffer_tests).

-include_lib("eunit/include/eunit.hrl").

%--- Fixture ------------------------------------------------------------------

gaffer_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Driver) ->
        {inparallel, [
            {with, Driver, [T]}
         || T <:- [
                fun start_stop/1,
                fun create_queue/1,
                fun get_queue/1,
                fun update_queue/1,
                fun delete_queue/1,
                fun list_queues/1,
                fun insert/1,
                fun insert_with_opts/1,
                fun cancel/1,
                fun get_job/1,
                fun list_jobs/1
            ]
        ]}
    end}.

setup() ->
    {ok, _} = application:ensure_all_started(gaffer),
    DS = gaffer_driver_ets:start(#{}),
    {gaffer_driver_ets, DS}.

cleanup({gaffer_driver_ets, DS}) ->
    gaffer_driver_ets:stop(DS),
    application:stop(gaffer).

%--- Application tests --------------------------------------------------------

start_stop(_Driver) -> ok.

%--- Queue management tests ---------------------------------------------------

create_queue(Driver) ->
    Conf = #{name => ?FUNCTION_NAME, driver => Driver},
    ?assertEqual(ok, gaffer:create_queue(Conf)),
    ?assertMatch(
        #{name := create_queue},
        gaffer:get_queue(?FUNCTION_NAME)
    ),
    ?assertEqual(
        {error, already_exists}, gaffer:create_queue(Conf)
    ).

get_queue(Driver) ->
    Conf = #{name => ?FUNCTION_NAME, driver => Driver},
    ok = gaffer:create_queue(Conf),
    ?assertEqual(Conf, gaffer:get_queue(?FUNCTION_NAME)).

update_queue(Driver) ->
    ok = gaffer:create_queue(#{
        name => ?FUNCTION_NAME,
        driver => Driver,
        global_max_workers => 5
    }),
    ok = gaffer:update_queue(?FUNCTION_NAME, #{global_max_workers => 10}),
    Updated = gaffer:get_queue(?FUNCTION_NAME),
    ?assertEqual(10, maps:get(global_max_workers, Updated)),
    ?assertEqual(Driver, maps:get(driver, Updated)).

delete_queue(Driver) ->
    ok = gaffer:create_queue(
        #{name => ?FUNCTION_NAME, driver => Driver}
    ),
    ?assertEqual(ok, gaffer:delete_queue(?FUNCTION_NAME)),
    ?assertError(
        {unknown_queue, delete_queue},
        gaffer:get_queue(?FUNCTION_NAME)
    ),
    ?assertError(
        {unknown_queue, delete_queue},
        gaffer:delete_queue(?FUNCTION_NAME)
    ).

list_queues(Driver) ->
    ok = gaffer:create_queue(
        #{name => list_queues_1, driver => Driver}
    ),
    ok = gaffer:create_queue(
        #{name => list_queues_2, driver => Driver}
    ),
    Queues = gaffer:list_queues(),
    Names = [Name || {Name, _} <:- Queues],
    ?assert(lists:member(list_queues_1, Names)),
    ?assert(lists:member(list_queues_2, Names)).

%--- Enqueueing tests ---------------------------------------------------------

insert(Driver) ->
    ok = gaffer:create_queue(#{name => ?FUNCTION_NAME, driver => Driver}),
    Job = gaffer:insert(?FUNCTION_NAME, #{task => 1}),
    ?assertMatch(
        #{
            queue := insert,
            payload := #{task := 1},
            state := available,
            priority := 0,
            max_attempts := 3,
            attempt := 0
        },
        Job
    ).

insert_with_opts(Driver) ->
    ok = gaffer:create_queue(#{name => ?FUNCTION_NAME, driver => Driver}),
    Opts = #{priority => 5, max_attempts => 10},
    Job = gaffer:insert(?FUNCTION_NAME, #{task => 1}, Opts),
    ?assertMatch(
        #{
            queue := insert_with_opts,
            payload := #{task := 1},
            priority := 5,
            max_attempts := 10
        },
        Job
    ).

%--- Lifecycle tests ----------------------------------------------------------

cancel(Driver) ->
    ok = gaffer:create_queue(
        #{name => ?FUNCTION_NAME, driver => Driver}
    ),
    #{id := Id} = gaffer:insert(?FUNCTION_NAME, #{task => 1}),
    {ok, Job} = gaffer:cancel(?FUNCTION_NAME, Id),
    ?assertMatch(#{state := cancelled, id := Id}, Job).

%--- Query tests --------------------------------------------------------------

get_job(Driver) ->
    ok = gaffer:create_queue(
        #{name => ?FUNCTION_NAME, driver => Driver}
    ),
    #{id := Id} = gaffer:insert(?FUNCTION_NAME, #{task => 1}),
    {ok, Job} = gaffer:get(?FUNCTION_NAME, Id),
    ?assertMatch(
        #{id := Id, queue := get_job, payload := #{task := 1}}, Job
    ).

list_jobs(Driver) ->
    ok = gaffer:create_queue(
        #{name => ?FUNCTION_NAME, driver => Driver}
    ),
    _ = gaffer:insert(?FUNCTION_NAME, #{task => 1}),
    _ = gaffer:insert(?FUNCTION_NAME, #{task => 2}),
    Jobs = gaffer:list(#{queue => ?FUNCTION_NAME}),
    ?assertEqual(2, length(Jobs)).
