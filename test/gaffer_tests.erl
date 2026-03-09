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
                fun retry/1,
                fun drain/1,
                fun get_job/1,
                fun list_jobs/1
            ]
        ]}
    end}.

setup() ->
    {ok, _} = application:ensure_all_started(gaffer),
    {ok, DS} = gaffer_driver_ets:start(#{}),
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
        {ok, #{name := create_queue}},
        gaffer:get_queue(?FUNCTION_NAME)
    ),
    ?assertEqual(
        {error, already_exists}, gaffer:create_queue(Conf)
    ).

get_queue(Driver) ->
    Conf = #{name => ?FUNCTION_NAME, driver => Driver},
    ok = gaffer:create_queue(Conf),
    ?assertEqual({ok, Conf}, gaffer:get_queue(?FUNCTION_NAME)).

update_queue(Driver) ->
    ok = gaffer:create_queue(#{
        name => ?FUNCTION_NAME,
        driver => Driver,
        concurrency => 5
    }),
    ok = gaffer:update_queue(?FUNCTION_NAME, #{concurrency => 10}),
    {ok, Updated} = gaffer:get_queue(?FUNCTION_NAME),
    ?assertEqual(10, maps:get(concurrency, Updated)),
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
    {ok, Queues} = gaffer:list_queues(),
    Names = [maps:get(name, Q) || Q <:- Queues],
    ?assert(lists:member(list_queues_1, Names)),
    ?assert(lists:member(list_queues_2, Names)).

%--- Enqueueing tests ---------------------------------------------------------

insert(_Driver) ->
    ?assertError(
        not_implemented,
        gaffer:insert(test_q, #{task => 1})
    ).

insert_with_opts(_Driver) ->
    Opts = #{priority => 5, max_attempts => 10},
    ?assertError(
        not_implemented,
        gaffer:insert(test_q, #{task => 1}, Opts)
    ).

%--- Lifecycle tests ----------------------------------------------------------

cancel(_Driver) ->
    ?assertError(
        not_implemented, gaffer:cancel(<<"job-1">>)
    ).

retry(_Driver) ->
    ?assertError(
        not_implemented, gaffer:retry(<<"job-1">>)
    ).

drain(_Driver) ->
    ?assertEqual(
        #{completed => 0, failed => 0},
        gaffer:drain(#{queue => test_q})
    ).

%--- Query tests --------------------------------------------------------------

get_job(_Driver) ->
    ?assertError(
        not_implemented, gaffer:get(<<"job-1">>)
    ).

list_jobs(_Driver) ->
    ?assertError(
        not_implemented,
        gaffer:list(#{queue => test_q})
    ).
