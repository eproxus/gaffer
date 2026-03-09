-module(gaffer_tests).

-include_lib("eunit/include/eunit.hrl").

%--- Application tests --------------------------------------------------------

start_stop_test() ->
    with_app(fun() -> ok end).

%--- Queue management tests ---------------------------------------------------

create_queue_test() ->
    with_app(fun() ->
        Conf = #{name => test_q, concurrency => 5},
        ?assertError(not_implemented, gaffer:create_queue(Conf))
    end).

get_queue_test() ->
    with_app(fun() ->
        ?assertError(not_implemented, gaffer:get_queue(test_q))
    end).

update_queue_test() ->
    with_app(fun() ->
        ?assertError(
            not_implemented,
            gaffer:update_queue(test_q, #{concurrency => 10})
        )
    end).

delete_queue_test() ->
    with_app(fun() ->
        ?assertError(
            not_implemented, gaffer:delete_queue(test_q)
        )
    end).

list_queues_test() ->
    with_app(fun() ->
        ?assertError(not_implemented, gaffer:list_queues())
    end).

%--- Enqueueing tests ---------------------------------------------------------

insert_test() ->
    with_app(fun() ->
        ?assertError(
            not_implemented,
            gaffer:insert(test_q, #{task => 1})
        )
    end).

insert_with_opts_test() ->
    with_app(fun() ->
        Opts = #{priority => 5, max_attempts => 10},
        ?assertError(
            not_implemented,
            gaffer:insert(test_q, #{task => 1}, Opts)
        )
    end).

%--- Lifecycle tests ----------------------------------------------------------

cancel_test() ->
    with_app(fun() ->
        ?assertError(
            not_implemented, gaffer:cancel(<<"job-1">>)
        )
    end).

retry_test() ->
    with_app(fun() ->
        ?assertError(
            not_implemented, gaffer:retry(<<"job-1">>)
        )
    end).

drain_test() ->
    with_app(fun() ->
        ?assertEqual(
            #{completed => 0, failed => 0},
            gaffer:drain(#{queue => test_q})
        )
    end).

%--- Query tests --------------------------------------------------------------

get_test() ->
    with_app(fun() ->
        ?assertError(
            not_implemented, gaffer:get(<<"job-1">>)
        )
    end).

list_test() ->
    with_app(fun() ->
        ?assertError(
            not_implemented,
            gaffer:list(#{queue => test_q})
        )
    end).

%--- Helpers ------------------------------------------------------------------

with_app(Fun) ->
    {ok, Apps} = application:ensure_all_started(gaffer),
    try
        Fun()
    after
        [application:stop(A) || A <:- lists:reverse(Apps)]
    end.
