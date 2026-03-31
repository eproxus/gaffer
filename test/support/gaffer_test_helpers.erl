-module(gaffer_test_helpers).

-export([harness/3]).
-export([notify_hook/2, await_hook/0, await_hooks/1]).
-export([pgo_pool_config/0, reset_database/1, stop_pool/1]).
-export([normalize/1]).
-export([wait_for/2, wait_for/3]).

%--- API ----------------------------------------------------------------------

harness(DriverMod, Parallel, Sequential) ->
    Setup = fun() -> setup(DriverMod) end,
    Teardown = fun teardown/1,
    [
        {setup, Setup, Teardown, fun(#{driver := Driver}) ->
            {inparallel, [{with, Driver, [T]} || T <:- Parallel]}
        end},
        {setup, Setup, Teardown, fun(#{driver := Driver}) ->
            [{with, Driver, [T]} || T <:- Sequential]
        end}
    ].

notify_hook(Pid, Events) ->
    fun
        (post, Event, Data) ->
            case lists:member(Event, Events) of
                true -> Pid ! {gaffer_hook, Event, Data};
                false -> ok
            end,
            Data;
        (pre, _Event, Data) ->
            Data
    end.

await_hook() ->
    receive
        {gaffer_hook, _Event, _Data} -> ok
    after 5000 -> error(timeout)
    end.

await_hooks(0) ->
    ok;
await_hooks(N) ->
    await_hook(),
    await_hooks(N - 1).

pgo_pool_config() ->
    {ok, Props} = application:get_env(gaffer, postgres),
    Config = maps:with(
        [host, port, database, user, password], maps:from_list(Props)
    ),
    Config#{pool_size => 2}.

reset_database(Pool) ->
    Opts = #{pool => Pool},
    pgo:query(~"DROP SCHEMA public CASCADE", [], Opts),
    pgo:query(~"CREATE SCHEMA public", [], Opts),
    ok.

stop_pool(Pool) ->
    case whereis(Pool) of
        undefined -> ok;
        Pid -> supervisor:terminate_child(pgo_sup, Pid)
    end.

wait_for(Fun, Init) -> wait_for(Fun, Init, #{}).

wait_for(Fun, Init, Opts) ->
    Timeout = maps:get(timeout, Opts, 5000),
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_loop(Fun, Init, maps:get(interval, Opts, 10), Deadline).

wait_loop(Fun, State, Interval, Deadline) ->
    case Fun(State) of
        {result, Result} ->
            Result;
        {wait, NewState} ->
            case erlang:monotonic_time(millisecond) < Deadline of
                true ->
                    timer:sleep(Interval),
                    wait_loop(Fun, NewState, Interval, Deadline);
                false ->
                    error(timeout)
            end
    end.

normalize(Map) when is_map(Map) ->
    maps:fold(
        fun(K, V, Acc) -> Acc#{normalize(K) => normalize(V)} end, #{}, Map
    );
normalize(List) when is_list(List) ->
    [normalize(E) || E <:- List];
normalize(B) when is_binary(B) ->
    try
        binary_to_existing_atom(B)
    catch
        error:badarg -> B
    end;
normalize(Other) ->
    Other.

%--- Internal -----------------------------------------------------------------

setup(DriverMod) ->
    error_logger:tty(false),
    {Driver, Apps0} = setup_driver(DriverMod),
    {ok, Apps1} = application:ensure_all_started(gaffer),
    #{driver => Driver, gaffer_apps => Apps1, driver_apps => Apps0}.

teardown(#{
    driver := Driver, gaffer_apps := GafferApps, driver_apps := DriverApps
}) ->
    [application:stop(A) || A <:- lists:reverse(GafferApps)],
    teardown_driver(Driver),
    [application:stop(A) || A <:- lists:reverse(DriverApps)],
    error_logger:tty(true).

setup_driver(gaffer_driver_ets) ->
    DS = gaffer_driver_ets:start(#{}),
    {{gaffer_driver_ets, DS}, []};
setup_driver(gaffer_driver_pgo) ->
    {ok, Apps} = application:ensure_all_started(pgo),
    stop_pool(test_pool),
    {ok, _} = pgo:start_pool(test_pool, pgo_pool_config()),
    reset_database(test_pool),
    DS = gaffer_driver_pgo:start(#{pool => test_pool}),
    {{gaffer_driver_pgo, DS}, Apps}.

teardown_driver({gaffer_driver_ets, DS}) ->
    gaffer_driver_ets:stop(DS);
teardown_driver({gaffer_driver_pgo, DS}) ->
    gaffer_driver_pgo:stop(DS),
    reset_database(test_pool),
    stop_pool(test_pool).
