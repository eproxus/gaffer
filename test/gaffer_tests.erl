-module(gaffer_tests).

-include_lib("eunit/include/eunit.hrl").

%--- Application tests --------------------------------------------------------

start_stop_test() ->
    {ok, Apps} = application:ensure_all_started(gaffer),
    [
        ?assertEqual(ok, application:stop(A))
     || A <:- Apps
    ].
