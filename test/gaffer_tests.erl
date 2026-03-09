-module(gaffer_tests).

-include_lib("eunit/include/eunit.hrl").

%--- Tests ---------------------------------------------------------------------

app_test() -> ?assertEqual(ok, gaffer:stop(normal)).
