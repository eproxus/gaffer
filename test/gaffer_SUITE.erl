-module(gaffer_SUITE).

-behaviour(ct_suite).

-include_lib("stdlib/include/assert.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [
        Fun
     || {Fun, 1} <:- ?MODULE:module_info(exports),
        re:run(atom_to_binary(Fun), <<"_test$">>) =/= nomatch
    ].

%--- Tests ---------------------------------------------------------------------

start_stop_test(_Config) ->
    Start = application:ensure_all_started(gaffer),
    ?assertMatch({ok, _}, Start),
    {ok, Apps} = Start,
    [?assertEqual(ok, application:stop(A)) || A <:- Apps].
