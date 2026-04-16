-module(gaffer_test_driver).
-moduledoc false.

-behaviour(gaffer_driver).

% API
-export([wrap/2]).
% gaffer_driver callbacks
-export([start/1, stop/1]).
-export([queue_insert/2, queue_exists/2, queue_list/1, queue_delete/2]).
-export([job_write/2, job_get/2, job_list/2, job_delete/2]).
-export([job_claim/3, job_prune/3]).
-export([info/2]).

%--- API ----------------------------------------------------------------------

wrap({_Mod, _DS} = Driver, Overrides) ->
    {?MODULE, #{inner => Driver, overrides => Overrides}}.

%--- gaffer_driver callbacks --------------------------------------------------

start(_) -> error(use_wrap).
stop(#{inner := {Mod, DS}}) -> Mod:stop(DS).

queue_insert(Name, S) -> dispatch(?FUNCTION_NAME, [Name], S).
queue_exists(Name, S) -> dispatch(?FUNCTION_NAME, [Name], S).
queue_list(S) -> dispatch(?FUNCTION_NAME, [], S).
queue_delete(Name, S) -> dispatch(?FUNCTION_NAME, [Name], S).
job_write(Jobs, S) -> dispatch(?FUNCTION_NAME, [Jobs], S).
job_get(ID, S) -> dispatch(?FUNCTION_NAME, [ID], S).
job_list(Opts, S) -> dispatch(?FUNCTION_NAME, [Opts], S).
job_delete(ID, S) -> dispatch(?FUNCTION_NAME, [ID], S).
job_claim(Opts, Changes, S) -> dispatch(?FUNCTION_NAME, [Opts, Changes], S).
job_prune(Queue, Opts, S) -> dispatch(?FUNCTION_NAME, [Queue, Opts], S).
info(Queue, S) -> dispatch(?FUNCTION_NAME, [Queue], S).

%--- Internal -----------------------------------------------------------------

dispatch(Fun, Args, #{inner := {Mod, DS}, overrides := Overrides}) ->
    case Overrides of
        #{Fun := Override} ->
            Inner = inner_fun(Mod, Fun, DS, length(Args)),
            apply(Override, [Inner | Args]);
        _ ->
            apply(Mod, Fun, Args ++ [DS])
    end.

inner_fun(Mod, Fun, DS, 0) -> fun() -> Mod:Fun(DS) end;
inner_fun(Mod, Fun, DS, 1) -> fun(A) -> Mod:Fun(A, DS) end;
inner_fun(Mod, Fun, DS, 2) -> fun(A, B) -> Mod:Fun(A, B, DS) end;
inner_fun(Mod, Fun, DS, 3) -> fun(A, B, C) -> Mod:Fun(A, B, C, DS) end.
