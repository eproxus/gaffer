-module(gaffer_hooks).
-moduledoc "Hook behaviour for observing queue and job events.".

-doc "Hook event path.".
-type event() :: [atom()].
-doc "A hook callback function.".
-type hook_fun() :: fun((event(), term()) -> term()).
-doc "A hook module or function.".
-type hook() :: module() | hook_fun().

-export_type([event/0]).
-export_type([hook_fun/0]).
-export_type([hook/0]).

-doc "Called after queue and job events with event specific payload.".
-callback gaffer_hook(event(), term()) -> term().

% API
-export([notify/3]).

%--- API -----------------------------------------------------------------------

-doc false.
-spec notify([hook()], event(), term()) -> ok.
notify(QueueHooks, Event, Data) ->
    Hooks = application:get_env(gaffer, hooks, []) ++ QueueHooks,
    lists:foreach(fun(H) -> call(H, Event, Data) end, Hooks).

%--- Internal ------------------------------------------------------------------

call(Hook, Event, Data) ->
    try
        call_hook(Hook, Event, Data)
    catch
        Class:Reason:Stack ->
            logger:warning(
                ~"Hook ~p crashed: ~p:~p~n~p",
                [Hook, Class, Reason, Stack]
            )
    end.

call_hook(Fun, Event, Data) when is_function(Fun, 2) -> Fun(Event, Data);
call_hook(Mod, Event, Data) when is_atom(Mod) -> Mod:gaffer_hook(Event, Data).
