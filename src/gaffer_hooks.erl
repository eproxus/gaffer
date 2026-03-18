-module(gaffer_hooks).

% Behaviour
-callback gaffer_hook(pre | post, gaffer:event(), term()) -> term().

% API
-export([with_hooks/4]).

%--- API -----------------------------------------------------------------------

-spec with_hooks(
    gaffer:hooks(),
    gaffer:event(),
    term(),
    fun((term()) -> term())
) ->
    term().
with_hooks(QueueHooks, Event, Data, Fun) ->
    case resolve(QueueHooks) of
        [] ->
            Fun(Data);
        Hooks ->
            Data1 = run_hooks(pre, Hooks, Event, Data),
            Data2 = Fun(Data1),
            run_hooks(post, Hooks, Event, Data2)
    end.

%--- Internal ------------------------------------------------------------------

resolve(QueueHooks) ->
    application:get_env(gaffer, hooks, []) ++ QueueHooks.

run_hooks(_Phase, [], _Event, Acc) ->
    Acc;
run_hooks(Phase, [Hook | Rest], Event, Acc) ->
    Acc1 =
        try
            call_hook(Hook, Phase, Event, Acc)
        catch
            Class:Reason:Stack ->
                logger:warning(
                    ~"Hook ~p crashed: ~p:~p~n~p",
                    [Hook, Class, Reason, Stack]
                ),
                Acc
        end,
    run_hooks(Phase, Rest, Event, Acc1).

call_hook(Fun, Phase, Event, Data) when is_function(Fun, 3) ->
    Fun(Phase, Event, Data);
call_hook(Mod, Phase, Event, Data) when is_atom(Mod) ->
    Mod:gaffer_hook(Phase, Event, Data).
