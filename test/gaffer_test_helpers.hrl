-define(assertHook(Event, Pattern),
    receive
        {gaffer_hook, Event, Pattern} -> ok
    after 5000 -> error({hook_not_fired, ??Event, ??Pattern})
    end
).
