-module(gaffer_worker).
-moduledoc "Worker behaviour for gaffer job queues.".

% API
-export([perform/2]).

-doc "Executes a `t:gaffer:job/0`.".
-callback perform(Job :: gaffer:job()) ->
    complete
    | {complete, term()}
    | {fail, term()}
    | {cancel, binary()}
    | {schedule, gaffer:timestamp()}.

%--- API -----------------------------------------------------------------------

-doc false.
-spec perform(module(), gaffer:job()) ->
    complete
    | {complete, term()}
    | {fail, term()}
    | {cancel, binary()}
    | {schedule, gaffer:timestamp()}.
perform(Mod, Job) ->
    Mod:perform(Job).
