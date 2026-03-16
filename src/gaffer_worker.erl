-module(gaffer_worker).

% Worker behaviour for gaffer job queues.
%
% Implement the `perform/1` callback to define how a job executes.
% Operational parameters (max_attempts, timeout, backoff) come from
% persisted queue config or per-job opts at insert time.

% API
-export([perform/2]).

-callback perform(Job :: gaffer:job()) ->
    complete
    | {complete, term()}
    | {fail, term()}
    | {cancel, binary()}
    | {schedule, gaffer:timestamp()}.

%--- API -----------------------------------------------------------------------

-spec perform(module(), gaffer:job()) ->
    complete
    | {complete, term()}
    | {fail, term()}
    | {cancel, binary()}
    | {schedule, gaffer:timestamp()}.
perform(Mod, Job) ->
    Mod:perform(Job).
