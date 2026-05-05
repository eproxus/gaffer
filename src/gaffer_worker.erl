-module(gaffer_worker).
-moduledoc "Worker behaviour for gaffer job queues.".

-export([perform/2]).
-export_type([worker/0, result/0]).

-doc "The result of executing a job.".
-type result() ::
    complete
    | {complete, term()}
    | {fail, term()}
    | {cancel, binary()}
    | {schedule, gaffer:timestamp()}.

-doc "A worker is either a callback module or a fun.".
-type worker() :: module() | fun((gaffer:job()) -> result()).

-doc "Executes a `t:gaffer:job/0`.".
-callback perform(Job :: gaffer:job()) -> result().

%--- API -----------------------------------------------------------------------

-doc false.
-spec perform(worker(), gaffer:job()) -> result().
perform(Worker, Job) when is_function(Worker, 1) ->
    validate_result(Worker(Job));
perform(Mod, Job) when is_atom(Mod) ->
    validate_result(Mod:perform(Job)).

validate_result(complete) -> complete;
validate_result({complete, _} = R) -> R;
validate_result({fail, _} = R) -> R;
validate_result({cancel, B} = R) when is_binary(B) -> R;
validate_result({schedule, T} = R) when is_integer(T) -> R;
validate_result({schedule, {TU, T}} = R) when is_integer(T), is_atom(TU) -> R;
validate_result(R) -> {fail, {invalid_worker_result, R}}.
