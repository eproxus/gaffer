-module(gaffer_queue_sup).
-moduledoc false.

-behaviour(supervisor).

% API
-ignore_xref(start_link/1).
-export([start_link/1]).
-export([ensure/1]).
-export([pid/1]).

% Callbacks
-export([init/1]).

%--- API -----------------------------------------------------------------------

-spec start_link(gaffer:queue()) -> supervisor:startlink_ret().
start_link(Name) ->
    supervisor:start_link({local, proc_name(Name)}, ?MODULE, Name).

-doc "Ensures the queue's supervisor tree is running. Returns created | updated.".
-spec ensure(gaffer:queue()) -> created | updated.
ensure(Name) ->
    case gaffer_sup:start_queue(Name) of
        {ok, _Pid} ->
            created;
        {error, {already_started, _Pid}} ->
            gaffer_queue_runner:reconfigure(Name),
            gaffer_queue_pruner:reconfigure(Name),
            updated
    end.

-spec pid(gaffer:queue()) -> pid().
pid(Name) ->
    Pid = erlang:whereis(proc_name(Name)),
    true = is_pid(Pid),
    Pid.

%--- Callbacks -----------------------------------------------------------------

init(Name) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },
    Children = [
        #{
            id => gaffer_queue_runner,
            start => {gaffer_queue_runner, start_link, [Name]},
            restart => transient,
            shutdown => 5_000
        },
        #{
            id => gaffer_queue_pruner,
            start => {gaffer_queue_pruner, start_link, [Name]},
            restart => transient,
            shutdown => 5_000
        }
    ],
    {ok, {SupFlags, Children}}.

%--- Internal ------------------------------------------------------------------

proc_name(Name) ->
    % elp:ignore W0023 - bounded by queue count, not user input
    binary_to_atom(
        <<"gaffer_queue_sup_", (atom_to_binary(Name))/binary>>
    ).
