-module(gaffer_queue_sup).
-moduledoc false.

-behaviour(supervisor).

% API
-ignore_xref(start_link/2).
-export([start_link/2]).
-export([pid/1]).

% Callbacks
-export([init/1]).

%--- API -----------------------------------------------------------------------

-spec start_link(gaffer:queue(), gaffer_queue:queue_conf()) ->
    supervisor:startlink_ret().
start_link(Name, Conf) ->
    supervisor:start_link({local, proc_name(Name)}, ?MODULE, {Name, Conf}).

-spec pid(gaffer:queue()) -> pid().
pid(Name) ->
    Pid = erlang:whereis(proc_name(Name)),
    true = is_pid(Pid),
    Pid.

%--- Callbacks -----------------------------------------------------------------

init({Name, Conf}) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },
    Children = [
        #{
            id => gaffer_queue_runner,
            start => {gaffer_queue_runner, start_link, [Name, Conf]},
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
