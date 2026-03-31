-module(gaffer_queue_pruner).
-moduledoc false.

-behaviour(gen_statem).

% API
-ignore_xref(start_link/1).
-export([start_link/1]).
-ignore_xref(prune/1).
-export([prune/1]).
-ignore_xref(reconfigure/1).
-export([reconfigure/1]).

% gen_statem Callbacks
-export([callback_mode/0]).
-export([init/1]).
-ignore_xref(active/3).
-export([active/3]).

%--- API -----------------------------------------------------------------------

start_link(Name) ->
    gen_statem:start_link({local, proc_name(Name)}, ?MODULE, Name, []).

prune(Name) -> call(Name, prune).

reconfigure(Name) -> call(Name, reconfigure).

%--- gen_statem Callbacks ------------------------------------------------------

callback_mode() -> state_functions.

init(Name) ->
    #{prune := #{interval := Interval}} = gaffer_queue:conf(Name),
    {ok, active, #{name => Name}, [{state_timeout, Interval, prune}]}.

active({call, From}, prune, #{name := Name} = Data) ->
    {IDs, Actions} = do_prune(Name),
    {keep_state, Data, [{reply, From, IDs} | Actions]};
active(state_timeout, prune, #{name := Name} = Data) ->
    {_IDs, Actions} = do_prune(Name),
    {keep_state, Data, Actions};
active({call, From}, reconfigure, #{name := Name}) ->
    #{prune := #{interval := Interval}} = gaffer_queue:conf(Name),
    {keep_state_and_data, [{reply, From, ok}, {state_timeout, Interval, prune}]}.

%--- Internal ------------------------------------------------------------------

call(Name, Msg) -> gen_statem:call(proc_name(Name), Msg).

do_prune(Name) ->
    #{prune := #{max_age := MaxAge, interval := Interval}} =
        gaffer_queue:conf(Name),
    {gaffer_queue:prune_jobs(Name, MaxAge), [{state_timeout, Interval, prune}]}.

proc_name(Name) ->
    % elp:ignore W0023 - bounded by queue count, not user input
    binary_to_atom(
        <<"gaffer_queue_pruner_", (atom_to_binary(Name))/binary>>
    ).
