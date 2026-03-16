-module(gaffer_queue_runner).

-behaviour(gen_statem).

% Public API
-export([start_link/1]).
-export([complete/2]).
-export([fail/3]).
-export([schedule/3]).
-export([claim/2]).
-export([prune/2]).

% gen_statem callbacks
-export([callback_mode/0]).
-export([init/1]).
-export([handle_event/4]).

-ignore_xref([
    start_link/1,
    complete/2,
    fail/3,
    schedule/3,
    claim/2,
    prune/2
]).

%--- Public API ---------------------------------------------------------------

-spec start_link(gaffer:queue_name()) ->
    gen_statem:start_ret().
start_link(Name) ->
    gen_statem:start_link(
        {local, proc_name(Name)},
        ?MODULE,
        Name,
        []
    ).

-spec complete(gaffer:queue_name(), gaffer:job_id()) ->
    {ok, gaffer:job()} | {error, term()}.
complete(Name, Id) ->
    gen_statem:call(proc_name(Name), {complete, Id}).

-spec fail(
    gaffer:queue_name(), gaffer:job_id(), gaffer:job_error()
) ->
    {ok, gaffer:job()} | {error, term()}.
fail(Name, Id, Error) ->
    gen_statem:call(
        proc_name(Name), {fail, Id, Error}
    ).

-spec schedule(
    gaffer:queue_name(), gaffer:job_id(), gaffer:timestamp()
) ->
    {ok, gaffer:job()} | {error, term()}.
schedule(Name, Id, At) ->
    gen_statem:call(
        proc_name(Name), {schedule, Id, At}
    ).

-spec claim(gaffer:queue_name(), gaffer:claim_opts()) ->
    [gaffer:job()].
claim(Name, Opts) ->
    gen_statem:call(proc_name(Name), {claim, Opts}).

-spec prune(gaffer:queue_name(), gaffer:prune_opts()) ->
    non_neg_integer().
prune(Name, Opts) ->
    gen_statem:call(proc_name(Name), {prune, Opts}).

%--- gen_statem callbacks -----------------------------------------------------

callback_mode() -> handle_event_function.

init(Name) ->
    Data = #{name => Name},
    {ok, idle, Data}.

handle_event(
    {call, From}, Cmd, _State, #{name := Name} = Data
) ->
    Driver = gaffer_queue:lookup(Name),
    Reply = dispatch(Cmd, Driver),
    {keep_state, Data, [{reply, From, Reply}]}.

%--- Internal -----------------------------------------------------------------

dispatch({complete, Id}, Driver) ->
    gaffer_queue:complete_job(Id, Driver);
dispatch({fail, Id, Error}, Driver) ->
    gaffer_queue:fail_job(Id, Error, Driver);
dispatch({schedule, Id, At}, Driver) ->
    gaffer_queue:schedule_job(Id, At, Driver);
dispatch({claim, Opts}, Driver) ->
    gaffer_queue:claim_jobs(Opts, Driver);
dispatch({prune, Opts}, Driver) ->
    gaffer_queue:prune_jobs(Opts, Driver).

proc_name(Name) ->
    % elp:ignore W0023 - bounded by queue count, not user input
    binary_to_atom(
        <<"gaffer_queue_runner_", (atom_to_binary(Name))/binary>>
    ).
