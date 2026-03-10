-module(gaffer_queue_runner).

-behaviour(gen_statem).

%% Public API
-export([start_link/2]).
-export([insert/3]).
-export([get/2]).
-export([list/2]).
-export([cancel/2]).
-export([complete/2]).
-export([fail/3]).
-export([schedule/3]).
-export([claim/2]).
-export([prune/2]).

%% gen_statem callbacks
-export([callback_mode/0]).
-export([init/1]).
-export([handle_event/4]).

-ignore_xref([
    start_link/2,
    insert/3,
    get/2,
    list/2,
    cancel/2,
    complete/2,
    fail/3,
    schedule/3,
    claim/2,
    prune/2
]).

%--- Public API ---------------------------------------------------------------

-spec start_link(gaffer:queue_name(), gaffer_queue:driver()) ->
    gen_statem:start_ret().
start_link(Name, Driver) ->
    gen_statem:start_link(
        {local, proc_name(Name)},
        ?MODULE,
        {Name, Driver},
        []
    ).

-spec insert(gaffer:queue_name(), map(), gaffer:job_opts()) ->
    gaffer:job().
insert(Name, Args, Opts) ->
    gen_statem:call(
        proc_name(Name), {insert, Name, Args, Opts}
    ).

-spec get(gaffer:queue_name(), gaffer:job_id()) ->
    {ok, gaffer:job()} | {error, not_found}.
get(Name, Id) ->
    Driver =
        gen_statem:call(proc_name(Name), get_driver),
    gaffer_queue:get(Id, Driver).

-spec list(gaffer:queue_name(), gaffer:list_opts()) ->
    [gaffer:job()].
list(Name, Opts) ->
    Driver =
        gen_statem:call(proc_name(Name), get_driver),
    gaffer_queue:list(Opts, Driver).

-spec cancel(gaffer:queue_name(), gaffer:job_id()) ->
    {ok, gaffer:job()} | {error, term()}.
cancel(Name, Id) ->
    gen_statem:call(proc_name(Name), {cancel, Id}).

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

init({Name, Driver}) ->
    Data = #{name => Name, driver => Driver},
    {ok, idle, Data}.

handle_event(
    {call, From}, Cmd, _State, #{driver := Driver} = Data
) ->
    case dispatch(Cmd, Driver) of
        {mutated, Reply, NewDriver} ->
            {keep_state, Data#{driver := NewDriver}, [{reply, From, Reply}]};
        {readonly, Reply} ->
            {keep_state, Data, [{reply, From, Reply}]}
    end.

%--- Internal -----------------------------------------------------------------

dispatch({insert, Queue, Args, Opts}, Driver) ->
    {Job, D1} = gaffer_queue:insert(Queue, Args, Opts, Driver),
    {mutated, Job, D1};
dispatch({cancel, Id}, Driver) ->
    tag(gaffer_queue:cancel(Id, Driver));
dispatch({complete, Id}, Driver) ->
    tag(gaffer_queue:complete(Id, Driver));
dispatch({fail, Id, Error}, Driver) ->
    tag(gaffer_queue:fail(Id, Error, Driver));
dispatch({schedule, Id, At}, Driver) ->
    tag(gaffer_queue:schedule(Id, At, Driver));
dispatch({claim, Opts}, Driver) ->
    {Claimed, D1} = gaffer_queue:claim(Opts, Driver),
    {mutated, Claimed, D1};
dispatch({prune, Opts}, Driver) ->
    {Count, D1} = gaffer_queue:prune(Opts, Driver),
    {mutated, Count, D1};
dispatch(get_driver, Driver) ->
    {readonly, Driver}.

tag({ok, Value, D1}) -> {mutated, {ok, Value}, D1};
tag({error, _} = Err) -> {readonly, Err}.

-spec proc_name(gaffer:queue_name()) -> atom().
proc_name(Name) ->
    binary_to_atom(
        <<"gaffer_queue_runner_", (atom_to_binary(Name))/binary>>
    ).
