-module(gaffer).

-behaviour(application).

%% Application callbacks
-export([start/2]).
-export([stop/1]).

%% Queue management
-export([create_queue/1]).
-export([get_queue/1]).
-export([update_queue/2]).
-export([delete_queue/1]).
-export([list_queues/0]).

%% Enqueueing
-export([insert/2]).
-export([insert/3]).

%% Lifecycle
-export([cancel/2]).

%% Querying
-export([get/2]).
-export([list/1]).

-ignore_xref([
    create_queue/1,
    get_queue/1,
    update_queue/2,
    delete_queue/1,
    list_queues/0,
    insert/2,
    insert/3,
    cancel/2,
    get/2,
    list/1
]).

%--- Types --------------------------------------------------------------------

-type job_id() :: binary().
-type job_state() ::
    available
    | scheduled
    | executing
    | completed
    | failed
    | cancelled
    | discarded.
-type queue_name() :: atom().

-type timestamp() :: integer().
%% Microseconds since Unix epoch (1970-01-01T00:00:00Z).
%% Maps directly to Postgres timestamptz.

-type job() :: #{
    id := job_id(),
    queue := queue_name(),
    args := map(),
    state := job_state(),
    attempt := non_neg_integer(),
    max_attempts := pos_integer(),
    priority := non_neg_integer(),
    scheduled_at => timestamp(),
    inserted_at := timestamp(),
    attempted_at => timestamp(),
    completed_at => timestamp(),
    cancelled_at => timestamp(),
    discarded_at => timestamp(),
    errors := [job_error()],
    tags := [binary()],
    meta := map()
}.

-type job_opts() :: #{
    queue => queue_name(),
    max_attempts => pos_integer(),
    priority => non_neg_integer(),
    scheduled_at => timestamp(),
    tags => [binary()],
    meta => map()
}.

-type job_error() :: #{
    attempt := non_neg_integer(),
    error := term(),
    at := timestamp()
}.

-type queue_conf() :: #{
    name := queue_name(),
    driver => {module(), gaffer_driver:driver_state()},
    worker => module(),
    global_max_workers => pos_integer(),
    max_workers => pos_integer(),
    poll_interval => pos_integer() | infinity,
    shutdown_timeout => pos_integer(),
    max_attempts => pos_integer(),
    timeout => pos_integer(),
    backoff => pos_integer(),
    priority => non_neg_integer(),
    on_discard => queue_name()
}.

-type list_opts() :: #{
    queue => queue_name(),
    state => job_state()
}.

-type claim_opts() :: #{
    queue => queue_name(),
    limit => pos_integer()
}.

-type job_changes() :: #{
    state => job_state(),
    atom() => term()
}.

-type prune_opts() :: #{
    states => [job_state()]
}.

-export_type([
    job_id/0,
    job_state/0,
    queue_name/0,
    timestamp/0,
    job/0,
    job_opts/0,
    job_error/0,
    queue_conf/0,
    list_opts/0,
    claim_opts/0,
    job_changes/0,
    prune_opts/0
]).

%--- Application callbacks ----------------------------------------------------

start(_StartType, _StartArgs) ->
    gaffer_queues = ets:new(gaffer_queues, [
        named_table, public, set, {read_concurrency, true}
    ]),
    gaffer_sup:start_link().

stop(_State) ->
    ets:delete(gaffer_queues),
    ok.

%--- Queue management ---------------------------------------------------------

-spec create_queue(queue_conf()) -> ok | {error, already_exists}.
create_queue(#{name := Name, driver := Driver} = Conf) ->
    case ets:insert_new(gaffer_queues, {Name, Driver}) of
        true ->
            Driver1 = gaffer_queue:put_conf(Conf, Driver),
            true = ets:insert(gaffer_queues, {Name, Driver1}),
            {ok, _Pid} = gaffer_sup:start_queue(Name, Driver1),
            ok;
        false ->
            {error, already_exists}
    end.

-spec get_queue(queue_name()) -> queue_conf().
get_queue(Name) ->
    Driver = lookup(Name),
    {Conf, _Driver1} = gaffer_queue:get_conf(Name, Driver),
    Conf.

-spec update_queue(queue_name(), map()) -> ok.
update_queue(Name, Updates) ->
    Driver = lookup(Name),
    {Conf, Driver0} = gaffer_queue:get_conf(Name, Driver),
    Merged = maps:merge(Conf, maps:remove(name, Updates)),
    Driver1 = gaffer_queue:put_conf(Merged, Driver0),
    true = ets:insert(gaffer_queues, {Name, Driver1}),
    ok.

-spec delete_queue(queue_name()) -> ok.
delete_queue(Name) ->
    Driver = lookup(Name),
    true = ets:delete(gaffer_queues, Name),
    ok = gaffer_sup:stop_queue(Name),
    _Driver1 = gaffer_queue:delete_conf(Name, Driver),
    ok.

-spec list_queues() -> [queue_conf()].
list_queues() ->
    Entries = ets:tab2list(gaffer_queues),
    [queue_from_entry(E) || E <:- Entries].

queue_from_entry({Name, Driver}) ->
    {Conf, _Driver1} = gaffer_queue:get_conf(Name, Driver),
    Conf.

%--- Enqueueing ---------------------------------------------------------------

-spec insert(queue_name(), map()) ->
    job().
insert(Queue, Args) -> insert(Queue, Args, #{}).

-spec insert(queue_name(), map(), job_opts()) ->
    job().
insert(Queue, Args, Opts) ->
    gaffer_queue_proc:insert(Queue, Args, Opts).

%--- Lifecycle ----------------------------------------------------------------

-spec cancel(queue_name(), job_id()) ->
    {ok, job()} | {error, term()}.
cancel(Queue, JobId) ->
    gaffer_queue_proc:cancel(Queue, JobId).

%--- Querying -----------------------------------------------------------------

-spec get(queue_name(), job_id()) ->
    {ok, job()} | {error, term()}.
get(Queue, JobId) ->
    gaffer_queue_proc:get(Queue, JobId).

-spec list(list_opts()) -> [job()].
list(#{queue := Queue} = Opts) ->
    gaffer_queue_proc:list(Queue, Opts).

%--- Internal -----------------------------------------------------------------

-spec lookup(queue_name()) -> gaffer_queue:driver().
lookup(Name) ->
    case ets:lookup(gaffer_queues, Name) of
        [{_, Driver}] -> Driver;
        [] -> error({unknown_queue, Name})
    end.
