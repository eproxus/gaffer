-module(gaffer).
-moduledoc "Main API for managing job queues.".

-hank([{unnecessary_function_arguments, [drain, flush]}]).

-behaviour(application).

% Application Callbacks
-export([start/2]).
-export([stop/1]).

% API
% Queue management
-ignore_xref(create_queue/1).
-export([create_queue/1]).
-ignore_xref(ensure_queue/1).
-export([ensure_queue/1]).
-ignore_xref(get_queue/1).
-export([get_queue/1]).
-ignore_xref(update_queue/2).
-export([update_queue/2]).
-ignore_xref(delete_queue/1).
-export([delete_queue/1]).
-ignore_xref(delete_queue/2).
-export([delete_queue/2]).
-ignore_xref(orphaned_queues/1).
-export([orphaned_queues/1]).
-ignore_xref(list_queues/0).
-export([list_queues/0]).
% Enqueueing
-ignore_xref(insert/2).
-export([insert/2]).
-ignore_xref(insert/3).
-export([insert/3]).
% Job Lifecycle
-ignore_xref(cancel/2).
-export([cancel/2]).
-ignore_xref(drain/1).
-export([drain/1]).
-ignore_xref(drain/2).
-export([drain/2]).
-ignore_xref(flush/1).
-export([flush/1]).
-ignore_xref(flush/2).
-export([flush/2]).
-ignore_xref(prune/1).
-export([prune/1]).
% Queue Introspection
-ignore_xref(info/1).
-export([info/1]).
% Querying
-ignore_xref(get/2).
-export([get/2]).
-ignore_xref(list/1).
-export([list/1]).
-ignore_xref(list/2).
-export([list/2]).
-ignore_xref(delete/2).
-export([delete/2]).

%--- Types ---------------------------------------------------------------------

-doc #{group => "Job Types"}.
-doc "Unique job identifier.".
-type job_id() :: keysmith:uuid().
-doc #{group => "Job Types"}.
-doc "Possible states of a job.".
-type job_state() ::
    available
    | executing
    | completed
    | cancelled
    | discarded.
-doc #{group => "Queue Types"}.
-doc "Queue identifier.".
-type queue() :: atom().

-doc #{group => "Job Types"}.
-doc """
Job timestamp.

An `erlang:system_time/0` integer or a `{Unit, Value}` pair.
""".
-type timestamp() :: integer() | {erlang:time_unit(), integer()}.

-doc #{group => "Job Types"}.
-doc "An age in milliseconds.".
-type age() :: non_neg_integer() | infinity.

-doc #{group => "Job Types"}.
-doc "Maximum execution attempts for a job.".
-type max_attempts() :: pos_integer().
-doc #{group => "Job Types"}.
-doc "Job priority. Higher values are processed first.".
-type priority() :: integer().
-doc #{group => "Job Types"}.
-doc "Execution timeout in milliseconds.".
-type timeout_ms() :: pos_integer().
-doc #{group => "Job Types"}.
-doc "Retry backoff strategy.".
-type backoff() :: non_neg_integer() | [non_neg_integer()].
-doc #{group => "Job Types"}.
-doc "Grace period for worker shutdown in milliseconds.".
-type shutdown_timeout() :: pos_integer().

-doc #{group => "Job Types"}.
-doc "A job.".
-type job() :: #{
    id := job_id(),
    queue := queue(),
    payload := term(),
    state := job_state(),
    attempt := non_neg_integer(),
    max_attempts := max_attempts(),
    priority := priority(),
    timeout := timeout_ms(),
    backoff := backoff(),
    shutdown_timeout := shutdown_timeout(),
    result => term(),
    scheduled_at => timestamp(),
    inserted_at := timestamp(),
    attempted_at => timestamp(),
    completed_at => timestamp(),
    cancelled_at => timestamp(),
    discarded_at => timestamp(),
    errors := [job_error()]
}.

-doc #{group => "Job Types"}.
-doc "Per-job options at insert time.".
-type job_opts() :: #{
    queue => queue(),
    max_attempts => max_attempts(),
    priority => priority(),
    timeout => timeout_ms(),
    backoff => backoff(),
    shutdown_timeout => shutdown_timeout(),
    scheduled_at => timestamp()
}.

-doc #{group => "Job Types"}.
-doc "A recorded execution error.".
-type job_error() :: #{
    attempt := non_neg_integer(),
    error := term(),
    at := timestamp()
}.

-doc #{group => "Queue Types"}.
-doc "Maximum number of concurrent workers.".
-type max_workers() :: pos_integer() | infinity.

-doc #{group => "Queue Types"}.
-doc "An interval in milliseconds.".
-type interval() :: pos_integer() | infinity.

-doc #{group => "Queue Types"}.
-doc "Maximum age per job state, in milliseconds.".
-type max_age() :: #{job_state() | '_' => age()}.

-doc #{group => "Queue Types"}.
-doc """
Pruning configuration for a queue.

When set, a per-queue pruner process periodically deletes jobs in terminal
states older than the configured `max_age` (in milliseconds).
""".
-type prune_conf() :: #{interval := interval(), max_age => max_age()}.

-doc #{group => "Queue Types"}.
-doc "Queue configuration.".
-type queue_conf() :: #{
    name := queue(),
    driver => gaffer_driver:driver(),
    worker := gaffer_worker:worker(),
    global_max_workers => max_workers(),
    max_workers => max_workers(),
    poll_interval => interval(),
    shutdown_timeout => shutdown_timeout(),
    max_attempts => max_attempts(),
    timeout => timeout_ms(),
    backoff => backoff(),
    priority => priority(),
    on_discard => queue(),
    hooks => [gaffer_hooks:hook()],
    prune => prune_conf()
}.

-doc #{group => "Queue Types"}.
-doc "Information about a job state.".
-type state_info() :: #{
    count := non_neg_integer(),
    oldest => timestamp(),
    newest => timestamp()
}.

-doc #{group => "Queue Types"}.
-doc "Information about a queue.".
-type queue_info() :: #{
    jobs := #{
        available := state_info(),
        executing := state_info(),
        completed := state_info(),
        cancelled := state_info(),
        discarded := state_info()
    },
    workers := #{
        active := non_neg_integer(),
        max := #{local := max_workers(), global := max_workers()}
    }
}.

-doc #{group => "Job Types"}.
-doc "Filter options for listing jobs.".
-type job_filter() :: #{
    state => job_state()
}.

-export_type([job_id/0]).
-export_type([job_state/0]).
-export_type([queue/0]).
-export_type([timestamp/0]).
-export_type([age/0]).
-export_type([max_attempts/0]).
-export_type([priority/0]).
-export_type([timeout_ms/0]).
-export_type([backoff/0]).
-export_type([shutdown_timeout/0]).
-export_type([job/0]).
-export_type([job_opts/0]).
-export_type([job_error/0]).
-export_type([max_workers/0]).
-export_type([interval/0]).
-export_type([max_age/0]).
-export_type([prune_conf/0]).
-export_type([queue_conf/0]).
-export_type([job_filter/0]).
-export_type([state_info/0]).
-export_type([queue_info/0]).

%--- Application Callbacks -----------------------------------------------------

-doc false.
start(_StartType, _StartArgs) ->
    gaffer_queue:init(),
    _ = gaffer_driver_ets:start(#{}),
    gaffer_sup:start_link().

-doc false.
stop(_State) ->
    gaffer_queue:teardown(),
    {_, DS} = gaffer_driver:lookup(ets),
    gaffer_driver_ets:stop(DS).

%--- API -----------------------------------------------------------------------

% Queue management

-doc #{group => "Queue Management"}.
-doc "Creates a new queue.".
-spec create_queue(queue_conf()) -> ok | {error, already_exists}.
create_queue(Conf) ->
    gaffer_queue:create(Conf).

-doc #{group => "Queue Management"}.
-doc "Creates a queue or updates it if it already exists.".
-spec ensure_queue(queue_conf()) -> ok.
ensure_queue(Conf) ->
    gaffer_queue:ensure(Conf).

-doc #{group => "Queue Management"}.
-doc "Gets the configuration of a queue.".
-spec get_queue(queue()) -> queue_conf().
get_queue(Name) ->
    gaffer_queue:get(Name).

-doc #{group => "Queue Management"}.
-doc "Updates the configuration of a queue.".
-spec update_queue(queue(), map()) -> ok.
update_queue(Name, Updates) ->
    gaffer_queue:update(Name, Updates).

-doc #{group => "Queue Management"}.
-doc "Deletes a queue.".
-spec delete_queue(queue()) -> ok.
delete_queue(Name) ->
    gaffer_queue:delete(Name).

-doc #{group => "Queue Management"}.
-doc """
Deletes a queue using an explicit driver.

Also works for orphaned queues not initialized at runtime, as returned by
`orphaned_queues/1`.
""".
-spec delete_queue(queue(), gaffer_driver:driver()) -> ok.
delete_queue(Name, Driver) -> gaffer_queue:delete(Name, Driver).

-doc #{group => "Queue Management"}.
-doc "Lists queues in storage that are not in the runtime configuration.".
-spec orphaned_queues(gaffer_driver:driver()) -> [queue()].
orphaned_queues(Driver) -> gaffer_queue:orphaned(Driver).

-doc #{group => "Queue Management"}.
-doc "Lists all queues.".
-spec list_queues() -> [{queue(), queue_conf()}].
list_queues() ->
    gaffer_queue:list().

% Enqueueing

-doc #{group => "Job Management"}.
-doc #{equiv => insert(Queue, Payload, #{})}.
-spec insert(queue(), term()) -> job().
insert(Queue, Payload) ->
    insert(Queue, Payload, #{}).

-doc #{group => "Job Management"}.
-doc "Inserts a job into a queue.".
-spec insert(queue(), term(), job_opts()) -> job().
insert(Queue, Payload, Opts) ->
    gaffer_queue:insert_job(Queue, Payload, Opts).

% Job Lifecycle

-doc #{group => "Job Lifecycle"}.
-doc "Cancels a job, preventing further execution.".
-spec cancel(queue(), job_id()) ->
    {ok, job()} | {error, {invalid_transition, term()}}.
cancel(Queue, ID) ->
    case gaffer_queue:cancel_job(Queue, ID) of
        {error, not_found} -> error({unknown_job, ID});
        {error, {invalid_transition, _}} = Err -> Err;
        {ok, _} = Ok -> Ok
    end.

-doc #{group => "Job Lifecycle"}.
-doc #{equiv => drain(Queue, 5000)}.
-spec drain(queue()) -> ok.
drain(Queue) ->
    drain(Queue, 5000).

-doc #{group => "Job Lifecycle"}.
-doc "Waits for the active workers to finish their jobs.".
-spec drain(queue(), timeout()) -> ok.
% elp:ignore W0048 - stubs (see TODO below)
-dialyzer([
    {no_return, [drain/1, drain/2, flush/1, flush/2]},
    {no_contracts, [drain/1, drain/2, flush/1, flush/2]}
]).
drain(_Queue, _Timeout) ->
    % TODO: stop claiming, wait for in-flight workers
    error(not_implemented).

-doc #{group => "Job Lifecycle"}.
-doc #{equiv => flush(Queue, infinity)}.
-spec flush(queue()) -> ok.
flush(Queue) ->
    flush(Queue, infinity).

-doc #{group => "Job Lifecycle"}.
-doc "Waits for *all* jobs in the queue to finish.".
-spec flush(queue(), timeout()) -> ok.
flush(_Queue, _Timeout) ->
    % TODO: process all remaining items in the queue until empty
    error(not_implemented).

-doc #{group => "Job Lifecycle"}.
-doc "Triggers an immediate prune of stale jobs in the given queue.".
-spec prune(queue()) -> [job_id()].
prune(Queue) -> gaffer_queue_pruner:prune(Queue).

% Queue Introspection

-doc #{group => "Queue Introspection"}.
-doc "Returns current queue information.".
-spec info(queue()) -> queue_info().
info(Queue) ->
    gaffer_queue:info(Queue).

% Querying

-doc #{group => "Job Management"}.
-doc "Gets the definition of a job.".
-spec get(queue(), job_id()) -> job().
get(Queue, ID) ->
    case gaffer_queue:get_job(Queue, ID) of
        not_found -> error({unknown_job, ID});
        Job -> Job
    end.

-doc #{group => "Job Management"}.
-doc "Lists all jobs in the given queue.".
-spec list(queue()) -> [job()].
list(Queue) -> list(Queue, #{}).

-doc #{group => "Job Management"}.
-doc "Lists jobs in the given queue matching the filter options.".
-spec list(queue(), job_filter()) -> [job()].
list(Queue, Filters) ->
    gaffer_queue:list_jobs(Filters#{queue => Queue}).

-doc #{group => "Job Management"}.
-doc "Deletes a job.".
-spec delete(queue(), job_id()) -> ok.
delete(Queue, ID) ->
    gaffer_queue:delete_job(Queue, ID).
