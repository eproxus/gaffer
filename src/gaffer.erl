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
% Queue Introspection
-ignore_xref(info/1).
-export([info/1]).
% Querying
-ignore_xref(get/2).
-export([get/2]).
-ignore_xref(list/1).
-export([list/1]).
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
    | failed
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
-doc "Maximum execution attempts for a job.".
-type max_attempts() :: pos_integer().
-doc #{group => "Job Types"}.
-doc "Job priority.".
-type priority() :: non_neg_integer().
-doc #{group => "Job Types"}.
-doc "Execution timeout in milliseconds.".
-type timeout_ms() :: pos_integer().
-doc #{group => "Job Types"}.
-doc "Retry backoff strategy.".
-type backoff() :: [non_neg_integer()].
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
-doc "Queue configuration.".
-type queue_conf() :: #{
    name := queue(),
    driver => {module(), gaffer_driver:driver_state()},
    worker := module(),
    global_max_workers => pos_integer(),
    max_workers => pos_integer(),
    poll_interval => pos_integer() | infinity,
    shutdown_timeout => shutdown_timeout(),
    max_attempts => max_attempts(),
    timeout => timeout_ms(),
    backoff => backoff(),
    priority => priority(),
    on_discard => queue(),
    hooks => [gaffer_hooks:hook()]
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
        failed := state_info(),
        cancelled := state_info(),
        discarded := state_info()
    },
    workers := #{
        active := non_neg_integer(),
        max := #{local := pos_integer(), global := pos_integer()}
    }
}.

-doc #{group => "Job Types"}.
-doc "Filter options for listing jobs.".
-type job_filter() :: #{
    queue => queue(),
    state => job_state()
}.

-export_type([job_id/0]).
-export_type([job_state/0]).
-export_type([queue/0]).
-export_type([timestamp/0]).
-export_type([max_attempts/0]).
-export_type([priority/0]).
-export_type([timeout_ms/0]).
-export_type([backoff/0]).
-export_type([shutdown_timeout/0]).
-export_type([job/0]).
-export_type([job_opts/0]).
-export_type([job_error/0]).
-export_type([queue_conf/0]).
-export_type([job_filter/0]).
-export_type([state_info/0]).
-export_type([queue_info/0]).

%--- Application Callbacks -----------------------------------------------------

-doc false.
start(_StartType, _StartArgs) ->
    gaffer_queue:init(),
    gaffer_sup:start_link().

-doc false.
stop(_State) ->
    gaffer_queue:teardown().

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
cancel(Queue, JobId) ->
    case gaffer_queue:cancel_job(Queue, JobId) of
        {error, not_found} -> error({unknown_job, JobId});
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
get(Queue, JobId) ->
    case gaffer_queue:get_job(Queue, JobId) of
        not_found -> error({unknown_job, JobId});
        Job -> Job
    end.

-doc #{group => "Job Management"}.
-doc "Lists jobs matching the given filter options.".
-spec list(job_filter()) -> [job()].
list(Opts) ->
    gaffer_queue:list_jobs(Opts).

-doc #{group => "Job Management"}.
-doc "Deletes a job.".
-spec delete(queue(), job_id()) -> ok.
delete(Queue, JobId) ->
    gaffer_queue:delete_job(Queue, JobId).
