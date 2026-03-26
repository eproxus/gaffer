-module(gaffer_driver).
-moduledoc "Persistence driver behaviour for gaffer.".

-hank([unused_callbacks]).

% API
-export([register/2, unregister/1, lookup/1]).

%--- Types ---------------------------------------------------------------------

-doc "Opaque driver state, defined by each implementation.".
-type driver_state() :: term().

-doc "A driver reference: either a registered name or a `{module, state}` tuple.".
-type driver() :: atom() | {module(), driver_state()}.

-doc "Options for claiming jobs.".
-type claim_opts() :: #{
    queue := gaffer:queue(),
    limit := pos_integer(),
    global_max_workers := pos_integer()
}.

-doc "Fields to update when transitioning a job.".
-type job_changes() :: #{
    state := gaffer:job_state(),
    attempted_at := gaffer:timestamp()
}.

-doc "Options for pruning jobs.".
-type prune_opts() :: #{
    states := [gaffer:job_state()]
}.

-doc "Errors returned by queue operations.".
-type queue_error() :: not_found | has_jobs.

-export_type([driver/0]).
-export_type([driver_state/0]).
-export_type([claim_opts/0]).
-export_type([job_changes/0]).
-export_type([prune_opts/0]).
-export_type([queue_error/0]).

%--- Callbacks -----------------------------------------------------------------

% Lifecycle
-doc "Starts the driver.".
-callback start(Opts :: map()) -> driver_state().

-doc "Stops the driver.".
-callback stop(driver_state()) -> any().

% Queues
-doc """
Registers a queue name in storage.

This operation is idempotent: if the queue name is already registered, the
call must succeed without error.
""".
-callback queue_insert(gaffer:queue(), driver_state()) -> ok.

-doc """
Checks whether a queue name is registered in storage.

Used to validate `on_discard` references when creating or updating queues.
""".
-callback queue_exists(gaffer:queue(), driver_state()) -> boolean().

-doc """
Removes a queue name from storage.

Returns `{error, not_found}` if the queue name is not registered, or
`{error, has_jobs}` if the queue still has associated jobs in storage.
""".
-callback queue_delete(gaffer:queue(), driver_state()) ->
    ok | {error, queue_error()}.

% Jobs
-doc "Inserts a job into storage and returns it.".
-callback job_insert(gaffer:job(), driver_state()) -> gaffer:job().

-doc "Fetches a job by ID.".
-callback job_get(gaffer:job_id(), driver_state()) -> gaffer:job() | not_found.

-doc "Lists jobs matching the given filter options.".
-callback job_list(gaffer:job_filter(), driver_state()) -> [gaffer:job()].

-doc "Deletes a job by ID.".
-callback job_delete(gaffer:job_id(), driver_state()) -> ok | not_found.

-doc "Atomically claims available jobs for execution.".
-callback job_claim(claim_opts(), job_changes(), driver_state()) ->
    [gaffer:job()].

-doc "Updates a job in storage.".
-callback job_update(gaffer:job(), driver_state()) -> ok.

-doc "Prunes jobs in terminal states and returns the count removed.".
-callback job_prune(prune_opts(), driver_state()) -> non_neg_integer().

% Introspection
-doc "Returns aggregate job counts and timestamps per state for a queue.".
-callback info(gaffer:queue(), driver_state()) ->
    #{jobs := #{gaffer:job_state() => gaffer:state_info()}}.

%--- API -----------------------------------------------------------------------

-doc "Registers a driver under a name for later lookup.".
-spec register(atom(), {module(), driver_state()}) -> ok.
register(Name, {_Mod, _DS} = Driver) ->
    persistent_term:put({gaffer_driver, Name}, Driver),
    ok.

-doc "Unregisters a previously registered driver.".
-spec unregister(atom()) -> ok.
unregister(Name) ->
    _ = persistent_term:erase({gaffer_driver, Name}),
    ok.

-doc "Looks up a registered driver by name.".
-spec lookup(atom()) -> {module(), driver_state()}.
lookup(Name) ->
    case persistent_term:get({gaffer_driver, Name}, undefined) of
        undefined -> error({unknown_driver, Name});
        Driver -> Driver
    end.
