-module(gaffer_driver).
-moduledoc "Persistence driver behaviour for gaffer.".

-hank([unused_callbacks]).

%--- Types ---------------------------------------------------------------------

-doc "Opaque driver state, defined by each implementation.".
-type driver_state() :: term().

-doc "Options for claiming jobs.".
-type claim_opts() :: #{
    queue => gaffer:queue(),
    limit => pos_integer()
}.

-doc "Fields to update when transitioning a job.".
-type job_changes() :: #{
    state := gaffer:job_state(),
    attempted_at := gaffer:timestamp()
}.

-doc "Options for pruning jobs.".
-type prune_opts() :: #{
    states => [gaffer:job_state()]
}.

-export_type([driver_state/0]).
-export_type([claim_opts/0]).
-export_type([job_changes/0]).
-export_type([prune_opts/0]).

%--- Callbacks -----------------------------------------------------------------

% Lifecycle
-doc "Starts the driver.".
-callback start(Opts :: map()) -> driver_state().

-doc "Stops the driver.".
-callback stop(driver_state()) -> any().

% Queues
-doc "Persists a new queue configuration.".
-callback queue_insert(gaffer:queue_conf(), driver_state()) -> ok.

-doc "Updates an existing queue configuration.".
-callback queue_update(gaffer:queue(), map(), driver_state()) -> ok.

-doc "Fetches a queue configuration by name.".
-callback queue_get(gaffer:queue(), driver_state()) ->
    gaffer:queue_conf() | not_found.

-doc "Deletes a queue configuration by name.".
-callback queue_delete(gaffer:queue(), driver_state()) -> ok.

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
-callback job_claim(
    claim_opts(), job_changes(), driver_state()
) ->
    [gaffer:job()].

-doc "Updates a job in storage.".
-callback job_update(gaffer:job(), driver_state()) -> ok.

-doc "Prunes jobs in terminal states and returns the count removed.".
-callback job_prune(prune_opts(), driver_state()) -> non_neg_integer().

% Introspection
-doc "Returns aggregate job counts and timestamps per state for a queue.".
-callback info(gaffer:queue(), driver_state()) ->
    #{jobs := #{gaffer:job_state() => gaffer:state_info()}}.
