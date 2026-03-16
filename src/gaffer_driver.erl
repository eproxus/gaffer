-module(gaffer_driver).

-hank([unused_callbacks]).

% Persistence driver behaviour for gaffer.
%
% Stateful drivers — each driver manages its own internal state
% (e.g. a connection pool, ETS tables). Callbacks receive immutable
% driver_opts() and return bare values.

-type driver_opts() :: term().
-export_type([driver_opts/0]).

% Lifecycle — called by the operator, not by gaffer
-callback start(Opts :: map()) -> driver_opts().
-callback stop(driver_opts()) -> any().

% Queue config
-callback queue_insert(gaffer:queue_conf(), driver_opts()) ->
    ok.
-callback queue_update(gaffer:queue_name(), map(), driver_opts()) ->
    ok.
-callback queue_get(gaffer:queue_name(), driver_opts()) ->
    gaffer:queue_conf() | not_found.
-callback queue_delete(gaffer:queue_name(), driver_opts()) ->
    ok.

% Job CRUD
-callback job_insert(gaffer:new_job(), driver_opts()) ->
    gaffer:job().
-callback job_get(gaffer:job_id(), driver_opts()) ->
    gaffer:job() | not_found.
-callback job_list(gaffer:list_opts(), driver_opts()) ->
    [gaffer:job()].
-callback job_delete(gaffer:job_id(), driver_opts()) ->
    ok | not_found.

% Atomic claim — find matching jobs, apply changes, return them
-callback job_claim(
    gaffer:claim_opts(), gaffer:job_changes(), driver_opts()
) ->
    [gaffer:job()].

% Persist a fully-prepared job (caller has done all mutations)
-callback job_update(gaffer:job(), driver_opts()) ->
    ok.

% Bulk prune
-callback job_prune(gaffer:prune_opts(), driver_opts()) ->
    non_neg_integer().
