-module(gaffer_driver).

%% Persistence driver behaviour for gaffer.
%%
%% Pure functional interface — no processes. All mutating callbacks
%% return updated driver_state(). Callers own and thread state.

-type driver_state() :: term().
-export_type([driver_state/0]).

%% Lifecycle — called by the operator, not by gaffer
-callback start(Opts :: map()) -> {ok, driver_state()}.
-callback stop(driver_state()) -> ok.

%% Queue config
-callback queue_put(gaffer:queue_conf(), driver_state()) ->
    {ok, driver_state()}.
-callback queue_get(gaffer:queue_name(), driver_state()) ->
    {ok, gaffer:queue_conf()}.
-callback queue_delete(gaffer:queue_name(), driver_state()) ->
    {ok, driver_state()}.

%% Job CRUD
-callback job_insert(gaffer:job(), driver_state()) ->
    {ok, gaffer:job(), driver_state()}.
-callback job_get(gaffer:job_id(), driver_state()) ->
    {ok, gaffer:job()} | {error, not_found}.
-callback job_list(gaffer:list_opts(), driver_state()) ->
    {ok, [gaffer:job()]}.
-callback job_delete(gaffer:job_id(), driver_state()) ->
    {ok, driver_state()}.

%% Atomic claim — find matching jobs, apply changes, return them
-callback job_claim(
    gaffer:claim_opts(), gaffer:job_changes(), driver_state()
) ->
    {ok, [gaffer:job()], driver_state()}.

%% Persist a fully-prepared job (caller has done all mutations)
-callback job_update(gaffer:job(), driver_state()) ->
    {ok, gaffer:job(), driver_state()}.

%% Bulk prune
-callback job_prune(gaffer:prune_opts(), driver_state()) ->
    {ok, non_neg_integer(), driver_state()}.
