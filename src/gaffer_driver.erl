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
-callback queue_put(gaffer:queue_conf(), driver_state()) -> ok.
-callback queue_get(gaffer:queue_name(), driver_state()) ->
    {ok, gaffer:queue_conf()}.
-callback queue_delete(gaffer:queue_name(), driver_state()) ->
    ok.

%% Jobs
-callback job_insert(gaffer:job(), driver_state()) ->
    {ok, gaffer:job()}.
-callback job_get(gaffer:job_id(), driver_state()) ->
    {ok, gaffer:job()} | {error, not_found}.
-callback job_list(gaffer:list_opts(), driver_state()) ->
    {ok, [gaffer:job()]}.
-callback job_fetch(gaffer:fetch_opts(), driver_state()) ->
    {ok, [gaffer:job()]}.
-callback job_complete(gaffer:job_id(), driver_state()) ->
    {ok, gaffer:job()}.
-callback job_fail(
    gaffer:job_id(), gaffer:job_error(), driver_state()
) ->
    {ok, gaffer:job()}.
-callback job_cancel(gaffer:job_id(), driver_state()) ->
    {ok, gaffer:job()}.
-callback job_retry(
    gaffer:job_id(), calendar:datetime(), driver_state()
) ->
    {ok, gaffer:job()}.
-callback job_snooze(
    gaffer:job_id(), pos_integer(), driver_state()
) ->
    {ok, gaffer:job()}.
-callback job_prune(gaffer:prune_opts(), driver_state()) ->
    {ok, non_neg_integer()}.
