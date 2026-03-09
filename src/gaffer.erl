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
-export([cancel/1]).
-export([retry/1]).
-export([drain/1]).

%% Querying
-export([get/1]).
-export([list/1]).

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

-type job() :: #{
    id := job_id(),
    queue := queue_name(),
    args := map(),
    state := job_state(),
    attempt := non_neg_integer(),
    max_attempts := pos_integer(),
    priority := non_neg_integer(),
    scheduled_at => calendar:datetime(),
    inserted_at := calendar:datetime(),
    attempted_at => calendar:datetime(),
    completed_at => calendar:datetime(),
    cancelled_at => calendar:datetime(),
    discarded_at => calendar:datetime(),
    errors := [job_error()],
    tags := [binary()],
    meta := map()
}.

-type job_opts() :: #{
    queue => queue_name(),
    max_attempts => pos_integer(),
    priority => non_neg_integer(),
    scheduled_at => calendar:datetime(),
    tags => [binary()],
    meta => map()
}.

-type job_error() :: #{
    attempt := non_neg_integer(),
    error := term(),
    at := calendar:datetime()
}.

-type queue_conf() :: #{
    name := queue_name(),
    worker => module(),
    concurrency => pos_integer(),
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

-type fetch_opts() :: #{
    queue => queue_name(),
    limit => pos_integer()
}.

-type prune_opts() :: #{
    states => [job_state()]
}.

-type drain_opts() :: #{
    queue => queue_name()
}.

-export_type([
    job_id/0,
    job_state/0,
    queue_name/0,
    job/0,
    job_opts/0,
    job_error/0,
    queue_conf/0,
    list_opts/0,
    fetch_opts/0,
    prune_opts/0,
    drain_opts/0
]).

%--- Application callbacks ----------------------------------------------------

start(_StartType, _StartArgs) -> gaffer_sup:start_link().

stop(_State) -> ok.

%--- Queue management ---------------------------------------------------------

-spec create_queue(queue_conf()) -> ok | {error, term()}.
create_queue(_Conf) -> {error, not_implemented}.

-spec get_queue(queue_name()) ->
    {ok, queue_conf()} | {error, term()}.
get_queue(_Name) -> {error, not_implemented}.

-spec update_queue(queue_name(), map()) ->
    ok | {error, term()}.
update_queue(_Name, _Updates) -> {error, not_implemented}.

-spec delete_queue(queue_name()) -> ok | {error, term()}.
delete_queue(_Name) -> {error, not_implemented}.

-spec list_queues() -> {ok, [queue_conf()]} | {error, term()}.
list_queues() -> {error, not_implemented}.

%--- Enqueueing ---------------------------------------------------------------

-spec insert(queue_name(), map()) ->
    {ok, job()} | {error, term()}.
insert(Queue, Args) -> insert(Queue, Args, #{}).

-spec insert(queue_name(), map(), job_opts()) ->
    {ok, job()} | {error, term()}.
insert(_Queue, _Args, _Opts) -> {error, not_implemented}.

%--- Lifecycle ----------------------------------------------------------------

-spec cancel(job_id()) -> {ok, job()} | {error, term()}.
cancel(_JobId) -> {error, not_implemented}.

-spec retry(job_id()) -> {ok, job()} | {error, term()}.
retry(_JobId) -> {error, not_implemented}.

-spec drain(drain_opts()) ->
    #{completed := integer(), failed := integer()}.
drain(_Opts) -> #{completed => 0, failed => 0}.

%--- Querying -----------------------------------------------------------------

-spec get(job_id()) -> {ok, job()} | {error, term()}.
get(_JobId) -> {error, not_implemented}.

-spec list(list_opts()) -> {ok, [job()]} | {error, term()}.
list(_Opts) -> {error, not_implemented}.
