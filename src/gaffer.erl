-module(gaffer).

-behaviour(application).

% Application callbacks
-export([start/2]).
-export([stop/1]).

% Queue management
-export([create_queue/1]).
-export([get_queue/1]).
-export([update_queue/2]).
-export([delete_queue/1]).
-export([list_queues/0]).

% Enqueueing
-export([insert/2]).
-export([insert/3]).

% Lifecycle
-export([cancel/2]).
-export([drain/1]).
-export([drain/2]).
-export([flush/1]).
-export([flush/2]).

% Querying
-export([get/2]).
-export([list/1]).
-export([delete/2]).

-ignore_xref([
    create_queue/1,
    get_queue/1,
    update_queue/2,
    delete_queue/1,
    list_queues/0,
    insert/2,
    insert/3,
    cancel/2,
    drain/1,
    drain/2,
    flush/1,
    flush/2,
    get/2,
    list/1,
    delete/2
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

-type timestamp() :: integer() | {erlang:time_unit(), integer()}.
% Native time units (erlang:system_time/0) or `{Unit, Value}`.
% Drivers normalize to their own precision.

-type job() :: #{
    id := job_id(),
    queue := queue_name(),
    payload := term(),
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
    errors := [job_error()]
}.

-type new_job() :: #{
    id := job_id(),
    queue := queue_name(),
    payload := term(),
    state := job_state(),
    attempt := non_neg_integer(),
    max_attempts := pos_integer(),
    priority := non_neg_integer(),
    scheduled_at => timestamp(),
    inserted_at := timestamp(),
    errors := [job_error()]
}.

-type job_opts() :: #{
    queue => queue_name(),
    max_attempts => pos_integer(),
    priority => non_neg_integer(),
    scheduled_at => timestamp()
}.

-type job_error() :: #{
    attempt := non_neg_integer(),
    error := term(),
    at := timestamp()
}.

-type queue_conf() :: gaffer_queue:queue_conf().

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
    new_job/0,
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
    gaffer_queue:init(),
    gaffer_sup:start_link().

stop(_State) ->
    gaffer_queue:teardown().

%--- Queue management ---------------------------------------------------------

-spec create_queue(queue_conf()) -> ok | {error, already_exists}.
create_queue(Conf) ->
    gaffer_queue:create(Conf).

-spec get_queue(queue_name()) -> queue_conf().
get_queue(Name) ->
    gaffer_queue:get(Name).

-spec update_queue(queue_name(), map()) -> ok.
update_queue(Name, Updates) ->
    gaffer_queue:update(Name, Updates).

-spec delete_queue(queue_name()) -> ok.
delete_queue(Name) ->
    gaffer_queue:delete(Name).

-spec list_queues() -> [{queue_name(), queue_conf()}].
list_queues() ->
    gaffer_queue:list().

%--- Enqueueing ---------------------------------------------------------------

-spec insert(queue_name(), term()) -> job().
insert(Queue, Payload) ->
    insert(Queue, Payload, #{}).

-spec insert(queue_name(), term(), job_opts()) -> job().
insert(Queue, Payload, Opts) ->
    gaffer_queue:insert_job(Queue, Payload, Opts).

%--- Lifecycle ----------------------------------------------------------------

-spec cancel(queue_name(), job_id()) ->
    {ok, job()} | {error, {invalid_transition, term()}}.
cancel(Queue, JobId) ->
    case gaffer_queue:cancel_job(Queue, JobId) of
        {error, not_found} -> error({unknown_job, JobId});
        {error, {invalid_transition, _}} = Err -> Err;
        {ok, _} = Ok -> Ok
    end.

-spec drain(queue_name()) -> ok.
drain(Queue) ->
    drain(Queue, 5000).

-spec drain(queue_name(), timeout()) -> ok.
% elp:ignore W0048 - stubs (see TODO below)
-dialyzer([
    {no_return, [drain/1, drain/2, flush/1, flush/2]},
    {no_contracts, [drain/1, drain/2, flush/1, flush/2]}
]).
drain(_Queue, _Timeout) ->
    % TODO: stop claiming, wait for in-flight workers
    error(not_implemented).

-spec flush(queue_name()) -> ok.
flush(Queue) ->
    flush(Queue, infinity).

-spec flush(queue_name(), timeout()) -> ok.
flush(_Queue, _Timeout) ->
    % TODO: process all remaining items in the queue until empty
    error(not_implemented).

%--- Querying -----------------------------------------------------------------

-spec get(queue_name(), job_id()) -> job().
get(Queue, JobId) ->
    case gaffer_queue:get_job(Queue, JobId) of
        not_found -> error({unknown_job, JobId});
        Job -> Job
    end.

-spec list(list_opts()) -> [job()].
list(Opts) ->
    gaffer_queue:list_jobs(Opts).

-spec delete(queue_name(), job_id()) -> ok.
delete(Queue, JobId) ->
    gaffer_queue:delete_job(Queue, JobId).
