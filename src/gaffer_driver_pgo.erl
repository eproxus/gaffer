-module(gaffer_driver_pgo).
-moduledoc "Postgres driver for gaffer using pgo.".

-behaviour(gaffer_driver).

-include_lib("kernel/include/logger.hrl").

% Lifecycle
-export([start/1]).
-export([stop/1]).
-ignore_xref(rollback/2).
-export([rollback/2]).
-ignore_xref(migrations/1).
-export([migrations/1]).
% Queues
-export([queue_insert/2]).
-export([queue_exists/2]).
-export([queue_list/1]).
-export([queue_delete/2]).
% Jobs
-export([job_write/2]).
-export([job_get/2]).
-export([job_list/2]).
-export([job_delete/2]).
-export([job_claim/3]).
-export([job_prune/3]).

-doc "PGO pool configuration passed to `pgo:start_pool/2`.".
-type pool_config() :: map().

-doc """
Options for starting the PGO driver.

Use a PGO pool with the identifier `pool`. If `start` options are supplied, the
driver starts its own PGO pool with that name and those options. Otherwise an
existing pool is used, and ensuring this pool is started is the responsibility
of the user.
""".
-type start_opts() :: #{
    pool := atom(),
    start => pool_config()
}.

-type pool_owner() :: driver | user.

-doc "PGO driver state.".
-opaque driver_state() :: #{
    pool := atom(),
    pool_owner := pool_owner()
}.

-export_type([pool_config/0]).
-export_type([start_opts/0]).
-export_type([driver_state/0]).

-define(IS_TIMESTAMP(K),
    K =:= created_at;
    K =:= scheduled_at;
    K =:= attempted_at;
    K =:= completed_at;
    K =:= cancelled_at;
    K =:= failed_at
).

%--- gaffer_driver Callbacks ---------------------------------------------------

% Lifecycle

-doc """
Starts the driver and optionally a pool.

Runs any pending migrations atomically inside one transaction guarded by a
transaction-scoped advisory lock, so concurrent boots serialise. Verifies the
SHA-256 checksum of every already-applied migration against the static SQL in
the current version of this library.

Raises `error({migration_checksum_mismatch, Version, Stored, Expected})` if a
stored migration's SQL has changed since it was applied.
""".
start(Opts) ->
    State = start_pool(Opts),
    #{pool := Pool} = State,
    All = gaffer_postgres:migrations(#{}),
    migrate_transaction(Pool, fun() ->
        Applied = applied_checksums(Pool),
        verify_checksums(Applied, All),
        AppliedVersions = [V || {V, _} <:- Applied],
        Pending = [
            M
         || {V, _, _} = M <:- All, not lists:member(V, AppliedVersions)
        ],
        run_migrations(Pool, fun gaffer_postgres:migrate_up/1, Pending)
    end),
    State.

-doc """
Stop the driver.

Also stops the connection pool if started by the driver.
""".
stop(State) ->
    stop_pool(State).

-doc """
Rolls back migrations down to the given version.

Runs the entire rollback inside one transaction guarded by a transaction-scoped
advisory lock.

Raises `error({migration_checksum_mismatch, Version, Stored, Expected})` if a
stored migration's SQL has changed since it was applied.
""".
-spec rollback(TargetVersion :: non_neg_integer(), driver_state()) -> ok.
rollback(Target, #{pool := Pool}) ->
    All = gaffer_postgres:migrations(#{}),
    migrate_transaction(Pool, fun() ->
        Applied = applied_checksums(Pool),
        verify_checksums(Applied, All),
        AppliedVersions = [V || {V, _} <:- Applied],
        ToRollback = [
            find_migration(V, All)
         || V <:- lists:reverse(lists:sort(AppliedVersions)), V > Target
        ],
        run_migrations(Pool, fun gaffer_postgres:migrate_down/1, ToRollback)
    end),
    ok.

-doc """
Lists known and applied migration versions.

Returns a map with two keys. `all` is the static list of versions known to this
binary, sorted ascending. `applied` is the set of versions recorded in the
migrations table, sorted ascending.
""".
-spec migrations(driver_state()) ->
    #{all := [non_neg_integer()], applied := [non_neg_integer()]}.
migrations(#{pool := Pool}) ->
    All = [V || {V, _, _} <:- gaffer_postgres:migrations(#{})],
    #{all => All, applied => applied_versions(Pool)}.

% Queues

-doc false.
queue_insert(Name, #{pool := Pool}) ->
    queries(Pool, gaffer_postgres:queue_insert(Name)),
    ok.

-doc false.
queue_exists(Name, #{pool := Pool}) ->
    query(Pool, gaffer_postgres:queue_exists(Name)) =/= [].

-doc false.
queue_list(#{pool := Pool}) ->
    Rows = query(Pool, gaffer_postgres:queue_list()),
    [binary_to_existing_atom(Name) || #{name := Name} <:- Rows].

-doc false.
queue_delete(Name, #{pool := Pool}) ->
    try queries(Pool, gaffer_postgres:queue_delete(Name)) of
        [#{num_rows := 1}] -> ok;
        [#{num_rows := 0}] -> {error, not_found}
    catch
        error:{pgsql_error, #{code := ~"23503"}} ->
            {error, has_jobs}
    end.

% Jobs

-doc false.
job_write(Jobs, #{pool := Pool}) ->
    Queries = lists:flatmap(
        fun(Job) -> gaffer_postgres:job_write(encode_job(Job)) end,
        Jobs
    ),
    Results = queries(Pool, Queries),
    [decode_job(Row) || #{rows := [Row]} <:- Results].

-doc false.
job_get(ID, #{pool := Pool}) ->
    case query(Pool, gaffer_postgres:job_get(ID)) of
        [Row] -> decode_job(Row);
        [] -> not_found
    end.

-doc false.
job_list(Opts, #{pool := Pool}) ->
    Encoded = encode_list_opts(Opts),
    [decode_job(R) || R <:- query(Pool, gaffer_postgres:job_list(Encoded))].

-doc false.
job_delete(ID, #{pool := Pool}) ->
    [#{num_rows := N}] =
        queries(Pool, gaffer_postgres:job_delete(ID)),
    case N of
        1 -> ok;
        0 -> not_found
    end.

-doc false.
job_claim(Opts, Changes, #{pool := Pool}) ->
    {EncodedOpts, EncodedChanges} = encode_claim(Opts, Changes),
    decode_claim_rows(
        pgo:transaction(
            fun() -> claim_available(Opts, EncodedOpts, EncodedChanges) end,
            #{pool => Pool}
        )
    ).

-doc false.
job_prune(Queue, Opts, #{pool := Pool}) ->
    Encoded = maps:map(fun(_State, TS) -> encode_timestamp(TS) end, Opts),
    Rows = query(Pool, gaffer_postgres:job_prune(Queue, Encoded)),
    [ID || #{id := ID} <:- Rows].

%--- Internal ------------------------------------------------------------------

start_pool(#{pool := Pool, start := PgoConfig}) ->
    {ok, _} = pgo:start_pool(Pool, PgoConfig),
    #{pool => Pool, pool_owner => driver};
start_pool(#{pool := Pool}) ->
    #{pool => Pool, pool_owner => user}.

% pgo does not expose a public stop_pool API, so we reach into its
% internal supervisor. If pgo changes its supervision tree, update here.
stop_pool(#{pool_owner := user}) ->
    ok;
stop_pool(#{pool := Pool, pool_owner := driver}) ->
    case whereis(Pool) of
        undefined -> ok;
        Pid -> ok = supervisor:terminate_child(pgo_sup, Pid)
    end.

applied_versions(Pool) ->
    [V || #{version := V} <:- query(Pool, gaffer_postgres:applied_versions())].

find_migration(V, All) ->
    case lists:keyfind(V, 1, All) of
        false -> error({unknown_migration_version, V});
        M -> M
    end.

% Wraps the migrate flow in one transaction: take the advisory lock, ensure
% the migrations table exists, then call Fun. Calls to query/2 and
% transaction/2 inside Fun reuse the open connection (pgo:transaction/2 is
% reentrant). The lock and any partial schema changes are released on commit
% or rollback.
migrate_transaction(Pool, Fun) ->
    pgo:transaction(
        fun() ->
            query(Pool, gaffer_postgres:advisory_lock()),
            query(Pool, gaffer_postgres:ensure_migrations_table()),
            Fun()
        end,
        #{pool => Pool}
    ).

applied_checksums(Pool) ->
    Rows = query(Pool, gaffer_postgres:applied_checksums()),
    [{V, C} || #{version := V, sql_checksum := C} <:- Rows].

verify_checksums(Applied, All) ->
    [verify_checksum(V, S, find_migration(V, All)) || {V, S} <:- Applied].

verify_checksum(Version, Stored, {_, UpQueries, _}) ->
    case gaffer_postgres:checksum(UpQueries) of
        Stored ->
            ok;
        Expected ->
            error({migration_checksum_mismatch, Version, Stored, Expected})
    end.

run_migrations(Pool, ToQueries, Migrations) ->
    [queries(Pool, ToQueries(M)) || M <:- Migrations],
    ok.

query(Pool, Queries) ->
    [#{rows := Rows}] = queries(Pool, Queries),
    Rows.

queries(Pool, Queries) when is_list(Queries) ->
    pgo:transaction(fun() -> exec_queries(Queries) end, #{pool => Pool}).

exec_queries(Queries) -> [exec_query(Query) || Query <:- Queries].

exec_query({SQL, Params}) ->
    DecodeOpts = [return_rows_as_maps, column_name_as_atom],
    case pgo:query(SQL, Params, #{decode_opts => DecodeOpts}) of
        {error, {pgsql_error, #{code := <<"40", _/binary>>} = Err} = Original} ->
            ?LOG_WARNING(#{
                event => transient_pgsql_error,
                sql => iolist_to_binary(SQL),
                code => maps:get(code, Err),
                message => maps:get(message, Err, undefined)
            }),
            error({transient, Original});
        {error, Error} ->
            error(Error);
        #{command := _} = Result ->
            Result
    end.

claim_available(#{global_max_workers := infinity}, EncodedOpts, Changes) ->
    claim_jobs(EncodedOpts, Changes);
claim_available(
    #{global_max_workers := GlobalMax, limit := Limit},
    #{queue := Queue} = EncodedOpts,
    Changes
) ->
    % Two-step claim: count executing under finite global_max_workers (skipped
    % on infinity), compute the effective limit in Erlang, then run the claim
    % with a static LIMIT. The split lets the planner use the partial
    % available-jobs index instead of falling back to a sequential scan on
    % large backlogs. See gaffer_postgres:job_claim/2 for the SQL queries.
    Available = max(0, min(Limit, GlobalMax - count_executing(Queue))),
    case Available of
        0 -> [];
        L -> claim_jobs(EncodedOpts#{limit := L}, Changes)
    end.

count_executing(Queue) ->
    Query = gaffer_postgres:executing_count(Queue),
    [#{rows := [#{n := Executing}]}] = exec_queries(Query),
    Executing.

claim_jobs(EncodedOpts, Changes) ->
    [#{rows := Rows}] = exec_queries(
        gaffer_postgres:job_claim(EncodedOpts, Changes)
    ),
    Rows.

decode_claim_rows(Rows) when is_list(Rows) ->
    [decode_job(R) || R <:- Rows].

encode_job(Job) ->
    maps:map(
        fun
            (K, V) when K =:= queue; K =:= state -> atom_to_binary(V);
            (payload, V) -> json:encode(V);
            (backoff, V) -> json:encode(V);
            (result, undefined) -> json:encode(null);
            (result, V) -> json:encode(V);
            (errors, V) -> json:encode(encode_errors(V));
            (K, V) when ?IS_TIMESTAMP(K) -> encode_timestamp(V);
            (_K, V) -> V
        end,
        Job
    ).

encode_errors(Errors) ->
    [encode_error_entry(E) || E <:- Errors].

encode_error_entry(Entry) ->
    maps:map(
        fun
            (at, V) -> encode_timestamp(V);
            (_K, V) -> V
        end,
        Entry
    ).

encode_claim(Opts, Changes) ->
    #{queue := Queue, limit := Limit} = Opts,
    #{state := State, attempted_at := AttemptedAt} = Changes,
    {
        #{queue => atom_to_binary(Queue), limit => Limit},
        #{
            state => atom_to_binary(State),
            attempted_at => encode_timestamp(AttemptedAt)
        }
    }.

encode_list_opts(Opts) ->
    maps:map(
        fun
            (_K, V) when is_atom(V) -> atom_to_binary(V);
            (_K, V) -> V
        end,
        Opts
    ).

encode_timestamp(all) ->
    all;
encode_timestamp(Native) ->
    erlang:convert_time_unit(Native, native, microsecond).

decode_timestamp(Microseconds) ->
    erlang:convert_time_unit(Microseconds, microsecond, native).

decode_job(Row) ->
    maps:filtermap(
        fun
            (_K, null) -> false;
            (queue, V) -> {true, binary_to_existing_atom(V)};
            (state, V) -> {true, binary_to_existing_atom(V)};
            (payload, V) -> {true, json:decode(V)};
            (backoff, V) -> {true, json:decode(V)};
            (result, V) -> {true, decode_result(json:decode(V))};
            (errors, V) -> {true, decode_errors(json:decode(V))};
            (K, V) when ?IS_TIMESTAMP(K) -> {true, decode_timestamp(V)};
            (_K, V) -> {true, V}
        end,
        Row
    ).

decode_result(null) -> undefined;
decode_result(V) -> V.

decode_errors(Errors) ->
    [decode_error_entry(E) || E <:- Errors].

decode_error_entry(ErrorMap) ->
    maps:fold(
        fun
            (~"attempt", V, Acc) -> Acc#{attempt => V};
            (~"error", V, Acc) -> Acc#{error => V};
            (~"at", V, Acc) -> Acc#{at => decode_timestamp(V)};
            (K, V, Acc) -> Acc#{K => V}
        end,
        #{},
        ErrorMap
    ).
