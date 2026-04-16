-module(gaffer_driver_pgo).
-moduledoc "Postgres driver for gaffer using pgo.".

-behaviour(gaffer_driver).

% Lifecycle
-export([start/1]).
-export([stop/1]).
-ignore_xref(rollback/2).
-export([rollback/2]).
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
% Introspection
-export([info/2]).

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
    K =:= inserted_at;
    K =:= scheduled_at;
    K =:= attempted_at;
    K =:= completed_at;
    K =:= cancelled_at;
    K =:= discarded_at
).

%--- gaffer_driver Callbacks ---------------------------------------------------

% Lifecycle

-doc """
Starts the driver and optionally a pool.

Runs any pending migrations.
""".
start(Opts) ->
    State = start_pool(Opts),
    #{pool := Pool} = State,
    ensure_migrations_table(Pool),
    Current = applied_version(Pool),
    Migrations = gaffer_postgres:migrations(#{}),
    Pending = [M || {V, _, _} = M <:- Migrations, V > Current],
    run_migrations(Pool, fun gaffer_postgres:migrate_up/1, Pending),
    State.

-doc """
Stop the driver.

Also stops the connection pool if started by the driver.
""".
stop(State) ->
    stop_pool(State).

-doc "Rolls back migrations down to the given version.".
-spec rollback(TargetVersion :: non_neg_integer(), driver_state()) -> ok.
rollback(TargetVersion, #{pool := Pool}) ->
    Current = applied_version(Pool),
    AllMigrations = gaffer_postgres:migrations(#{}),
    ToRollback = [
        lists:keyfind(V, 1, AllMigrations)
     || V <:- lists:seq(Current, TargetVersion + 1, -1)
    ],
    run_migrations(Pool, fun gaffer_postgres:migrate_down/1, ToRollback),
    ok.

% Queues

-doc false.
queue_insert(Name, #{pool := Pool}) ->
    transaction(Pool, gaffer_postgres:queue_insert(Name)),
    ok.

-doc false.
queue_exists(Name, #{pool := Pool}) ->
    [#{rows := Rows}] =
        transaction(Pool, gaffer_postgres:queue_exists(Name)),
    Rows =/= [].

-doc false.
queue_list(#{pool := Pool}) ->
    [#{rows := Rows}] = transaction(Pool, gaffer_postgres:queue_list()),
    [binary_to_existing_atom(Name) || #{name := Name} <:- Rows].

-doc false.
queue_delete(Name, #{pool := Pool}) ->
    try transaction(Pool, gaffer_postgres:queue_delete(Name)) of
        [#{num_rows := 1}] -> ok;
        [#{num_rows := 0}] -> {error, not_found}
    catch
        error:{pgsql_error, #{code := ~"23503"}} ->
            {error, has_jobs}
    end.

% Introspection

-doc false.
info(Queue, #{pool := Pool}) ->
    [#{rows := Rows}] =
        transaction(Pool, gaffer_postgres:info(Queue)),
    Empty = #{
        available => #{count => 0},
        executing => #{count => 0},
        completed => #{count => 0},
        cancelled => #{count => 0},
        discarded => #{count => 0}
    },
    Jobs = lists:foldl(fun decode_info_row/2, Empty, Rows),
    #{jobs => Jobs}.

decode_info_row(#{state := State, count := Count} = Row, Acc) ->
    StateAtom = binary_to_existing_atom(State),
    Entry = #{count => Count},
    Entry1 =
        case Row of
            #{oldest := null} ->
                Entry;
            #{oldest := Oldest, newest := Newest} ->
                Entry#{
                    oldest => decode_timestamp(Oldest),
                    newest => decode_timestamp(Newest)
                }
        end,
    Acc#{StateAtom := Entry1}.

% Jobs

-doc false.
job_write(Jobs, #{pool := Pool}) ->
    Queries = lists:flatmap(
        fun(Job) -> gaffer_postgres:job_write(encode_job(Job)) end,
        Jobs
    ),
    Results = transaction(Pool, Queries),
    [decode_job(Row) || #{rows := [Row]} <:- Results].

-doc false.
job_get(ID, #{pool := Pool}) ->
    [#{rows := Rows}] =
        transaction(Pool, gaffer_postgres:job_get(ID)),
    case Rows of
        [Row] -> decode_job(Row);
        [] -> not_found
    end.

-doc false.
job_list(Opts, #{pool := Pool}) ->
    Encoded = encode_list_opts(Opts),
    [#{rows := Rows}] =
        transaction(Pool, gaffer_postgres:job_list(Encoded)),
    [decode_job(R) || R <:- Rows].

-doc false.
job_delete(ID, #{pool := Pool}) ->
    [#{num_rows := N}] =
        transaction(Pool, gaffer_postgres:job_delete(ID)),
    case N of
        1 -> ok;
        0 -> not_found
    end.

-doc false.
job_claim(Opts, Changes, #{pool := Pool}) ->
    {EncodedOpts, EncodedChanges} = encode_claim(Opts, Changes),
    [#{rows := Rows}] =
        transaction(
            Pool, gaffer_postgres:job_claim(EncodedOpts, EncodedChanges)
        ),
    [decode_job(R) || R <:- Rows].

-doc false.
job_prune(Queue, Opts, #{pool := Pool}) ->
    Encoded = maps:map(fun(_State, TS) -> encode_timestamp(TS) end, Opts),
    [#{rows := Rows}] = transaction(
        Pool, gaffer_postgres:job_prune(Queue, Encoded)
    ),
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

ensure_migrations_table(Pool) ->
    transaction(Pool, gaffer_postgres:ensure_migrations_table()).

run_migrations(Pool, ToQueries, Migrations) ->
    lists:foreach(
        fun(Migration) -> transaction(Pool, ToQueries(Migration)) end,
        Migrations
    ).

applied_version(Pool) ->
    [#{rows := [#{version := Version}]}] =
        transaction(Pool, gaffer_postgres:applied_version()),
    Version.

% Runs a list of queries in a single transaction, returning [pgo:result()].
% pgo:query/3 inside a transaction uses the implicit connection from
% the process dictionary, set by pgo:transaction/2.
transaction(Pool, Queries) ->
    DecodeOpts = [return_rows_as_maps, column_name_as_atom],
    pgo:transaction(
        fun() ->
            [
                case pgo:query(SQL, Params, #{decode_opts => DecodeOpts}) of
                    {error, {pgsql_error, Error}} ->
                        error({pgsql_error, Error});
                    #{command := _} = Result ->
                        Result
                end
             || {SQL, Params} <:- Queries
            ]
        end,
        #{pool => Pool}
    ).

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
    #{queue := Queue, limit := Limit, global_max_workers := GlobalMax} = Opts,
    #{state := State, attempted_at := AttemptedAt} = Changes,
    {
        #{
            queue => atom_to_binary(Queue),
            limit => Limit,
            global_max_workers => GlobalMax
        },
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
