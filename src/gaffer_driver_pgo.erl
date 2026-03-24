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
-export([queue_upsert/2]).
-export([queue_get/2]).
-export([queue_delete/2]).
% Jobs
-export([job_insert/2]).
-export([job_get/2]).
-export([job_list/2]).
-export([job_delete/2]).
-export([job_claim/3]).
-export([job_update/2]).
-export([job_prune/2]).
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
-spec start(start_opts()) -> driver_state().
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
-spec stop(driver_state()) -> ok.
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
-spec queue_insert(gaffer:queue_conf(), driver_state()) -> ok.
queue_insert(Conf, #{pool := Pool} = State) ->
    Encoded = encode_conf(Conf),
    case queue_transaction(Pool, gaffer_postgres:queue_insert(Encoded), Conf) of
        [#{num_rows := 1}] ->
            % Row inserted. New queue created
            ok;
        [#{num_rows := 0}] ->
            % Queue already exists
            Name = maps:get(name, Conf),
            Existing = queue_get(Name, State),
            case Existing =:= Conf of
                true ->
                    % We are inserting the same queue
                    ok;
                false ->
                    % Configuration mismatch
                    error(
                        {queue_config_mismatch, Name, #{
                            expected => Conf, stored => Existing
                        }}
                    )
            end
    end.

-doc false.
-spec queue_upsert(gaffer:queue_conf(), driver_state()) -> ok.
queue_upsert(Conf, #{pool := Pool}) ->
    Encoded = encode_conf(Conf),
    queue_transaction(
        Pool, gaffer_postgres:queue_upsert(Encoded), Conf
    ),
    ok.

-doc false.
-spec queue_get(gaffer:queue(), driver_state()) ->
    gaffer:queue_conf() | not_found.
queue_get(Name, #{pool := Pool}) ->
    [#{rows := Rows}] =
        transaction(Pool, gaffer_postgres:queue_get(Name)),
    case Rows of
        [Row] -> decode_conf(Row);
        [] -> not_found
    end.

-doc false.
-spec queue_delete(gaffer:queue(), driver_state()) -> ok.
queue_delete(Name, #{pool := Pool}) ->
    transaction(Pool, gaffer_postgres:queue_delete(Name)),
    ok.

% Introspection

-doc false.
-spec info(gaffer:queue(), driver_state()) ->
    #{jobs := #{gaffer:job_state() => gaffer:state_info()}}.
info(Queue, #{pool := Pool}) ->
    [#{rows := Rows}] =
        transaction(Pool, gaffer_postgres:info(Queue)),
    Empty = #{
        available => #{count => 0},
        executing => #{count => 0},
        completed => #{count => 0},
        failed => #{count => 0},
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
-spec job_insert(gaffer:job(), driver_state()) -> gaffer:job().
job_insert(Job, #{pool := Pool}) ->
    Encoded = encode_job(Job),
    [#{rows := [Row]}] =
        transaction(Pool, gaffer_postgres:job_insert(Encoded)),
    decode_job(Row).

-doc false.
-spec job_get(gaffer:job_id(), driver_state()) -> gaffer:job() | not_found.
job_get(Id, #{pool := Pool}) ->
    [#{rows := Rows}] =
        transaction(Pool, gaffer_postgres:job_get(Id)),
    case Rows of
        [Row] -> decode_job(Row);
        [] -> not_found
    end.

-doc false.
-spec job_list(gaffer:job_filter(), driver_state()) -> [gaffer:job()].
job_list(Opts, #{pool := Pool}) ->
    Encoded = encode_list_opts(Opts),
    [#{rows := Rows}] =
        transaction(Pool, gaffer_postgres:job_list(Encoded)),
    [decode_job(R) || R <:- Rows].

-doc false.
-spec job_delete(gaffer:job_id(), driver_state()) -> ok | not_found.
job_delete(Id, #{pool := Pool}) ->
    [#{num_rows := N}] =
        transaction(Pool, gaffer_postgres:job_delete(Id)),
    case N of
        1 -> ok;
        0 -> not_found
    end.

-doc false.
-spec job_claim(
    gaffer_driver:claim_opts(), gaffer_driver:job_changes(), driver_state()
) -> [gaffer:job()].
job_claim(Opts, Changes, #{pool := Pool}) ->
    {EncodedOpts, EncodedChanges} = encode_claim(Opts, Changes),
    [#{rows := Rows}] =
        transaction(
            Pool, gaffer_postgres:job_claim(EncodedOpts, EncodedChanges)
        ),
    [decode_job(R) || R <:- Rows].

-doc false.
-spec job_update(gaffer:job(), driver_state()) -> ok.
job_update(Job, #{pool := Pool}) ->
    Encoded = encode_job(Job),
    transaction(Pool, gaffer_postgres:job_update(Encoded)),
    ok.

-doc false.
-spec job_prune(gaffer_driver:prune_opts(), driver_state()) ->
    non_neg_integer().
job_prune(Opts, #{pool := Pool}) ->
    [#{num_rows := Count}] =
        transaction(Pool, gaffer_postgres:job_prune(Opts)),
    Count.

%--- Internal ------------------------------------------------------------------

queue_transaction(Pool, Queries, Conf) ->
    try
        transaction(Pool, Queries)
    catch
        error:{pgsql_error, #{
            code := ~"23503",
            constraint := ~"gaffer_queues_on_discard_fkey"
        }} ->
            error({on_discard_queue_not_found, maps:get(on_discard, Conf)})
    end.

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

encode_conf(Conf) ->
    Template = gaffer_queue:queue_conf_template(),
    maps:map(
        fun(K, V) ->
            case maps:find(K, Template) of
                {ok, #{type := atom}} -> atom_to_binary(V);
                {ok, #{type := json}} -> json:encode(V);
                {ok, _} -> V;
                error when is_atom(V) -> atom_to_binary(V);
                error -> V
            end
        end,
        Conf
    ).

decode_conf(Row) ->
    Template = gaffer_queue:queue_conf_template(),
    maps:filtermap(
        fun
            (_K, null) -> false;
            (K, V) -> {true, decode_conf_field(K, V, Template)}
        end,
        Row
    ).

decode_conf_field(K, V, Template) ->
    case maps:find(K, Template) of
        {ok, #{type := atom}} -> binary_to_existing_atom(V);
        {ok, #{type := json}} -> json:decode(V);
        {ok, _} -> V;
        error when is_binary(V) -> binary_to_existing_atom(V);
        error -> V
    end.

encode_job(Job) ->
    maps:map(
        fun
            (K, V) when K =:= queue; K =:= state -> atom_to_binary(V);
            (payload, V) -> json:encode(V);
            (backoff, V) -> json:encode(V);
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
            (errors, V) -> {true, decode_errors(json:decode(V))};
            (K, V) when ?IS_TIMESTAMP(K) -> {true, decode_timestamp(V)};
            (_K, V) -> {true, V}
        end,
        Row
    ).

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
