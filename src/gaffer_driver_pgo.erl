-module(gaffer_driver_pgo).

-behaviour(gaffer_driver).

% Lifecycle
-export([start/1]).
-export([stop/1]).
-export([rollback/2]).

% Queue config
-export([queue_insert/2]).
-export([queue_update/3]).
-export([queue_get/2]).
-export([queue_delete/2]).

% Job CRUD
-export([job_insert/2]).
-export([job_get/2]).
-export([job_list/2]).
-export([job_delete/2]).
-export([job_claim/3]).
-export([job_update/2]).
-export([job_prune/2]).

% rollback/2 is an operational tool (shell use), not called from app code
-ignore_xref([rollback/2]).

-type pool_owner() :: driver | user.
-type state() :: #{
    pool := atom(),
    pool_owner := pool_owner()
}.

-export_type([state/0]).

%--- Lifecycle ----------------------------------------------------------------

-spec start(map()) -> state().
start(Opts) ->
    State = start_pool(Opts),
    #{pool := Pool} = State,
    ensure_migrations_table(Pool),
    Current = applied_version(Pool),
    Migrations = gaffer_postgres:migrations(#{}),
    Pending = [M || {V, _, _} = M <:- Migrations, V > Current],
    run_migrations(Pool, fun gaffer_postgres:migrate_up/1, Pending),
    State.

-spec stop(state()) -> ok.
stop(State) ->
    stop_pool(State).

-spec rollback(TargetVersion :: non_neg_integer(), state()) -> ok.
rollback(TargetVersion, #{pool := Pool}) ->
    Current = applied_version(Pool),
    AllMigrations = gaffer_postgres:migrations(#{}),
    ToRollback = [
        lists:keyfind(V, 1, AllMigrations)
     || V <:- lists:seq(Current, TargetVersion + 1, -1)
    ],
    run_migrations(Pool, fun gaffer_postgres:migrate_down/1, ToRollback),
    ok.

%--- Queue config -------------------------------------------------------------

-spec queue_insert(gaffer:queue_conf(), state()) -> ok.
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

-spec queue_update(gaffer:queue_name(), map(), state()) -> ok.
queue_update(Name, Updates, #{pool := Pool}) ->
    Encoded = encode_conf(Updates),
    queue_transaction(
        Pool, gaffer_postgres:queue_update(Name, Encoded), Updates
    ),
    ok.

-spec queue_get(gaffer:queue_name(), state()) ->
    gaffer:queue_conf() | not_found.
queue_get(Name, #{pool := Pool}) ->
    [#{rows := Rows}] =
        transaction(Pool, gaffer_postgres:queue_get(Name)),
    case Rows of
        [Row] -> row_to_queue_conf(Row);
        [] -> not_found
    end.

-spec queue_delete(gaffer:queue_name(), state()) -> ok.
queue_delete(Name, #{pool := Pool}) ->
    transaction(Pool, gaffer_postgres:queue_delete(Name)),
    ok.

%--- Job CRUD ----------------------------------------------------------------

-spec job_insert(gaffer:new_job(), state()) -> gaffer:job().
job_insert(Job, #{pool := Pool}) ->
    Encoded = encode_job(Job),
    [#{rows := [Row]}] =
        transaction(Pool, gaffer_postgres:job_insert(Encoded)),
    row_to_job(Row).

-spec job_get(gaffer:job_id(), state()) -> gaffer:job() | not_found.
job_get(Id, #{pool := Pool}) ->
    [#{rows := Rows}] =
        transaction(Pool, gaffer_postgres:job_get(Id)),
    case Rows of
        [Row] -> row_to_job(Row);
        [] -> not_found
    end.

-spec job_list(gaffer:list_opts(), state()) -> [gaffer:job()].
job_list(Opts, #{pool := Pool}) ->
    Encoded = encode_list_opts(Opts),
    [#{rows := Rows}] =
        transaction(Pool, gaffer_postgres:job_list(Encoded)),
    [row_to_job(R) || R <:- Rows].

-spec job_delete(gaffer:job_id(), state()) -> ok | not_found.
job_delete(Id, #{pool := Pool}) ->
    [#{num_rows := N}] =
        transaction(Pool, gaffer_postgres:job_delete(Id)),
    case N of
        1 -> ok;
        0 -> not_found
    end.

-spec job_claim(
    gaffer:claim_opts(), gaffer:job_changes(), state()
) -> [gaffer:job()].
job_claim(Opts, Changes, #{pool := Pool}) ->
    {EncodedOpts, EncodedChanges} = encode_claim(Opts, Changes),
    [#{rows := Rows}] =
        transaction(
            Pool, gaffer_postgres:job_claim(EncodedOpts, EncodedChanges)
        ),
    [row_to_job(R) || R <:- Rows].

-spec job_update(gaffer:job(), state()) -> ok.
job_update(Job, #{pool := Pool}) ->
    Encoded = encode_job(Job),
    transaction(Pool, gaffer_postgres:job_update(Encoded)),
    ok.

-spec job_prune(gaffer:prune_opts(), state()) -> non_neg_integer().
job_prune(Opts, #{pool := Pool}) ->
    [#{num_rows := Count}] =
        transaction(Pool, gaffer_postgres:job_prune(Opts)),
    Count.

%--- Internal -----------------------------------------------------------------

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
        Pid -> supervisor:terminate_child(pgo_sup, Pid)
    end,
    ok.

ensure_migrations_table(Pool) ->
    lists:foreach(
        fun({SQL, Params}) ->
            pgo:query(SQL, Params, #{pool => Pool})
        end,
        gaffer_postgres:ensure_migrations_table()
    ).

run_migrations(Pool, ToQueries, Migrations) ->
    lists:foreach(
        fun(Migration) -> transaction(Pool, ToQueries(Migration)) end,
        Migrations
    ).

applied_version(Pool) ->
    [{SQL, Params}] = gaffer_postgres:applied_version(),
    #{rows := [{Version}]} = pgo:query(SQL, Params, #{pool => Pool}),
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
            (payload, V) ->
                json:encode(V);
            (errors, V) ->
                json:encode(encode_errors(V));
            (K, V) when
                K =:= inserted_at;
                K =:= scheduled_at;
                K =:= attempted_at;
                K =:= completed_at;
                K =:= cancelled_at;
                K =:= discarded_at
            ->
                to_microsecond(V);
            (_K, V) ->
                V
        end,
        Job
    ).

row_to_job(Row) ->
    TimestampKeys = [
        scheduled_at,
        inserted_at,
        attempted_at,
        completed_at,
        cancelled_at,
        discarded_at
    ],
    maps:filtermap(
        fun
            (_K, null) ->
                false;
            (queue, V) ->
                {true, binary_to_existing_atom(V)};
            (state, V) ->
                {true, binary_to_existing_atom(V)};
            (payload, V) ->
                {true, json:decode(V)};
            (errors, V) ->
                {true, normalize_errors(json:decode(V))};
            (K, V) ->
                case lists:member(K, TimestampKeys) of
                    true -> {true, {microsecond, V}};
                    false -> {true, V}
                end
        end,
        Row
    ).

normalize_errors(Errors) ->
    [normalize_error_keys(E) || E <:- Errors].

normalize_error_keys(ErrorMap) ->
    maps:fold(
        fun
            (~"attempt", V, Acc) -> Acc#{attempt => V};
            (~"error", V, Acc) -> Acc#{error => V};
            (~"at", V, Acc) -> Acc#{at => {microsecond, V}};
            (K, V, Acc) -> Acc#{K => V}
        end,
        #{},
        ErrorMap
    ).

encode_errors(Errors) ->
    [encode_error_entry(E) || E <:- Errors].

encode_error_entry(Entry) ->
    maps:map(
        fun
            (at, V) -> to_microsecond(V);
            (error, V) -> iolist_to_binary(io_lib:format(~"~0tp", [V]));
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
            attempted_at => to_microsecond(AttemptedAt)
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

to_microsecond({Unit, V}) ->
    erlang:convert_time_unit(V, Unit, microsecond);
to_microsecond(Native) ->
    erlang:convert_time_unit(Native, native, microsecond).

encode_conf(Conf) ->
    Template = gaffer_queue:queue_conf_template(),
    maps:map(
        fun(K, V) ->
            case maps:find(K, Template) of
                {ok, #{type := atom}} -> atom_to_binary(V);
                {ok, _} -> V;
                error when is_atom(V) -> atom_to_binary(V);
                error -> V
            end
        end,
        Conf
    ).

row_to_queue_conf(Row) ->
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
        {ok, _} -> V;
        error when is_binary(V) -> binary_to_existing_atom(V);
        error -> V
    end.
