-module(gaffer_postgres).
-moduledoc "Pure SQL query builder and serializer for Postgres drivers.".

% API
% Migrations
-export([migrations/1]).
-export([migrate_up/1]).
-export([migrate_down/1]).
-export([ensure_migrations_table/0]).
-export([applied_version/0]).
% Queues
-export([queue_insert/1]).
-export([queue_exists/1]).
-export([queue_delete/1]).
% Introspection
-export([info/1]).
% Jobs
-export([job_insert/1]).
-export([job_get/1]).
-export([job_list/1]).
-export([job_delete/1]).
-export([job_claim/2]).
-export([job_update/1]).
-export([job_prune/1]).

-doc """
A parameterized SQL query.

The `QueryString` will contain `$1` etc. which each will correspond to the nth
value in `Values`.
""".
-type query() :: {QueryString :: iodata(), Values :: list()}.
-doc "A list of queries to be run in one transaction.".
-type queries() :: [query()].

-export_type([query/0]).
-export_type([queries/0]).

%--- API -----------------------------------------------------------------------

% Migrations

-doc "Sorted list of all schema migrations as `{Version, Up, Down}` tuples.".
-spec migrations(Opts :: map()) ->
    [{Version :: pos_integer(), Up :: queries(), Down :: queries()}].
migrations(#{}) ->
    [
        {1,
            queries([
                % Queues
                ~"""
                CREATE TABLE gaffer_queues (
                    name TEXT PRIMARY KEY
                )
                """,
                % Jobs
                ~"""
                CREATE TABLE gaffer_jobs (
                    id               UUID PRIMARY KEY,
                    queue            TEXT NOT NULL
                                         REFERENCES gaffer_queues(name),
                    state            TEXT NOT NULL
                                         CHECK (state IN ('available', 'executing',
                                                          'completed', 'failed', 'cancelled',
                                                          'discarded')),
                    payload          JSONB NOT NULL,
                    attempt          INTEGER NOT NULL,
                    max_attempts     INTEGER NOT NULL,
                    priority         INTEGER NOT NULL,
                    timeout          INTEGER,
                    backoff          JSONB,
                    shutdown_timeout INTEGER,
                    result           JSONB,
                    errors           JSONB NOT NULL,
                    scheduled_at     TIMESTAMPTZ,
                    inserted_at      TIMESTAMPTZ NOT NULL,
                    attempted_at     TIMESTAMPTZ,
                    completed_at     TIMESTAMPTZ,
                    cancelled_at     TIMESTAMPTZ,
                    discarded_at     TIMESTAMPTZ
                )
                """,
                % Query indexes
                ~"""
                CREATE INDEX idx_gaffer_jobs_claimable
                    ON gaffer_jobs (queue, priority, inserted_at)
                    WHERE state = 'available'
                """,
                ~"""
                CREATE INDEX idx_gaffer_jobs_queue_state
                    ON gaffer_jobs (queue, state)
                """,
                % Maintenance indexes
                ~"""
                CREATE INDEX idx_gaffer_jobs_state
                    ON gaffer_jobs (state)
                """,
                ~"""
                CREATE INDEX idx_gaffer_jobs_scheduled
                    ON gaffer_jobs (scheduled_at)
                    WHERE state = 'available' AND scheduled_at IS NOT NULL
                """
            ]),
            queries([~"DROP TABLE gaffer_jobs", ~"DROP TABLE gaffer_queues"])}
    ].

-doc "Queries to apply a migration and record its version.".
-spec migrate_up({pos_integer(), queries(), _}) -> queries().
migrate_up({Version, UpQueries, _DownQueries}) ->
    UpQueries ++
        [{~"UPDATE gaffer_schema_version SET version = $1", [Version]}].

-doc "Queries to roll back a migration and decrement the version.".
-spec migrate_down({pos_integer(), _, queries()}) -> queries().
migrate_down({Version, _UpQueries, DownQueries}) ->
    DownQueries ++
        [{~"UPDATE gaffer_schema_version SET version = $1", [Version - 1]}].

-doc "Queries to create the migrations table if it does not exist.".
-spec ensure_migrations_table() -> queries().
ensure_migrations_table() ->
    queries([
        ~"""
        CREATE TABLE IF NOT EXISTS gaffer_schema_version (
            version BIGINT NOT NULL DEFAULT 0
        )
        """,
        ~"""
        INSERT INTO gaffer_schema_version (version)
        SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM gaffer_schema_version)
        """
    ]).

-doc "Query that fetches the current schema version.".
-spec applied_version() -> queries().
applied_version() ->
    [{~"SELECT version FROM gaffer_schema_version", []}].

% Queues

-doc "Query to register a queue name. No-op if already registered.".
-spec queue_insert(gaffer:queue()) -> queries().
queue_insert(Name) ->
    [
        {
            ~"INSERT INTO gaffer_queues (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
            [atom_to_binary(Name)]
        }
    ].

-doc "Query to check whether a queue name exists.".
-spec queue_exists(gaffer:queue()) -> queries().
queue_exists(Name) ->
    [{~"SELECT 1 FROM gaffer_queues WHERE name = $1", [atom_to_binary(Name)]}].

-doc "Query to delete a queue by name.".
-spec queue_delete(gaffer:queue()) -> queries().
queue_delete(Name) ->
    [{~"DELETE FROM gaffer_queues WHERE name = $1", [atom_to_binary(Name)]}].

% Introspection

-doc "Query that aggregates job counts and timestamps per state.".
-spec info(gaffer:queue()) -> queries().
info(Queue) ->
    TSCase =
        ~"""
    CASE state
        WHEN 'available' THEN inserted_at
        WHEN 'executing' THEN attempted_at
        WHEN 'completed' THEN completed_at
        WHEN 'failed'    THEN attempted_at
        WHEN 'cancelled' THEN cancelled_at
        WHEN 'discarded' THEN discarded_at
    END
    """,
    SQL = [
        ~"SELECT state, COUNT(*) AS count, ",
        ts_column([~"MIN(", TSCase, ~")"], ~"oldest"),
        ~", ",
        ts_column([~"MAX(", TSCase, ~")"], ~"newest"),
        ~" FROM gaffer_jobs WHERE queue = $1 GROUP BY state"
    ],
    [{SQL, [atom_to_binary(Queue)]}].

% Jobs

-doc "Returns a query to insert a job and return the inserted row.".
-spec job_insert(map()) -> queries().
job_insert(Encoded) ->
    {Cols, Phs, Vals} = columns_and_values(Encoded),
    SQL = [
        ~"INSERT INTO gaffer_jobs (",
        lists:join(~", ", Cols),
        ~") VALUES (",
        lists:join(~", ", Phs),
        ~") RETURNING ",
        job_columns()
    ],
    [{SQL, Vals}].

-doc "Query to fetch a job by ID.".
-spec job_get(term()) -> queries().
job_get(Id) ->
    SQL = [
        ~"SELECT ", job_columns(), ~" FROM gaffer_jobs WHERE id = $1"
    ],
    [{SQL, [Id]}].

-doc "Query to list jobs matching the given filters.".
-spec job_list(map()) -> queries().
job_list(#{queue := Queue} = Opts) ->
    {SQL, Params} =
        case Opts of
            #{state := State} ->
                {
                    [
                        ~"SELECT ",
                        job_columns(),
                        ~" FROM gaffer_jobs WHERE queue = $1 AND state = $2"
                    ],
                    [Queue, State]
                };
            _ ->
                {
                    [
                        ~"SELECT ",
                        job_columns(),
                        ~" FROM gaffer_jobs WHERE queue = $1"
                    ],
                    [Queue]
                }
        end,
    [{SQL, Params}].

-doc "Query to delete a job by ID.".
-spec job_delete(term()) -> queries().
job_delete(Id) ->
    [{~"DELETE FROM gaffer_jobs WHERE id = $1", [Id]}].

job_columns() ->
    job_columns(~"").

job_columns(Prefix) ->
    lists:join(~", ", [
        [Prefix, ~"id"],
        [Prefix, ~"queue"],
        [Prefix, ~"state"],
        [Prefix, ~"payload"],
        [Prefix, ~"attempt"],
        [Prefix, ~"max_attempts"],
        [Prefix, ~"priority"],
        [Prefix, ~"timeout"],
        [Prefix, ~"backoff"],
        [Prefix, ~"shutdown_timeout"],
        [Prefix, ~"result"],
        [Prefix, ~"errors"]
        | [
            ts_column([Prefix, C], C)
         || C <:- ts_column_names()
        ]
    ]).

ts_column(Expr, Alias) ->
    [
        ~"EXTRACT(EPOCH FROM date_trunc('second', ",
        Expr,
        ~"))::bigint * 1000000 + MOD(EXTRACT(MICROSECONDS FROM ",
        Expr,
        ~")::bigint, 1000000) AS ",
        Alias
    ].

ts_column_names() ->
    [
        ~"scheduled_at",
        ~"inserted_at",
        ~"attempted_at",
        ~"completed_at",
        ~"cancelled_at",
        ~"discarded_at"
    ].

% Job lifecycle

-doc "Query to atomically claim available jobs for execution.".
-spec job_claim(map(), map()) -> queries().
job_claim(
    #{queue := Queue, limit := Limit, global_max_workers := GlobalMax},
    #{state := State, attempted_at := AttemptedAt}
) ->
    Now = AttemptedAt,
    {LimitClause, LimitParams} = job_claim_effective_limit(Limit, GlobalMax),
    SQL = [
        ~"""
        WITH candidates AS (
            SELECT id FROM gaffer_jobs
            WHERE queue = $1
              AND state = 'available'
              AND (scheduled_at IS NULL
                   OR scheduled_at <= to_timestamp($2::bigint / 1000000.0))
            ORDER BY priority ASC, inserted_at ASC
        """,
        LimitClause,
        ~"""

            FOR UPDATE SKIP LOCKED
        )
        UPDATE gaffer_jobs j
        SET state = $3,
            attempted_at = to_timestamp($4::bigint / 1000000.0)
        FROM candidates c
        WHERE j.id = c.id
        RETURNING
        """,
        ~" ",
        job_columns(~"j.")
    ],
    [{SQL, [Queue, Now, State, Now | LimitParams]}].

-doc "Query to update a job's fields.".
-spec job_update(map()) -> queries().
job_update(Encoded) ->
    #{id := Id} = Encoded,
    Fields = maps:remove(id, Encoded),
    {Sets, Vals} = set_clause(Fields),
    N = length(Vals) + 1,
    SQL = [
        ~"UPDATE gaffer_jobs SET ",
        Sets,
        ~" WHERE id = $",
        integer_to_binary(N)
    ],
    [{SQL, Vals ++ [Id]}].

-doc "Query to delete jobs in terminal states.".
-spec job_prune(map()) -> queries().
job_prune(#{states := States}) ->
    TextArray = [atom_to_binary(S) || S <:- States],
    [
        {
            ~"DELETE FROM gaffer_jobs WHERE state = ANY($1::text[]) RETURNING id",
            [TextArray]
        }
    ].

%--- Internal ------------------------------------------------------------------

job_claim_effective_limit(infinity, infinity) ->
    {~"", []};
job_claim_effective_limit(Limit, infinity) ->
    {~"\n        LIMIT $5::bigint", [Limit]};
job_claim_effective_limit(Limit, GlobalMax) ->
    {
        ~"""

                LIMIT GREATEST(0, (
                    SELECT LEAST($5::bigint, $6::bigint - count(*))
                    FROM gaffer_jobs
                    WHERE queue = $1 AND state = 'executing'
                ))
        """,
        [Limit, GlobalMax]
    }.

set_clause(Map) ->
    {Cols, Phs, Vals} = columns_and_values(Map),
    Sets = lists:join(~", ", [[C, ~" = ", P] || {C, P} <:- lists:zip(Cols, Phs)]),
    {Sets, Vals}.

columns_and_values(Map) ->
    Pairs = maps:to_list(Map),
    Cols = [atom_to_binary(K) || {K, _} <:- Pairs],
    Vals = [V || {_, V} <:- Pairs],
    Phs = [
        [~"$", integer_to_binary(I)]
     || I <:- lists:seq(1, length(Cols))
    ],
    {Cols, Phs, Vals}.

queries(SQLs) ->
    [{SQL, []} || SQL <:- SQLs].
