-module(gaffer_postgres).
-moduledoc "Pure SQL query builder and serializer for Postgres drivers.".

% API
% Migrations
-export([migrations/1]).
-export([migrate_up/1]).
-export([migrate_down/1]).
-export([ensure_migrations_table/0]).
-export([applied_versions/0]).
-export([applied_checksums/0]).
-export([advisory_lock/0]).
-export([checksum/1]).
% Queues
-export([queue_insert/1]).
-export([queue_exists/1]).
-export([queue_list/0]).
-export([queue_delete/1]).
% Jobs
-export([job_write/1]).
-export([job_get/1]).
-export([job_list/1]).
-export([job_delete/1]).
-export([job_claim/2]).
-export([executing_count/1]).
-export([job_prune/2]).

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
                CREATE TABLE IF NOT EXISTS gaffer_queues (
                    name TEXT PRIMARY KEY
                )
                """,
                % Jobs
                ~"""
                CREATE TABLE IF NOT EXISTS gaffer_jobs (
                    id               UUID PRIMARY KEY,
                    queue            TEXT NOT NULL
                                         REFERENCES gaffer_queues(name),
                    state            TEXT NOT NULL
                                         CHECK (state IN ('available', 'executing',
                                                          'completed', 'cancelled',
                                                          'failed')),
                    payload          JSONB NOT NULL,
                    attempt          INTEGER NOT NULL DEFAULT 0
                        CONSTRAINT attempt_non_negative CHECK (attempt >= 0)
                        CONSTRAINT attempt_within_max   CHECK (attempt <= max_attempts),
                    max_attempts     INTEGER NOT NULL DEFAULT 1
                        CONSTRAINT max_attempts_at_least_one CHECK (max_attempts >= 1),
                    priority         INTEGER NOT NULL DEFAULT 0,
                    timeout          INTEGER,
                    backoff          JSONB,
                    shutdown_timeout INTEGER,
                    result           JSONB,
                    errors           JSONB NOT NULL,
                    scheduled_at     TIMESTAMPTZ,
                    created_at       TIMESTAMPTZ NOT NULL,
                    attempted_at     TIMESTAMPTZ,
                    completed_at     TIMESTAMPTZ,
                    cancelled_at     TIMESTAMPTZ,
                    failed_at        TIMESTAMPTZ
                )
                """,
                % Query indexes
                ~"""
                CREATE INDEX IF NOT EXISTS idx_gaffer_jobs_claimable
                    ON gaffer_jobs (queue, priority DESC, created_at ASC)
                    WHERE state = 'available'
                """,
                ~"""
                CREATE INDEX IF NOT EXISTS idx_gaffer_jobs_queue_state
                    ON gaffer_jobs (queue, state)
                """,
                ~"""
                CREATE INDEX IF NOT EXISTS idx_gaffer_jobs_completed
                    ON gaffer_jobs (queue, completed_at)
                    WHERE state = 'completed'
                """,
                ~"""
                CREATE INDEX IF NOT EXISTS idx_gaffer_jobs_cancelled
                    ON gaffer_jobs (queue, cancelled_at)
                    WHERE state = 'cancelled'
                """,
                ~"""
                CREATE INDEX IF NOT EXISTS idx_gaffer_jobs_failed
                    ON gaffer_jobs (queue, failed_at)
                    WHERE state = 'failed'
                """
            ]),
            queries([
                ~"DROP TABLE IF EXISTS gaffer_jobs",
                ~"DROP TABLE IF EXISTS gaffer_queues"
            ])},
        {2,
            queries([
                ~"ALTER TABLE gaffer_jobs ADD COLUMN chain TEXT",
                ~"""
                CREATE INDEX IF NOT EXISTS idx_gaffer_jobs_chain_active
                    ON gaffer_jobs (queue, chain, priority DESC, created_at ASC, id)
                    WHERE state IN ('available', 'executing')
                """
            ]),
            queries([
                ~"DROP INDEX IF EXISTS idx_gaffer_jobs_chain_active",
                ~"ALTER TABLE IF EXISTS gaffer_jobs DROP COLUMN IF EXISTS chain"
            ])}
    ].

-doc "Queries to apply a migration and record it as applied.".
-spec migrate_up({pos_integer(), queries(), _}) -> queries().
migrate_up({Version, UpQueries, _DownQueries}) ->
    Checksum = checksum(UpQueries),
    UpQueries ++
        [
            {
                ~"INSERT INTO gaffer_schema_migrations (version, sql_checksum) VALUES ($1, $2)",
                [Version, Checksum]
            }
        ].

-doc "Queries to roll back a migration and remove its applied record.".
-spec migrate_down({pos_integer(), _, queries()}) -> queries().
migrate_down({Version, _UpQueries, DownQueries}) ->
    DownQueries ++
        [
            {
                ~"DELETE FROM gaffer_schema_migrations WHERE version = $1",
                [Version]
            }
        ].

-doc "Queries to create the applied migrations table if it does not exist.".
-spec ensure_migrations_table() -> queries().
ensure_migrations_table() ->
    queries([
        ~"""
        CREATE TABLE IF NOT EXISTS gaffer_schema_migrations (
            version       BIGINT PRIMARY KEY,
            sql_checksum  BYTEA NOT NULL,
            applied_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """
    ]).

-doc "Query that fetches the currently applied migration versions.".
-spec applied_versions() -> queries().
applied_versions() ->
    [{~"SELECT version FROM gaffer_schema_migrations ORDER BY version", []}].

-doc "Query that fetches applied versions and their stored SQL checksums.".
-spec applied_checksums() -> queries().
applied_checksums() ->
    [
        {
            ~"SELECT version, sql_checksum FROM gaffer_schema_migrations ORDER BY version",
            []
        }
    ].

-doc """
Query that takes a transaction-scoped advisory lock on the migrations table.

The lock is released automatically when the surrounding transaction commits or
rolls back. Concurrent boots running this query inside their own migrate
transaction will serialise on it.
""".
-spec advisory_lock() -> queries().
advisory_lock() ->
    % Cast away the void return type so pgo can decode the row.
    [{~"SELECT pg_advisory_xact_lock($1)::text", [lock_key()]}].

-doc """
SHA-256 over the concatenated SQL strings of a query list.

Used to detect post-deploy edits to a migration's up-queries: the value
computed at apply time is stored in `gaffer_schema_migrations.sql_checksum`
and compared against the recomputed value on every subsequent boot.
""".
-spec checksum(queries()) -> binary().
checksum(Queries) ->
    crypto:hash(sha256, iolist_to_binary([SQL || {SQL, _} <:- Queries])).

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

-doc "Query to list all registered queue names.".
-spec queue_list() -> queries().
queue_list() -> [{~"SELECT name FROM gaffer_queues", []}].

-doc "Query to delete a queue by name.".
-spec queue_delete(gaffer:queue()) -> queries().
queue_delete(Name) ->
    [{~"DELETE FROM gaffer_queues WHERE name = $1", [atom_to_binary(Name)]}].

% Jobs

-doc "Returns upsert queries for a single encoded job.".
-spec job_write(map()) -> queries().
job_write(Encoded) ->
    {Cols, Phs, Vals} = columns_and_values(Encoded),
    Mutable = [C || C <:- Cols, not lists:member(C, immutable_columns())],
    Sets = lists:join(~", ", [[C, ~" = EXCLUDED.", C] || C <:- Mutable]),
    SQL = [
        ~"INSERT INTO gaffer_jobs (",
        lists:join(~", ", Cols),
        ~") VALUES (",
        lists:join(~", ", Phs),
        ~") ON CONFLICT (id) DO UPDATE SET ",
        Sets,
        ~" RETURNING ",
        job_columns()
    ],
    [{SQL, Vals}].

-doc "Query to fetch a job by ID.".
-spec job_get(term()) -> queries().
job_get(ID) ->
    SQL = [
        ~"SELECT ", job_columns(), ~" FROM gaffer_jobs WHERE id = $1"
    ],
    [{SQL, [ID]}].

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
job_delete(ID) ->
    [{~"DELETE FROM gaffer_jobs WHERE id = $1", [ID]}].

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
        [Prefix, ~"errors"],
        [Prefix, ~"chain"]
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
        ~"created_at",
        ~"attempted_at",
        ~"completed_at",
        ~"cancelled_at",
        ~"failed_at"
    ].

% Job lifecycle

-doc "Query to atomically claim available jobs for execution.".
-spec job_claim(map(), map()) -> queries().
job_claim(
    #{queue := Queue, limit := Limit},
    #{state := State, attempted_at := AttemptedAt}
) ->
    Now = AttemptedAt,
    {LimitClause, LimitParams} = job_claim_limit(Limit),
    SQL = [
        ~"""
        WITH candidates AS (
            SELECT j.id FROM gaffer_jobs j
            WHERE j.queue = $1
              AND j.state = 'available'
              AND (j.scheduled_at IS NULL
                   OR j.scheduled_at <= to_timestamp($2::bigint / 1000000.0))
              AND (
                j.chain IS NULL
                OR NOT EXISTS (
                  SELECT 1 FROM gaffer_jobs x
                  WHERE x.queue = j.queue
                    AND x.chain = j.chain
                    AND x.state IN ('available', 'executing')
                    AND (x.priority, j.created_at, j.id)
                      > (j.priority, x.created_at, x.id)
                )
              )
            ORDER BY j.priority DESC, j.created_at ASC, j.id ASC
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

-doc """
Query that counts jobs currently in the `executing` state for a queue.

Used by the Postgres driver before claiming, to compute how many slots remain
under `global_max_workers` without embedding the count in the claim query
itself (which prevents the planner from using the partial available-jobs
index).
""".
-spec executing_count(binary()) -> queries().
executing_count(Queue) ->
    SQL =
        ~"SELECT count(*) AS n FROM gaffer_jobs WHERE queue = $1 AND state = 'executing'",
    [{SQL, [Queue]}].

-doc """
Query to delete jobs older than per-state cutoffs from a queue.

The candidate rows are selected and locked in `id` order with `SKIP LOCKED`,
then deleted via a CTE. The stable lock order makes concurrent pruners safe
against deadlocks, and `SKIP LOCKED` lets pruners skip rows currently locked
by another pruner or worker upsert. Skipped rows are caught on the next run.
""".
-spec job_prune(gaffer:queue(), map()) -> queries().
job_prune(Queue, Opts) ->
    {Clauses, Params, N} = maps:fold(fun prune_clause/3, {[], [], 1}, Opts),
    QueueParam = [~"$", integer_to_binary(N)],
    SQL = [
        ~"WITH d AS (SELECT id FROM gaffer_jobs WHERE queue = ",
        QueueParam,
        ~" AND (",
        lists:join(~" OR ", lists:reverse(Clauses)),
        ~") ORDER BY id FOR UPDATE SKIP LOCKED)",
        ~" DELETE FROM gaffer_jobs j USING d WHERE j.id = d.id",
        ~" RETURNING j.id"
    ],
    [{SQL, lists:reverse(Params, [atom_to_binary(Queue)])}].

prune_clause(State, all, {Cs, Ps, N}) ->
    {[[~"(state = '", atom_to_binary(State), ~"')"] | Cs], Ps, N};
prune_clause(State, Cutoff, {Cs, Ps, N}) ->
    Col = state_timestamp_column(State),
    StateName = atom_to_binary(State),
    C = [~"(state = '", StateName, ~"' AND ", Col, older_than(N), ~")"],
    {[C | Cs], [Cutoff | Ps], N + 1}.

older_than(N) ->
    [~" < to_timestamp($", integer_to_binary(N), ~"::bigint / 1000000.0)"].

%--- Internal ------------------------------------------------------------------

% First 8 bytes of sha256("gaffer_schema_migrations") as a signed 64-bit
% integer, the type accepted by pg_advisory_xact_lock.
lock_key() ->
    <<Key:64/signed-integer, _/binary>> =
        crypto:hash(sha256, ~"gaffer_schema_migrations"),
    Key.

state_timestamp_column(available) -> ~"created_at";
state_timestamp_column(executing) -> ~"attempted_at";
state_timestamp_column(completed) -> ~"completed_at";
state_timestamp_column(cancelled) -> ~"cancelled_at";
state_timestamp_column(failed) -> ~"failed_at".

immutable_columns() -> [~"id", ~"queue", ~"created_at"].

job_claim_limit(infinity) -> {~"", []};
job_claim_limit(Limit) -> {~"\n        LIMIT $5::bigint", [Limit]}.

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
