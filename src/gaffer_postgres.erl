-module(gaffer_postgres).

% Pure SQL module — no pgo dependency. Contains all SQL and
% serialization logic for the Postgres driver.
%
% All public functions return [{SQL, Params}] — a uniform list
% of query tuples. The driver runs these in a transaction.

-export([migrations/1]).
-export([migrate_up/1]).
-export([migrate_down/1]).
-export([ensure_migrations_table/0]).
-export([applied_version/0]).

% Queue CRUD
-export([queue_insert/1]).
-export([queue_update/2]).
-export([queue_get/1]).
-export([queue_delete/1]).

% Job CRUD
-export([job_insert/1]).
-export([job_get/1]).
-export([job_list/1]).
-export([job_delete/1]).
-export([job_claim/2]).
-export([job_update/1]).
-export([job_prune/1]).

%--- Migrations ---------------------------------------------------------------

-spec migrations(Opts :: map()) ->
    [
        {
            Version :: pos_integer(),
            Up :: [{binary(), list()}],
            Down :: [{binary(), list()}]
        }
    ].
migrations(#{}) ->
    [
        {1,
            queries([
                % Queues
                ~"""
                CREATE TABLE gaffer_queues (
                    name               TEXT PRIMARY KEY,
                    global_max_workers INTEGER,
                    max_workers        INTEGER,
                    poll_interval      INTEGER,
                    shutdown_timeout   INTEGER,
                    max_attempts       INTEGER,
                    timeout            INTEGER,
                    backoff            INTEGER,
                    priority           INTEGER,
                    on_discard         TEXT REFERENCES gaffer_queues(name)
                )
                """,
                % Jobs
                ~"""
                CREATE TABLE gaffer_jobs (
                    id             UUID PRIMARY KEY,
                    queue          TEXT NOT NULL,
                    state          TEXT NOT NULL
                                       CHECK (state IN ('available', 'scheduled', 'executing',
                                                        'completed', 'failed', 'cancelled',
                                                        'discarded')),
                    payload        JSONB NOT NULL,
                    attempt        INTEGER NOT NULL,
                    max_attempts   INTEGER NOT NULL,
                    priority       INTEGER NOT NULL,
                    errors         JSONB NOT NULL,
                    scheduled_at   TIMESTAMPTZ,
                    inserted_at    TIMESTAMPTZ NOT NULL,
                    attempted_at   TIMESTAMPTZ,
                    completed_at   TIMESTAMPTZ,
                    cancelled_at   TIMESTAMPTZ,
                    discarded_at   TIMESTAMPTZ
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

-spec migrate_up({pos_integer(), [{binary(), list()}], _}) ->
    [{binary(), list()}].
migrate_up({Version, UpQueries, _DownQueries}) ->
    UpQueries ++
        [{~"UPDATE gaffer_schema_version SET version = $1", [Version]}].

-spec migrate_down({pos_integer(), _, [{binary(), list()}]}) ->
    [{binary(), list()}].
migrate_down({Version, _UpQueries, DownQueries}) ->
    DownQueries ++
        [{~"UPDATE gaffer_schema_version SET version = $1", [Version - 1]}].

-spec ensure_migrations_table() -> [{binary(), list()}].
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

% Returns a query guaranteed to return a single row with an integer version.
-spec applied_version() -> [{binary(), list()}].
applied_version() ->
    [{~"SELECT version FROM gaffer_schema_version", []}].

%--- Queue CRUD ---------------------------------------------------------------

-spec queue_insert(map()) -> [{binary(), list()}].
queue_insert(Conf) ->
    {Cols, Phs, Vals} = columns_and_values(Conf),
    SQL = iolist_to_binary([
        ~"INSERT INTO gaffer_queues (",
        lists:join(~", ", Cols),
        ~") VALUES (",
        lists:join(~", ", Phs),
        ~") ON CONFLICT (name) DO NOTHING"
    ]),
    [{SQL, Vals}].

-spec queue_update(gaffer:queue_name(), map()) -> [{binary(), list()}].
queue_update(Name, Changes) ->
    {Sets, Vals} = set_clause(Changes),
    N = length(Vals) + 1,
    SQL = iolist_to_binary([
        ~"UPDATE gaffer_queues SET ",
        Sets,
        ~" WHERE name = $",
        integer_to_binary(N)
    ]),
    [{SQL, Vals ++ [atom_to_binary(Name)]}].

-spec queue_get(gaffer:queue_name()) -> [{binary(), list()}].
queue_get(Name) ->
    [{~"SELECT * FROM gaffer_queues WHERE name = $1", [atom_to_binary(Name)]}].

-spec queue_delete(gaffer:queue_name()) -> [{binary(), list()}].
queue_delete(Name) ->
    [{~"DELETE FROM gaffer_queues WHERE name = $1", [atom_to_binary(Name)]}].

%--- Job CRUD ----------------------------------------------------------------

-spec job_insert(map()) -> [{binary(), list()}].
job_insert(Encoded) ->
    {Cols, Phs, Vals} = columns_and_values(Encoded),
    SQL = iolist_to_binary([
        ~"INSERT INTO gaffer_jobs (",
        lists:join(~", ", Cols),
        ~") VALUES (",
        lists:join(~", ", Phs),
        ~") RETURNING ",
        job_columns()
    ]),
    [{SQL, Vals}].

-spec job_get(term()) -> [{binary(), list()}].
job_get(Id) ->
    SQL = iolist_to_binary([
        ~"SELECT ", job_columns(), ~" FROM gaffer_jobs WHERE id = $1"
    ]),
    [{SQL, [Id]}].

-spec job_list(map()) -> [{binary(), list()}].
job_list(#{queue := Queue} = Opts) ->
    {SQL, Params} =
        case Opts of
            #{state := State} ->
                {
                    iolist_to_binary([
                        ~"SELECT ",
                        job_columns(),
                        ~" FROM gaffer_jobs WHERE queue = $1 AND state = $2"
                    ]),
                    [Queue, State]
                };
            _ ->
                {
                    iolist_to_binary([
                        ~"SELECT ",
                        job_columns(),
                        ~" FROM gaffer_jobs WHERE queue = $1"
                    ]),
                    [Queue]
                }
        end,
    [{SQL, Params}].

-spec job_delete(term()) -> [{binary(), list()}].
job_delete(Id) ->
    [{~"DELETE FROM gaffer_jobs WHERE id = $1", [Id]}].

job_columns() ->
    job_columns(<<>>).

job_columns(Prefix) ->
    iolist_to_binary(
        lists:join(~", ", [
            [Prefix, ~"id"],
            [Prefix, ~"queue"],
            [Prefix, ~"state"],
            [Prefix, ~"payload"],
            [Prefix, ~"attempt"],
            [Prefix, ~"max_attempts"],
            [Prefix, ~"priority"],
            [Prefix, ~"errors"]
            | [
                ts_column([Prefix, C], C)
             || C <:- ts_column_names()
            ]
        ])
    ).

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

%--- Job lifecycle ------------------------------------------------------------

-spec job_claim(map(), map()) -> [{binary(), list()}].
job_claim(
    #{queue := Queue, limit := Limit},
    #{state := State, attempted_at := AttemptedAt}
) ->
    Now = AttemptedAt,
    SQL = iolist_to_binary([
        ~"""
        WITH queue_config AS (
            SELECT global_max_workers FROM gaffer_queues WHERE name = $1
        ),
        executing_count AS (
            SELECT count(*) AS cnt FROM gaffer_jobs
            WHERE queue = $1 AND state = 'executing'
        ),
        effective_limit AS (
            SELECT LEAST(
                $3,
                COALESCE(
                    (SELECT global_max_workers FROM queue_config)
                        - (SELECT cnt FROM executing_count),
                    $3
                )
            ) AS lim
        ),
        candidates AS (
            SELECT id FROM gaffer_jobs
            WHERE queue = $1
              AND state = 'available'
              AND (scheduled_at IS NULL
                   OR scheduled_at <= to_timestamp($2::bigint / 1000000.0))
            ORDER BY priority ASC, inserted_at ASC
            LIMIT GREATEST(0, (SELECT lim FROM effective_limit))
            FOR UPDATE SKIP LOCKED
        )
        UPDATE gaffer_jobs j
        SET state = $4,
            attempted_at = to_timestamp($5::bigint / 1000000.0)
        FROM candidates c
        WHERE j.id = c.id
        RETURNING
        """,
        ~" ",
        job_columns(~"j.")
    ]),
    [{SQL, [Queue, Now, Limit, State, Now]}].

-spec job_update(map()) -> [{binary(), list()}].
job_update(Encoded) ->
    #{id := Id} = Encoded,
    Fields = maps:remove(id, Encoded),
    {Sets, Vals} = set_clause(Fields),
    N = length(Vals) + 1,
    SQL = iolist_to_binary([
        ~"UPDATE gaffer_jobs SET ",
        Sets,
        ~" WHERE id = $",
        integer_to_binary(N)
    ]),
    [{SQL, Vals ++ [Id]}].

-spec job_prune(map()) -> [{binary(), list()}].
job_prune(Opts) ->
    States = maps:get(states, Opts, [completed, discarded]),
    TextArray = [atom_to_binary(S) || S <:- States],
    [
        {
            ~"DELETE FROM gaffer_jobs WHERE state = ANY($1::text[]) RETURNING id",
            [TextArray]
        }
    ].

%--- Internal -----------------------------------------------------------------

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
