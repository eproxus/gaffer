-module(gaffer_postgres).

%% Pure SQL module — no pgo dependency. Contains all SQL and
%% serialization logic for the Postgres driver.
%%
%% All public functions return [{SQL, Params}] — a uniform list
%% of query tuples. The driver runs these in a transaction.

-export([migrations/1]).
-export([migrate_up/1]).
-export([migrate_down/1]).
-export([ensure_migrations_table/0]).
-export([applied_version/0]).

%--- Migrations ---------------------------------------------------------------

-spec migrations(Opts :: map()) ->
    [
        {
            Version :: pos_integer(),
            Up :: [{binary(), list()}],
            Down :: [{binary(), list()}]
        }
    ].
migrations(Opts) ->
    UUIDDefault = uuid_column_default(maps:get(uuid_format, Opts, v4)),
    [
        {1,
            queries([
                %% Queues
                ~"""
                CREATE TABLE gaffer_queues (
                    name               TEXT PRIMARY KEY,
                    worker             TEXT,
                    global_max_workers INTEGER,
                    max_workers        INTEGER,
                    poll_interval      INTEGER,
                    shutdown_timeout   INTEGER,
                    max_attempts       INTEGER,
                    timeout            INTEGER,
                    backoff            INTEGER,
                    priority           INTEGER DEFAULT 0,
                    on_discard         TEXT REFERENCES gaffer_queues(name)
                )
                """,
                %% Jobs
                iolist_to_binary([
                    ~"""
                    CREATE TABLE gaffer_jobs (
                        id             UUID PRIMARY KEY DEFAULT
                    """,
                    [~" ", UUIDDefault, ~","],
                    ~"""
                        queue          TEXT NOT NULL,
                        state          TEXT NOT NULL DEFAULT 'available'
                                           CHECK (state IN ('available', 'scheduled', 'executing',
                                                            'completed', 'failed', 'cancelled',
                                                            'discarded')),
                        payload        JSONB NOT NULL DEFAULT '{}',
                        attempt        INTEGER NOT NULL DEFAULT 0,
                        max_attempts   INTEGER NOT NULL DEFAULT 3,
                        priority       INTEGER NOT NULL DEFAULT 0,
                        errors         JSONB NOT NULL DEFAULT '[]',
                        scheduled_at   TIMESTAMPTZ,
                        inserted_at    TIMESTAMPTZ NOT NULL,
                        attempted_at   TIMESTAMPTZ,
                        completed_at   TIMESTAMPTZ,
                        cancelled_at   TIMESTAMPTZ,
                        discarded_at   TIMESTAMPTZ
                    )
                    """
                ]),
                %% Query indexes
                ~"""
                CREATE INDEX idx_gaffer_jobs_claimable
                    ON gaffer_jobs (queue, priority, inserted_at)
                    WHERE state = 'available'
                """,
                ~"""
                CREATE INDEX idx_gaffer_jobs_queue_state
                    ON gaffer_jobs (queue, state)
                """,
                %% Maintenance indexes
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

%% Returns a query guaranteed to return a single row with an integer version.
-spec applied_version() -> [{binary(), list()}].
applied_version() ->
    [{~"SELECT version FROM gaffer_schema_version", []}].

%--- Internal -----------------------------------------------------------------

queries(SQLs) ->
    [{SQL, []} || SQL <:- SQLs].

uuid_column_default(v4) -> ~"gen_random_uuid()";
uuid_column_default(v7) -> ~"uuidv7()".
