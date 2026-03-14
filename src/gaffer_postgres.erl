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

%% Queue CRUD
-export([queue_insert/1]).
-export([queue_update/2]).
-export([queue_get/1]).
-export([queue_delete/1]).

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
    {Cols, Phs, Vals} = columns_and_values(Changes),
    Sets = lists:join(~", ", [
        <<C/binary, " = ", P/binary>>
     || {C, P} <:- lists:zip(Cols, Phs)
    ]),
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

%--- Internal -----------------------------------------------------------------

-spec columns_and_values(map()) ->
    {Columns :: [binary()], Placeholders :: [binary()], Values :: [term()]}.
columns_and_values(Map) ->
    Pairs = maps:to_list(Map),
    Cols = [atom_to_binary(K) || {K, _} <:- Pairs],
    Vals = [V || {_, V} <:- Pairs],
    Phs = [
        iolist_to_binary([~"$", integer_to_binary(I)])
     || I <:- lists:seq(1, length(Cols))
    ],
    {Cols, Phs, Vals}.

queries(SQLs) ->
    [{SQL, []} || SQL <:- SQLs].

uuid_column_default(v4) -> ~"gen_random_uuid()";
uuid_column_default(v7) -> ~"uuidv7()".
