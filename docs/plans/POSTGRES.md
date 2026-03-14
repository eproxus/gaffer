# Postgres Driver for Gaffer

## Context

Gaffer needs a production-grade persistent storage backend. The current drivers
(mock and ETS) are for testing only. A Postgres driver enables durable job
storage, atomic claiming via `FOR UPDATE SKIP LOCKED`, and cluster-wide
coordination through shared state.

The design uses two layers so that apps using a Postgres library other than `pgo`
can reuse the SQL/serialization layer (`gaffer_postgres`) and only implement a
thin execution wrapper.

## Design Decisions

| Decision | Choice |
|---|---|
| Queue config | Persist to Postgres (cluster-global settings) |
| `errors` serialization | JSONB (system-generated, separate column) |
| `payload` | Single JSONB column |
| Job states | `TEXT` column with `CHECK` constraint (no lookup table) |
| `queue` field | `text` in PG, `binary_to_atom/2` on read |
| Timestamps | `timestamptz` in PG, microseconds in Erlang |
| Atomic claim | Single CTE with `FOR UPDATE SKIP LOCKED` |
| pgo pool | Accept pool name or pool config |
| Migrations | Up/down style, `gaffer_schema_version` table (single row) |
| Docker in CT | Rebar3 pre/post hooks for `docker compose` |
| JSON library | OTP 27+ built-in `json` module |
| UUID format | Configurable: `v4` (default) or `v7` (PG 18+) |

## Schema

### `gaffer_schema_version`

```sql
CREATE TABLE IF NOT EXISTS gaffer_schema_version (
    version BIGINT NOT NULL DEFAULT 0
);
```

### `gaffer_queues`

```sql
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
);
```

Only cluster-relevant settings are persisted. The `driver` key is runtime-only
and not stored.

### `gaffer_jobs`

```sql
CREATE TABLE gaffer_jobs (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(), -- or uuidv7() on PG 18+
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
);
```

### Indexes

```sql
-- Claim query: available jobs by priority then insertion time
CREATE INDEX idx_gaffer_jobs_claimable
    ON gaffer_jobs (queue, priority, inserted_at)
    WHERE state = 'available';

-- List by queue + state
CREATE INDEX idx_gaffer_jobs_queue_state
    ON gaffer_jobs (queue, state);

-- Prune by state
CREATE INDEX idx_gaffer_jobs_state
    ON gaffer_jobs (state);

-- Scheduled jobs needing promotion
CREATE INDEX idx_gaffer_jobs_scheduled
    ON gaffer_jobs (scheduled_at)
    WHERE state = 'available' AND scheduled_at IS NOT NULL;
```

## Timestamp Conversion

- **Write**: `to_timestamp($N::bigint / 1000000.0)` converts microseconds to
  `timestamptz`
- **Read**: `(EXTRACT(EPOCH FROM col) * 1000000)::bigint` converts back to
  microseconds
- Define a `?JOB_COLUMNS` macro with the EXTRACT expressions so all SELECT
  queries are consistent

## UUID Format

The job ID default is configurable via the `uuid_format` option passed to
`gaffer_postgres:migrations/1`:

| Value | SQL default | Requires |
|---|---|---|
| `v4` (default) | `gen_random_uuid()` | Postgres 13+ |
| `v7` | `uuidv7()` | Postgres 18+ |

UUIDv7 encodes a millisecond timestamp, making IDs roughly time-ordered. This
improves B-tree locality on the primary key and makes `inserted_at`-based
ordering nearly free. It is recommended for new deployments on Postgres 18+.

The format is baked into the migration DDL at `start/1` time — it is not a
runtime toggle. Changing the format after initial migration requires a manual
`ALTER TABLE` to update the column default.

## Module: `gaffer_postgres` (`src/gaffer_postgres.erl`)

Pure functional module — no pgo dependency. Contains all SQL query definitions.

### Exports

```
%% Migrations — returns versioned up/down query lists
migrations/1              -> [{Version, Up, Down}]      % accepts #{uuid_format => v4 | v7}
migrate_up/1              -> [{SQL, Params}]             % wrap up queries + version update
migrate_down/1            -> [{SQL, Params}]             % wrap down queries + version decrement
ensure_migrations_table() -> [{SQL, Params}]             % CREATE IF NOT EXISTS + seed
applied_version()         -> [{SQL, Params}]             % SELECT current version

%% Queue config queries — each returns [{SQL, Params}]
queue_put/1           (QueueConf)
queue_get/1           (QueueName)
queue_delete/1        (QueueName)

%% Job queries — each returns [{SQL, Params}]
job_insert/1          (NewJob)  — INSERT without id, RETURNING * to get driver-generated UUID
job_get/1             (JobId)
job_list/1            (ListOpts)
job_delete/1          (JobId)
job_claim/2           (ClaimOpts, JobChanges)
job_update/1          (Job)
job_prune/1           (PruneOpts)
```

### Claim Query (CTE)

```sql
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
RETURNING <JOB_COLUMNS>
```

- `queue_put/1` uses `INSERT ... ON CONFLICT (name) DO UPDATE SET ...` (upsert).

## Module: `gaffer_driver_pgo` (`src/gaffer_driver_pgo.erl`)

Implements `gaffer_driver` behaviour. Thin wrapper that calls `gaffer_postgres`
for SQL and `pgo:query/3` for execution.

### State

```erlang
-type state() :: #{
    pool := atom(),
    pool_owned := boolean()   % true if we started it, false if external
}.
```

### `start/1`

Accepts:
- `#{pool => atom()}` — use existing pool
- `#{pool => atom(), start => map()}` — start a dedicated pool via `pgo:start_pool/2`
- `#{uuid_format => v4 | v7}` — optional, defaults to `v4`

After pool is ready, runs migrations (up only) with
`gaffer_postgres:migrations(Opts)`:
1. `CREATE TABLE IF NOT EXISTS gaffer_schema_version ...` (seeds version 0)
2. Read current version
3. Apply pending migrations in order (each in a transaction)
4. Update version in `gaffer_schema_version` after each successful up

The `uuid_format` option from `start/1` is forwarded to `migrations/1` so the
correct UUID default is embedded in the `CREATE TABLE` DDL.

### `rollback/2`

```erlang
rollback(TargetVersion, #{pool := Pool}) -> ok.
```

Manual operational tool — rolls back all migrations newer than `TargetVersion`
in reverse order. For each rolled-back migration:
1. Run the down SQL in a transaction
2. Update version in `gaffer_schema_version`

Not exposed in the public `gaffer` API. Intended for shell/operational use only.

### `stop/1`

If `owns_pool` is true, stop the pool. Otherwise no-op.

### Serialization Notes

Each driver owns its own row deserialization (`row_to_job/2`, `row_to_queue_conf/2`
are private functions within the driver module).

- `errors` field: `error := term()` serialized via `io_lib:format("~0tp", [T])`
  for human readability. Stored as a JSON string within the JSONB array.
- `state` field: stored as `text` with a `CHECK` constraint. Read back
  with `binary_to_atom/2`. The constraint ensures only valid states are persisted.
- **Queue config atoms** (`name`, `worker`, `on_discard`): use
  `binary_to_existing_atom/2`. These atoms are guaranteed to already exist in the
  VM atom table — `name` was passed as an atom to `queue_get`, worker modules are
  loaded code, and `on_discard` references another queue name that must already be
  registered.
- **Job `queue` field**: use `binary_to_atom/2` (not `existing_atom`). On cold
  start the queue runner for that name may not have started yet, so the atom may
  not be in the table. This is the one exception where `binary_to_atom/2` is
  acceptable.
- NULL columns: omitted from the Erlang map (optional `=>` keys) via a
  `filter_nulls/1` helper.
- JSON encoding/decoding: use OTP `json` module.

### Callback Pattern

Each callback follows:

```erlang
job_insert(Job, #{pool := Pool} = State) ->
    {SQL, Params} = gaffer_postgres:job_insert(Job),
    #{rows := [Row], columns := Cols} = pgo:query(Pool, SQL, Params),
    {row_to_job(Cols, Row), State}.
```

Errors from pgo raise exceptions (project convention: prefer exceptions over
tagged returns for unrecoverable errors).

`queue_put/2` must catch FK violation on `on_discard` and raise
`error({on_discard_queue_not_found, Name})` so callers get a clear error when
referencing a non-existent discard queue.

## Test Infrastructure

### `docker-compose.yml` (project root)

```yaml
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: gaffer_test
      POSTGRES_USER: gaffer
      POSTGRES_PASSWORD: gaffer
    ports:
      - "54320:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U gaffer -d gaffer_test"]
      interval: 1s
      timeout: 5s
      retries: 10
```

### Rebar3 Hooks (`rebar.config` test profile)

```erlang
{pre_hooks, [{ct, "docker compose up -d --wait"}]},
{post_hooks, [{ct, "docker compose down"}]}
```

### CT Config (`config/test.config`)

```erlang
{postgres, [
    {host, "localhost"},
    {port, 54320},
    {database, "gaffer_test"},
    {user, "gaffer"},
    {password, "gaffer"}
]}.
```

### CT Suite: `test/gaffer_driver_pgo_SUITE.erl`

- `init_per_suite/1` — start pgo app, extract pool config from CT config
- `end_per_suite/1` — no-op (pool managed per testcase)
- `init_per_testcase/2` — start pool, reset database (DROP/CREATE schema)
- `end_per_testcase/2` — reset database, stop pool

**Test cases:**
- `migration_idempotent` — calling start twice doesn't fail
- `migration_rollback` — rollback removes tables, resets version
- `start_with_new_pool` — start with owned pool (`start` key), verify `owns_pool`
- `queue_put_get_delete` — queue config CRUD
- `insert_and_get` — round-trip all fields
- `insert_and_list` — list with queue/state filters
- `claim_basic` — claim transitions to executing
- `claim_priority_ordering` — lower priority claimed first
- `claim_scheduled_filtering` — future-scheduled jobs skipped
- `claim_skip_locked` — concurrent claims don't double-claim
- `claim_global_max_workers` — respects limit from queue config
- `cancel_job` — state transition
- `complete_and_fail` — lifecycle transitions
- `update_job` — update and re-read
- `prune` — delete terminal-state jobs, verify count

## Changes to Existing Files

| File | Change |
|---|---|
| `rebar.config` | Add `pgo` dep, add CT pre/post hooks, add `config/test.config` |
| `hank` ignore | Add `gaffer_driver_pgo.erl` unused_callbacks if needed |

`pgo` is an optional/user-supplied dep — not a gaffer dep. It is added as a
test-only dep in the test profile so CT can run. Users who want the pgo driver
add `{pgo, "0.14.0"}` to their own deps.

All `gaffer_postgres` functions return `[{SQL, Params}]` — a uniform list of
query tuples. The driver always runs these in a transaction. Migrations also
use `[{SQL, Params}]` tuples (with `[]` params for DDL).

## New Files

| File | Purpose |
|---|---|
| `src/gaffer_postgres.erl` | SQL query definitions |
| `src/gaffer_driver_pgo.erl` | pgo-based driver (behaviour impl) |
| `test/gaffer_driver_pgo_SUITE.erl` | CT test suite |
| `config/test.config` | Postgres connection config for CT |
| `docker-compose.yml` | Test Postgres container |

## Implementation Order

Organized as vertical slices — each milestone delivers end-to-end functionality
that compiles, passes `mise run verify` and `mise run test`, and can be committed
cleanly.

Each milestone follows TDD (red-green):

1. **Red** — Write CT test cases first (they must compile but fail)
2. **Green** — Implement until tests pass
3. **Verify** — `mise run verify` + `mise run test`

### Milestone 1: Foundation (migrations + start/stop + test infra)

Everything needed to boot the driver against a real Postgres and verify
migrations work.

- [x] `docker-compose.yml`
- [x] `config/test.config`
- [x] `rebar.config` — add `pgo` dep, CT pre/post hooks
- [x] `gaffer_postgres`: `migrations/1`, `migrate_up/1`, `migrate_down/1`, `ensure_migrations_table/0`, `applied_version/0`
- [x] `gaffer_driver_pgo`: `start/1`, `stop/1`, `rollback/2`
- [x] CT: `init_per_suite`, `end_per_suite`, `migration_idempotent`, `migration_rollback`, `start_with_new_pool`
- [x] Verify: `mise run verify` + `mise run test`

### Milestone 2: Queue config CRUD

- [ ] **Red**: CT `queue_put_get_delete` (calls driver queue callbacks, asserts round-trip)
- [ ] **Green**: `gaffer_postgres` `queue_put/1`, `queue_get/1`, `queue_delete/1`
- [ ] **Green**: `gaffer_driver_pgo` queue callbacks, `row_to_queue_conf/2`
- [ ] **Verify**: `mise run verify` + `mise run test`

### Milestone 3: Job basics (insert, get, list, delete)

- [ ] **Red**: CT `insert_and_get`, `insert_and_list`, `job_delete` (call driver job callbacks, assert round-trip)
- [ ] **Green**: `gaffer_postgres` `job_insert/1`, `job_get/1`, `job_list/1`, `job_delete/1`
- [ ] **Green**: `gaffer_driver_pgo` job CRUD callbacks, `row_to_job/2`
- [ ] **Verify**: `mise run verify` + `mise run test`

### Milestone 4: Job lifecycle (claim, update, state transitions)

- [ ] **Red**: CT `claim_basic`, `claim_priority_ordering`, `claim_scheduled_filtering`, `claim_skip_locked`, `claim_global_max_workers`, `update_job`, `complete_and_fail`, `cancel_job`
- [ ] **Green**: `gaffer_postgres` `job_claim/2`, `job_update/1`
- [ ] **Green**: `gaffer_driver_pgo` claim + update callbacks
- [ ] **Verify**: `mise run verify` + `mise run test`

### Milestone 5: Pruning + final verification

- [ ] **Red**: CT `prune` (insert terminal jobs, call prune, assert deleted)
- [ ] **Green**: `gaffer_postgres` `job_prune/1`
- [ ] **Green**: `gaffer_driver_pgo` prune callback
- [ ] **Verify**: `mise run verify` + `mise run test`

## Verification

```sh
mise run verify                # Full verification
mise run test                  # All tests
```
