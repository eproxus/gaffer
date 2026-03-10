# Refactor: Clean driver boundary & eliminate gaffer_job

## Context

Drivers currently contain domain logic (state transitions, retry/discard decisions, attempt counting) that should not be their concern. `gaffer_job` is a helper module with no clear ownership. This refactor moves all domain logic into `gaffer_queue`, reduces drivers to pure storage, and eliminates `gaffer_job`.

## Key Design Decisions

1. **Job stays one flat map** — No envelope/payload split. The `gaffer:job()` type is unchanged. Drivers store the full map and query on well-known fields (`state`, `queue`, `priority`, `scheduled_at`) as an implementation detail.
2. **Driver primitives: `job_claim` + `job_update` + CRUD** — Replace high-level lifecycle callbacks (`job_complete`, `job_fail`, `job_schedule`) with generic storage primitives. Drivers never call `gaffer_job` or contain state machine logic.
3. **`gaffer_queue` for domain logic + driver coordination** — State machine, attempt counting, retry/discard decisions, error accumulation, job construction. Wraps all driver calls internally. No process — takes `{Mod, DS}` as parameter. Unit-testable with `gaffer_driver_ets`.
4. **`gaffer_queue_runner` as the process** — gen_statem that owns the poll loop and worker management. Only calls `gaffer_queue` — never the driver directly. This is the module that `gaffer.erl` and external callers interact with.
5. **Eliminate `gaffer_job`** — All logic absorbed by `gaffer_queue`. Module is deleted.
6. **`gaffer.erl` becomes a thin router** — Public API delegates to `gaffer_queue_runner`.

## New Driver Behaviour (`gaffer_driver`)

```erlang
%% Lifecycle
-callback start(Opts :: map()) -> {ok, driver_state()}.
-callback stop(driver_state()) -> ok.

%% Queue config (unchanged)
-callback queue_put(gaffer:queue_conf(), driver_state()) -> ok.
-callback queue_get(gaffer:queue_name(), driver_state()) -> {ok, gaffer:queue_conf()}.
-callback queue_delete(gaffer:queue_name(), driver_state()) -> ok.

%% Job CRUD
-callback job_insert(gaffer:job(), driver_state()) -> {ok, gaffer:job()}.
-callback job_get(gaffer:job_id(), driver_state()) -> {ok, gaffer:job()} | {error, not_found}.
-callback job_list(gaffer:list_opts(), driver_state()) -> {ok, [gaffer:job()]}.
-callback job_delete(gaffer:job_id(), driver_state()) -> ok.

%% Atomic claim — find jobs matching criteria, apply changes, return them
-callback job_claim(gaffer:claim_opts(), gaffer:job_changes(), driver_state()) -> {ok, [gaffer:job()]}.

%% Persist a fully-prepared job (orchestration has already done all mutations)
-callback job_update(gaffer:job(), driver_state()) -> {ok, gaffer:job()}.

%% Bulk prune
-callback job_prune(gaffer:prune_opts(), driver_state()) -> {ok, non_neg_integer()}.
```

### `job_claim` semantics
- Atomically finds jobs matching `claim_opts` (state, queue, schedule, limit, priority ordering)
- Applies `job_changes` (a map of envelope field updates, e.g. `#{state => executing}`)
- Returns the updated jobs
- Postgres: single `UPDATE ... WHERE ... ORDER BY ... LIMIT ... RETURNING *`
- ETS: filter + move between tables

### `job_update` semantics
- Persists a complete job map — the driver replaces the stored version
- The caller has already applied all field changes (state, attempt, errors, timestamps)
- Driver just writes

## Module Responsibilities After Refactor

### `gaffer.erl` — Public API & thin router
- Queue CRUD (mostly unchanged)
- `insert/2,3` — builds the initial job map (absorbs `gaffer_job:new/3`), routes to `gaffer_queue_runner`
- `cancel/2`, `get/2`, `list/1` — routes to `gaffer_queue_runner`

### `gaffer_queue.erl` — Domain logic + driver coordination (no process)
- Takes `{Mod, DS}` (driver module + state) as parameter on all calls
- **Job construction**: absorbs `gaffer_job:new/3`
- **Validation**: absorbs `gaffer_job:validate/1`
- **State machine** (private): `valid_transition/2`, transition + timestamp bookkeeping
- **Operations** (coordinate domain logic + driver calls):
  - `insert(Job, Driver)` → validates, persists via `driver:job_insert`
  - `get(Id, Driver)` → `driver:job_get`
  - `list(Opts, Driver)` → `driver:job_list`
  - `cancel(Id, Driver)` → fetches job, applies cancel transition, `driver:job_update`
  - `complete(Id, Driver)` → fetches job, applies complete transition + attempt increment, `driver:job_update`
  - `fail(Id, Error, Driver)` → fetches job, applies fail transition + attempt + error + retry/discard, `driver:job_update`
  - `schedule(Id, At, Driver)` → fetches job, applies schedule transition, `driver:job_update`
  - `claim(Opts, Driver)` → `driver:job_claim(Opts, #{state => executing})`
  - `prune(Opts, Driver)` → `driver:job_prune`
- Unit-testable without processes — tests use a simple mock driver (not even `gaffer_driver_ets`), just a functional placeholder (e.g. map-based) that verifies business logic in isolation via step-by-step function calls

### `gaffer_queue_runner.erl` — gen_statem process (only calls `gaffer_queue`)
- **Public module API** (called by `gaffer.erl`):
  - `insert(Queue, Job)` → `gaffer_queue:insert(Job, Driver)`
  - `get(Queue, Id)` → `gaffer_queue:get(Id, Driver)` (no process serialization needed)
  - `list(Queue, Opts)` → `gaffer_queue:list(Opts, Driver)`
  - `cancel(Queue, Id)` → serialized through process → `gaffer_queue:cancel(Id, Driver)`
  - `complete(Queue, Id)` → serialized through process
  - `fail(Queue, Id, Error)` → serialized through process
  - `schedule(Queue, Id, At)` → serialized through process
- **Process internals**:
  - Poll tick → `gaffer_queue:claim(Opts, Driver)` → dispatch to workers
  - Worker result → `gaffer_queue:complete/fail/schedule(...)`
- Never calls the driver directly — all driver access goes through `gaffer_queue`
- Reads bypass the process; mutations serialize through it

### `gaffer_driver.erl` — Behaviour definition
- Updated callbacks as above
- No domain logic, pure storage contract

### `gaffer_job.erl` — DELETED
- `new/3` → absorbed by `gaffer_queue`
- `validate/1` → absorbed by `gaffer_queue`
- `transition/2` → absorbed by `gaffer_queue`
- `add_error/2` → trivial inline operation in `gaffer_queue`

### `gaffer_driver_ets.erl` (test driver) — Simplified
- No more `gaffer_job` calls
- `job_claim` replaces `job_fetch` — filters, sorts, moves between tables, applies changes map
- `job_update` replaces `job_complete`/`job_fail`/`job_schedule`/`job_cancel` — just persists
- `job_delete` added for single-job removal
- Significantly simpler implementation

## Files to Modify

| File | Action |
|------|--------|
| `src/gaffer_driver.erl` | Update callbacks |
| `src/gaffer.erl` | Absorb `new/3`, route to `gaffer_queue_runner` |
| `src/gaffer_job.erl` | Delete |
| `src/gaffer_queue.erl` | New — pure domain logic (absorbs `gaffer_job`) |
| `src/gaffer_queue_runner.erl` | New — gen_statem process, only consumer of `gaffer_queue` |
| `src/gaffer_sup.erl` | May need updates for `gaffer_queue_runner` supervision |
| `test/gaffer_job_tests.erl` | Migrate to `test/gaffer_queue_tests.erl` |
| `test/support/gaffer_driver_mock.erl` | New — minimal mock driver (map-based) for `gaffer_queue` unit tests |
| `test/support/gaffer_driver_ets.erl` | Rewrite to new driver API |
| `test/gaffer_tests.erl` | Update to work with new routing |

## Implementation Order

1. **Define new types** — `claim_opts()`, `job_changes()` in `gaffer.erl`
2. **Update `gaffer_driver` behaviour** — new callback set
3. **Rewrite `gaffer_driver_ets`** — implement new callbacks, remove all `gaffer_job` calls
4. **Create `gaffer_queue`** — pure domain logic, absorb `gaffer_job` functions
5. **Create `gaffer_queue_runner`** — gen_statem process, module API, uses `gaffer_queue` + driver
6. **Update `gaffer.erl`** — route through `gaffer_queue_runner`
7. **Delete `gaffer_job.erl`** — migrate tests to `gaffer_queue_tests.erl`
8. **Update existing tests** — `gaffer_tests.erl` and any test helpers

## Verification

```sh
rebar3 compile              # Must compile clean (warnings_as_errors)
rebar3 eunit                # All unit tests pass
rebar3 ct                   # All integration tests pass
rebar3 fmt --check          # Formatting
rebar3 lint                 # Elvis linter
rebar3 hank                 # No dead code
rebar3 dialyzer             # Type checking
rebar3 as test xref         # Cross-reference checking
```

Key things to verify manually:
- `gaffer_driver_ets` has zero calls to `gaffer_job`
- No module imports or references to `gaffer_job` remain
- State machine logic lives exclusively in `gaffer_queue`
- `gaffer_queue` has no process dependencies — testable with a mock driver via stepping
- `gaffer_queue_runner` only calls `gaffer_queue`, never the driver directly
- `gaffer_queue` is the only module that calls driver job callbacks
