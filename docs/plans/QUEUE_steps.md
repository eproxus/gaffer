# Refactor: Clean driver boundary & eliminate gaffer_job

## Context

Drivers currently contain domain logic (state transitions, retry/discard decisions, attempt counting) that should not be their concern. `gaffer_job` is a helper module with no clear ownership. This refactor moves all domain logic into `gaffer_queue`, reduces drivers to pure storage, and eliminates `gaffer_job`.

## Key Design Decisions

1. **Job stays one flat map** — `gaffer:job()` type unchanged.
2. **Driver primitives: `job_claim` + `job_update` + CRUD** — Replace high-level lifecycle callbacks with generic storage primitives.
3. **`gaffer_queue` for domain logic** — State machine, attempt counting, retry/discard, job construction. No process — takes `{Mod, DS}`.
4. **`gaffer_queue_proc` as the process** — gen_statem owning poll loop + worker management. Only calls `gaffer_queue`.
5. **Eliminate `gaffer_job`** — All logic absorbed by `gaffer_queue`.
6. **`gaffer.erl` becomes thin router** — Delegates to `gaffer_queue_proc`.

---

## Step 1: Add new types + update `gaffer_driver` behaviour

**Goal:** Define the new driver contract. Nothing else changes yet — old callbacks coexist temporarily.

**Files:**
- `src/gaffer.erl` — Add `claim_opts()`, `job_changes()` type exports; keep `fetch_opts()` for now
- `src/gaffer_driver.erl` — Replace `job_fetch`, `job_complete`, `job_fail`, `job_cancel`, `job_schedule` with `job_claim`, `job_update`, `job_delete`

**Verify:** `rebar3 compile` (will fail on ETS driver — expected, commit anyway with driver temporarily not implementing behaviour fully, OR update driver in same step)

**Decision:** Update `gaffer_driver_ets` in this same step so it compiles clean.

**Files (revised):**
- `src/gaffer.erl` — Add `claim_opts()`, `job_changes()` types
- `src/gaffer_driver.erl` — New callback set
- `test/support/gaffer_driver_ets.erl` — Rewrite to implement new callbacks (pure storage, no `gaffer_job` calls in `job_claim`/`job_update`)

**Note:** `gaffer_driver_ets` still needs `gaffer_job` in `job_claim` for the transition to `executing` — but per the new design, `job_claim` receives the changes map and applies it directly (no `gaffer_job:transition`). The claim logic moves to just: filter + apply changes map + move between tables.

**Verify:** `rebar3 compile && rebar3 fmt --check && rebar3 lint`

**Commit:** `refactor(driver): Replace lifecycle callbacks with storage primitives`

---

## Step 2: Create `gaffer_queue` — absorb `gaffer_job` logic

**Goal:** New module with all domain logic. `gaffer_job` still exists (not deleted yet).

**Files:**
- `src/gaffer_queue.erl` — New file containing:
  - `new/3` (job construction, from `gaffer_job:new/3`)
  - `validate/1` (from `gaffer_job:validate/1`)
  - `transition/2`, `valid_transition/2`, `set_timestamp/3` (from `gaffer_job`)
  - `add_error/2` (from `gaffer_job`)
  - Driver-coordinating operations: `insert/2`, `get/2`, `list/2`, `cancel/2`, `complete/2`, `fail/3`, `schedule/3`, `claim/2`, `prune/2`
  - All take `Driver :: {module(), driver_state()}` as last arg
- `test/gaffer_queue_tests.erl` — New unit tests for `gaffer_queue` using a mock driver
- `test/support/gaffer_driver_mock.erl` — Minimal map-based mock driver for isolated unit testing

**Verify:** `rebar3 compile && rebar3 eunit && rebar3 fmt --check && rebar3 lint`

**Commit:** `feat(queue): Add gaffer_queue domain logic module`

---

## Step 3: Wire `gaffer.erl` through `gaffer_queue`

**Goal:** `gaffer.erl` calls `gaffer_queue` instead of calling driver + `gaffer_job` directly.

**Files:**
- `src/gaffer.erl`:
  - `insert/3` → calls `gaffer_queue:new/3` then `gaffer_queue:insert/2`
  - `cancel/2` → calls `gaffer_queue:cancel/2`
  - `get/2` → calls `gaffer_queue:get/2`
  - `list/1` → calls `gaffer_queue:list/2`
  - Remove `gaffer_job` import/call

**Verify:** `rebar3 compile && rebar3 eunit && rebar3 fmt --check && rebar3 lint`

**Commit:** `refactor(gaffer): Route API through gaffer_queue`

---

## Step 4: Delete `gaffer_job` + migrate tests

**Goal:** Remove `gaffer_job.erl` entirely. All its logic now lives in `gaffer_queue`.

**Files:**
- `src/gaffer_job.erl` — Delete
- `test/gaffer_job_tests.erl` — Delete (tests already covered by `gaffer_queue_tests.erl` from Step 2)
- `rebar.config` — Remove `gaffer_job_tests` from `eunit_tests`, add `gaffer_queue_tests`
- `rebar.config` — Remove any hank/xref ignores for `gaffer_job`

**Verify:** `rebar3 compile && rebar3 eunit && rebar3 fmt --check && rebar3 lint && rebar3 hank && rebar3 dialyzer && rebar3 as test xref`

**Commit:** `refactor(job): Delete gaffer_job, tests migrated to gaffer_queue`

---

## Step 5: Create `gaffer_queue_proc` gen_statem

**Goal:** Process wrapper that serializes mutations through `gaffer_queue`. This prepares for the poll loop / worker dispatch (future work).

**Files:**
- `src/gaffer_queue_proc.erl` — New gen_statem:
  - Public API: `insert/2`, `get/2`, `list/2`, `cancel/2`, `complete/2`, `fail/3`, `schedule/3`
  - Reads bypass process (call driver via `gaffer_queue` directly)
  - Mutations serialize through gen_statem calls
  - Holds `{Mod, DS}` driver ref in state
- `src/gaffer_sup.erl` — Update if needed for supervision
- `src/gaffer.erl` — Route through `gaffer_queue_proc` instead of calling `gaffer_queue` directly
- `rebar.config` — Update hank ignores for new behaviour module

**Verify:** `rebar3 compile && rebar3 eunit && rebar3 ct && rebar3 fmt --check && rebar3 lint && rebar3 hank && rebar3 dialyzer && rebar3 as test xref`

**Commit:** `feat(queue_proc): Add gen_statem process wrapper`

---

## Step 6: Cleanup + remove `fetch_opts` type

**Goal:** Remove any leftover dead code from the old design.

**Files:**
- `src/gaffer.erl` — Remove `fetch_opts()` type if unused
- Any remaining references cleanup

**Verify:** Full verification suite:
```sh
rebar3 compile && rebar3 eunit && rebar3 ct && rebar3 fmt --check && rebar3 lint && rebar3 hank && rebar3 dialyzer && rebar3 as test xref
```

**Commit:** `chore: Remove dead fetch_opts type`

---

## Final Invariants

After all steps:
- `gaffer_driver_ets` has zero calls to `gaffer_job`
- No references to `gaffer_job` remain anywhere
- State machine logic lives exclusively in `gaffer_queue`
- `gaffer_queue` has no process dependencies
- `gaffer_queue_proc` only calls `gaffer_queue`, never the driver directly
- `gaffer_queue` is the only module that calls driver job callbacks
