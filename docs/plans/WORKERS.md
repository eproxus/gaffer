# Workers

Job execution layer for gaffer — fetches jobs and runs them through worker
callbacks.

## Goals

- One Erlang process per job (proper isolation)
- Node-local and global concurrency limits
- Monitored processes (no OTP restart — gaffer handles retries via the queue)
- Multi-node: multiple Erlang nodes produce/consume on the same queue
- Pluggable wake-up mechanism (polling baseline, LISTEN/NOTIFY as optimization)

## Design

### Concurrency Model

Two separate limits, clearly named:

- `global_max_workers` — total concurrent workers across all nodes for a queue.
  Stored in the driver (DB). Enforced at `job_fetch` time (e.g.
  `SELECT ... FOR UPDATE SKIP LOCKED` with a subquery checking executing count).
- `max_workers` — cap on how many workers *this node* runs for a queue.
  Operational/deployment concern, not stored in DB.

### Polling and Wake-Up

The queue runner polls the driver on a configurable interval. This is the
baseline mechanism that always works. Event-based drivers (e.g. Postgres
LISTEN/NOTIFY) can additionally send a wake-up signal to trigger an early
fetch.

```
Runner (gen_statem)
  |
  +-- state_timeout: poll every Interval ms ──→ fetch + spawn
  |
  +-- optional: driver wake-up event ──→ same fetch + spawn
```

Both paths lead to the same action: calculate available slots, call
`Driver:job_fetch/2`, spawn workers. The gen_statem serializes all events —
no concurrent fetches, no races.

#### Poll Interval

Configurable per queue via `poll_interval`:
- Default: `1000` (1 second)
- Set to `infinity` to disable polling entirely (pure push)

#### Wake-Up Signal

The driver receives a `notify` fun in `start/1` opts. Calling it sends a
`poll` message to the runner, triggering an immediate fetch cycle. For
LISTEN/NOTIFY drivers, this is called from the notification handler. For
simple drivers, it's optional — polling handles everything.

```erlang
%% notify fun provided to driver
Notify = fun() -> runner ! poll end
```

#### Why Poll Even with LISTEN/NOTIFY

- Missed notifications (connection drops, reconnects)
- Scheduled jobs becoming due (no notification at insert time)
- Startup (existing available jobs, no new insert to trigger notification)
- Crash recovery (stale `executing` jobs from dead nodes)

A LISTEN/NOTIFY driver would use a longer poll interval (e.g. 5-30s) as a
safety net, with notifications handling the fast path.

Note: Postgres LISTEN/NOTIFY carries no job data — it's just a signal that
triggers an early poll. The same `job_fetch` query runs regardless of trigger.

### Process Architecture

```
gaffer_sup (one_for_one)
  |
  +-- gaffer_queue_runner (gen_statem, one per queue)
        |
        +-- spawned worker process (monitored, one per job)
        +-- spawned worker process
        +-- ...
```

Worker processes are **spawned and monitored**, not supervised. When a worker
exits, the runner handles the outcome through the driver (complete/fail/cancel).
OTP restart strategies are not used — the queue *is* the retry mechanism.

### Queue Runner (`gaffer_queue_runner`)

A `gen_statem` per queue (using `handle_event_function` callback mode),
responsible for fetching and executing jobs on this node.

#### States

```
          poll/wake-up
     ┌──────────────────┐
     ▼                  │
  running ─────────→ paused
     │   pause/0        │
     │                  │
     │   resume/0       │
     ◄──────────────────┘
```

- **`running`** — actively polling and spawning workers. The poll cycle runs
  as a `state_timeout`, which is automatically cancelled on state change.
- **`paused`** — no new jobs are fetched. In-flight workers continue to
  completion. Transitioning back to `running` triggers an immediate poll.

#### Data

```erlang
#{
    queue := queue_name(),
    driver := {module(), driver_state()},
    worker := module(),
    max_workers := pos_integer(),
    poll_interval := pos_integer() | infinity,
    timeout := pos_integer(),            % job execution timeout (ms)
    shutdown_timeout := pos_integer(),   % grace period before kill (ms)
    active := #{pid() => job()}
}
```

#### Poll Cycle (Running State)

1. Enters `running` state or receives `state_timeout` / driver wake-up event
2. Calculates available slots: `max_workers - map_size(active)`
3. If slots > 0, fetches up to that many jobs via `Driver:job_fetch/2`
4. Spawns one monitored process per job
5. Sets a `{job_timeout, Pid}` event timer per job
6. Returns `{state_timeout, PollInterval, poll}` to schedule next cycle

The `state_timeout` is the natural fit here — it resets on every state
entry and is implicitly cancelled when the runner transitions to `paused`.

#### Paused State

In `paused`, the runner ignores poll timeouts and wake-up signals. Worker
`DOWN` messages and job timeouts are still handled — in-flight work runs
to completion. When the last active worker finishes in `paused` state, the
runner is idle and can be safely shut down for drain scenarios.

### Worker Callback (`gaffer_worker` behaviour)

```erlang
-callback perform(Job :: gaffer:job()) ->
    complete                    % success, no payload
    | {complete, term()}        % success with result
    | {fail, term()}            % known/expected failure (will retry)
    | {cancel, binary()}        % explicitly give up
    | {schedule, timestamp()}.  % retry at specific time
```

Each return value maps to a job state transition. Crashes and unexpected return
values are treated as failures too, but distinguished in error metadata.

| Return                | Job transition              | Semantics                    |
|-----------------------|-----------------------------|------------------------------|
| `complete`            | `executing -> completed`    | Success                      |
| `{complete, Result}`  | `executing -> completed`    | Success with result          |
| `{fail, Reason}`      | `executing -> failed`       | Known/expected failure       |
| `{cancel, Reason}`    | `executing -> cancelled`    | Explicitly give up           |
| `{schedule, At}`      | `executing -> scheduled`    | Retry at specific time       |
| crash / unexpected    | `executing -> failed`       | Unexpected failure           |

### Worker Process

Workers are plain spawned processes, not supervised. The runner uses
`spawn_monitor/1` to track each worker and handles all outcomes via the
`DOWN` message.

#### Spawn

```erlang
spawn_monitor(fun() -> exit({gaffer_result, Worker:perform(Job)}) end)
```

The return value is wrapped in a `{gaffer_result, _}` tag and encoded as the
exit reason. This lets the runner's `DOWN` handler distinguish callback return
values from raw crashes — any exit reason not matching the tag is a crash.

#### DOWN Handling

When the runner receives `{'DOWN', Ref, process, Pid, Reason}`, it first
unwraps the tag. A `{gaffer_result, Result}` reason is a callback return
value; anything else is a crash.

| Exit reason                          | Driver call                              |
|--------------------------------------|------------------------------------------|
| `{gaffer_result, complete}`          | `job_complete(Job, #{})`                 |
| `{gaffer_result, {complete, Result}}`| `job_complete(Job, #{result => Result})` |
| `{gaffer_result, {fail, Reason}}`    | `job_fail(Job, #{reason => Reason})`     |
| `{gaffer_result, {cancel, Reason}}`  | `job_cancel(Job, #{reason => Reason})`   |
| `{gaffer_result, {schedule, At}}`    | `job_schedule(Job, At)`                  |
| `{gaffer_result, Other}`             | `job_fail(Job, #{reason => {bad_return, Other}})` |
| Other (crash/kill/normal)            | `job_fail(Job, #{reason => Reason})`     |

The tag ensures no collision with exit reasons produced by libraries or OTP
internals. Unrecognized callback return values (`{gaffer_result, Other}`) and
crashes are both treated as failures with the reason preserved in metadata.

After the driver call, the runner removes the pid from `active`, cancels the
job's event timers (set to `infinity`), and triggers a new poll via a
`{next_event, internal, poll}` action to fill the freed slot.

### Timeout Handling

Per-job timeout using gen_statem event timers with graceful shutdown:

1. `{{timeout, {job, Pid}}, Timeout, shutdown}` — set when job starts
2. On timeout: `exit(Pid, shutdown)` — gives the worker a chance to clean up
3. `{{timeout, {kill, Pid}}, ShutdownTimeout, kill}` — grace period timer
4. On grace timeout (if still alive): `exit(Pid, kill)`

Event timers are keyed by `{job, Pid}` / `{kill, Pid}`, so each job gets
its own independent timer managed by the gen_statem runtime. No manual timer
cancellation bookkeeping — updating a named event timer to `infinity`
cancels it.

`shutdown_timeout` defaults to 5000ms, configurable per queue.

### Queue Configuration

```erlang
-type queue_conf() :: #{
    name := queue_name(),
    driver => {module(), gaffer_driver:driver_state()},
    worker => module(),
    global_max_workers => pos_integer(),
    max_workers => pos_integer(),
    poll_interval => pos_integer() | infinity,
    max_attempts => pos_integer(),
    timeout => pos_integer(),
    shutdown_timeout => pos_integer(),
    backoff => pos_integer(),
    priority => non_neg_integer(),
    on_discard => queue_name()
}.
```

## TODO

### Minimal First Pass

Each item is independently committable and should pass all checks.

- [x] Queue configuration
  - [x] Rename `concurrency` to `global_max_workers` in `queue_conf()` type
  - [x] Add `max_workers`, `poll_interval`, `shutdown_timeout` to `queue_conf()`
- [ ] Worker callback
  - [x] Update `gaffer_worker.erl` callback spec
- [ ] Driver behaviour + ETS implementation
  - [ ] Add `job_fetch/2`, `job_schedule/2` callbacks to `gaffer_driver`
  - [ ] Implement `job_fetch/2` in ETS driver (with `global_max_workers`)
  - [ ] Implement `job_complete/2`, `job_fail/2`, `job_cancel/2`, `job_schedule/2`
  - [ ] Store notify fun, call on `job_insert`
- [ ] Queue runner (`gaffer_queue_runner`)
  - [ ] Implement as `gen_statem` with `handle_event_function` callback mode
  - [ ] Process registration/naming for notify fun targeting
  - [ ] `running` state: poll cycle, spawn workers, DOWN handling
  - [ ] Per-job timeouts + graceful shutdown (exit + kill grace period)
- [ ] Supervision + wiring
  - [ ] Update `gaffer_sup.erl` — dynamic child specs via `supervisor:start_child/2`
  - [ ] Update `gaffer.erl` — start runner on `create_queue/1`, pass notify fun
- [ ] Tests
  - [ ] Job completes, job fails, job crashes, cancel, schedule
  - [ ] Job timeout and kill grace period
  - [ ] Concurrency limits (`max_workers` and `global_max_workers`)
  - [ ] Poll interval configuration (including `infinity`)
  - [ ] Driver wake-up triggers immediate poll

### Later

- [ ] Paused state in queue runner — the gen_statem already defines the
  `paused` state, but the initial implementation only needs `running`.
  Pause/resume can be added separately without changing the core design.
  - [ ] `pause/1` / `resume/1` API
  - [ ] Ignore poll and wake-up events in `paused`
  - [ ] Drain: wait for active workers to finish, then idle
- [ ] Automatic backoff scheduling on failure (failed -> scheduled with delay)
- [ ] Pruning old completed/discarded jobs
- [ ] Crash recovery (reap stale `executing` jobs)
- [ ] Telemetry / instrumentation
