-module(gaffer_prometheus).
-moduledoc """
Optional Prometheus metrics exporter for gaffer.

If you intend to use this hook then you need to include
[Prometheus](https://github.com/prometheus-erl/prometheus.erl) as a dependecy
in your application.

## Setup

The global `application:set_env(gaffer, hooks, ...)` registration is the
recommended configuration for full coverage:

```erlang
ok = application:ensure_all_started(prometheus),
ok = gaffer_prometheus:start(),
application:set_env(gaffer, hooks, [gaffer_prometheus]).
```

> #### Note {: .info}
> If hooks are enabled only on some queues, forwarded jobs will only fire events
> in the respective queues that have hooks configured.

## Metrics

Every metric carries a `queue` label and an `actor` label. `actor` identifies
which Gaffer process or the public API caused the event and is one of `user`,
`worker`, `runner`, or `pruner`. See `m:gaffer_hooks` for the actors emitted by
each event.

Counters:

* `gaffer_queues_created_total{queue, actor}`

  Queues created via `gaffer:create_queue/1`.

* `gaffer_queues_updated_total{queue, actor, source}`

  Queues updated. `source` is `ensure` for `gaffer:ensure_queue/1` (fires on
  every call, even when no fields change) or `update` for
  `gaffer:update_queue/2`.

* `gaffer_queues_paused_total{queue, actor}`

  Queues paused via `gaffer:pause/1`.

* `gaffer_queues_resumed_total{queue, actor}`

  Queues resumed via `gaffer:resume/1`.

* `gaffer_queues_deleted_total{queue, actor}`

  Queues deleted via `gaffer:delete_queue/1`.

* `gaffer_jobs_inserted_total{queue, actor}`

  Jobs inserted into a queue (direct user insert or a `worker` forwarding a
  terminal-state job to its `forward` target).

* `gaffer_jobs_claimed_total{queue, actor}`

  Jobs picked up by a runner for execution. Incremented by the number of
  jobs in the claim batch.

* `gaffer_jobs_completed_total{queue, actor}`

  Jobs that finished successfully.

* `gaffer_jobs_failed_total{queue, actor}`

  Terminal failures only (job exhausted retries).

* `gaffer_jobs_retries_total{queue, actor}`

  Retryable failures (job will be retried after backoff).

* `gaffer_jobs_cancelled_total{queue, actor}`

  Jobs cancelled before completion, either by user request or by the worker
  itself.

* `gaffer_jobs_scheduled_total{queue, actor}`

  Jobs rescheduled by their worker for a later run.

* `gaffer_jobs_deleted_total{queue, actor}`

  Jobs removed from a queue (e.g. via prune or explicit delete).

Histograms:

* `gaffer_job_claim_delay_seconds{queue, actor}`

  Wall clock between a job's `scheduled_at` and its `attempted_at`, in
  seconds. Captures how long a job waited before a runner picked it up.

* `gaffer_job_execution_duration_seconds{queue, actor, state}`

  Duration of each executed attempt, in seconds, observed with the
  post-event `state`. `state` is one of `completed`, `failed`, `cancelled`,
  or `available` (a retryable failure).

* `gaffer_job_attempts{queue, actor, state}`

  Final value of the job's `attempt` field at terminal events. `state` is
  `completed`, `failed`, or `cancelled`. User-cancels of jobs that never ran
  are excluded.

* `gaffer_job_claim_batch_size{queue, actor}`

  Number of jobs claimed per claim event. Useful for tuning batch size and
  spotting starved or overloaded queues.

## Cardinality

The `queue` label is bounded by deployment topology (atoms, small set).
`actor`, `state`, and `source` labels are bounded by definition.
Programmatically generated queue atoms would explode the series store.
""".

-behaviour(gaffer_hooks).

% API
-ignore_xref(start/0).
-export([start/0]).
-export([gaffer_hook/2]).

%--- API -----------------------------------------------------------------------

-doc """
Declare every gaffer metric.

Idempotent — safe to call multiple times. Call once at application
startup, after `application:ensure_all_started(prometheus)`.
""".
-spec start() -> ok.
start() ->
    lists:foreach(fun declare_counter/1, counters()),
    lists:foreach(fun declare_histogram/1, histograms()),
    ok.

-doc false.
-spec gaffer_hook(gaffer_hooks:event(), gaffer_hooks:event_data()) -> ok.
% Queue lifecycle
gaffer_hook([gaffer, queue, create], #{queue := Q, actor := A}) ->
    inc(gaffer_queues_created_total, [Q, A]);
gaffer_hook([gaffer, queue, update], #{queue := Q, actor := A, source := S}) ->
    inc(gaffer_queues_updated_total, [Q, A, S]);
gaffer_hook([gaffer, queue, pause], #{queue := Q, actor := A}) ->
    inc(gaffer_queues_paused_total, [Q, A]);
gaffer_hook([gaffer, queue, resume], #{queue := Q, actor := A}) ->
    inc(gaffer_queues_resumed_total, [Q, A]);
gaffer_hook([gaffer, queue, delete], #{queue := Q, actor := A}) ->
    inc(gaffer_queues_deleted_total, [Q, A]);
% Job lifecycle
gaffer_hook([gaffer, job, insert], #{job := #{queue := Q}, actor := A}) ->
    inc(gaffer_jobs_inserted_total, [Q, A]);
gaffer_hook([gaffer, job, claim], #{queue := Q, jobs := Jobs, actor := A}) ->
    observe_claim(Q, A, Jobs);
gaffer_hook([gaffer, job, complete], #{job := #{queue := Q} = J, actor := A}) ->
    inc(gaffer_jobs_completed_total, [Q, A]),
    observe_terminal(completed, A, J);
gaffer_hook([gaffer, job, fail], #{job := #{state := failed} = J, actor := A}) ->
    inc(gaffer_jobs_failed_total, [maps:get(queue, J), A]),
    observe_terminal(failed, A, J);
gaffer_hook([gaffer, job, fail], #{job := #{queue := Q} = J, actor := A}) ->
    inc(gaffer_jobs_retries_total, [Q, A]),
    observe_retry_duration(A, J);
gaffer_hook([gaffer, job, cancel], #{job := #{queue := Q} = J, actor := A}) ->
    inc(gaffer_jobs_cancelled_total, [Q, A]),
    observe_terminal(cancelled, A, J);
gaffer_hook([gaffer, job, schedule], #{job := #{queue := Q}, actor := A}) ->
    inc(gaffer_jobs_scheduled_total, [Q, A]);
gaffer_hook([gaffer, job, delete], #{queue := Q, actor := A}) ->
    inc(gaffer_jobs_deleted_total, [Q, A]);
gaffer_hook(_Event, _Data) ->
    ok.

%--- Internal ------------------------------------------------------------------

% erlfmt-ignore
counters() ->
    [
        {gaffer_queues_created_total, [queue, actor],         ~"Queues created."},
        {gaffer_queues_updated_total, [queue, actor, source], ~"Queues updated. `source` is `ensure` (every ensure_queue/1 call) or `update`."},
        {gaffer_queues_paused_total,  [queue, actor],         ~"Queues paused."},
        {gaffer_queues_resumed_total, [queue, actor],         ~"Queues resumed."},
        {gaffer_queues_deleted_total, [queue, actor],         ~"Queues deleted."},
        {gaffer_jobs_inserted_total,  [queue, actor],         ~"Jobs inserted."},
        {gaffer_jobs_claimed_total,   [queue, actor],         ~"Jobs claimed by runners."},
        {gaffer_jobs_completed_total, [queue, actor],         ~"Jobs completed."},
        {gaffer_jobs_failed_total,    [queue, actor],         ~"Jobs that have exhausted retries."},
        {gaffer_jobs_retries_total,   [queue, actor],         ~"Retryable failures (will be retried after backoff)."},
        {gaffer_jobs_cancelled_total, [queue, actor],         ~"Jobs cancelled."},
        {gaffer_jobs_scheduled_total, [queue, actor],         ~"Jobs rescheduled by their worker."},
        {gaffer_jobs_deleted_total,   [queue, actor],         ~"Jobs deleted."}
    ].

histograms() ->
    Duration = [
        0.005,
        0.01,
        0.05,
        0.1,
        0.25,
        0.5,
        1,
        2.5,
        5,
        10,
        30,
        60,
        300,
        1800,
        7200
    ],
    Delay = [
        0.001,
        0.0025,
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1,
        2,
        5,
        10,
        30,
        60
    ],
    Attempts = [1, 2, 3, 4, 5, 7, 10, 25, 100],
    Batch = [1, 2, 5, 10, 25, 50, 100, 250, 1000],
    [
        {
            gaffer_job_claim_delay_seconds,
            [queue, actor],
            ~"Wall clock between scheduled_at and attempted_at, in seconds.",
            Delay
        },
        {
            gaffer_job_execution_duration_seconds,
            [queue, actor, state],
            ~"Duration of an executed attempt, in seconds.",
            Duration
        },
        {
            gaffer_job_attempts,
            [queue, actor, state],
            ~"Attempt count at terminal events.",
            Attempts
        },
        {
            gaffer_job_claim_batch_size,
            [queue, actor],
            ~"Number of jobs claimed per claim event.",
            Batch
        }
    ].

declare_counter({Name, Labels, Help}) ->
    _ = prometheus_counter:declare(
        [{name, Name}, {labels, Labels}, {help, Help}]
    ),
    ok.

declare_histogram({Name, Labels, Help, Buckets}) ->
    _ = prometheus_histogram:declare([
        {name, Name}, {labels, Labels}, {help, Help}, {buckets, Buckets}
    ]),
    ok.

inc(Name, Labels) -> prometheus_counter:inc(Name, Labels).

observe(Name, Labels, Value) ->
    prometheus_histogram:observe(Name, Labels, Value).

% Skip empty batches: claim events fire on every poll, including idle
% polls that returned no jobs. Recording those would dilute the batch-size
% histogram and create no-op counter writes.
observe_claim(_Queue, _Actor, []) ->
    ok;
observe_claim(Queue, Actor, Jobs) ->
    Count = length(Jobs),
    _ = prometheus_counter:inc(
        gaffer_jobs_claimed_total, [Queue, Actor], Count
    ),
    observe(gaffer_job_claim_batch_size, [Queue, Actor], Count),
    lists:foreach(fun(J) -> observe_claim_delay(Queue, Actor, J) end, Jobs).

observe_claim_delay(Q, A, #{scheduled_at := SAt, attempted_at := AAt}) ->
    observe(gaffer_job_claim_delay_seconds, [Q, A], max(0, AAt - SAt));
observe_claim_delay(_Q, _A, _Job) ->
    ok.

% Terminal events: complete (completed_at), fail (failed_at), cancel
% (cancelled_at). Gating on `attempted_at` keeps user-cancels of jobs that
% never ran (no `attempted_at`) out of both histograms.
observe_terminal(
    State, Actor, #{queue := Q, attempted_at := AAt, attempt := N} = Job
) ->
    observe(
        gaffer_job_execution_duration_seconds,
        [Q, Actor, State],
        terminal_end(State, Job) - AAt
    ),
    observe(gaffer_job_attempts, [Q, Actor, State], N);
observe_terminal(_State, _Actor, _Job) ->
    ok.

terminal_end(completed, #{completed_at := T}) -> T;
terminal_end(failed, #{failed_at := T}) -> T;
terminal_end(cancelled, #{cancelled_at := T}) -> T.

% Retryable failure: end timestamp is the just-recorded error's `at` field
% (apply_failure/2 prepends to the errors list). The job was executing, so
% attempted_at is always set, and an error has just been recorded.
observe_retry_duration(
    Actor,
    #{queue := Q, attempted_at := AAt, errors := [#{at := EAt} | _]}
) ->
    observe(
        gaffer_job_execution_duration_seconds,
        [Q, Actor, available],
        EAt - AAt
    ).
