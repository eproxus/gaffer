-module(gaffer_hooks).
-moduledoc """
Hook behaviour for observing queue and job events.

Every hook callback receives an event path and a payload map. The payload
always carries an `actor` field identifying which Gaffer process (or the user)
caused the event. See each event's type below for its path, payload shape,
and actors.

* `[gaffer, queue, create]`: `t:queue_create_event/0`
* `[gaffer, queue, update]`: `t:queue_update_event/0`
* `[gaffer, queue, pause]`: `t:queue_pause_event/0`
* `[gaffer, queue, resume]`: `t:queue_resume_event/0`
* `[gaffer, queue, delete]`: `t:queue_delete_event/0`
* `[gaffer, job, insert]`: `t:job_insert_event/0`
* `[gaffer, job, claim]`: `t:job_claim_event/0`
* `[gaffer, job, complete]`: `t:job_complete_event/0`
* `[gaffer, job, fail]`: `t:job_fail_event/0`
* `[gaffer, job, cancel]`: `t:job_cancel_event/0`
* `[gaffer, job, schedule]`: `t:job_schedule_event/0`
* `[gaffer, job, delete]`: `t:job_delete_event/0`
""".

-doc "Hook event path.".
-type event() :: [atom()].
-doc "Identifies which Gaffer process or the public API caused an event.".
-type actor() :: user | worker | runner | pruner.

-doc """
Payload for `[gaffer, queue, create]`.

Triggered by actors:

* `user` via `gaffer:create_queue/1`, or via `gaffer:ensure_queue/1` when the
  queue does not yet exist.
""".
-type queue_create_event() :: #{
    actor := user,
    queue := gaffer:queue(),
    conf := gaffer:queue_conf()
}.

-doc """
Payload for `[gaffer, queue, update]`.

Triggered by actors:

* `user` via `gaffer:update_queue/2` (`source` is `update`), or via
  `gaffer:ensure_queue/1` when the queue already exists (`source` is `ensure`
  and `updates` is `#{}`).
""".
-type queue_update_event() :: #{
    actor := user,
    queue := gaffer:queue(),
    conf := gaffer:queue_conf(),
    updates := map(),
    source := ensure | update
}.

-doc """
Payload for `[gaffer, queue, pause]`.

Triggered by actors:

* `user` via `gaffer:pause/1`.
""".
-type queue_pause_event() :: #{
    actor := user,
    queue := gaffer:queue()
}.

-doc """
Payload for `[gaffer, queue, resume]`.

Triggered by actors:

* `user` via `gaffer:resume/1`.
""".
-type queue_resume_event() :: #{
    actor := user,
    queue := gaffer:queue()
}.

-doc """
Payload for `[gaffer, queue, delete]`.

Triggered by actors:

* `user` via `gaffer:delete_queue/1` or `gaffer:delete_queue/2`.
""".
-type queue_delete_event() :: #{
    actor := user,
    queue := gaffer:queue()
}.

-doc """
Payload for `[gaffer, job, insert]`.

Triggered by actors:

* `user` via `gaffer:insert/2` or `gaffer:insert/3`.
* `worker` when forwarding a failed job to its `on_discard` queue.
""".
-type job_insert_event() :: #{
    actor := user | worker,
    job := gaffer:job()
}.

-doc """
Payload for `[gaffer, job, claim]`.

Triggered by actors:

* `runner` when claiming jobs from the storage driver.
""".
-type job_claim_event() :: #{
    actor := runner,
    queue := gaffer:queue(),
    jobs := [gaffer:job()]
}.

-doc """
Payload for `[gaffer, job, complete]`.

Triggered by actors:

* `worker` when `c:gaffer_worker:perform/1` returns `complete` or
  `{complete, _}`.
""".
-type job_complete_event() :: #{
    actor := worker,
    job := gaffer:job()
}.

-doc """
Payload for `[gaffer, job, fail]`.

Triggered by actors:

* `worker` when `c:gaffer_worker:perform/1` returns `{fail, _}` or the worker
  process crashes.
* `runner` when killing the worker on timeout.
""".
-type job_fail_event() :: #{
    actor := worker | runner,
    job := gaffer:job()
}.

-doc """
Payload for `[gaffer, job, cancel]`.

Triggered by actors:

* `user` via `gaffer:cancel/2`.
* `worker` when `c:gaffer_worker:perform/1` returns `{cancel, _}`.
""".
-type job_cancel_event() :: #{
    actor := user | worker,
    job := gaffer:job()
}.

-doc """
Payload for `[gaffer, job, schedule]`.

Triggered by actors:

* `worker` when `c:gaffer_worker:perform/1` returns `{schedule, _}`.
""".
-type job_schedule_event() :: #{
    actor := worker,
    job := gaffer:job()
}.

-doc """
Payload for `[gaffer, job, delete]`.

Triggered by actors:

* `user` via `gaffer:delete/2` or `gaffer:prune/1`.
* `pruner` on its periodic prune timer.
""".
-type job_delete_event() :: #{
    actor := user | pruner,
    queue := gaffer:queue(),
    job_id := gaffer:job_id()
}.

-doc "Union of all hook event payloads.".
-type event_data() ::
    queue_create_event()
    | queue_update_event()
    | queue_pause_event()
    | queue_resume_event()
    | queue_delete_event()
    | job_insert_event()
    | job_claim_event()
    | job_complete_event()
    | job_fail_event()
    | job_cancel_event()
    | job_schedule_event()
    | job_delete_event().

-doc "A hook callback function.".
-type hook_fun() :: fun((event(), event_data()) -> term()).
-doc "A hook module or function.".
-type hook() :: module() | hook_fun().

-export_type([event/0]).
-export_type([actor/0]).
-export_type([queue_create_event/0]).
-export_type([queue_update_event/0]).
-export_type([queue_pause_event/0]).
-export_type([queue_resume_event/0]).
-export_type([queue_delete_event/0]).
-export_type([job_insert_event/0]).
-export_type([job_claim_event/0]).
-export_type([job_complete_event/0]).
-export_type([job_fail_event/0]).
-export_type([job_cancel_event/0]).
-export_type([job_schedule_event/0]).
-export_type([job_delete_event/0]).
-export_type([event_data/0]).
-export_type([hook_fun/0]).
-export_type([hook/0]).

-doc "Called after queue and job events with event-specific payload.".
-callback gaffer_hook(event(), event_data()) -> term().

% API
-export([notify/3]).

%--- API -----------------------------------------------------------------------

-doc false.
-spec notify([hook()], event(), event_data()) -> ok.
notify(QueueHooks, Event, Data) ->
    Hooks = application:get_env(gaffer, hooks, []) ++ QueueHooks,
    lists:foreach(fun(H) -> call(H, Event, Data) end, Hooks).

%--- Internal ------------------------------------------------------------------

call(Hook, Event, Data) ->
    try
        call_hook(Hook, Event, Data)
    catch
        Class:Reason:Stack ->
            logger:warning(
                ~"Hook ~p crashed: ~p:~p~n~p",
                [Hook, Class, Reason, Stack]
            )
    end.

call_hook(Fun, Event, Data) when is_function(Fun, 2) -> Fun(Event, Data);
call_hook(Mod, Event, Data) when is_atom(Mod) -> Mod:gaffer_hook(Event, Data).
