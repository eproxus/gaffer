-module(gaffer_job).
-moduledoc false.

% API
-export([execute/2]).
-export([create/3]).
-export([transition/2]).
-export([claim_changes/0]).
-export([handle_crash/3]).
-export([forward_payload/1]).
-export([all_states/0]).

% elp:ignore W0048 - dialyzer over-constrains types from internal call sites
-dialyzer({no_match, [validate/1, create/3]}).

%--- API -----------------------------------------------------------------------

-doc false.
-spec all_states() -> [gaffer:job_state()].
all_states() -> [available, executing, completed, cancelled, discarded].

-doc false.
-spec execute(gaffer_worker:worker(), gaffer:job()) -> no_return().
execute(Worker, Job) ->
    Result =
        try
            gaffer_worker:perform(Worker, Job)
        catch
            Class:Reason:Stack -> {fail, {Class, Reason, Stack}}
        end,
    {Event, Data, Updated} = apply_result(Job, Result, worker),
    exit({gaffer_job, Event, Data, Updated}).

-doc false.
-spec claim_changes() -> #{state := executing, attempted_at := integer()}.
claim_changes() -> #{state => executing, attempted_at => erlang:system_time()}.

-doc false.
-spec forward_payload(gaffer:job()) -> map().
forward_payload(Job) ->
    maps:with([payload, queue, attempt, errors, discarded_at], Job).

-doc false.
-spec handle_crash(gaffer:job(), term(), gaffer_hooks:actor()) ->
    {gaffer_hooks:event(), gaffer_hooks:event_data(), gaffer:job()}.
handle_crash(Job, Reason, Actor) -> apply_result(Job, {fail, Reason}, Actor).

-doc false.
-spec create(gaffer:queue_conf(), term(), gaffer:job_opts()) -> gaffer:job().
create(#{name := Queue} = Conf, Payload, Opts) ->
    Now = erlang:system_time(),
    ScheduledAt = maybe_scheduled_at(Opts),
    JobKeys = [max_attempts, priority, timeout, backoff, shutdown_timeout],
    Defaults = maps:with(JobKeys, Conf),
    JobOpts = maps:merge(Defaults, maps:without([scheduled_at], Opts)),
    Job = maps:merge(JobOpts, #{
        id => keysmith:uuid(7, binary),
        queue => Queue,
        payload => Payload,
        state => available,
        attempt => 0,
        inserted_at => Now,
        errors => []
    }),
    validate(Job),
    maybe_put(scheduled_at, ScheduledAt, Job).

-doc false.
-spec transition(gaffer:job(), gaffer:job_state()) ->
    {ok, gaffer:job()}
    | {error, {invalid_transition, {gaffer:job_state(), gaffer:job_state()}}}.
transition(#{state := From} = Job, To) ->
    case valid_transition(From, To) of
        true ->
            Now = erlang:system_time(),
            {ok, set_timestamp(To, Now, Job#{state := To})};
        false ->
            {error, {invalid_transition, {From, To}}}
    end.

%--- Internal ------------------------------------------------------------------

apply_result(Job, complete, Actor) ->
    apply_result(Job, {complete, undefined}, Actor);
apply_result(#{attempt := A} = Job, {complete, Result}, Actor) ->
    {ok, C0} = transition(Job, completed),
    C = C0#{attempt := A + 1, result => Result},
    {[gaffer, job, complete], #{job => C, actor => Actor}, C};
apply_result(Job, {fail, Reason}, Actor) ->
    Failed = apply_failure(Job, Reason),
    {[gaffer, job, fail], #{job => Failed, actor => Actor}, Failed};
apply_result(Job, {cancel, _}, Actor) ->
    {ok, C} = transition(Job, cancelled),
    {[gaffer, job, cancel], #{job => C, actor => Actor}, C};
apply_result(Job, {schedule, At}, Actor) ->
    {ok, Available0} = transition(Job, available),
    Available = Available0#{scheduled_at => timestamp(At)},
    {[gaffer, job, schedule], #{job => Available, actor => Actor}, Available}.

apply_failure(#{attempt := Attempt} = Job, Reason) ->
    case add_error(Job#{attempt := Attempt + 1}, Reason) of
        #{attempt := A, max_attempts := M} = Job1 when A >= M ->
            {ok, Discarded} = transition(Job1, discarded),
            Discarded;
        Job1 ->
            {ok, Available} = transition(schedule_backoff(Job1), available),
            Available
    end.

schedule_backoff(#{backoff := Backoff, attempt := Attempt} = Job) ->
    Now = erlang:system_time(millisecond),
    Time = timestamp({millisecond, Now + backoff(Attempt, Backoff)}),
    Job#{scheduled_at => Time}.

backoff(_Attempt, Backoff) when is_integer(Backoff) -> Backoff;
backoff(_Attempt, [Backoff]) -> Backoff;
backoff(1, [Backoff | _]) -> Backoff;
backoff(Attempt, [_ | Tail]) -> backoff(Attempt - 1, Tail).

add_error(#{errors := Errors, attempt := Attempt} = Job, Reason) ->
    Error = #{attempt => Attempt, error => Reason, at => erlang:system_time()},
    Job#{errors := [normalize_error(Error) | Errors]}.

-spec valid_transition(gaffer:job_state(), gaffer:job_state()) -> boolean().
valid_transition(available, executing) -> true;
valid_transition(available, cancelled) -> true;
valid_transition(executing, completed) -> true;
valid_transition(executing, cancelled) -> true;
valid_transition(executing, available) -> true;
valid_transition(executing, discarded) -> true;
valid_transition(_, _) -> false.

-spec set_timestamp(gaffer:job_state(), integer(), gaffer:job()) ->
    gaffer:job().
set_timestamp(executing, TS, Job) -> Job#{attempted_at => TS};
set_timestamp(completed, TS, Job) -> Job#{completed_at => TS};
set_timestamp(cancelled, TS, Job) -> Job#{cancelled_at => TS};
set_timestamp(discarded, TS, Job) -> Job#{discarded_at => TS};
set_timestamp(_State, _TS, Job) -> Job.

-spec timestamp(gaffer:timestamp()) -> integer().
timestamp({Unit, V}) -> erlang:convert_time_unit(V, Unit, native);
timestamp(Native) when is_integer(Native) -> Native.

normalize_error(#{error := Err} = Error) ->
    Error#{error := normalize_error_term(Err)}.

normalize_error_term(T) when is_atom(T); is_binary(T); is_number(T) ->
    T;
normalize_error_term(T) when is_list(T) ->
    [normalize_error_term(E) || E <:- T];
normalize_error_term(T) when is_map(T) ->
    maps:fold(
        fun(K, V, Acc) ->
            Key = normalize_error_term(K),
            Acc#{Key => normalize_error_term(V)}
        end,
        #{},
        T
    );
normalize_error_term(T) ->
    iolist_to_binary(io_lib:format(~"~0tp", [T])).

validate(#{queue := Queue} = Job) ->
    Checks = [
        {
            fun() -> is_atom(Queue) end,
            {invalid_queue, Queue}
        },
        {
            fun() -> maps:get(max_attempts, Job, 1) >= 1 end,
            invalid_max_attempts
        },
        {
            fun() -> is_integer(maps:get(priority, Job, 0)) end,
            invalid_priority
        }
    ],
    run_checks(Checks).

maybe_scheduled_at(#{scheduled_at := At}) -> timestamp(At);
maybe_scheduled_at(#{}) -> undefined.

maybe_put(_Key, undefined, Map) -> Map;
maybe_put(Key, Value, Map) -> Map#{Key => Value}.

run_checks([]) ->
    ok;
run_checks([{Check, Reason} | Rest]) ->
    case Check() of
        true -> run_checks(Rest);
        false -> error({invalid_job, Reason})
    end.
