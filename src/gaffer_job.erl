-module(gaffer_job).

-export([new/3]).
-export([validate/1]).
-export([transition/2]).
-export([add_error/2]).

%--- API ----------------------------------------------------------------------

-spec new(gaffer:queue_name(), map(), gaffer:job_opts()) ->
    gaffer:job().
new(Queue, Args, Opts) ->
    Now = calendar:universal_time(),
    ScheduledAt = maps:get(scheduled_at, Opts, undefined),
    State =
        case ScheduledAt of
            undefined -> available;
            _ -> scheduled
        end,
    Job = #{
        id => keysmith:uuid(7),
        queue => Queue,
        args => Args,
        state => State,
        attempt => 0,
        max_attempts =>
            maps:get(max_attempts, Opts, 3),
        priority =>
            maps:get(priority, Opts, 0),
        inserted_at => Now,
        errors => [],
        tags => maps:get(tags, Opts, []),
        meta => maps:get(meta, Opts, #{})
    },
    maybe_put(scheduled_at, ScheduledAt, Job).

-spec validate(map()) -> ok | {error, term()}.
validate(#{id := Id, queue := Queue, args := Args} = Job) ->
    Checks = [
        {
            fun() -> is_binary(Id) andalso byte_size(Id) > 0 end,
            {invalid_id, Id}
        },
        {fun() -> is_atom(Queue) end, {invalid_queue, Queue}},
        {fun() -> is_map(Args) end, {invalid_args, Args}},
        {
            fun() ->
                maps:get(max_attempts, Job, 1) >= 1
            end,
            invalid_max_attempts
        },
        {
            fun() ->
                maps:get(priority, Job, 0) >= 0
            end,
            invalid_priority
        }
    ],
    run_checks(Checks);
validate(_) ->
    {error, missing_required_fields}.

-spec transition(gaffer:job(), gaffer:job_state()) ->
    {ok, gaffer:job()} | {error, {invalid_transition, term()}}.
transition(#{state := From} = Job, To) ->
    case valid_transition(From, To) of
        true ->
            Now = calendar:universal_time(),
            {ok, set_timestamp(To, Now, Job#{state := To})};
        false ->
            {error, {invalid_transition, {From, To}}}
    end.

-spec add_error(gaffer:job(), gaffer:job_error()) ->
    gaffer:job().
add_error(#{errors := Errors} = Job, Error) ->
    Job#{errors := [Error | Errors]}.

%--- Internal -----------------------------------------------------------------

-spec valid_transition(gaffer:job_state(), gaffer:job_state()) ->
    boolean().
valid_transition(available, executing) -> true;
valid_transition(available, cancelled) -> true;
valid_transition(scheduled, available) -> true;
valid_transition(scheduled, cancelled) -> true;
valid_transition(executing, completed) -> true;
valid_transition(executing, failed) -> true;
valid_transition(executing, cancelled) -> true;
valid_transition(executing, scheduled) -> true;
valid_transition(failed, discarded) -> true;
valid_transition(failed, scheduled) -> true;
valid_transition(discarded, scheduled) -> true;
valid_transition(cancelled, scheduled) -> true;
valid_transition(_, _) -> false.

set_timestamp(executing, Now, Job) ->
    Job#{attempted_at => Now};
set_timestamp(completed, Now, Job) ->
    Job#{completed_at => Now};
set_timestamp(cancelled, Now, Job) ->
    Job#{cancelled_at => Now};
set_timestamp(discarded, Now, Job) ->
    Job#{discarded_at => Now};
set_timestamp(_, _Now, Job) ->
    Job.

maybe_put(_Key, undefined, Map) -> Map;
maybe_put(Key, Value, Map) -> Map#{Key => Value}.

run_checks([]) ->
    ok;
run_checks([{Check, Error} | Rest]) ->
    case Check() of
        true -> run_checks(Rest);
        false -> {error, Error}
    end.
