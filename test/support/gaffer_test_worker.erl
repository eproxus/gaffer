-module(gaffer_test_worker).

-behaviour(gaffer_worker).

-export([perform/1]).
-export([encode_pid/1]).

% API

perform(#{payload := #{~"action" := Action, ~"test_pid" := PidBin}} = Job) ->
    perform(Action, Job, decode_pid(PidBin));
perform(#{payload := #{~"action" := Action}} = Job) ->
    perform(Action, Job, undefined);
perform(#{id := ID}) ->
    error({no_matching_action, ID}).

-spec encode_pid(pid()) -> binary().
encode_pid(Pid) -> base64:encode(term_to_binary(Pid)).

% Internal

perform(~"complete_result", #{payload := #{~"result" := Result}} = Job, Pid) ->
    notify(Pid, job_executed, Job),
    {complete, Result};
perform(~"complete", Job, Pid) ->
    notify(Pid, job_executed, Job),
    complete;
perform(~"crash", Job, _Pid) ->
    error({test_crash, metadata(Job)});
perform(~"fail", _Job, _Pid) ->
    {fail, [#{reason => {badrpc, nodedown}}]};
perform(~"schedule", #{payload := #{~"offset_seconds" := Seconds}} = Job, Pid) ->
    At =
        erlang:system_time() +
            erlang:convert_time_unit(Seconds, second, native),
    notify(Pid, job_executed, Job),
    {schedule, At};
perform(~"block", Job, Pid) ->
    notify(Pid, job_started, Job),
    receive
        continue ->
            notify(Pid, job_executed, Job),
            complete
    after 30000 ->
        error({test_timeout, metadata(Job)})
    end.

decode_pid(Bin) -> binary_to_term(base64:decode(Bin)).

notify(Pid, Event, Job) -> Pid ! {Event, metadata(Job)}.

metadata(#{id := ID}) -> #{id => ID, worker => self(), node => node()}.
