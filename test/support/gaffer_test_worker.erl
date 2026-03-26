-module(gaffer_test_worker).

-behaviour(gaffer_worker).

-export([perform/1]).
-export([encode_pid/1]).

perform(
    #{
        payload := #{
            ~"action" := ~"complete_with_result",
            ~"test_pid" := PidBin,
            ~"result" := Result
        }
    } = Job
) ->
    Pid = binary_to_term(base64:decode(PidBin)),
    Pid ! {job_executed, metadata(Job)},
    {complete, Result};
perform(#{payload := #{~"action" := ~"complete", ~"test_pid" := PidBin}} = Job) ->
    Pid = binary_to_term(base64:decode(PidBin)),
    Pid ! {job_executed, metadata(Job)},
    complete;
perform(#{payload := #{~"action" := ~"crash"}} = Job) ->
    error({test_crash, metadata(Job)});
perform(#{payload := #{~"action" := ~"block", ~"test_pid" := PidBin}} = Job) ->
    Pid = binary_to_term(base64:decode(PidBin)),
    Pid ! {job_started, metadata(Job)},
    receive
        continue ->
            Pid ! {job_executed, metadata(Job)},
            complete
    after 30000 ->
        error({test_timeout, metadata(Job)})
    end.

-spec encode_pid(pid()) -> binary().
encode_pid(Pid) -> base64:encode(term_to_binary(Pid)).

metadata(#{id := Id}) -> #{id => Id, worker => self(), node => node()}.
