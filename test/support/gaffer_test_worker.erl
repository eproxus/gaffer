-module(gaffer_test_worker).

-behaviour(gaffer_worker).

-export([perform/1]).
-export([encode_pid/1]).

perform(#{
    id := Id, payload := #{~"action" := ~"complete", ~"test_pid" := PidBin}
}) ->
    Pid = binary_to_term(base64:decode(PidBin)),
    Pid ! {job_executed, Id},
    complete;
perform(#{payload := #{~"action" := ~"crash"}}) ->
    error(test_crash);
perform(#{id := Id, payload := #{~"action" := ~"block", ~"test_pid" := PidBin}}) ->
    Pid = binary_to_term(base64:decode(PidBin)),
    Pid ! {job_started, Id},
    receive
        continue -> complete
    after 30000 -> {fail, ~"timeout"}
    end.

-spec encode_pid(pid()) -> binary().
encode_pid(Pid) ->
    base64:encode(term_to_binary(Pid)).
