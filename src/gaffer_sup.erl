-module(gaffer_sup).
-moduledoc false.

-behaviour(supervisor).

% API
-export([start_link/0]).
-export([start_queue/2]).
-export([stop_queue/1]).

% Callbacks
-export([init/1]).

%--- API -----------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, _InitArgs = {}).

-spec start_queue(gaffer:queue(), gaffer_queue:queue_conf()) ->
    {ok, pid()} | {error, {already_started, pid()}}.
start_queue(Name, Conf) ->
    case supervisor:start_child(?MODULE, [Name, Conf]) of
        {ok, Pid} when is_pid(Pid) ->
            {ok, Pid};
        {error, {already_started, Pid}} when is_pid(Pid) ->
            {error, {already_started, Pid}}
    end.

-spec stop_queue(pid()) -> ok.
stop_queue(Pid) ->
    _ = supervisor:terminate_child(?MODULE, Pid),
    ok.

%--- Callbacks -----------------------------------------------------------------

init({} = _InitArgs) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 5,
        period => 10
    },
    ChildSpec = #{
        id => gaffer_queue_runner,
        start => {gaffer_queue_runner, start_link, []},
        restart => transient,
        shutdown => 5000
    },
    {ok, {SupFlags, [ChildSpec]}}.
