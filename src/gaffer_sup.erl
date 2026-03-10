-module(gaffer_sup).

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

-spec start_queue(gaffer:queue_name(), gaffer_queue:driver()) ->
    {ok, pid()}.
start_queue(Name, Driver) ->
    ChildSpec = #{
        id => Name,
        start =>
            {gaffer_queue_runner, start_link, [Name, Driver]},
        restart => transient,
        shutdown => 5000
    },
    {ok, Pid} = supervisor:start_child(?MODULE, ChildSpec),
    true = is_pid(Pid),
    {ok, Pid}.

-spec stop_queue(gaffer:queue_name()) -> ok.
stop_queue(Name) ->
    _ = supervisor:terminate_child(?MODULE, Name),
    _ = supervisor:delete_child(?MODULE, Name),
    ok.

%--- Callbacks -----------------------------------------------------------------

init({} = _InitArgs) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },
    {ok, {SupFlags, []}}.
