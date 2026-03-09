-module(gaffer_sup).

-behaviour(supervisor).

% API
-export([start_link/0]).

% Callbacks
-export([init/1]).

%--- API -----------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, _InitArgs = {}).

%--- Callbacks -----------------------------------------------------------------

init({} = _InitArgs) -> {ok, {#{}, []}}.
