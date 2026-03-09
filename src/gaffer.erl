-module(gaffer).

-behaviour(application).

% Callbacks
-export([start/2]).
-export([stop/1]).

%--- Callbacks -----------------------------------------------------------------

start(_StartType, _StartArgs) -> gaffer_sup:start_link().

stop(_State) -> ok.
