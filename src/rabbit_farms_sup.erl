-module(rabbit_farms_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_link/1, start_link/2]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).
-define(CHILD_SUP(Name, Mod, Type, Args), {Name, {Mod, start_link, Args}, permanent, 5000, Type, [Mod]}).

-define(SERVER, ?MODULE).


%% ===================================================================
%% API functions
%% ===================================================================
-spec start_link()-> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).
start_link(_Args)->
	start_link().

start_link(Type, Args)-> 
    supervisor:start_link({local, ?SERVER}, ?MODULE, Args).
%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
-spec init(list())-> {ok, term()} | {error, term()}.
init([]) ->
	Children = [
		?CHILD(rabbit_farms,worker)
	],
    {ok, { {one_for_one, 5, 10}, Children} }.

