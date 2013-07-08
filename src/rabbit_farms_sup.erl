
-module(rabbit_farms_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).
-define(CHILD_SUP(Name, Mod, Type, Args), {Name, {Mod, start_link, Args}, permanent, 5000, Type, [Mod]}).
%% ===================================================================
%% API functions
%% ===================================================================
-spec start_link()-> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
-spec init(list())-> {ok, term()} | {error, term()}.
init([]) ->
	ConfDir = "./conf",
	FileName = "spark_consumer.config",
	Children = [
%		?CHILD_SUP(spark_app_config_sup, spark_app_config_sup,supervisor,[[ConfDir, FileName]]),
		?CHILD(rabbit_farms,worker)
	],
    {ok, { {one_for_one, 5, 10}, Children} }.

