-module(rabbit_consumer_sup).
-behaviour(supervisor).

-include_lib("rabbit_farms_internal.hrl").

%% API
-export([start_link/0, start_link/1]).

%% Supervisor callbacks
-export([init/0, init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(Name, Mod, Type, Args), {Name, {Mod, start_link, Args}, permanent, 5000, Type, [Mod]}).

%% ===================================================================
%% API functions
%% ===================================================================
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_link(RabbitFarmModel::#consumer_state{})-> {ok, pid()} | {error, term()}.
start_link(RabbitFarmModel) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [RabbitFarmModel]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
init()-> init([]).
-spec init(list())-> {ok, term()} | {error, term()}.
init(_Args) ->
	ConfDir = "./conf",
	FileName = "spark_consumer.config",
    Children = [
 %   		?CHILD(spark_app_config_sup, spark_app_config_sup,supervisor,[ConfDir, FileName]),
    		?CHILD([], rabbit_consumer, worker, 
         	[])],
    {ok, { {one_for_one, 5, 10}, Children }}.

