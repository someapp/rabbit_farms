-module(rabbit_consumer_sup).
-behaviour(supervisor).

-include_lib("rabbit_farms_internal.hrl").

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(Name, Mod, Type, Args), {Name, {Mod, start_link, Args}, permanent, 5000, Type, [Mod]}).

%% ===================================================================
%% API functions
%% ===================================================================
-spec start_link(RabbitFarmModel::#consumer_state{})-> {ok, pid()} | {error, term()}.
start_link(RabbitFarmModel) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [RabbitFarmModel]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
-spec init(list())-> {ok, term()} | {error, term()}.
init([RabbitFarmModel]) ->
	#consumer_state{farm_name = FarmName} = RabbitFarmModel,
	ConfDir = "./conf",
	FileName = "spark_consumer.config",
    Children = [
    		?CHILD(spark_app_config_sup, spark_app_config_sup,supervisor,[ConfDir, FileName]),
    		?CHILD(FarmName, rabbit_consumer, worker, 
         	[RabbitFarmModel])],
    {ok, { {one_for_one, 5, 10}, Children }}.

