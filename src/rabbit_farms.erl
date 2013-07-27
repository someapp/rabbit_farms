%% -------------------------------------------------------------------
%% Copyright (c) 2013 Xujin Zheng (zhengxujin@adsage.com)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%routing_key = <<"">>,
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%% -------------------------------------------------------------------

-module(rabbit_farms).
-behaviour(gen_server2).

-include("rabbit_farms.hrl").
-include("rabbit_farms_internal.hrl").
-include_lib("lager/include/lager.hrl").

-define(SERVER,?MODULE).
-define(APP,rabbit_farms).
-define(ETS_FARMS,ets_rabbit_farms).

-record(state,{status = uninitialized}).

%% API
-export([start_/0, start/0, stop/0, start_link/0]).

-export([publish/2]).
-export([native_cast/2, native_cast/3]).
-export([native_call/2, native_call/3]).
-export([get_status/0, get_farm_pid/0, ping/0]).

%% gen_server2 callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, handle_info/3,
        terminate/2, code_change/3]).

start_()->
    ok = app_util:start_app(syntax_tools),
    ok = app_util:start_app(compiler),
    ok = app_util:start_app(goldrush),
    ok = lager:start(),
    ok = app_util:start_app(gen_server2),
    ok = app_util:start_app(rabbit_common),
    ok = app_util:start_app(amqp_client),
    ok = app_util:start_app(crypto),
    ok = app_util:start_app(uuid),
    ok = app_util:start_app(?APP).

start()->
	ok = lager:start(),
    ok = app_util:start_app(?APP).

stop()->
 	gen_server2:call(?SERVER,{stop, normal}).

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

get_status()->
    gen_server2:call(?SERVER, {get_status}).

get_farm_pid()->
	gen_server2:call(?SERVER, {get_farm_pid}).

ping()->
	gen_server2:call(?SERVER,{ping}).

publish(cast, Message)
				when is_record(Message,rabbit_message)->
	gen_server2:cast(?SERVER, {publish, Message});
publish(cast, Messages)
				when is_record(Messages,rabbit_messages)->
	gen_server2:cast(?SERVER, {publish, Messages});
publish(call, Message)
				when is_record(Message,rabbit_message)->
	gen_server2:call(?SERVER, {publish, Message});
publish(call, Messages)
				when is_record(Messages,rabbit_messages)->
	gen_server2:call(?SERVER, {publish, Messages}).

native_cast(FarmName, Method)->
	gen_server2:cast(?SERVER, {native, {FarmName, Method, none}}).

native_cast(FarmName, Method, Content)->
	gen_server2:cast(?SERVER, {native, {FarmName, Method, Content}}).

native_call(FarmName, Method)->
	gen_server2:cast(?SERVER, {native, {FarmName, Method, none}}).

native_call(FarmName, Method, Content)->
	gen_server2:cast(?SERVER, {native, {FarmName, Method, Content}}).

%%%===================================================================
%%% gen_server2 callbacks
%%%===================================================================

init([]) ->
    erlang:send_after(0, self(), {init}),
    {ok, #state{}}.

handle_call({stop, Reason}, From, State)->
 	error_logger:info_msg("Rabbit Farm Handle stop Reason ~p ",[Reason]),
	Reply = terminate(Reason, State),
	{reply, Reply, State};
handle_call({publish, Message}, From, State) ->
	spawn(fun()-> 
				Reply = publish_rabbit_message(call, Message),
				gen_server2:reply(From, Reply)
		  end),
	{noreply, State};
handle_call({publish, Messages}, From, State) 
					when is_record(Messages,rabbit_messages) ->
    spawn(fun()-> 
    		 Reply = publish_rabbit_messages(cast,Messages),
    		 gen_server2:reply(From, Reply)
    	 end),
    {noreply, State};

handle_call({native, {FarmName, Method, Content}}, From, State) ->
	spawn(fun()-> 
				Reply = native_rabbit_call(call, FarmName, Method, Content),
				gen_server2:reply(From, Reply)
		  end),
	{noreply, State};
handle_call({get_status}, _From, State)->
	{reply, {ok, State}, State};
handle_call({get_farm_pid}, _From, State)->
    Farms = ets:tab2list(?ETS_FARMS),
    {reply, {ok, Farms}, State};
handle_call(_Request, _From, State) ->
    Reply = {error, function_clause},
    {reply, Reply, State}.

handle_cast({publish, Message}, State) ->
    spawn(fun()-> publish_rabbit_message(cast, Message) end),
    {noreply, State};
handle_cast({publish, Messages}, State) ->
    spawn(fun()-> 
    		 publish_rabbit_messages(cast, Messages)
    	 end),
    {noreply, State};
handle_cast({native, {FarmName, Method, Content}}, State) ->
	spawn(fun()-> native_rabbit_call(cast, FarmName, Method, Content) end),
	{noreply, State};
handle_cast({on_rabbit_farm_created, RabbitFarmInstance}, State) ->
	true = ets:insert(?ETS_FARMS, RabbitFarmInstance),
	lager:log(info,"rabbit farm have been working:~n~p~n", [RabbitFarmInstance]),
	{noreply, State};
handle_cast({on_rabbit_farm_die, _Reason, RabbitFarm}, State) 
					when is_record(RabbitFarm, rabbit_farm) ->
    InitRabbitFarm = RabbitFarm#rabbit_farm{status = inactive, connection = undefined, channels = orddict:new()},
    true           = ets:insert(?ETS_FARMS, InitRabbitFarm),
    {noreply, State};
handle_cast(Info, State) ->
	erlang:display(Info),
    {noreply, State}.

handle_info({init}, State) ->
	ets:new(?ETS_FARMS,[protected, named_table, {keypos, #rabbit_farm.farm_name}, {read_concurrency, true}]),
	{ok, NewState} = init_rabbit_farm(State),
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State, _Extra) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(Reason, State) ->
	{ok, Farms} = application:get_env(?APP, rabbit_farms),
	error_logger:info_msg("Terminate farms ~p ",[Farms]),
	[ begin
		FarmNodeName      = ?TO_FARM_NODE_NAME(FarmName),
		{ok, FarmOptions} = application:get_env(?APP, FarmNodeName),
		error_logger:info_msg("Terminate Options FarmName ~p Opts ~p~n",[FarmNodeName, FarmOptions]),
		delete_rabbit_farm_instance(FarmName, FarmNodeName)
	  end
	  || FarmName <-Farms],
	error_logger:info_msg("Terminated farms ~p ",[Farms]),
    ok.


init_rabbit_farm(State)->
	{ok, Farms} = application:get_env(?APP, rabbit_farms),
	[ begin
		FarmNodeName      = ?TO_FARM_NODE_NAME(FarmName),
		{ok, FarmOptions} = application:get_env(?APP, FarmNodeName),
		RabbitFarmModel   = create_rabbit_farm_model(FarmName, FarmOptions),
		create_rabbit_farm_instance(RabbitFarmModel)
	  end
	  || FarmName <-Farms],
	{ok, State#state{status = initialized}}.


create_rabbit_farm_model(FarmName, FarmOptions) when is_list(FarmOptions)->
	FeedsOpt	= proplists:get_value(feeders,FarmOptions,[]),
	AmqpParam	= rabbit_farms_config:get_connection_setting(FarmOptions),
	Callbacks 	= proplists:get_value(callbacks, FarmOptions,[]),
	Feeders =
	[begin 

		ChannelCount = proplists:get_value(channel_count,FeedOpt, 1),
	    QueueCount	= proplists:get_value(queue_count, FeedOpt, 1),
		#rabbit_feeder{ count   = ChannelCount,
					    queue_count = QueueCount,
						declare = rabbit_farms_config:get_exchange_setting(FeedOpt),
						queue_declare = rabbit_farms_config:get_queue_setting(FeedOpt),
						queue_bind = rabbit_farms_config:get_queue_bind(FeedOpt),
						callbacks = Callbacks
				 	  }
	end
	||FeedOpt <- FeedsOpt],
	#rabbit_farm{farm_name = FarmName, 
				 amqp_params = AmqpParam, 
				 feeders = Feeders}.

create_rabbit_farm_instance(RabbitFarmModel)->
	#rabbit_farm{farm_name = FarmName} = RabbitFarmModel,
	FarmSups   = supervisor:which_children(rabbit_farms_sup),
	MatchedSup =
	[{Id, Child, Type, Modules} 
	  ||{Id, Child, Type, Modules} 
	    <-FarmSups, Id =:= FarmName], 
	
	case length(MatchedSup) > 0 of 
		false->
			supervisor:start_child(rabbit_farms_sup,{rabbit_farm_keeper_sup, 
													{rabbit_farm_keeper_sup, start_link, 
													[RabbitFarmModel]}, 
													permanent, 5000, supervisor, 
													[rabbit_farm_keeper_sup]});
		true->
			lager:log(error,"create rabbit farm keeper failed, farm:~n~p~n",[RabbitFarmModel])
	end,
	ok.


delete_rabbit_farm_instance(FarmName, FarmOptions)->
	case ets:lookup(?ETS_FARMS, FarmName) of 
		 [RabbitFarm] ->
		 	#rabbit_farm{connection = Connection, channels = Channels} = RabbitFarm,
		 	error_logger:info_msg("Delete rabbit farm instance, Conn ~p Channels ~p~n",[Connection, Channels]),
		 	case erlang:is_process_alive(Connection) of 
		 		true->
				 	ChannelSize  = orddict:size(Channels),
				 	error_logger:info_msg("Closing ~p channels ~p~n",[RabbitFarm, Channels]),
				 	orddict:map(fun(_,C)-> amqp_channel:close(C) end, Channels),
					error_logger:info_msg("Closing amqp connection ~p~n",[Connection]),
				 	amqp_connection:close(Connection, 3);
				false->
					error_logger:error_msg("The farm ~p died~n",[FarmName]),
					{error, farm_died}
			end;
		 _->
		 	error_logger:info_msg("Cannot find rabbit farm:~p~n",[FarmName]),
		 	{warn, farm_not_exist}
	end.

publish_a_message(Type, #rabbit_message{
								 farm_name    = FarmName,
								 exchange     = Exchange,
								 routing_key  = RoutingKey,
								 payload      = Message,
								 content_type = ContentType
							}  = CMessage)
				->
	F = publish_fun(Type, Exchange, RoutingKey, Message, ContentType),
	call_wrapper(FarmName, F).

publish_rabbit_message(Type, <<"Test Message">> = Message)->
    publish_a_message(Type, Message);

publish_rabbit_message(Type, #rabbit_message{
								 farm_name    = FarmName,
								 exchange     = Exchange,
								 routing_key  = RoutingKey,
								 payload      = Message,
								 content_type = ContentType
							}  = Message)
				when is_record(Message,rabbit_message)->
    publish_a_message(Type, Message).


publish_rabbit_messages(Type, #rabbit_messages{
								 farm_name            = FarmName,
								 exchange             = Exchange,
								 rabbit_message_bodies = RabbitCarrotBodies,
								 content_type = ContentType
							}  = Messages)
				when is_record(Messages,rabbit_messages)->
	 
	FunList=
	[publish_fun(Type, Exchange, RoutingKey, Message, ContentType)
	||#rabbit_message_body{routing_key = RoutingKey, payload = Message} <- RabbitCarrotBodies],
	Funs= fun(Channel) ->
			[F(Channel)||F<-FunList]
		  end,
	call_wrapper(FarmName, Funs).

native_rabbit_call(Type, FarmName, Method, Content)->
	F = rabbit_farm_util:get_fun(Type, Method, Content),
	call_wrapper(FarmName, F).

%%TODO put that intto gen-server 
call_wrapper(FarmName, Fun) 
					when is_function(Fun,1) ->	
	case ets:lookup(?ETS_FARMS, FarmName) of 
		 [RabbitFarm] ->
		 	#rabbit_farm{connection = Connection, channels = Channels} = RabbitFarm,
		 	case erlang:is_process_alive(Connection) of 
		 		true->
				 	ChannelSize  = orddict:size(Channels),
				 	case ChannelSize > 0 of
				 		 true->
	    				 	random:seed(os:timestamp()),
							ChannelIndex = random:uniform(ChannelSize),
							Channel      = orddict:fetch(ChannelIndex, Channels),
						    error_logger:info_msg("Channel ~p, Channels ~p ",[Channel, Channels]),
							{ok, Fun(Channel)};
	    				 false->
	    				 	lager:log(error,"can not find channel from rabbit farm:~p~n",[FarmName])
	    			end;
				false->
					lager:log(error,"the farm ~p died~n",[FarmName]),
					{error, farm_died}
			end;
		 _->
		 	lager:log(error,"can not find rabbit farm:~p~n",[FarmName]),
		 	{error, farm_not_exist}
	end.


publish_fun(Type, Exchange, RoutingKey, Message, ContentType)->
	rabbit_farm_util:get_fun(Type, 
			#'basic.publish'{ exchange    = Exchange,
	    					  routing_key = rabbit_farm_util:ensure_binary(RoutingKey)},
	    	#amqp_msg{props = #'P_basic'{content_type = ContentType, message_id=message_id()}, 
	    			  %message_id = message_id(),
	    			  payload = rabbit_farm_util:ensure_binary(Message)}).

message_id()->
	uuid:uuid4().
