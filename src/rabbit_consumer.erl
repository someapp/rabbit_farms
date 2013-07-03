-module(rabbit_consumer).
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

-export([get_status/0, ping/0]).
-export([subscribe/2, 
	     register_callback/1,
	     start_consume/0]).

%% gen_server2 callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, handle_info/3,
        terminate/2, code_change/3]).

-record(state,{status = uninitialized}).

start_()->
    ok = application:start(syntax_tools),
    ok = application:start(compiler),
    ok = application:start(goldrush),
    ok = lager:start(),
    ok = application:start(gen_server2),
    ok = application:start(rabbit_common),
    ok = application:start(amqp_client),
    ok = application:start(crypto),
    ok = application:start(?APP).

start()->
    ok = application:start(?APP).

stop()->
 	gen_server2:call(?SERVER,{stop, normal}).

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

ping()->
	gen_server2:call(?SERVER,{ping}).

get_status()->
    gen_server2:call(?SERVER, {get_status}).

get_farm_pid()->
	gen_server2:call(?SERVER, {get_farm_pid}).

	subscribe(call, Subscription) 
			when is_record(Subscription, rabbit_processor) ->
	gen_server2:call(?SERVER, {subscribe, Subscription}).

%%%===================================================================
%%% gen_server2 callbacks
%%%===================================================================

init([]) ->
    erlang:send_after(0, self(), {init}),
    {ok, #state{}}.

handle_call({stop, Reason}, From, State)->
 	error_logger:info_msg("Rabbit Consumers stopping with reason ~p ",[Reason]),
	Reply = terminate(Reason, State),
	{reply, Reply, State};

handle_call({subscribe, Subscription}, From, State) 
					when is_record(Subscription, rabbit_processor)->
    spawn(fun()-> 
     		 Reply = subscribe_with_callback(call, Subscription),
     		 gen_server2:reply(From, Reply)
    	 end),
    {noreply, State};

handle_call({get_status}, _From, State)->
	{reply, {ok, State}, State};

handle_call({ping}, _From, State)->
	{reply, {ok, pong}, State};

handle_call(_Request, _From, State) ->
    Reply = {error, function_clause},
    {reply, Reply, State}.

handle_cast(Info, State) ->
	erlang:display(Info),
    {noreply, State}.

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
handle_call({publish, RabbitCarrot}, From, State)
					when is_record(RabbitCarrot, rabbit_carrot) ->
	spawn(fun()-> 
				Reply = publish_rabbit_carrot(call, RabbitCarrot),
				gen_server2:reply(From, Reply)
		  end),
	{noreply, State};
handle_call({publish, RabbitCarrots}, From, State) 
					when is_record(RabbitCarrots,rabbit_carrots) ->
    spawn(fun()-> 
    		 Reply = publish_rabbit_carrots(cast,RabbitCarrots),
    		 gen_server2:reply(From, Reply)
    	 end),
    {noreply, State};

%%TODO put that intto gen-server 
handle_call({subscribe, Subscription}, From, State) 
					when is_record(Subscription, rabbit_processor)->
    spawn(fun()-> 
     		 Reply = subscribe_with_callback(call, Subscription),
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

handle_cast({publish, RabbitCarrot}, State) 
					when is_record(RabbitCarrot, rabbit_carrot) ->
    spawn(fun()-> publish_rabbit_carrot(cast, RabbitCarrot) end),
    {noreply, State};
handle_cast({publish, RabbitCarrots}, State) 
					when is_record(RabbitCarrots,rabbit_carrots) ->
    spawn(fun()-> 
    		 publish_rabbit_carrots(cast, RabbitCarrots)
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

%%TODO put that intto gen-server 
handle_info({#'basic.consume_ok'{}}, State)->
	{reply, ok, State};
%%TODO put that intto gen-server 
handle_info({#'basic.cancel_ok'{}}, State)->
	{reply, ok, State};
%%TODO put that intto gen-server 
handle_info({#'basic.deliver'{consumer_tag = Tag},
			 #amqp_msg{payload = Msg}}, State
			) ->
    Reply = try 
    	Message = binary_to_term(Msg),
    	consume_carrot_from_rabbit(Message, State),
    	rabbit_farm_util:get_fun(cast, #'basic.ack'{delivery_tag = Tag}, [])
	catch
		C:R -> lager:log(error,"Cannot parse message"), 
			   {C, R}
	end,
	{reply, Reply, State};

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


consume_carrot_from_rabbit(Message, State)->
	Cbks = State#rabbit_feeder.callbacks,
	Ret = lists:map(fun([M,F,Message])-> erlang:apply(M,F,[Message]) end, Cbks),
	Ret.

subscribe_with_callback(Type, #rabbit_processor {
								farm_name = FarmName,
								queue_declare = QDeclare,
								queue_bind = QBind,
								routing_key = RKey,
								callbacks = []
							  } = Subscription) 
				when is_record(Subscription, rabbit_processor)->
    FarmNodeName      = ?TO_FARM_NODE_NAME(FarmName),
    {ok, FarmOptions} = application:get_env(?APP, FarmNodeName),
    FeedsOpt	= proplists:get_value(feeders,FarmOptions,[]),
    Consumer = rabbit_farms_config:get_consumer(FeedsOpt),
	Declare = Subscription#rabbit_processor.queue_declare,
	Bind = Subscription#rabbit_processor.queue_bind,

    DeclareFun = rabbit_farm_util:get_fun(Type, Declare),
    BindFun = rabbit_farm_util:get_fun(Type, Bind),
    ConsumerFun = rabbit_farm_util:get_fun(Type, Consumer),
    call_wrapper(FarmName, DeclareFun),
    call_wrapper(FarmName, BindFun),
    call_wrapper(FarmName, ConsumerFun).

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


