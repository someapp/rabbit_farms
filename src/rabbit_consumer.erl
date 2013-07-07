-module(rabbit_consumer).
-behaviour(gen_server).

-include("rabbit_farms.hrl").
-include("rabbit_farms_internal.hrl").
-include("rest_conn.hrl").
-include_lib("lager/include/lager.hrl").

-define(SERVER,?MODULE).
-define(APP,rabbit_consumer).
-define(DELAY, 10).
-define(RECON_TIMEOUT, 5000).
-define(ETS_FARMS,ets_rabbit_farms).
-define(RESPONSE_TIMEOUT,2000).

-define(AMQP_CONF, "spark_amqp.config").
-define(REST_CONF, "spark_rest.config").

-record(consumer_state, {
		farm_name = [] ::string(),
%		status = uninitialized  :: initialized | uninitialized,
		consumer_pid :: pid(),
		connection :: pid(),
		connection_ref ::reference(),
		channel :: pid(),
		channel_ref :: reference(),
		transform_module :: module(),
		restart_timeout = 5000 :: pos_integer(), % restart timeout
		amqp_params = #amqp_params_network{} ::#amqp_params_network{},
		rest_params = #rest_conf{} ::#rest_conf{}
%		retry_strategy = undef ::atom()
}).

%% API
-export([start_/0, start/0, stop/0, start_link/0]).

-export([get_status/0, ping/0]).

-export([
		connect/0,
		reconnect/0,
		disconnect/0,
		subscribe/2,
		register_callback/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, handle_info/3,
        terminate/2, code_change/3]).

start_()->
	ok = spark_app_config_srv:start(?APP),
    ok = app_util:start_app(syntax_tools),
    ok = app_util:start_app(compiler),
    ok = app_util:start_app(goldrush),
    ok = lager:start(),
    ok = app_util:start_app(gen_server),
    ok = app_util:start_app(rabbit_common),
    ok = app_util:start_app(amqp_client),
    ok = app_util:start_app(crypto),
    ok = app_util:start_app(restc),
    ok = app_util:start_app(?APP).

start()->
	ok = spark_app_config_srv:start(?APP),
    ok = app_util:start_app(?APP).

stop()->
 	gen_server:call(?SERVER,{stop, normal}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

ping()->
	gen_server:call(?SERVER,{ping}).

get_status()->
    gen_server:call(?SERVER, {get_status}).

get_farm_pid()->
	gen_server:call(?SERVER, {get_farm_pid}).

connect()->
	gen_server:call(?SERVER, {connect}).

reconnect()->
	gen_server:call(?SERVER, {reconnect}).

disconnect()->
	gen_server:call(?SERVER, {disconnect}).

subscribe(call, Mod) 
			when is_atom(Mod) ->
	gen_server:call(?SERVER, {subscribe, Mod});

subscribe(cast, Mod) 
			when is_atom(Mod) ->
	gen_server:cast(?SERVER, {subscribe, Mod}).

register_callback(Mod)->
	gen_server:call(?SERVER, {register_callback, 
							  Mod}).


%% -------------------------------------------------------------------------
%% Connection methods
%% -------------------------------------------------------------------------
-spec connection_start() -> {'ok', pid()} | {'error', any()}.
connection_start(Amqp_params_network) when is_record(Amqp_params_network,
								 #amqp_params_network{}) ->
	amqp_connection:start(Amqp_params_network).
-spec connection_start() -> {'ok', pid()} | {'error', any()}.
connection_start() ->
    Params = #amqp_params_network{virtual_host = just_app:get_env(amqp_vhost),
                                  username = just_app:get_env(amqp_username),
                                  password = just_app:get_env(amqp_password),
                                  host = just_app:get_env(amqp_host),
                                  port = just_app:get_env(amqp_port)},
	connection_start(Params).

-spec connection_close(pid()) -> 'ok'.
connection_close(Conn) ->
	case is_process_alive(ConPid) of
		true-> catch(amqp_connection:close(Conn)), ok;
		_ -> ok
    end.
-spec connection_close(pid()) -> 'ok'.
connection_close(Conn, Timeout) ->
	case is_process_alive(ConPid) of
		true-> catch(amqp_connection:close(Conn, Timeout)), ok;
		_ -> ok
    end.

%% -------------------------------------------------------------------------
%% Channel methods
%% -------------------------------------------------------------------------

-spec channel_open(pid()) -> {'ok', pid()} | {'error', any()}.
channel_open(ChanPid) ->
	case is_process_alive(ChanPid) of
		true-> catch(amqp_connection:channel_open(ChanPid)), ok;
		_ -> ok
    end.

-spec channel_close(pid()) -> {'ok', pid()} | {'error', any()}.
channel_close(ChanPid) ->
	case is_process_alive(ChanPid) of
		true-> catch(amqp_connection:close_channel(ChanPid)), ok;
		_ -> ok
    end.


-spec qos(pid(), non_neg_integer()) -> 'ok' | {'error', any()}.
qos(Chan, PrefetchCount) ->
    Method = #'basic.qos'{prefetch_count = PrefetchCount},
    try amqp_channel:call(Chan, Method) of
        #'basic.qos_ok'{} -> ok;
        Other             -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

-spec ack(pid(), integer()) -> ok | {error, noproc | closing}.
ack(Channel, DeliveryTag) ->
    Method = #'basic.ack'{delivery_tag = DeliveryTag, multiple = false},
    try amqp_channel:call(Channel, Method) of
        ok      -> ok;
        closing -> {error, closing}
    catch
        _:{noproc, _} -> {error, noproc}
    end.

-spec subscribe(pid(), binary()) -> {ok, binary()} | error.
subscribe(Channel, Queue) ->
    Method = #'basic.consume'{queue = Queue, no_ack = false},
    amqp_channel:subscribe(Channel, Method, self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} -> {ok, CTag}
    after
        ?RESPONSE_TIMEOUT -> {error, timeout}
    end.

-spec unsubscribe(pid(), binary()) -> ok | error.
unsubscribe(Channel, CTag) ->
    Method = #'basic.cancel'{consumer_tag = CTag},
    amqp_channel:call(Channel, Method),
    receive
        #'basic.cancel_ok'{consumer_tag = CTag} -> ok
    after
        ?RESPONSE_TIMEOUT -> {error, timeout}
    end.

%% -------------------------------------------------------------------------
%% queue functions
%% -------------------------------------------------------------------------

-spec declare_queue(pid(), binary()) -> ok.
declare_queue(Channel, Queue) ->
    Method = #'queue.declare'{queue = Queue, durable = true},
    #'queue.declare_ok'{} = amqp_channel:call(Channel, Method),
    ok.

-spec declare_queue(pid(), binary(), boolean(), boolean(), boolean()) -> ok.
declare_queue(Channel, Queue, Durable, Exclusive, Autodelete) ->
    Method = #'queue.declare'{
		queue = Queue,
		durable = Durable,
		exclusive = Exclusive,
		auto_delete = Autodelete},
    #'queue.declare_ok'{} = amqp_channel:call(Channel, Method),
    ok.

%% -------------------------------------------------------------------------
%% confirm functions
%% -------------------------------------------------------------------------

-spec confirm_select(pid()) -> ok.
confirm_select(Channel) ->
    Method = #'confirm.select'{},
    #'confirm.select_ok'{} = amqp_channel:call(Channel, Method),
    ok.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
 	process_flag(trap_exit, true),
 	lager:log(info,"Initializing rabbit consumer client"),
    erlang:send_after(?DELAY, self(), {init}),
    {ok, #consumer_state{
    	connection = self(),
    	restart_timeout = ?RECON_TIMEOUT
    }}.

load_state()->
	

	#consumer_state{

	}.

handle_call({connect}, _From, State)->
 	{ok, ConPid} = connection_start(State#amqp_params_network),
	{reply, ConPid, 
		State#consumer_state{connection=ConPid}};

handle_call({reconnect}, _From, State)->
	
	{reply, {ok, State}, State};

handle_call({disconnect}, _From, State)->
	ok = close_channel(State#consumer_state.channel),
	ok = close_connection(State#consumer_state.connection),
	{reply, {ok, disconnected}, 
		State#consumer_state{
		connection = undef, 
		connection_ref = undef,
		channel = undef,
		channel_ref =undef}
	};

handle_call({stop, Reason}, From, State)->
 	error_logger:info_msg("Rabbit Consumers stopping with reason ~p ",[Reason]),
	Reply = terminate(Reason, State),
	{reply, Reply, State};

handle_call({subscribe, Mod}, From, State) 
					when is_atom(Mod)->
    spawn(fun()-> 
     		 Reply = subscribe_with_callback(call, Mod),
     		 gen_server:reply(From, Reply)
    	 end),
    {noreply, State};

handle_call({get_status}, _From, State)->
	{reply, {ok, State}, State};

handle_call({ping}, _From, State)->
	{reply, {ok, pong}, State};

handle_call({register_callback, Module},
			 From, State) ->
	State#consumer_state.transform_module = Module,
	{reply, ok, State};

handle_call({consume}, From, State)->
 	Reply = ope
	{reply, Reply, State};

handle_call(_Request, _From, State) ->
    Reply = {error, function_clause},
    {reply, Reply, State}.

handle_cast(Info, State) ->
	erlang:display(Info),
    {noreply, State}.

handle_info({#'basic.consume_ok'{}}, State)->
	{reply, ok, State};
handle_info({#'basic.cancel_ok'{}}, State)->
	{reply, ok, State};
handle_info({#'basic.deliver'{consumer_tag = Tag},
			 Content}, State
			) ->
    #amqp_msg{payload = Payload, props = Props} = Content,
    #'P_basic'{
    	content_type = ContentType,
    	message_id = MsgId,
    	reply-to = ReplyTo,
    	timestamp = TimeStamp
    } = Props,
    {ResponsePayload, ResponstType} = process_message(ContentType, Payload, 
    								  State#consumer_state.transform_module),
	{reply, {ResponsePayload, ResponstType} , State};

handle_info({'EXIT', Pid, Reason}, State)->
	lager:log(error , "amqp connection (~p) down ",[State#consumer_state.connection]),
	{noreply, State#consumer_state{
		connection = undef, 
		connection_ref = undef,
		channel = undef,
		channel_ref =undef}
	}.
handle_info({init}, State) ->
	ets:new(?ETS_FARMS,[protected, named_table, 
		   {keypos, #rabbit_farm.farm_name},
		   {read_concurrency, true}]),
	{ok, NewState} = init_rabbit_farm(State),
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State, _Extra) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(Reason, State) ->
	gen_server:call(?SERVER, {disconnect}),
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
	{ok, State#consumer_state{status = initialized}}.


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
				 	orddict:map(fun(_,C)-> close_channel(C) end, Channels),
					error_logger:info_msg("Closing amqp connection ~p~n",[Connection]),
				 	close_connection(Connection, 3);
				false->
					error_logger:error_msg("The farm ~p died~n",[FarmName]),
					{error, farm_died}
			end;
		 _->
		 	error_logger:info_msg("Cannot find rabbit farm:~p~n",[FarmName]),
		 	{warn, farm_not_exist}
	end.

process_message(chat,Payload, Module)->
	{ResponstType, ResponsePayload} = Module:process_message(Payload),
	{ResponstType, ResponsePayload}.

process_message(ContentType, Payload, State)->
	{unsupported, ContentType}.

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

is_alive(P)->
 	erlang:is_process_alive(P).

get_rest_config()->
	{ok, [ConfList]} = spark_app_config_srv:load_config("spark_rest.config"),

	#rest_conf{


	}.

get_amqp_config()->
	{ok, [ConfList]} = spark_app_config_srv:load_config("spark_amqp.config"),

	#amqp_params_network{


	}.
