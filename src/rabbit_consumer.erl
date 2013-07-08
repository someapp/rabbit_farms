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
-spec connection_start(Amqp_params_network :: #amqp_params_network{}) -> {'ok', pid()} | {'error', any()}.
connection_start(Amqp_params_network) when is_record(Amqp_params_network,
								 #amqp_params_network{}) ->
	amqp_connection:start(Amqp_params_network).

-spec connection_close(pid()) -> 'ok'.
connection_close(ConPid) ->
	case is_process_alive(ConPid) of
		true-> catch(amqp_connection:close(ConPid)), ok;
		_ -> ok
    end.
-spec connection_close(pid(), pos_integer()) -> 'ok'.
connection_close(ConPid, Timeout) ->
	case is_process_alive(ConPid) of
		true-> catch(amqp_connection:close(ConPid, Timeout)), ok;
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
do_subscribe(Channel, Queue) ->
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
-spec declare_queue(Channel::pid()) -> ok.
declare_queue(Channel) ->
    Method = get_queue_config(),
    #'queue.declare_ok'{} = amqp_channel:call(Channel, Method),
    ok.
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
    {'queue.declare_ok', _, _, _} = amqp_channel:call(Channel, Method),
    ok.

-spec bind_queue(Channel::pid()) -> ok.
bind_queue(Channel)->
	Method = get_queue_binding_config(),
	{'queue.bind_ok'} = amqp_channel:call(Channel, Method),
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
    R = {ok, #consumer_state{
    	connection = self(),
    	rest_params = get_rest_config(),
    	amqp_params = get_amqp_config(),
    	exchange = spark_app_config_srv:lookup(exchange, <<"im.conversation">>),
    	queue_name = spark_app_config_srv:lookup(queue, <<"chat">>),
    	routing_key = spark_app_config_srv:lookup(routing_key, <<"spark.chat">>),
    	durable = spark_app_config_srv:lookup(durable, false),
    	transform_module = spark_app_config_srv:lookup(transform_module, required),
    	restart_timeout = spark_app_config_srv:lookup(transform_module,?RECON_TIMEOUT)

    }},
    R.

handle_call({connect}, _From, State)->
 	{ok, ConPid} = connection_start(State#consumer_state.amqp_params),
	{reply, ConPid, 
		State#consumer_state{connection=ConPid}};

handle_call({reconnect}, _From, State)->
	
	{reply, {ok, State}, State};

handle_call({disconnect}, _From, State)->
	ok = channel_close(State#consumer_state.channel),
	ok = connection_close(State#consumer_state.connection),
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

handle_call({get_status}, _From, State)->
	{reply, {ok, State}, State};

handle_call({ping}, _From, State)->
	{reply, {ok, pong}, State};

handle_call({register_callback, Module},
			 From, State) ->
	State#consumer_state{transform_module = Module},
	{reply, ok, State};

handle_call({subscribe}, From, State)->
	ConPid = State#rabbit_consumer.connection,
	{ok, ChanPid} = channel_open(ConPid),
	declare_queue(ChanPid),
	bind_queue(ChanPid), 
    #'queue.bind'{
					queue = Queue,
					exchange = Exchange,
					routing_key = RoutingKey

				} = get_queue_binding_config(),
 	Reply = do_subscribe(ChanPid,Queue),
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
    	reply_to = ReplyTo,
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

handle_info({init}, State)->
	lager:log(info , "Setting up initial connection, channel, and queue"),
	{ok, ConPid} = connect(),
	{ok, ChanPid} = channel_open(ConPid),
 	declare_queue(ChanPid),
	bind_queue(ChanPid),
	{noreply, State#consumer_state{
		connection = undef, 
		connection_ref = undef,
		channel = undef,
		channel_ref =undef}
	};

handle_info(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State, _Extra) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(Reason, State) ->
	gen_server:call(?SERVER, {disconnect}),
	ok.

process_message(chat,Payload, Module)->
	{ResponstType, ResponsePayload} = Module:process_message(Payload),
	{ResponstType, ResponsePayload}.

process_message(ContentType, Payload, State)->
	{unsupported, ContentType}.


is_alive(P)->
 	erlang:is_process_alive(P).

get_rest_config()->
 	{ok, ConfDir} = spark_app_config_srv:lookup(confdir, required),
	{ok, FileName} = spark_app_config_srv:lookup(spark_rest_config, required),
 	{ok, ConfList} = spark_app_config_srv:reload_state(ConfDir,FileName),
  	#spark_restc_config {
    	spark_api_endpoint = spark_app_config_srv:lookup(spark_api_endpoint),
   	 	spark_app_id = spark_app_config_srv:lookup(spark_app_id),
    	spark_brand_id = spark_app_config_srv:lookup(spark_brand_id),
    	spark_client_secret = spark_app_config_srv:lookup(spark_client_secret),
    	spark_create_oauth_accesstoken =
              spark_app_config_srv:lookup(spark_create_oauth_accesstoken),
    	auth_profile_miniProfile = spark_app_config_srv:lookup(auth_profile_miniProfile),
    	profile_memberstatus = spark_app_config_srv:lookup(profile_memberstatus),
    	community2brandId = spark_app_config_srv:lookup(community2brandId)
  	}.

get_amqp_config()->
 	{ok, ConfDir} = spark_app_config_srv:lookup(confdir, required),
	{ok, FileName} = spark_app_config_srv:lookup(spark_rest_config, required),
 	{ok, ConfList} = spark_app_config_srv:reload_state(ConfDir,FileName),
	{ok, [Amqp_params]} = spark_app_config_srv:lookup(amqp_param, required),
	rabbit_farms_config:get_connection_setting(Amqp_params).


get_exhange_config() ->
	{ok, [Amqp_params]} = spark_app_config_srv:lookup(amqp_param, required),
	rabbit_farms_config:get_exchange_setting(Amqp_params).

get_queue_config() ->
	{ok, [Amqp_params]} = spark_app_config_srv:lookup(amqp_param, required),
	rabbit_farms_config:get_queue_setting(Amqp_params).

get_queue_binding_config()-> 
	{ok, [Amqp_params]} = spark_app_config_srv:lookup(amqp_param, required),
	rabbit_farms_config:get_queue_bind(Amqp_params).

get_channel_pid(State)->
	ConPid = case is_alive(State#rabbit_consumer.connection) of
 		true -> State#rabbit_consumer.connection;
 		Else -> erlang:send_after(?DELAY, self(), {connect})
 	end,
 	 
 	case is_alive(ConPid) of
 		true -> {ok, ChanPid} = channel_open(ConPid),
 				ChanPid;
 		Else -> {error, Else}
 	end.