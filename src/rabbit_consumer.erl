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
-define(CONFPATH,"conf").
-define(AMQP_CONF, "spark_amqp.config").
-define(REST_CONF, "spark_rest.config").
-define(HEARTBEAT, 5).

%% API
-export([start_/0, start/0, stop/0, start_link/0]).

-export([get_status/0, ping/0]).

-export([
		connect/0,
		open_channel/0,
		close_channel/0,
		reconnect/0,
		disconnect/0,
		subscribe/1,
		register_callback/1
]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, handle_info/3,
        terminate/2, code_change/3]).

-spec start_() -> ok.
start_()->
	
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

-spec start() -> ok.
start()->
	
    ok = app_util:start_app(?APP).
-spec stop() -> ok.
stop()->
 	gen_server:call(?SERVER,{stop, normal}).

-spec start_link() -> pid().
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec ping() -> pang.
ping()->
	gen_server:call(?SERVER,{ping}).

-spec get_status() -> #consumer_state{}.
get_status()->
    gen_server:call(?SERVER, {get_status}).

-spec get_farm_pid() -> pid().
get_farm_pid()->
	gen_server:call(?SERVER, {get_farm_pid}).

-spec connect()-> pid().
connect()->
	gen_server:call(?SERVER, {connect}).

-spec open_channel()-> pid().
open_channel()->
	gen_server:call(?SERVER, {open_channel}).

-spec close_channel()-> ok.
close_channel()->
	gen_server:call(?SERVER, {close_channel}).

-spec reconnect() -> pid().
reconnect()->
	gen_server:call(?SERVER, {reconnect}).

-spec disconnect() -> ok.
disconnect()->
	gen_server:call(?SERVER, {disconnect}).

-spec subscribe(call|cast) -> ok.
subscribe(call)  ->
	gen_server:call(?SERVER, {subscribe});

subscribe(cast) ->
	gen_server:cast(?SERVER, {subscribe}).

-spec register_callback(atom()) -> ok.
register_callback(Mod)->
	gen_server:call(?SERVER, {register_callback, 
							  Mod}).


%% -------------------------------------------------------------------------
%% Connection methods
%% -------------------------------------------------------------------------
-spec connection_start(Amqp_params_network :: #amqp_params_network{}) -> {'ok', pid()} | {'error', any()}.
connection_start(Amqp_params_network) 
			when is_record(Amqp_params_network,amqp_params_network) ->
	amqp_connection:start(Amqp_params_network).

-spec connection_close(pid()) -> 'ok'.
connection_close(undef) -> undef;
connection_close(ConPid) ->
	case is_alive(ConPid) of
		true-> amqp_connection:close(ConPid);
		Why -> Why
    end.
-spec connection_close(pid(), pos_integer()) -> 'ok'.
connection_close(undef, _) -> undef;
connection_close(ConPid, Timeout) ->
	case is_alive(ConPid) of
		true-> amqp_connection:close(ConPid, Timeout);
		Why-> Why
    end.

%% -------------------------------------------------------------------------
%% Channel methods
%% -------------------------------------------------------------------------

-spec channel_open(pid()) -> {'ok', pid()} | {'error', any()}.
channel_open(undef) -> {ok, undef};
channel_open(ConPid) ->
	case is_alive(ConPid) of
		true-> amqp_connection:open_channel(ConPid);
		Why -> Why
    end.

-spec channel_close(pid()) -> {'ok', pid()} | {'error', any()}.
channel_close(undef) -> ok;
channel_close(ChanPid) ->
	case is_alive(ChanPid) of
		true-> amqp_channel:close(ChanPid);
		Why -> Why
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

-spec do_subscribe(pid(), binary, pid()) -> {ok, binary()} | error.
do_subscribe(Channel, Queue, Pid) ->
    Method = #'basic.consume'{queue = Queue, no_ack = false},
    #'basic.consume_ok'{consumer_tag = CTag} = amqp_channel:subscribe(Channel, Method, Pid),
    error_logger:info_msg("subscribe ok Ctag ~p on pid ~p",[CTag ,Pid]),
    CTag.
%    receive
%        #'basic.consume_ok'{consumer_tag = CTag} -> {ok, CTag};
%        Unknown -> error_logger:info_msg("Receive unknown message format ~p",[Unknown]), 
%        		  {ok, Unknown}
%    after
%        ?RESPONSE_TIMEOUT -> {error, timeout}
%    end.

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
    {'queue.declare_ok', _, _, _} = amqp_channel:call(Channel, Method),
    error_logger:info_msg("Queue declared OK Q:~p",[Queue]),
    ok.

-spec bind_queue(pid(), binary(), binary(), binary()) -> ok.
bind_queue(Channel, Queue, Exchange, RoutingKey)->
	Method = #'queue.bind'{
					queue = Queue,
					exchange = Exchange,
					routing_key = RoutingKey

				},
	{'queue.bind_ok'} = amqp_channel:call(Channel, Method),
    error_logger:info_msg("Queue bind OK ~p, ~p, ~p, ~p",[Channel, Exchange, Queue, RoutingKey]),
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
 	error_logger:info_msg("Initializing rabbit consumer client"),

    {ok, [ConfList]} = load_config(?AMQP_CONF),
    Amqp_ConfList = proplists:get_value(amqp_param, ConfList, []),
    Feeder_ConfList = proplists:get_value(feeders, ConfList, []),
    {ok, [Rest_ConfList]} = load_config(?REST_CONF),

    R = {ok, #consumer_state{
    	connection = undef,
    	rest_params = get_rest_config(Rest_ConfList),
    	amqp_params = get_amqp_config(Amqp_ConfList),
    	exchange = proplists:get_value(exchange, Feeder_ConfList, <<"im.conversation">>),
    	queue = proplists:get_value(queue,Feeder_ConfList, <<"chat">>),
    	routing_key = proplists:get_value(routing_key,Feeder_ConfList, <<"spark.chat">>),
    	durable = proplists:get_value(durable,Feeder_ConfList, false),
 		exclusive = proplists:get_value(exclusive,Feeder_ConfList, false),
		auto_delete = proplists:get_value(auto_delete,Feeder_ConfList,false),
    	transform_module = proplists:get_value(transform_module, ConfList, undef),
    	restart_timeout = proplists:get_value(restart_timeout, ConfList, ?RECON_TIMEOUT),
    	consumer_pid = self()
    }},
    R.

handle_call({connect}, _From, State)->
 	{ok, ConPid} = connection_start(State#consumer_state.amqp_params),
 	error_logger:info_msg("Established connection",[]),
	{reply, ConPid, 
		State#consumer_state{connection=ConPid}};

handle_call({open_channel}, _From, State)->
	Start = os_now(),
	ChanPid =get_channel_pid(State),
% 	ConPid = State#consumer_state.connection,
% 	error_logger:info_msg("Connection Pid ~p, is_alive? ~p",
% 		[ConPid, is_alive(ConPid)]),
% 	{ok, ChanPid} = channel_open(ConPid),
 	End = os_now(),
 	TSpan = timespan(Start, End),
 	error_logger:info_msg("Connected Channel ~p. Timespan ~p",[ChanPid, TSpan]),
	{reply, ChanPid, 
		State#consumer_state{channel=ChanPid}};		

handle_call({close_channel}, _From, State)->
 	Start = os_now(),
 	ChanPid = State#consumer_state.channel,
 	error_logger:info_msg("Channel Pid ~p, is_alive? ~p",
 		[ChanPid, is_alive(ChanPid)]),
 	R = channel_close(ChanPid),
 	End = os_now(),
 	TSpan = timespan(Start, End),
 	error_logger:info_msg("Closed Channel: Ret: ~p. Timespan ~p",[R, TSpan]),
	{reply, closed_channel, 
		State#consumer_state{channel=undef}};		

handle_call({reconnect}, _From, State)->
	Start = os_now(),
	gen_server:call(?SERVER, {disconnect}),
	Reply = gen_server:call(?SERVER, {connect}),
	End = os_now(),
	TSpan = timespan(Start, End),
	error_logger:info_msg("Reconnect ~p. Timespan: ~p",[Reply, TSpan]),

	{reply, Reply, State};

handle_call({disconnect}, _From, State)->
	Start = os_now(),
	error_logger:info_msg("Disconnecting ....",[]),
	R1 = channel_close(State#consumer_state.channel),
	error_logger:info_msg("Disconnected Channel ~p",[R1]),
	R2 = connection_close(State#consumer_state.connection),
	End = os_now(),
    TSpan = timespan(Start, End),
	error_logger:info_msg("Disconnected Connection ~p. Timespan: ~p",[R2, TSpan]),
	{reply, {ok, disconnected}, 
		State#consumer_state{
		connection = undef, 
		connection_ref = undef,
		channel = undef,
		channel_ref =undef}
	};

handle_call({stop, Reason}, _From, State)->
 	error_logger:info_msg("Rabbit Consumers stopping with reason ~p ",[Reason]),
	Reply = terminate(Reason, State),
	{reply, Reply, State};

handle_call({get_status}, _From, State)->
	{reply, {ok, State}, State};

handle_call({ping}, _From, State)->
	{reply, {ok, pong}, State};

handle_call({register_callback, Module},
			 _From, State) ->
	State#consumer_state{transform_module = Module},
	{reply, ok, State};

handle_call({subscribe}, _From, State)->
	Start = os_now(),
%	ConPid = State#consumer_state.connection,
%	ChanPid = State#consumer_state.channel,
%	{ok, ChanPid} = channel_open(ConPid),
	ChanPid =get_channel_pid(State),
	Queue = State#consumer_state.queue,
	Exchange = State#consumer_state.exchange,
	RoutingKey = State#consumer_state.routing_key,
	Durable = State#consumer_state.durable,
	Exclusive = State#consumer_state.exclusive,
	Autodelete = State#consumer_state.auto_delete,
	declare_queue(ChanPid, Queue, Durable, Exclusive, Autodelete),
	bind_queue(ChanPid, Queue, Exchange, RoutingKey),
 	Reply = do_subscribe(ChanPid, Queue, State#consumer_state.consumer_pid),
	End = os_now(),
	TSpan = timespan(Start, End),
 	error_logger:info_msg("handle subscribe ok ~p. Timespan: ~p",[Reply, TSpan]),
	{reply, Reply, State};

handle_call(_Request, _From, State) ->
    Reply = {error, function_clause},
    {reply, Reply, State}.

handle_cast(Info, State) ->
	erlang:display(Info),
    {noreply, State}.

handle_info({#'basic.consume_ok'{}}, State)->
	{reply, ok, State};
handle_info({#'basic.consume_ok'{consumer_tag = CTag}, _}, State)->
	error_logger:info_msg("Handle info consume_ok ~p",[CTag]),
	{reply, CTag, State};
handle_info(#'basic.cancel'{}, State) ->
    {noreply, State};
handle_info({#'basic.cancel_ok'{}}, State)->
	{reply, ok, State};
handle_info({#'basic.cancel_ok'{},_}, State)->
	{reply, ok, State};
handle_info({#'basic.deliver'
			  {consumer_tag = CTag,
			   delivery_tag = DTag,
			   redelivered = Redelivered,
			   exchange = Exchange,
			   routing_key  =RoutingKey
			  },
			 Content}, State
			) ->
	Start = os_now(),
    #amqp_msg{props = Props, payload = Payload} = Content,
    #'P_basic'{
    	content_type = ContentType
    } = Props,

    {ResponstType, ResponsePayload} = process_message(ContentType, Payload, 
    								  State#consumer_state.transform_module),
	
    error_logger:info_msg("Publish ChanPid ~p DTag ~p",[State#consumer_state.channel, DTag]),
	Ret = ack(State#consumer_state.channel,DTag),
    End = os_now(),
    TSpan = timespan(Start, End),
    error_logger:info_msg("Publish Delivery Ack ~p. Timespan: ~p",[Ret, TSpan]),
	{noreply, State};

handle_info({Any, Content}, State) ->
	error_logger:info_msg("Unsupported message ~p, unknown conntent ~p~n",[Any, Content]),
	{noreply, State};

handle_info({Any}, State) ->
	error_logger:info_msg("Unsupported message ~p",[Any]),
	{noreply, State};

handle_info({'DOWN', _MRef, process, Pid, Info}, State) ->
    error_logger:error("DOWN Pid ~p, Info ~p",[Pid, Info]),
    {noreply, State};

handle_info({'EXIT', Pid, Reason}, State)->
	error_logger:error("amqp connection (~p) down ",[State#consumer_state.connection]),
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
%	gen_server:call(?SERVER, {disconnect}),
	ok.

process_message(chat,Payload, Module)->
	{ResponstType, ResponsePayload} = Module:process_message(Payload),
	{ResponstType, ResponsePayload};

process_message(undefined, Payload, State)->
	{cannot_process_message, undefined};

process_message(ContentType, Payload, State)->
	{cannot_process_message, ContentType}.

-spec is_alive(pid() | atom()) -> true | false.
is_alive(undef) -> false;
is_alive(P)->
 	erlang:is_process_alive(P).

-spec get_rest_config(list()) -> #spark_restc_config{}.
get_rest_config(Rest_ConfList)->
   	#spark_restc_config {
    	spark_api_endpoint = proplists:get_value(spark_api_endpoint, Rest_ConfList),
   	 	spark_app_id = proplists:get_value(spark_app_id, Rest_ConfList),
    	spark_brand_id = proplists:get_value(spark_brand_id, Rest_ConfList),
    	spark_client_secret = proplists:get_value(spark_client_secret, Rest_ConfList),
    	spark_create_oauth_accesstoken =
              proplists:get_value(spark_create_oauth_accesstoken, Rest_ConfList),
    	auth_profile_miniProfile = proplists:get_value(auth_profile_miniProfile, Rest_ConfList),
    	profile_memberstatus = proplists:get_value(profile_memberstatus, Rest_ConfList),
    	community2brandId = proplists:get_value(community2brandId, Rest_ConfList)
  	}.

-spec get_amqp_config(list()) ->#'amqp_params_network'{}.
get_amqp_config(FarmOptions) ->
	UserName    = proplists:get_value(username,FarmOptions,<<"guest">>),
	SecPassword    = proplists:get_value(password,FarmOptions,<<"V2pOV2JHTXpVVDA9">>),
	true = password:is_secure(SecPassword),
	VirtualHost = proplists:get_value(virtual_host,FarmOptions,<<"/">>),
	Host        = proplists:get_value(host,FarmOptions,"localhost"),
	Port        = proplists:get_value(port,FarmOptions,5672),

	#amqp_params_network{
				username     = rabbit_farm_util:ensure_binary(UserName),
				password     = password:decode_password(SecPassword),
				virtual_host = rabbit_farm_util:ensure_binary(VirtualHost),
				host         = Host,
				port         = Port,
			    heartbeat = ?HEARTBEAT
				}.
-spec get_exhange_config(list())-> #'exchange.declare'{}.
get_exhange_config(Amqp_params) ->
	rabbit_farms_config:get_exchange_setting(Amqp_params).

-spec get_queue_config(list()) -> #'queue.declare'{}.
get_queue_config(Amqp_params) -> 
	rabbit_farms_config:get_queue_setting(Amqp_params).

-spec get_queue_binding_config(list()) ->#'queue.bind'{}.
get_queue_binding_config(Amqp_params)-> 
	rabbit_farms_config:get_queue_bind(Amqp_params).

-spec get_channel_pid(#'consumer_state'{}) -> pid() | {error, atom()}.
get_channel_pid(State)->
	ConPid = case is_alive(State#consumer_state.connection) of
 		true -> State#consumer_state.connection;
 		Else -> erlang:send_after(?DELAY, self(), {connect})
 	end,
 	 
 	case is_alive(ConPid) of
 		true -> {ok, ChanPid} = channel_open(ConPid),
 				ChanPid;
 		_Reason -> {error, channel_closed}
 	end.

-spec load_config()-> list().
load_config()->
  {ok, ConfDir}= cwd(),
  load_config(ConfDir, "spark_consumer.config").

-spec load_config(string())-> list().
load_config(File) ->
  {ok, ConfDir}= cwd(),
  load_config(ConfDir,File).

-spec load_config(string(), string())-> list().
load_config(ConfDir,File) when is_list(ConfDir), 
			  is_list(File)->
  FileFullPath = lists:concat([ConfDir,"/", File]),
  error_logger:info_msg("Loading config: ~p",[FileFullPath]),
  {ok, [ConfList]}= file:consult(FileFullPath),
  {ok, [ConfList]}.

-spec cwd()-> {ok, string()}.
cwd()->
  {ok, Cwd} = file:get_cwd(),
  {ok, lists:concat([Cwd,"/",?CONFPATH])}.

-spec os_now() -> datetime1970().
os_now()->
  R =os:timestamp(),
  calendar:now_to_universal_time(R).

%-spec timespan( datetime1970(), datetime1970())-> datetime1970().
timespan(A,B)->
  calendar:time_difference(A,B).