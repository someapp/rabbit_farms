-module(spark_rabbit_config).
-export([
	get_connection_setting/1,
	get_exchange_setting/1,
	get_queue_setting/1,
 	get_routing_key/1,
	get_queue_bind/3,
	get_consumer/1, clone_queue_setting/2

]).

-include_lib("amqp_client/include/amqp_client.hrl").

-spec get_connection_setting(list()) ->#'amqp_params_network'{}.
get_connection_setting(ConfFList) ->
      {ok, AmqpConfList} = app_config_util:config_val(amqp_connection, ConfFList, []),
	UserName    = proplists:get_value(username, AmqpConfList, undefined),
	Password    = proplists:get_value(password,AmqpConfList,undefined),
	%true = password:is_secure(Password),
	VirtualHost = proplists:get_value(virtual_host, AmqpConfList,undefined),
	Host        = proplists:get_value(host, AmqpConfList, undefined),
	Port        = proplists:get_value(port, AmqpConfList,5672),

	#amqp_params_network{
				username     = rabbit_farm_util:ensure_binary(UserName),
				password     = Password,
				virtual_host = rabbit_farm_util:ensure_binary(VirtualHost),
				host         = Host,
				port         = Port
	 }.
-spec get_exchange_setting(list())-> #'exchange.declare'{}.
get_exchange_setting(ConfList)->
  {ok, ExchangeConfList} = app_config_util:config_val(amqp_exchange, ConfList, []),
	Ticket       = proplists:get_value(ticket,ExchangeConfList,0),
	Exchange     = proplists:get_value(exchange,ExchangeConfList),
	Type         = proplists:get_value(type,ExchangeConfList,<<"direct">>),
	Passive      = proplists:get_value(passive,ExchangeConfList,false),
	Durable      = proplists:get_value(durable,ExchangeConfList,false),
	AutoDelete   = proplists:get_value(auto_delete,ExchangeConfList,false),
	Internal     = proplists:get_value(internal,ExchangeConfList,false),
	NoWait       = proplists:get_value(nowait,ExchangeConfList,false),
	Arguments    = proplists:get_value(arguments,ExchangeConfList,[]),
	#'exchange.declare'{
				ticket      = Ticket,
				exchange    = rabbit_farm_util:ensure_binary(Exchange),
				type        = rabbit_farm_util:ensure_binary(Type),
				passive     = Passive,
				durable     = Durable,
				auto_delete = AutoDelete,
				internal    = Internal,
				nowait      = NoWait,
				arguments   = Arguments
				}.

-spec get_queue_setting(list())-> #'queue.declare'{}.
get_queue_setting(ConfList)->
     {ok, AmqpQueueConfList} = app_config_util:config_val(amqp_queue, ConfList, []),
     QTicket		 = proplists:get_value(qticket, AmqpQueueConfList, 0),
	Queue 		 = proplists:get_value(queue, AmqpQueueConfList, <<"">>),
	QPassive	 = proplists:get_value(qpassive, AmqpQueueConfList, false),
	QDurable	 = proplists:get_value(qdurable, AmqpQueueConfList, false),
	QExclusive	 = proplists:get_value(qexclusive, AmqpQueueConfList, false),
	QAutoDelete	 = proplists:get_value(qauto_delete, AmqpQueueConfList, false),
	QNoWait 	 = proplists:get_value(qnowait, AmqpQueueConfList, false),
	QArguments	 = proplists:get_value(qarguments, AmqpQueueConfList, []),

	#'queue.declare'{
			   ticket = QTicket,
			   queue = Queue,
			   passive     = QPassive,
			   durable     = QDurable,
			   auto_delete = QAutoDelete,
			   exclusive   = QExclusive,
			   nowait      = QNoWait,
			   arguments = QArguments}.

-spec clone_queue_setting(list(), integer())-> [#'queue.declare'{}].
clone_queue_setting(ConfList, HowMany) when is_integer(HowMany), HowMany >=0 ->
   BaseSetting = get_queue_setting(ConfList),   
   clone_queue(BaseSetting, [BaseSetting], HowMany).

clone_queue(BaseSetting, Queues, 0)-> [];   
clone_queue(BaseSetting, [H|_], I) ->
   NewName = index_queue_name(BaseSetting#'queue.declare'.queue, I),
   NewQueue = BaseSetting#'queue.declare'{queue = NewName},
   clone_queue(BaseSetting, [H|NewQueue], I-1).

index_queue_name(Name, I) when is_binary(Name) ->
   <<Name/binary,I/binary>>.	

-spec get_routing_key(list()) -> binary().	
get_routing_key(ConfList)->
   {ok, QueueConfList} = app_config_util:config_val(amqp_queue, ConfList, []),
   proplists:get_value(routing_key,QueueConfList). 
  
-spec get_queue_bind(binary(), binary(), binary())->#'queue.bind'{}.
get_queue_bind(Queue, Exchange, RoutingKey)->
   #'queue.bind'{
		queue = Queue,
		exchange = Exchange,
		routing_key = RoutingKey
		}.

-spec get_consumer(list())-> #'basic.consume'{}.
get_consumer(ConfList) ->
   {ok, ConsumerConfList} = app_config_util:config_val(consumer_queue, ConfList, []),	
   Consumer_tag = proplists:get_value(consumer_queue, ConsumerConfList, <<"">>),
   Queue = proplists:get_value(queue, ConsumerConfList, <<"">>),
   Ticket = proplists:get_value(ticket, ConsumerConfList, 0),
   NoLocal = proplists:get_value(no_local, ConsumerConfList, false),
   No_ack = proplists:get_value(no_ack, ConsumerConfList, false),
   Exclusive = proplists:get_value(exclusive, ConsumerConfList, false),
   Nowait = proplists:get_value(nowait, ConsumerConfList,false),
   Arguments = proplists:get_value(arguments, ConsumerConfList,[]),
   #'basic.consume'{
		ticket = Ticket,
		queue = Queue,
		no_local = NoLocal,
		no_ack = No_ack,
		exclusive = Exclusive,
		nowait = Nowait,
		consumer_tag = Consumer_tag,
		arguments= Arguments
	}.



print_queue_bind(Queue,Exchange, RoutingKey) ->
   error_logger:info_msg("Queue ~p",[Queue]),
   error_logger:info_msg("Exchange ~p",[Exchange]),
   error_logger:info_msg("RoutingKey ~p",[RoutingKey]). 

print_amqp(#amqp_params_network{} = R) ->
   error_logger:info_msg("Username: ~p",[R#amqp_params_network.username] ),
   error_logger:info_msg("Password: ~p",[R#amqp_params_network.password] ),
   error_logger:info_msg("VirtualHost: ~p",[R#amqp_params_network.virtual_host] ),
   error_logger:info_msg("Host: ~p",[R#amqp_params_network.host] ),
   error_logger:info_msg("Port: ~p",[R#amqp_params_network.port] ).
