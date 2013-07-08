-module(rabbit_consumer_config).
-export([
%	get_connection_setting/0,
	get_connection_setting/1,
%	get_exchange_setting/0,
	get_exchange_setting/1,
%	get_queue_setting/0,
	get_queue_setting/1,
%	get_queue_bind/0,
	get_queue_bind/1,

 %	get_consumer/0,
	get_consumer/1

]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("lager/include/lager.hrl").

-spec get_connection_setting(list()) ->#'amqp_params_network'{}.
get_connection_setting(ConfList) ->
	UserName    = proplists:get_value(username,ConfList,<<"guest">>),
	Password    = proplists:get_value(password,ConfList,<<"V2pOV2JHTXpVVDA9">>),
	true = password:is_secure(Password),
	VirtualHost = proplists:get_value(virtual_host,ConfList,<<"/">>),
	Host        = proplists:get_value(host,ConfList,"localhost"),
	Port        = proplists:get_value(port,ConfList,5672),
	#amqp_params_network{
				username     = rabbit_farm_util:ensure_binary(UserName),
				password     = Password,
				virtual_host = rabbit_farm_util:ensure_binary(VirtualHost),
				host         = Host,
				port         = Port
				}.
-spec get_exchange_setting(list())-> #'exchange.declare'{}.
get_exchange_setting(FeedOpt)->
	Ticket       = proplists:get_value(ticket,FeedOpt,0),
	Exchange     = proplists:get_value(exchange,FeedOpt),
	Type         = proplists:get_value(type,FeedOpt,<<"direct">>),
	Passive      = proplists:get_value(passive,FeedOpt,false),
	Durable      = proplists:get_value(durable,FeedOpt,false),
	AutoDelete   = proplists:get_value(auto_delete,FeedOpt,false),
	Internal     = proplists:get_value(internal,FeedOpt,false),
	NoWait       = proplists:get_value(nowait,FeedOpt,false),
	Arguments    = proplists:get_value(arguments,FeedOpt,[]),
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
get_queue_setting(FeedOpt)->
	QTicket		 = proplists:get_value(qticket, FeedOpt, 0),
	Queue 		 = proplists:get_value(queue, FeedOpt, <<"">>),
	QPassive	 = proplists:get_value(qpassive, FeedOpt, false),
	QDurable	 = proplists:get_value(qdurable, FeedOpt, false),
	QExclusive	 = proplists:get_value(qexclusive, FeedOpt, false),
	QAutoDelete	 = proplists:get_value(qauto_delete, FeedOpt, false),
	QNoWait 	 = proplists:get_value(qnowait, FeedOpt, false),
	QArguments	 = proplists:get_value(qarguments, FeedOpt, []),

	#'queue.declare'{
					ticket 		= QTicket,
					queue 		= Queue,
					passive     = QPassive,
					durable     = QDurable,
					auto_delete = QAutoDelete,
					exclusive   = QExclusive,
					nowait      = QNoWait,
					arguments   = QArguments									
					}.	

-spec get_queue_bind(list())->#'queue.bind'{}.
get_queue_bind(FeedOpt)->
	Queue 		 = proplists:get_value(queue, FeedOpt, <<"">>),
	Exchange     = proplists:get_value(exchange,FeedOpt),
	RoutingKey   = proplists:get_value(routing_key,FeedOpt),
	#'queue.bind'{
					queue = Queue,
					exchange = Exchange,
					routing_key = RoutingKey

				}.
-spec get_consumer(list())-> #'basic.consume'{}.
get_consumer(FeedOpt) ->
	Consumer_tag = proplists:get_value(consumer_tag, FeedOpt, <<"">>),
	Queue 		 = proplists:get_value(queue, FeedOpt, <<"">>),
	Ticket 		 = proplists:get_value(ticket, FeedOpt, 0),
	NoLocal		= proplists:get_value(no_local, FeedOpt, false),
	No_ack		= proplists:get_value(no_ack, FeedOpt, false),
	Exclusive	= proplists:get_value(exclusive, FeedOpt, false),
	Nowait      = proplists:get_value(nowait,FeedOpt,false),
	Arguments 	= proplists:get_value(arguments,FeedOpt,[]),
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

