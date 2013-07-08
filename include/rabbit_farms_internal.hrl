-include_lib("amqp_client/include/amqp_client.hrl").

-record(rabbit_farm,{ farm_name    = default,
					  amqp_params  = #amqp_params_network{},
					  feeders	   = [],
					  status 	   = inactive,
					  connection,
					  channels     = orddict:new()}).

-record(rabbit_processor, { farm_name = default,
							queue_declare = #'queue.declare'{},
							queue_bind = #'queue.bind'{},
							routing_key = <<"">>,
							callbacks = []
							}).

-record(rabbit_feeder,{count = 1 , 
					   queue_count = 1,
					   queue = <<"">>,
					   declare = #'exchange.declare'{},
					   queue_declare = #'queue.declare'{},
					   queue_bind = #'queue.bind'{},
					   routing_key = <<"">>,
					   callbacks = []
					   }).

-define(TO_FARM_NODE_NAME(V),list_to_atom("farm_" ++ atom_to_list((V)))).

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
		
		rest_params = #spark_rest_config{} ::#spark_rest_config{}

%		retry_strategy = undef ::atom()
}).
