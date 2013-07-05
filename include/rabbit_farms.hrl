-record(rabbit_message,{ farm_name   = default, 
						exchange    = <<"">>, 
						routing_key = <<"">>, 
						id = <<"">>,
						payload     = <<"">>,
						content_type}).

-record(rabbit_message_body,{id = <<"">>,routing_key = <<"">>, payload = <<"">>}).

-record(rabbit_messages,{ farm_name            = default, 
					 	 exchange             = <<"">>, 
 						 rabbit_message_bodies = [],
					 	 content_type}).
