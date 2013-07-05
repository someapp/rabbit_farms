-module(rabbit_farm_util).
-export([ensure_binary/1, 
	     get_fun/2,
		 get_fun/3]).

-include("rabbit_farms.hrl").
-include("rabbit_farms_internal.hrl").
-include_lib("lager/include/lager.hrl").

-spec get_fun(cast, atom())-> fun().

get_fun(cast, Method)->
	fun(Channel)->

			amqp_channel:cast(Channel, Method)
	end;
get_fun(call, Method)->
	fun(Channel)->
			amqp_channel:call(Channel, Method)
	end.

-spec get_fun(cast, atom(), term())-> fun().
get_fun(cast, Method, Content)->
	fun(Channel)->
			amqp_channel:cast(Channel, Method, Content)
	end;
get_fun(call, Method, Content)->
	fun(Channel)->
			amqp_channel:call(Channel, Method, Content)
	end.
-spec ensure_binary(any())-> bitstring().
ensure_binary(undefined)->
	undefined;
ensure_binary(Value) when is_binary(Value)->
	Value;
ensure_binary(Value) when is_list(Value)->
	list_to_binary(Value).