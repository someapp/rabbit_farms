%% -------------------------------------------------------------------
%% Copyright (c) 2013 Xujin Zheng (zhengxujin@adsage.com)
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%% -------------------------------------------------------------------

-module(rabbit_farm_keeper).
-behaviour(gen_server2).

-compile([parse_transform, lager_transform]).

-include("rabbit_farms.hrl").
-include("rabbit_farms_internal.hrl").
-include_lib("lager/include/lager.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, handle_info/3, terminate/2, code_change/3]).
-export([stop/1]).
-define(RABBIT_FARMS,rabbit_farms).
-define(SERVER,?MODULE).
-define(RECONNECT_TIME,5000).

-record(state, {status = inactive, rabbit_farm = #rabbit_farm{}}).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1, get_status/1]).

start_link(RabbitFarmModel) ->
	ok = lager:start(),
	#rabbit_farm{farm_name = FarmName} = RabbitFarmModel,
    gen_server2:start_link({local, ?TO_FARM_NODE_NAME(FarmName)},
    					   ?MODULE, [RabbitFarmModel], []).

get_status(FarmName) ->
	FarmNode = ?TO_FARM_NODE_NAME(FarmName),
    gen_server2:call(FarmNode,{get_status}).

stop(FarmName)->
	FarmNode = ?TO_FARM_NODE_NAME(FarmName),
    gen_server2:call(FarmNode,{stop, normal}).

%% ====================================================================
%% Behavioural functions 
%% ====================================================================

init([RabbitFarm]) when is_record(RabbitFarm, rabbit_farm)->
    erlang:send_after(0, self(), {init, RabbitFarm}),
    ok = lager:start(),
    {ok, #state{}}.

handle_call({get_status}, _From, State)->
	{reply, {ok, State}, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({on_rabbit_farm_die,Reason,RabbitFarm}, State) 
					when is_record(RabbitFarm,rabbit_farm) ->
    NewState = State#state{status = inactive, rabbit_farm = RabbitFarm#rabbit_farm{connection = undefined, channels = orddict:new()}},
    Server = self(),
    spawn_link(fun()->
		try 
			erlang:send_after(?RECONNECT_TIME, Server, {init, RabbitFarm})
	    catch
	    	Class:Reason -> {Class, Reason} 
	    end
  	end),
    {noreply, NewState};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({init, RabbitFarm}, State) ->
	NewState = 
	case create_rabbit_farm_instance(RabbitFarm) of 
		{ok, FarmInstance}->
			gen_server2:cast(?RABBIT_FARMS,{on_rabbit_farm_created,FarmInstance}),
			State#state{status = activated, rabbit_farm = FarmInstance};
		_->
			State#state{status = inactive}
	end,
	{noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State, _Extra) ->
    {noreply, State}.

terminate(_eason, State) ->
    Connection = State#rabbit_farm.connection,
    Channels = State#rabbit_farm.channels,
    error_logger:info_msg("Farm Keeper stopping ~p Connection ~p Channels ~p~n",[?MODULE, Connection, Channels]),
    case erlang:is_process_alive(Connection) of 
	 	 true->
		 		R = orddict:map(fun(_,C) -> amqp_channel:close(C) end, Channels),
		 		error_logger:info_msg("Farm Keeper ~p closing channels ~p~n",
		 			[?MODULE, R]),
		 		amqp_connection:close(Connection, 3);
		 false->
		 		FarmName = State#rabbit_farm.farm_name,
				lager:log(error,"the farm ~p: ~p~n",[FarmName, {error, farm_died}])
	end,	
	ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================
create_rabbit_farm_instance(#rabbit_farm{amqp_params    = AmqpParams,
		 		  		   				 feeders        = Feeders,
		 		  		   				 farm_name      = FarmName} = Farm) 
								when is_record(Farm,rabbit_farm)->
	SecPassword	 	 = AmqpParams#amqp_params_network.password,
	DecodedAmqpParms = AmqpParams#amqp_params_network{password = password:decode_password(SecPassword)},
	case amqp_connection:start(DecodedAmqpParms) of
		{ok, Connection}->
				watch_rabbit_farm( Connection, 
								   Farm, 
								   fun(FarmPid, RabbitFarm, Reason) -> 
											on_rabbit_farm_exception(FarmName, FarmPid, RabbitFarm, Reason)
								   end),
				ChannelList = lists:flatten( [[ 
										  	begin 
												{ok, Channel}           = amqp_connection:open_channel(Connection),
												{'exchange.declare_ok'} = amqp_channel:call(Channel, Declare),
												error_logger:info_msg("exchange declared ok",[]),
												R = amqp_channel:call(Channel, QDeclare),
												{'queue.declare_ok', _, _, _} = R,
												error_logger:info_msg("queue declared ok ~p",[R]),					
												R1 = amqp_channel:call(Channel, BindQueue),
												{'queue.bind_ok'} =R1,
												error_logger:info_msg("queue bind ok ~p",[R1]),												
												Channel
										    end
									  	    || _I <-lists:seq(1,ChannelCount)]
									   	  || #rabbit_feeder{count = ChannelCount, declare = Declare, queue_declare = QDeclare, queue_bind = BindQueue} <- Feeders]),
				IndexedChannels =  lists:zip(lists:seq(1,length(ChannelList)),ChannelList),
				Channels        =  orddict:from_list(IndexedChannels),
				{ok, Farm#rabbit_farm{connection = Connection,
				    channels = Channels, 
				    status = activated}};
		{error,Reason}->
				{error, Reason}
	end.
	
on_rabbit_farm_exception(FarmName, FarmPid, RabbitFarmInstance, Reason)->
	FarmNodeName = ?TO_FARM_NODE_NAME(FarmName),
	gen_server2:cast(?RABBIT_FARMS,{on_rabbit_farm_die,Reason,RabbitFarmInstance}),
	gen_server2:cast(FarmNodeName, {on_rabbit_farm_die,Reason,RabbitFarmInstance}),
	lager:log(error, "farm_pid:~n~p~nrabbit_farm:~n~p~nreason:~n~p~n",[FarmPid,RabbitFarmInstance,Reason]).

watch_rabbit_farm(FarmPid,RabbitFarm,Fun) when   is_pid(FarmPid),
									 			 is_record(RabbitFarm,rabbit_farm)->
	 spawn_link(fun() ->
				process_flag(trap_exit, true),
				link(FarmPid),
			 	receive
			 		{'EXIT', FarmPid, Reason} -> 
			 			Fun(FarmPid, RabbitFarm, Reason)
	 			end
 	end).

