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

-include("rabbit_farms.hrl").
-include("rabbit_farms_internal.hrl").
-include_lib("lager/include/lager.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-define(RABBIT_FARMS,rabbit_farms).
-define(SERVER,?MODULE).
-define(RECONNECT_TIME,5000).
%% ====================================================================
%% API functions
%% ====================================================================
-export([start_link/1, get_status/1]).

start_link(RabbitFarmModel) ->
	#rabbit_farm{farm_name = FarmName} = RabbitFarmModel,
    gen_server2:start_link({local, ?TO_FARM_NODE_NAME(FarmName)}, ?MODULE, [RabbitFarmModel], []).

get_status(FarmName) ->
	FarmNode = ?TO_FARM_NODE_NAME(FarmName),
    gen_server2:call(FarmNode,{get_status}).

%% ====================================================================
%% Behavioural functions 
%% ====================================================================
-record(state, {status = inactive, rabbit_farm = #rabbit_farm{}}).

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
			State#state{status = actived, rabbit_farm = FarmInstance};
		_->
			State#state{status = inactive}
	end,
	{noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
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
	DecodedAmqpParms = AmqpParams#amqp_params_network{password = password:decode_base64(SecPassword)},
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
												lager:log(info,"qdeclare ~p",[QDeclare]),
												R	= amqp_channel:call(Channel, QDeclare),
												%{'queue.declare_ok', QName, _, _} = R,
												lager:log(info,"qdeclare ~p with return ~p",[QDeclare, R]),
												lager:log(info,"qdeclare ~p",[BindQueue]),
												R1 = amqp_channel:call(Channel, BindQueue),
												lager:log(info,"Bindqueue ~p with return ~p",[BindQueue, R1]),
												%{'queue.bind', QBind} =R1,
												Channel
										    end
									  	    || _I <-lists:seq(1,ChannelCount)]
									   	  || #rabbit_feeder{count = ChannelCount, declare = Declare, queue_declare = QDeclare, queue_bind = BindQueue} <- Feeders]),
				IndexedChannels =  lists:zip(lists:seq(1,length(ChannelList)),ChannelList),
				Channels        =  orddict:from_list(IndexedChannels),
				{ok, Farm#rabbit_farm{connection = Connection, channels = Channels, status = actived}};
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

