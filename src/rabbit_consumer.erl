-module(rabbit_consumer).
-behaviour(gen_server2).

-include("rabbit_farms.hrl").
-include("rabbit_farms_internal.hrl").
-include_lib("lager/include/lager.hrl").

-define(SERVER,?MODULE).
-define(APP,rabbit_farms).
-define(ETS_FARMS,ets_rabbit_farms).

-record(state,{status = uninitialized}).

%% API
-export([start_/0, start/0, stop/0, start_link/0]).

-export([get_status/0, get_farm_pid/0]).
-export([subscribe/2, 
	     register_callback/1,
	     start_consume/0]).

%% gen_server2 callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, handle_info/3,
        terminate/2, code_change/3]).

-record(state,{status = uninitialized}).

start_()->
    ok = application:start(syntax_tools),
    ok = application:start(compiler),
    ok = application:start(goldrush),
    ok = lager:start(),
    ok = application:start(gen_server2),
    ok = application:start(rabbit_common),
    ok = application:start(amqp_client),
    ok = application:start(crypto),
    ok = application:start(?APP).

start()->
    ok = application:start(?APP).

stop()->
 	gen_server2:call(?SERVER,{stop, normal}).

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

get_status()->
    gen_server2:call(?SERVER, {get_status}).

get_farm_pid()->
	gen_server2:call(?SERVER, {get_farm_pid}).

	subscribe(call, Subscription) 
			when is_record(Subscription, rabbit_processor) ->
	gen_server2:call(?SERVER, {subscribe, Subscription}).

%%%===================================================================
%%% gen_server2 callbacks
%%%===================================================================

init([]) ->
    erlang:send_after(0, self(), {init}),
    {ok, #state{}}.

handle_call({stop, Reason}, From, State)->
 	error_logger:info_msg("Rabbit Consumers stopping with reason ~p ",[Reason]),
	Reply = terminate(Reason, State),
	{reply, Reply, State};

handle_call({subscribe, Subscription}, From, State) 
					when is_record(Subscription, rabbit_processor)->
    spawn(fun()-> 
     		 Reply = subscribe_with_callback(call, Subscription),
     		 gen_server2:reply(From, Reply)
    	 end),
    {noreply, State};

handle_call({get_status}, _From, State)->
	{reply, {ok, State}, State};
handle_call({get_farm_pid}, _From, State)->
    Farms = ets:tab2list(?ETS_FARMS),
    {reply, {ok, Farms}, State};
handle_call(_Request, _From, State) ->
    Reply = {error, function_clause},
    {reply, Reply, State}.

handle_cast(Info, State) ->
	erlang:display(Info),
    {noreply, State}.

