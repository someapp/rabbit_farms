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

-export([native_cast/2, native_cast/3]).
-export([native_call/2, native_call/3]).
-export([get_status/0, get_farm_pid/0]).
-export([subscribe/2, 
	     register_callback/1,
	     start_consume/0]).

%% gen_server2 callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, handle_info/3,
        terminate/2, code_change/3]).