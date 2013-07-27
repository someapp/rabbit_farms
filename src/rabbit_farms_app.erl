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

-module(rabbit_farms_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-export([start/0, stop/0]).

-define(APP, ?MODULE).


start()->
  application:start(?APP, permanent).

stop()->
  application:stop(?APP).


%% ===================================================================
%% Application callbacks
%% ===================================================================
-spec start(atom(), list())-> {ok, pid()} | {error, tuple()}.
start(StartType, StartArgs) ->
    error_logger:info_msg("Starting application ~p with Type ~p Args ~p",[?APP, StartType, StartArgs]),	    
    rabbit_farms_sup:start_link(StartType, StartArgs).
    
-spec stop(atom()) -> ok.
stop(_State) ->
    ok.


